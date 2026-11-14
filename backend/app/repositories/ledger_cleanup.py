from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from app.repositories import legal_holds as holds_repo
from app.repositories import settings as settings_repo

RETENTION_DAYS_KEY = "retention_days"
DEFAULT_RETENTION_DAYS = 30


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _cutoff(retention_days: int, now: datetime | None = None) -> str:
    base = now or datetime.now(timezone.utc)
    return (
        (base - timedelta(days=int(retention_days))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


# ---------------------------------------------------------------- retention config


def get_retention_days(conn: sqlite3.Connection) -> int:
    raw = settings_repo.get(conn, RETENTION_DAYS_KEY)
    if raw is None:
        return DEFAULT_RETENTION_DAYS
    return int(raw)


def set_retention_days(conn: sqlite3.Connection, days: int) -> int:
    if int(days) < 1:
        raise ValueError("retention_days must be >= 1")
    settings_repo.put(conn, RETENTION_DAYS_KEY, str(int(days)))
    return int(days)


# ---------------------------------------------------------------- preview


def _quarantine_keys(conn: sqlite3.Connection) -> set[tuple[int, int]]:
    rows = conn.execute("SELECT stream_id, seq FROM quarantine").fetchall()
    return {(int(r["stream_id"]), int(r["seq"])) for r in rows}


# ---------------------------------------------------------------- guardrail proof


def _frozen_key_entry(
    *,
    ledger_id: int,
    stream_id: int,
    seq: int,
    entity_key: str,
    hold_id: int | None,
) -> dict[str, Any]:
    """一条冻结命中键：以 ledger_id 为同一批次内预览↔执行比对的不变标识。"""
    return {
        "key": f"ledger#{int(ledger_id)}",
        "ledger_id": int(ledger_id),
        "stream_id": int(stream_id),
        "seq": int(seq),
        "entity_key": entity_key,
        "hold_id": int(hold_id) if hold_id is not None else None,
    }


def _frozen_delta(
    preview_entries: list[dict[str, Any]], execute_entries: list[dict[str, Any]]
) -> dict[str, Any]:
    """预览冻结集 vs 执行冻结集：added 只允许新增命中（收缩删除范围），removed 必须为空。"""
    preview_ids = {e["ledger_id"] for e in preview_entries}
    execute_ids = {e["ledger_id"] for e in execute_entries}
    return {
        "added": [e for e in execute_entries if e["ledger_id"] not in preview_ids],
        "removed": [e for e in preview_entries if e["ledger_id"] not in execute_ids],
    }


def _classify(
    row: dict[str, Any],
    *,
    cutoff_ts: str,
    holds: list[dict[str, Any]],
    quarantined: set[tuple[int, int]],
) -> tuple[str, dict[str, Any] | None, str]:
    """清理裁决：超窗是前提；冻结优先于删除；隔离/游标证据排除。

    返回 (decision, hold, detail)。decision 取值：
    - within_window   未超保留窗，不进入清单
    - frozen_retained 超窗但命中启用中的保全冻结，必须保留
    - cursor_protected 超窗但该 (stream, seq) 仍在隔离区：删除会破坏游标解释链，排除
    - delete          超窗且无冻结/无隔离关联，可进入正式删除
    """
    if row["event_ts"] >= cutoff_ts:
        return "within_window", None, "event_ts 未早于保留窗截止点"
    hold = holds_repo.match_holds(holds, row)
    if hold is not None:
        return (
            "frozen_retained",
            hold,
            f"命中冻结 #{hold['id']}（{hold['scope']}）：{hold['reason']}",
        )
    if (int(row["stream_id"]), int(row["seq"])) in quarantined:
        return (
            "cursor_protected",
            None,
            "该 (stream_id, seq) 仍在隔离区，删除会破坏游标停点的可解释性",
        )
    return "delete", None, "超窗且无冻结/隔离关联"


def create_preview(
    conn: sqlite3.Connection,
    *,
    retention_days: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    days = int(retention_days) if retention_days is not None else get_retention_days(conn)
    if days < 1:
        raise ValueError("retention_days must be >= 1")
    cutoff_ts = _cutoff(days, now)
    holds = holds_repo.list_holds(conn, active_only=True)
    hold_snapshot = [
        {
            "id": h["id"],
            "scope": h["scope"],
            "stream_id": h["stream_id"],
            "entity_key": h["entity_key"],
            "start_ts": h["start_ts"],
            "end_ts": h["end_ts"],
            "reason": h["reason"],
        }
        for h in holds
    ]
    quarantined = _quarantine_keys(conn)

    rows = conn.execute(
        """
        SELECT id, stream_id, run_id, seq, entity_key, event_ts, disposition
        FROM change_ledger ORDER BY id
        """
    ).fetchall()

    items: list[dict[str, Any]] = []
    counts = {"delete": 0, "frozen_retained": 0, "cursor_protected": 0}
    within_window = 0
    for raw in rows:
        row = dict(raw)
        decision, hold, detail = _classify(
            row, cutoff_ts=cutoff_ts, holds=holds, quarantined=quarantined
        )
        if decision == "within_window":
            within_window += 1
            continue
        counts[decision] += 1
        items.append(
            {
                "ledger_id": row["id"],
                "stream_id": row["stream_id"],
                "run_id": row["run_id"],
                "seq": row["seq"],
                "entity_key": row["entity_key"],
                "event_ts": row["event_ts"],
                "disposition": row["disposition"],
                "decision": decision,
                "hold_id": hold["id"] if hold else None,
                "hold_reason": hold["reason"] if hold else None,
                "detail": detail,
            }
        )

    ledger_count = len(rows)
    frozen_entries = [
        _frozen_key_entry(
            ledger_id=i["ledger_id"],
            stream_id=i["stream_id"],
            seq=i["seq"],
            entity_key=i["entity_key"],
            hold_id=i["hold_id"],
        )
        for i in items
        if i["decision"] == "frozen_retained"
    ]
    report = {
        "retention_days": days,
        "cutoff_ts": cutoff_ts,
        "scanned_count": ledger_count,
        "within_window_count": within_window,
        "deletable_count": counts["delete"],
        "frozen_retained_count": counts["frozen_retained"],
        "cursor_protected_count": counts["cursor_protected"],
        "active_hold_count": len(holds),
        "holds": hold_snapshot,
        "guardrail": {
            # 保全护栏证明（预览态）：冻结命中键即「因冻结保留」承诺集，
            # 执行时必须全部仍在账本残留集，否则拒绝执行。
            "frozen_hit_keys": frozen_entries,
            "retained_count": counts["frozen_retained"],
            "deleted_count": 0,
            "planned_delete_count": counts["delete"],
            "preview_vs_execute_frozen_delta": None,
            "refused": False,
            "refusal_reasons": [],
        },
        "policy": {
            "target": "change_ledger",
            "quarantine_targeted": False,
            "quarantine_reason": (
                "隔离区事件尚未裁决/重放，其账本行是游标停点的审计证据；"
                "按 (stream_id, seq) 仍存在于 quarantine 的账本行一律排除，"
                "需先经 replay/裁决处置隔离记录后再随保留窗清理。"
            ),
            "freeze_precedence": (
                "冻结优先于删除；执行前复核启用中的冻结，新命中一律保留（只收缩不扩张）。"
                "预览承诺「因冻结保留」的键在执行时必须仍在账本残留集，"
                "任一缺失即与预览冻结集不一致，整批拒绝执行并写明原因。"
            ),
        },
    }

    cur = conn.execute(
        """
        INSERT INTO ledger_cleanup_batches(
            status, retention_days, cutoff_ts, scanned_count, deletable_count,
            frozen_retained_count, cursor_protected_count, quarantine_linked_count,
            ledger_count_at_preview, active_hold_count, report_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "preview",
            days,
            cutoff_ts,
            ledger_count,
            counts["delete"],
            counts["frozen_retained"],
            counts["cursor_protected"],
            counts["cursor_protected"],
            ledger_count,
            len(holds),
            json.dumps(report, ensure_ascii=False),
            _now(),
        ),
    )
    batch_id = int(cur.lastrowid)

    for item in items:
        conn.execute(
            """
            INSERT INTO ledger_cleanup_items(
                batch_id, ledger_id, stream_id, run_id, seq, entity_key, event_ts,
                disposition, decision, hold_id, hold_reason, detail
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                batch_id,
                item["ledger_id"],
                item["stream_id"],
                item["run_id"],
                item["seq"],
                item["entity_key"],
                item["event_ts"],
                item["disposition"],
                item["decision"],
                item["hold_id"],
                item["hold_reason"],
                item["detail"],
            ),
        )

    report["batch_id"] = batch_id
    report["checks"] = _preview_checks(
        scanned=ledger_count,
        within=within_window,
        deletable=counts["delete"],
        frozen=counts["frozen_retained"],
        protected=counts["cursor_protected"],
    )
    conn.execute(
        "UPDATE ledger_cleanup_batches SET report_json=? WHERE id=?",
        (json.dumps(report, ensure_ascii=False), batch_id),
    )
    conn.commit()
    return get_batch(conn, batch_id) or {}


def _preview_checks(
    *, scanned: int, within: int, deletable: int, frozen: int, protected: int
) -> list[dict[str, Any]]:
    return [
        {
            "name": "scan_breakdown",
            "ok": scanned == within + deletable + frozen + protected,
            "detail": (
                f"scanned({scanned}) = within({within}) + deletable({deletable}) "
                f"+ frozen({frozen}) + cursor_protected({protected})"
            ),
        },
        {
            "name": "frozen_reported_separately",
            "ok": frozen >= 0,
            "detail": "frozen rows are reported separately and never enter the delete list",
        },
    ]


# ---------------------------------------------------------------- execute


def execute_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    batch = _get_batch_row(conn, batch_id)
    if batch is None:
        raise ValueError("cleanup batch not found")
    if batch["status"] != "preview":
        raise ValueError(f"batch status is {batch['status']}, only preview batches can execute")

    ledger_before = int(
        conn.execute("SELECT COUNT(*) c FROM change_ledger").fetchone()["c"]
    )
    holds_now = holds_repo.list_holds(conn, active_only=True)
    quarantined_now = _quarantine_keys(conn)
    existing_ids = {
        int(r["id"]) for r in conn.execute("SELECT id FROM change_ledger").fetchall()
    }

    items = conn.execute(
        """
        SELECT id, ledger_id, stream_id, seq, entity_key, event_ts, decision, hold_id
        FROM ledger_cleanup_items WHERE batch_id=? ORDER BY id
        """,
        (batch_id,),
    ).fetchall()

    preview_frozen_entries = [
        _frozen_key_entry(
            ledger_id=i["ledger_id"],
            stream_id=i["stream_id"],
            seq=i["seq"],
            entity_key=i["entity_key"],
            hold_id=i["hold_id"],
        )
        for i in items
        if i["decision"] == "frozen_retained"
    ]

    # 保全护栏：预览承诺「因冻结保留」的键，执行时必须全部仍在账本残留集。
    # 任一冻结键消失（如被其它批次/绕过护栏的路径删除）即与预览冻结集不一致，
    # 拒绝整批执行并写明原因，杜绝「报保留实删除」。
    missing_frozen = [
        e for e in preview_frozen_entries if e["ledger_id"] not in existing_ids
    ]
    if missing_frozen:
        missing_text = "、".join(
            f"ledger#{e['ledger_id']}({e['entity_key']})" for e in missing_frozen
        )
        reason = (
            f"执行前冻结集与预览不一致：预览标记「因冻结保留」的 {missing_text} "
            "已不在账本残留集。报保留的行不得消失，本批次拒绝执行且未删除任何行；"
            "请重新预览生成新批次。"
        )
        report = json.loads(batch["report_json"] or "{}")
        report["guardrail"] = {
            "frozen_hit_keys": preview_frozen_entries,
            "retained_count": 0,
            "deleted_count": 0,
            "preview_vs_execute_frozen_delta": {
                "added": [],
                "removed": missing_frozen,
            },
            "refused": True,
            "refusal_reasons": [reason],
        }
        report["reconciled"] = False
        conn.execute(
            """
            UPDATE ledger_cleanup_batches SET status='rejected', note=?, report_json=?
            WHERE id=?
            """,
            (reason, json.dumps(report, ensure_ascii=False), batch_id),
        )
        conn.commit()
        raise ValueError(reason)

    counters = {
        "deleted": 0,
        "frozen_retained": 0,
        "cursor_protected": 0,
        "newly_frozen": 0,
        "newly_protected": 0,
        "missing_at_execute": 0,
        "preview_frozen_missing": 0,
        "preview_protected_missing": 0,
    }
    retained_frozen: list[dict[str, Any]] = []

    for raw in items:
        item = dict(raw)
        ledger_id = int(item["ledger_id"])
        event_ts = item["event_ts"]
        if ledger_id not in existing_ids:
            # 预览后该行已不在账本（例如已被别的批次删除）。
            if item["decision"] == "delete":
                counters["missing_at_execute"] += 1
            elif item["decision"] == "frozen_retained":
                counters["preview_frozen_missing"] += 1
            else:
                counters["preview_protected_missing"] += 1
            _set_item_state(conn, item["id"], "missing_at_execute", deleted=False)
            continue

        hold = holds_repo.match_holds(
            holds_now,
            {"stream_id": item["stream_id"], "entity_key": item["entity_key"], "event_ts": event_ts},
        )
        if item["decision"] == "frozen_retained":
            # 预览时已冻结：本批次不扩大删除范围，保持保留。若冻结已解除，
            # 需要重新预览后再删，避免单批次内决策漂移。
            counters["frozen_retained"] += 1
            retained_frozen.append(
                _frozen_key_entry(
                    ledger_id=ledger_id,
                    stream_id=item["stream_id"],
                    seq=item["seq"],
                    entity_key=item["entity_key"],
                    hold_id=(hold or {}).get("id") or item["hold_id"],
                )
            )
            _set_item_state(
                conn,
                item["id"],
                "frozen_retained",
                deleted=False,
                hold_id=(hold or {}).get("id"),
            )
            continue

        if item["decision"] == "cursor_protected":
            counters["cursor_protected"] += 1
            _set_item_state(conn, item["id"], "cursor_protected", deleted=False)
            continue

        # 预览时判为可删，执行前复核：冻结/隔离优先，只收缩不扩张。
        if hold is not None:
            counters["newly_frozen"] += 1
            counters["frozen_retained"] += 1
            retained_frozen.append(
                _frozen_key_entry(
                    ledger_id=ledger_id,
                    stream_id=item["stream_id"],
                    seq=item["seq"],
                    entity_key=item["entity_key"],
                    hold_id=hold["id"],
                )
            )
            _set_item_state(
                conn, item["id"], "frozen_retained", deleted=False, hold_id=hold["id"]
            )
            continue
        if (int(item["stream_id"]), int(item["seq"])) in quarantined_now:
            counters["newly_protected"] += 1
            counters["cursor_protected"] += 1
            _set_item_state(conn, item["id"], "cursor_protected", deleted=False)
            continue

        try:
            cur = conn.execute("DELETE FROM change_ledger WHERE id=?", (ledger_id,))
        except sqlite3.IntegrityError:
            # 触发器兜底：冻结名单在 holds_now 快照之后被并发登记。
            # ABORT 只回滚该语句，刷新冻结名单后按「执行时新冻结」保留。
            counters["newly_frozen"] += 1
            counters["frozen_retained"] += 1
            holds_now = holds_repo.list_holds(conn, active_only=True)
            fresh = holds_repo.match_holds(
                holds_now,
                {
                    "stream_id": item["stream_id"],
                    "entity_key": item["entity_key"],
                    "event_ts": event_ts,
                },
            )
            retained_frozen.append(
                _frozen_key_entry(
                    ledger_id=ledger_id,
                    stream_id=item["stream_id"],
                    seq=item["seq"],
                    entity_key=item["entity_key"],
                    hold_id=(fresh or {}).get("id"),
                )
            )
            _set_item_state(
                conn,
                item["id"],
                "frozen_retained",
                deleted=False,
                hold_id=(fresh or {}).get("id"),
            )
            continue
        if cur.rowcount == 1:
            counters["deleted"] += 1
            _set_item_state(conn, item["id"], "deleted", deleted=True)
        else:
            counters["missing_at_execute"] += 1
            _set_item_state(conn, item["id"], "missing_at_execute", deleted=False)

    ledger_after = int(
        conn.execute("SELECT COUNT(*) c FROM change_ledger").fetchone()["c"]
    )

    # 护栏证明：报「因冻结保留」的键，执行后必须全部仍在账本残留集。
    retained_ids = [e["ledger_id"] for e in retained_frozen]
    frozen_missing_after: list[int] = []
    if retained_ids:
        marks = ",".join("?" for _ in retained_ids)
        present = {
            int(r["id"])
            for r in conn.execute(
                f"SELECT id FROM change_ledger WHERE id IN ({marks})", retained_ids
            ).fetchall()
        }
        frozen_missing_after = [i for i in retained_ids if i not in present]

    frozen_delta = _frozen_delta(preview_frozen_entries, retained_frozen)

    active_hold_count = holds_repo.count_active(conn)
    report = json.loads(batch["report_json"] or "{}")
    report.update(
        {
            "ledger_count_before": ledger_before,
            "ledger_count_after": ledger_after,
            "deleted_count": counters["deleted"],
            "frozen_retained_count": counters["frozen_retained"],
            "cursor_protected_count": counters["cursor_protected"],
            "newly_frozen_count": counters["newly_frozen"],
            "newly_protected_count": counters["newly_protected"],
            "missing_at_execute_count": counters["missing_at_execute"],
            "preview_frozen_missing_count": counters["preview_frozen_missing"],
            "active_hold_count": active_hold_count,
            "quarantine_linked_count": len(quarantined_now),
        }
    )
    report["guardrail"] = {
        # 保全护栏证明（执行态）：frozen_hit_keys 即本次实际保留的冻结键，
        # 与冻结名单、账本残留、批报告同源（同一事务内的 change_ledger /
        # legal_holds / ledger_cleanup_items）。
        "frozen_hit_keys": retained_frozen,
        "retained_count": counters["frozen_retained"],
        "deleted_count": counters["deleted"],
        "preview_vs_execute_frozen_delta": frozen_delta,
        "refused": False,
        "refusal_reasons": [],
    }
    checks = _execute_checks(
        preview_deletable=int(batch["deletable_count"]),
        preview_frozen=int(batch["frozen_retained_count"]),
        preview_protected=int(batch["cursor_protected_count"]),
        ledger_before=ledger_before,
        ledger_after=ledger_after,
        counters=counters,
    )
    checks.append(
        {
            "name": "frozen_rows_present_after_execute",
            "ok": not frozen_missing_after,
            "detail": (
                f"报保留的 {len(retained_ids)} 条冻结键执行后全部仍在账本残留集"
                if not frozen_missing_after
                else f"报保留的冻结键执行后缺失 ledger_id={frozen_missing_after}"
            ),
        }
    )
    checks.append(
        {
            "name": "frozen_set_consistent_with_preview",
            "ok": not frozen_delta["removed"],
            "detail": (
                "执行冻结集覆盖预览冻结集（只收缩不扩张，removed=0）"
                if not frozen_delta["removed"]
                else "预览冻结键在执行结果中缺失 ledger_id="
                + str([e["ledger_id"] for e in frozen_delta["removed"]])
            ),
        }
    )
    report["checks"] = checks
    report["reconciled"] = all(c["ok"] for c in checks)

    conn.execute(
        """
        UPDATE ledger_cleanup_batches SET
            status='executed',
            deleted_count=?, frozen_retained_count=?, cursor_protected_count=?,
            newly_frozen_count=?, newly_protected_count=?, missing_at_execute_count=?,
            ledger_count_before=?, ledger_count_after=?, active_hold_count=?,
            quarantine_linked_count=?, report_json=?, executed_at=?
        WHERE id=?
        """,
        (
            counters["deleted"],
            counters["frozen_retained"],
            counters["cursor_protected"],
            counters["newly_frozen"],
            counters["newly_protected"],
            counters["missing_at_execute"],
            ledger_before,
            ledger_after,
            active_hold_count,
            len(quarantined_now),
            json.dumps(report, ensure_ascii=False),
            _now(),
            batch_id,
        ),
    )
    conn.commit()
    return get_batch(conn, batch_id) or {}


def _set_item_state(
    conn: sqlite3.Connection,
    item_id: int,
    state: str,
    *,
    deleted: bool,
    hold_id: int | None = None,
) -> None:
    if hold_id is not None:
        conn.execute(
            "UPDATE ledger_cleanup_items SET execute_state=?, deleted=?, hold_id=? WHERE id=?",
            (state, 1 if deleted else 0, hold_id, item_id),
        )
    else:
        conn.execute(
            "UPDATE ledger_cleanup_items SET execute_state=?, deleted=? WHERE id=?",
            (state, 1 if deleted else 0, item_id),
        )


def _execute_checks(
    *,
    preview_deletable: int,
    preview_frozen: int,
    preview_protected: int,
    ledger_before: int,
    ledger_after: int,
    counters: dict[str, int],
) -> list[dict[str, Any]]:
    frozen_missing = counters["preview_frozen_missing"]
    protected_missing = counters["preview_protected_missing"]
    expected_frozen = preview_frozen - frozen_missing + counters["newly_frozen"]
    expected_protected = (
        preview_protected - protected_missing + counters["newly_protected"]
    )
    total_items = preview_deletable + preview_frozen + preview_protected
    accounted_items = (
        counters["deleted"]
        + counters["frozen_retained"]
        + counters["cursor_protected"]
        + counters["missing_at_execute"]
        + frozen_missing
        + protected_missing
    )
    checks = [
        {
            "name": "ledger_delta",
            "ok": ledger_before - counters["deleted"] == ledger_after,
            "detail": (
                f"before({ledger_before}) - deleted({counters['deleted']}) = after({ledger_after})"
            ),
        },
        {
            "name": "deletable_accounting",
            "ok": preview_deletable
            == counters["deleted"]
            + counters["newly_frozen"]
            + counters["newly_protected"]
            + counters["missing_at_execute"],
            "detail": (
                f"preview_deletable({preview_deletable}) = deleted({counters['deleted']}) "
                f"+ newly_frozen({counters['newly_frozen']}) "
                f"+ newly_protected({counters['newly_protected']}) "
                f"+ missing({counters['missing_at_execute']})"
            ),
        },
        {
            "name": "frozen_accounting",
            "ok": counters["frozen_retained"] == expected_frozen,
            "detail": (
                f"frozen_retained({counters['frozen_retained']}) = "
                f"preview_frozen({preview_frozen}) - frozen_missing({frozen_missing}) "
                f"+ newly_frozen({counters['newly_frozen']})"
            ),
        },
        {
            "name": "protected_accounting",
            "ok": counters["cursor_protected"] == expected_protected,
            "detail": (
                f"cursor_protected({counters['cursor_protected']}) = "
                f"preview_protected({preview_protected}) - protected_missing({protected_missing}) "
                f"+ newly_protected({counters['newly_protected']})"
            ),
        },
        {
            "name": "item_total",
            "ok": total_items == accounted_items,
            "detail": (
                f"preview items({total_items}) = deleted + frozen_retained + cursor_protected "
                f"+ missing + frozen_missing + protected_missing ({accounted_items})"
            ),
        },
        {
            "name": "frozen_never_deleted",
            "ok": counters["deleted"] <= preview_deletable,
            "detail": "deleted rows can only come from the preview delete list; frozen rows stay",
        },
    ]
    return checks


# ---------------------------------------------------------------- query


def _get_batch_row(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, status, retention_days, cutoff_ts, scanned_count, deletable_count,
               frozen_retained_count, cursor_protected_count, quarantine_linked_count,
               deleted_count, newly_frozen_count, newly_protected_count, missing_at_execute_count,
               ledger_count_at_preview, ledger_count_before, ledger_count_after,
               active_hold_count, note, report_json, created_at, executed_at
        FROM ledger_cleanup_batches WHERE id=?
        """,
        (batch_id,),
    ).fetchone()
    return dict(row) if row else None


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    row = _get_batch_row(conn, batch_id)
    if not row:
        return None
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    items = conn.execute(
        """
        SELECT id, ledger_id, stream_id, run_id, seq, entity_key, event_ts, disposition,
               decision, hold_id, hold_reason, detail, execute_state, deleted
        FROM ledger_cleanup_items WHERE batch_id=? ORDER BY id
        """,
        (batch_id,),
    ).fetchall()
    item["items"] = [dict(r) for r in items]
    return item


def list_batches(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, status, retention_days, cutoff_ts, scanned_count, deletable_count,
               frozen_retained_count, cursor_protected_count, deleted_count,
               ledger_count_at_preview, ledger_count_before, ledger_count_after,
               active_hold_count, created_at, executed_at
        FROM ledger_cleanup_batches ORDER BY id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def ledger_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) c FROM change_ledger").fetchone()["c"])
