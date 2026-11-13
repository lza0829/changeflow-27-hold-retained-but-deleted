"""可重复执行的演示数据造数：超窗可删 + 冻结命中 + 隔离区证据。

只在对应记录尚不存在时写入，重复调用不会重复造数。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from app.config import FIXTURES_DIR
from app.engines.parse_change import parse_change_fixture
from app.repositories import legal_holds as holds_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import streams as streams_repo

ARCHIVE_STREAM = "Orders Archive CDC"
ARCHIVE_RUN_MARK = "__archive_demo__"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def seed_archive_ledger(conn: sqlite3.Connection) -> dict[str, object]:
    stream = next(
        (s for s in streams_repo.list_streams(conn) if s["name"] == ARCHIVE_STREAM), None
    )
    if stream is None:
        return {"ok": False, "reason": "archive stream missing; restart to reseed"}

    # 以造数 run 标记判断，而不是账本条数：清理删光后重启不应重新灌入历史数据。
    already = conn.execute(
        "SELECT COUNT(*) c FROM apply_runs WHERE stream_id=? AND report_json LIKE ?",
        (stream["id"], f"%{ARCHIVE_RUN_MARK}%"),
    ).fetchone()["c"]
    if already:
        return {"ok": True, "created": 0, "reason": "archive demo already seeded"}

    events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
    cursor_before = int(stream["apply_cursor"] or 0)
    run_id = runs_repo.create_run(conn, stream["id"], cursor_before)
    for event in events:
        conn.execute(
            """
            INSERT INTO change_ledger(
                stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                stream["id"],
                run_id,
                int(event["seq"]),
                event["op"],
                event["entity_key"],
                event["event_ts"],
                json.dumps(event.get("payload") or {}, ensure_ascii=False),
                "applied",
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO applied_seqs(stream_id, seq, run_id, applied_at)
            VALUES (?,?,?,?)
            """,
            (stream["id"], int(event["seq"]), run_id, _now()),
        )
        conn.execute("UPDATE streams SET apply_cursor=? WHERE id=?", (int(event["seq"]), stream["id"]))

    runs_repo.finish_run(
        conn,
        run_id,
        status="succeeded",
        loaded=len(events),
        applied=len(events),
        quarantined=0,
        skipped=0,
        cursor_after=int(events[-1]["seq"]) if events else cursor_before,
        report={
            "seeded": True,
            "kind": ARCHIVE_RUN_MARK,
            "note": "历史账本演示数据（未写入 Sink），用于过期清理 + 保全冻结",
        },
    )
    return {"ok": True, "created": len(events), "stream_id": stream["id"], "run_id": run_id}


def seed_quarantine_evidence(conn: sqlite3.Connection) -> dict[str, object]:
    """造一条超窗且仍在隔离区的账本行，验证清理会排除它（游标可解释性）。"""
    stream = streams_repo.get_stream(conn, 1)
    if stream is None:
        return {"ok": False, "reason": "orders stream missing"}
    already = conn.execute(
        "SELECT COUNT(*) c FROM apply_runs WHERE stream_id=? AND report_json LIKE ?",
        (stream["id"], "%__quarantine_evidence__%"),
    ).fetchone()["c"]
    if already:
        return {"ok": True, "created": 0, "reason": "quarantine evidence already seeded"}

    run_id = runs_repo.create_run(conn, stream["id"], int(stream["apply_cursor"] or 0))
    seq = 90
    event_ts = "2025-08-01T00:00:00Z"
    payload = {"status": "held", "amount": 10.0, "sku": "SKU-Q"}
    conn.execute(
        """
        INSERT INTO change_ledger(stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (stream["id"], run_id, seq, "upsert", "ORD-0790", event_ts,
         json.dumps(payload, ensure_ascii=False), "quarantine_gap"),
    )
    quarantine_repo.insert(
        conn,
        stream_id=stream["id"],
        run_id=run_id,
        seq=seq,
        entity_key="ORD-0790",
        reason="gap_before_seq",
        payload=payload,
    )
    runs_repo.finish_run(
        conn,
        run_id,
        status="partial",
        loaded=1,
        applied=0,
        quarantined=1,
        skipped=0,
        cursor_after=int(stream["apply_cursor"] or 0),
        report={
            "seeded": True,
            "kind": "__quarantine_evidence__",
            "note": "超窗 + 隔离区：清理应排除该账本行以保持游标可解释",
        },
    )
    return {"ok": True, "created": 1, "seq": seq}


def seed_demo(conn: sqlite3.Connection) -> dict[str, object]:
    return {
        "archive": seed_archive_ledger(conn),
        "quarantine_evidence": seed_quarantine_evidence(conn),
        "active_holds": [
            {
                "id": h["id"],
                "scope": h["scope"],
                "entity_key": h["entity_key"],
                "start_ts": h["start_ts"],
                "end_ts": h["end_ts"],
                "reason": h["reason"],
            }
            for h in holds_repo.list_holds(conn, active_only=True)
        ],
    }
