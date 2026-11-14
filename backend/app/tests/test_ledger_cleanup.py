"""账本过期清理 + 保全冻结的端到端测试（SQLite 真实库）。"""
from __future__ import annotations

import sqlite3

import pytest

from app.repositories import legal_holds as holds_repo
from app.repositories import ledger_cleanup as cleanup_repo
from app.repositories import settings as settings_repo


@pytest.fixture()
def conn(tmp_path):
    # 复用生产建表脚本，确保触发器/列/约束与真实环境一致。
    from app import db as db_module
    from app import seed

    db_file = tmp_path / "app.db"
    original = db_module.DB_PATH
    db_module.DB_PATH = db_file
    try:
        seed.init_db()
    finally:
        db_module.DB_PATH = original

    c = sqlite3.connect(db_file)
    c.row_factory = sqlite3.Row
    yield c
    c.close()


def _add_ledger(conn, *, stream_id, seq, entity_key, event_ts, disposition="applied"):
    conn.execute(
        """
        INSERT INTO change_ledger(
            stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (stream_id, 999, seq, "upsert", entity_key, event_ts, "{}", disposition),
    )


def test_retention_roundtrip(conn):
    assert cleanup_repo.get_retention_days(conn) == 30
    assert cleanup_repo.set_retention_days(conn, 7) == 7
    assert settings_repo.get(conn, "retention_days") == "7"
    with pytest.raises(ValueError):
        cleanup_repo.set_retention_days(conn, 0)


def test_frozen_rows_retained_and_reported(conn):
    old = "2020-01-01T00:00:00Z"
    _add_ledger(conn, stream_id=1, seq=1, entity_key="FREE", event_ts="2020-01-02T00:00:00Z")
    _add_ledger(conn, stream_id=1, seq=2, entity_key="HELD", event_ts=old)
    _add_ledger(conn, stream_id=1, seq=3, entity_key="WINDOW", event_ts=old)
    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="HELD",
        start_ts=None, end_ts=None, reason="hold",
    )
    holds_repo.insert(
        conn, scope="time_range", stream_id=None, entity_key=None,
        start_ts="2019-12-31T00:00:00Z", end_ts="2020-01-01T00:00:00Z", reason="window",
    )
    conn.commit()

    batch = cleanup_repo.create_preview(conn)
    assert batch["scanned_count"] == 3
    assert batch["deletable_count"] == 1
    assert batch["frozen_retained_count"] == 2
    assert batch["report"]["reconciled"] if "reconciled" in batch["report"] else True
    assert all(c["ok"] for c in batch["report"]["checks"])
    decisions = {(i["entity_key"], i["decision"]) for i in batch["items"]}
    assert ("HELD", "frozen_retained") in decisions
    assert ("WINDOW", "frozen_retained") in decisions
    assert ("FREE", "delete") in decisions

    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["deleted_count"] == 1
    assert executed["ledger_count_before"] - executed["deleted_count"] == executed["ledger_count_after"]
    assert all(c["ok"] for c in executed["report"]["checks"])
    assert executed["report"]["reconciled"] is True
    remaining = {r["entity_key"] for r in conn.execute("SELECT entity_key FROM change_ledger")}
    assert remaining == {"HELD", "WINDOW"}


def test_hold_added_after_preview_is_honored(conn):
    _add_ledger(conn, stream_id=1, seq=1, entity_key="X", event_ts="2020-01-01T00:00:00Z")
    _add_ledger(conn, stream_id=1, seq=2, entity_key="Y", event_ts="2020-01-01T00:00:00Z")
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["deletable_count"] == 2

    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="X",
        start_ts=None, end_ts=None, reason="late hold",
    )
    conn.commit()
    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["newly_frozen_count"] == 1
    assert executed["deleted_count"] == 1
    assert all(c["ok"] for c in executed["report"]["checks"])
    assert {r["entity_key"] for r in conn.execute("SELECT entity_key FROM change_ledger")} == {"X"}


def test_released_hold_requires_new_preview(conn):
    _add_ledger(conn, stream_id=1, seq=1, entity_key="X", event_ts="2020-01-01T00:00:00Z")
    hold_id = holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="X",
        start_ts=None, end_ts=None, reason="hold",
    )
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["deletable_count"] == 0

    holds_repo.release(conn, hold_id)
    conn.commit()
    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["deleted_count"] == 0
    assert executed["frozen_retained_count"] == 1

    fresh = cleanup_repo.create_preview(conn)
    assert fresh["deletable_count"] == 1


def test_trigger_blocks_direct_delete_of_frozen_row(conn):
    _add_ledger(conn, stream_id=1, seq=1, entity_key="HELD", event_ts="2020-01-01T00:00:00Z")
    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="HELD",
        start_ts=None, end_ts=None, reason="hold",
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM change_ledger WHERE entity_key='HELD'")


def test_quarantine_linked_row_excluded(conn):
    from app.repositories import quarantine as quarantine_repo

    _add_ledger(conn, stream_id=1, seq=90, entity_key="Q", event_ts="2020-01-01T00:00:00Z",
                disposition="quarantine_gap")
    quarantine_repo.insert(
        conn, stream_id=1, run_id=999, seq=90, entity_key="Q",
        reason="gap_before_seq", payload={},
    )
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["cursor_protected_count"] == 1
    assert batch["deletable_count"] == 0
    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["deleted_count"] == 0
    assert conn.execute("SELECT COUNT(*) FROM change_ledger").fetchone()[0] == 1


def test_execute_is_single_use(conn):
    batch = cleanup_repo.create_preview(conn)
    cleanup_repo.execute_batch(conn, batch["id"])
    with pytest.raises(ValueError):
        cleanup_repo.execute_batch(conn, batch["id"])


def test_quarantine_added_after_preview_is_protected(conn):
    from app.repositories import quarantine as quarantine_repo

    _add_ledger(conn, stream_id=1, seq=1, entity_key="X", event_ts="2020-01-01T00:00:00Z")
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["deletable_count"] == 1
    quarantine_repo.insert(
        conn, stream_id=1, run_id=999, seq=1, entity_key="X",
        reason="gap_before_seq", payload={},
    )
    conn.commit()
    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["newly_protected_count"] == 1
    assert executed["deleted_count"] == 0
    assert all(c["ok"] for c in executed["report"]["checks"])


# ---------------------------------------------------------------- guardrail proof


def test_guardrail_proof_preview_and_execute(conn):
    """护栏块：frozen_hit_keys / retained_count / deleted_count / 预览↔执行冻结集差额。"""
    old = "2020-01-01T00:00:00Z"
    _add_ledger(conn, stream_id=1, seq=1, entity_key="FREE", event_ts="2020-01-02T00:00:00Z")
    _add_ledger(conn, stream_id=1, seq=2, entity_key="HELD", event_ts=old)
    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="HELD",
        start_ts=None, end_ts=None, reason="hold",
    )
    conn.commit()

    batch = cleanup_repo.create_preview(conn)
    guard = batch["report"]["guardrail"]
    assert guard["retained_count"] == 1
    assert guard["deleted_count"] == 0
    assert guard["planned_delete_count"] == 1
    assert guard["preview_vs_execute_frozen_delta"] is None
    assert guard["refused"] is False
    assert [k["entity_key"] for k in guard["frozen_hit_keys"]] == ["HELD"]
    assert guard["frozen_hit_keys"][0]["hold_id"] is not None

    executed = cleanup_repo.execute_batch(conn, batch["id"])
    guard = executed["report"]["guardrail"]
    assert guard["retained_count"] == 1
    assert guard["deleted_count"] == 1
    assert [k["entity_key"] for k in guard["frozen_hit_keys"]] == ["HELD"]
    delta = guard["preview_vs_execute_frozen_delta"]
    assert delta == {"added": [], "removed": []}
    # 报保留的键执行后仍在账本残留集（presence 校验项通过对账暴露）。
    presence = next(c for c in executed["report"]["checks"] if c["name"] == "frozen_rows_present_after_execute")
    assert presence["ok"] is True
    consistency = next(c for c in executed["report"]["checks"] if c["name"] == "frozen_set_consistent_with_preview")
    assert consistency["ok"] is True
    assert {r["entity_key"] for r in conn.execute("SELECT entity_key FROM change_ledger")} == {"HELD"}


def test_guardrail_delta_records_newly_frozen(conn):
    """预览后新登记冻结：执行冻结集 = 预览集 + 新增，delta.added 记录，不拒绝。"""
    _add_ledger(conn, stream_id=1, seq=1, entity_key="X", event_ts="2020-01-01T00:00:00Z")
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["report"]["guardrail"]["frozen_hit_keys"] == []

    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="X",
        start_ts=None, end_ts=None, reason="late hold",
    )
    conn.commit()
    executed = cleanup_repo.execute_batch(conn, batch["id"])
    assert executed["newly_frozen_count"] == 1
    assert executed["deleted_count"] == 0
    guard = executed["report"]["guardrail"]
    delta = guard["preview_vs_execute_frozen_delta"]
    assert [k["entity_key"] for k in delta["added"]] == ["X"]
    assert delta["removed"] == []
    assert guard["retained_count"] == 1
    assert executed["report"]["reconciled"] is True


def test_execute_refused_when_preview_frozen_row_vanishes(conn):
    """预览承诺保留的冻结键在执行前消失：整批拒绝、一行不删、写明原因。"""
    _add_ledger(conn, stream_id=1, seq=1, entity_key="HELD", event_ts="2020-01-01T00:00:00Z")
    _add_ledger(conn, stream_id=1, seq=2, entity_key="FREE", event_ts="2020-01-01T00:00:00Z")
    hold_id = holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="HELD",
        start_ts=None, end_ts=None, reason="hold",
    )
    conn.commit()
    batch = cleanup_repo.create_preview(conn)
    assert batch["frozen_retained_count"] == 1
    assert batch["deletable_count"] == 1

    # 冻结键从残留集消失：解除冻结 → 直接删行 → 重新登记同名冻结（名单仍显示生效）。
    holds_repo.release(conn, hold_id)
    conn.execute("DELETE FROM change_ledger WHERE entity_key='HELD'")
    holds_repo.insert(
        conn, scope="entity_key", stream_id=None, entity_key="HELD",
        start_ts=None, end_ts=None, reason="hold re-registered",
    )
    conn.commit()

    with pytest.raises(ValueError, match="拒绝执行"):
        cleanup_repo.execute_batch(conn, batch["id"])

    rejected = cleanup_repo.get_batch(conn, batch["id"])
    assert rejected["status"] == "rejected"
    assert rejected["deleted_count"] == 0
    assert "拒绝执行" in (rejected["note"] or "")
    guard = rejected["report"]["guardrail"]
    assert guard["refused"] is True
    assert guard["refusal_reasons"]
    removed = guard["preview_vs_execute_frozen_delta"]["removed"]
    assert [k["entity_key"] for k in removed] == ["HELD"]
    # 整批拒绝：连可删的 FREE 行也未被动过。
    assert {r["entity_key"] for r in conn.execute("SELECT entity_key FROM change_ledger")} == {"FREE"}
    # 拒绝后的批次不可再执行。
    with pytest.raises(ValueError):
        cleanup_repo.execute_batch(conn, batch["id"])
