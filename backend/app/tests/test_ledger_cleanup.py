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
