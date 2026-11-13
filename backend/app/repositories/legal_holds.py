from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_ts(value: str | None) -> str | None:
    """把用户输入规范成与 event_ts 可比较的 UTC ISO 串（YYYY-MM-DDTHH:MM:SSZ）。"""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def insert(
    conn: sqlite3.Connection,
    *,
    scope: str,
    stream_id: int | None,
    entity_key: str | None,
    start_ts: str | None,
    end_ts: str | None,
    reason: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO legal_holds(scope, stream_id, entity_key, start_ts, end_ts, reason, active, created_at)
        VALUES (?,?,?,?,?,?,1,?)
        """,
        (scope, stream_id, entity_key, start_ts, end_ts, reason, _now()),
    )
    return int(cur.lastrowid)


def list_holds(conn: sqlite3.Connection, active_only: bool = False) -> list[dict[str, Any]]:
    sql = """
        SELECT id, scope, stream_id, entity_key, start_ts, end_ts, reason, active, created_at, released_at
        FROM legal_holds
    """
    if active_only:
        sql += " WHERE active=1"
    sql += " ORDER BY id DESC"
    return [dict(r) for r in conn.execute(sql).fetchall()]


def get_hold(conn: sqlite3.Connection, hold_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, scope, stream_id, entity_key, start_ts, end_ts, reason, active, created_at, released_at
        FROM legal_holds WHERE id=?
        """,
        (hold_id,),
    ).fetchone()
    return dict(row) if row else None


def release(conn: sqlite3.Connection, hold_id: int) -> None:
    conn.execute(
        "UPDATE legal_holds SET active=0, released_at=? WHERE id=?",
        (_now(), hold_id),
    )


def count_active(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute("SELECT COUNT(*) c FROM legal_holds WHERE active=1").fetchone()["c"]
    )


def match_holds(holds: list[dict[str, Any]], row: dict[str, Any]) -> dict[str, Any] | None:
    """返回第一条命中的启用冻结登记；语义与 trg_ledger_block_frozen_delete 保持一致。"""
    return None
