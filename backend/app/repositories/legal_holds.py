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
    """返回第一条命中的启用冻结登记；语义与 trg_ledger_block_frozen_delete 保持一致。

    命中条件（与触发器 WHEN 子句一一对应）：
    - 冻结处于启用（active=1）；
    - hold.stream_id 为空（不限流）或等于该行 stream_id；
    - scope='entity_key' 且 entity_key 相同；
      或 scope='time_range' 且 event_ts 落在 [start_ts, end_ts]（端点可空=不限）。
    """
    stream_id = row.get("stream_id")
    entity_key = row.get("entity_key")
    event_ts = row.get("event_ts")
    for hold in holds:
        if not int(hold.get("active") or 0):
            continue
        hold_stream = hold.get("stream_id")
        if hold_stream is not None:
            if stream_id is None or int(hold_stream) != int(stream_id):
                continue
        scope = hold.get("scope")
        if scope == "entity_key":
            hold_key = hold.get("entity_key")
            if hold_key is not None and hold_key == entity_key:
                return hold
        elif scope == "time_range":
            if event_ts is None:
                continue
            start_ts = hold.get("start_ts")
            end_ts = hold.get("end_ts")
            if start_ts is not None and event_ts < start_ts:
                continue
            if end_ts is not None and event_ts > end_ts:
                continue
            return hold
    return None
