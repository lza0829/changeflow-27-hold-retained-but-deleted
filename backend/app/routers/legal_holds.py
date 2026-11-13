from fastapi import APIRouter, HTTPException

from app.db import connect
from app.repositories import legal_holds as holds_repo
from app.schemas.entities import LegalHoldCreate

router = APIRouter(tags=["legal-holds"])


@router.get("/legal-holds")
def list_holds():
    conn = connect()
    try:
        return holds_repo.list_holds(conn)
    finally:
        conn.close()


@router.post("/legal-holds")
def create_hold(body: LegalHoldCreate):
    if body.scope not in ("entity_key", "time_range"):
        raise HTTPException(400, "scope must be entity_key or time_range")
    if body.scope == "entity_key" and not (body.entity_key or "").strip():
        raise HTTPException(400, "entity_key hold requires entity_key")
    if body.scope == "time_range" and not (body.start_ts or body.end_ts):
        raise HTTPException(400, "time_range hold requires start_ts and/or end_ts")
    try:
        start_ts = holds_repo.normalize_ts(body.start_ts)
        end_ts = holds_repo.normalize_ts(body.end_ts)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(400, "start_ts must be <= end_ts")
    if not (body.reason or "").strip():
        raise HTTPException(400, "reason is required")
    conn = connect()
    try:
        hold_id = holds_repo.insert(
            conn,
            scope=body.scope,
            stream_id=body.stream_id,
            entity_key=(body.entity_key or "").strip() or None,
            start_ts=start_ts,
            end_ts=end_ts,
            reason=body.reason.strip(),
        )
        conn.commit()
        return holds_repo.get_hold(conn, hold_id)
    finally:
        conn.close()


@router.post("/legal-holds/{hold_id}/release")
def release_hold(hold_id: int):
    conn = connect()
    try:
        hold = holds_repo.get_hold(conn, hold_id)
        if not hold:
            raise HTTPException(404, "legal hold not found")
        holds_repo.release(conn, hold_id)
        conn.commit()
        return holds_repo.get_hold(conn, hold_id)
    finally:
        conn.close()
