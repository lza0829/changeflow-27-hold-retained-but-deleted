from fastapi import APIRouter, HTTPException

from app.db import connect
from app.repositories import ledger_cleanup as cleanup_repo
from app.schemas.entities import CleanupPreviewRequest, RetentionPut

router = APIRouter(tags=["ledger-cleanup"])


@router.get("/ledger/cleanup/retention")
def get_retention():
    conn = connect()
    try:
        return {
            "retention_days": cleanup_repo.get_retention_days(conn),
            "ledger_count": cleanup_repo.ledger_count(conn),
        }
    finally:
        conn.close()


@router.put("/ledger/cleanup/retention")
def put_retention(body: RetentionPut):
    conn = connect()
    try:
        days = cleanup_repo.set_retention_days(conn, body.retention_days)
        conn.commit()
        return {"retention_days": days}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    finally:
        conn.close()


@router.get("/ledger/cleanup/batches")
def list_batches():
    conn = connect()
    try:
        return cleanup_repo.list_batches(conn)
    finally:
        conn.close()


@router.get("/ledger/cleanup/batches/{batch_id}")
def get_batch(batch_id: int):
    conn = connect()
    try:
        batch = cleanup_repo.get_batch(conn, batch_id)
        if not batch:
            raise HTTPException(404, "cleanup batch not found")
        return batch
    finally:
        conn.close()


@router.post("/ledger/cleanup/preview")
def create_preview(body: CleanupPreviewRequest | None = None):
    conn = connect()
    try:
        body = body or CleanupPreviewRequest()
        return cleanup_repo.create_preview(
            conn, retention_days=body.retention_days
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    finally:
        conn.close()


@router.post("/ledger/cleanup/batches/{batch_id}/execute")
def execute_batch(batch_id: int):
    conn = connect()
    try:
        return cleanup_repo.execute_batch(conn, batch_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    finally:
        conn.close()
