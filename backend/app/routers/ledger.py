from fastapi import APIRouter

from app.db import connect
from app.repositories import ledger as ledger_repo
from app.repositories import ledger_cleanup as cleanup_repo

router = APIRouter(tags=["ledger"])


@router.get("/ledger")
def list_ledger():
    conn = connect()
    try:
        return ledger_repo.list_ledger(conn)
    finally:
        conn.close()


@router.get("/ledger/stats")
def ledger_stats():
    """同源对账用：账本总条数、未超窗条数，供前端与清理批次对照。"""
    conn = connect()
    try:
        return {"ledger_count": cleanup_repo.ledger_count(conn)}
    finally:
        conn.close()
