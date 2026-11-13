from fastapi import APIRouter

from app.db import connect
from app.repositories import legal_holds as holds_repo
from app.repositories import ledger_cleanup as cleanup_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
def dashboard():
    conn = connect()
    try:
        streams = streams_repo.list_streams(conn)
        runs = runs_repo.list_runs(conn, limit=5)
        batches = cleanup_repo.list_batches(conn, limit=1)
        return {
            "stream_count": len(streams),
            "enabled_streams": sum(1 for s in streams if s["enabled"]),
            "sink_count": sink_repo.count_sink(conn),
            "quarantine_count": quarantine_repo.count_quarantine(conn),
            "run_count": len(runs_repo.list_runs(conn, limit=10000)),
            "recent_runs": runs,
            "ledger_count": cleanup_repo.ledger_count(conn),
            "active_hold_count": holds_repo.count_active(conn),
            "retention_days": cleanup_repo.get_retention_days(conn),
            "latest_cleanup": batches[0] if batches else None,
        }
    finally:
        conn.close()
