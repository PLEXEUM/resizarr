from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.db.database import get_connection
from app.core.scheduler import execute_run, is_running, get_next_run_time
from app.utils.logger import get_logger
import io

router = APIRouter()
logger = get_logger()


@router.post("/run")
async def trigger_run(dry_run: bool = False):
    """Manually trigger a scanner run."""
    if is_running():
        raise HTTPException(
            status_code=409,
            detail="A run is already in progress"
        )

    logger.info(f"Manual run triggered (dry_run={dry_run})")

    # Run in background
    import asyncio
    asyncio.create_task(execute_run(dry_run=dry_run))

    return {
        "success": True,
        "message": "Run started",
        "dry_run": dry_run
    }


@router.get("/status")
async def get_status():
    """Get current run status and last run summary."""
    conn = get_connection()

    # Get last run
    last_run = conn.execute("""
        SELECT * FROM run_history
        ORDER BY started_at DESC
        LIMIT 1
    """).fetchone()

    # Get run history (last 10)
    history = conn.execute("""
        SELECT * FROM run_history
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    return {
        "is_running": is_running(),
        "next_run": get_next_run_time(),
        "last_run": dict(last_run) if last_run else None,
        "history": [dict(r) for r in history]
    }


@router.get("/run/csv")
async def download_csv():
    """Download the most recent dry run CSV."""
    conn = get_connection()
    last_dry_run = conn.execute("""
        SELECT * FROM run_history
        WHERE dry_run = 1
        ORDER BY started_at DESC
        LIMIT 1
    """).fetchone()
    conn.close()

    if not last_dry_run:
        raise HTTPException(
            status_code=404,
            detail="No dry run CSV available"
        )

    # Read CSV from logs directory
    import os
    csv_path = "/app/logs/last_dry_run.csv"
    if not os.path.exists(csv_path):
        raise HTTPException(
            status_code=404,
            detail="CSV file not found"
        )

    with open(csv_path, "r") as f:
        content = f.read()

    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=resizarr_dry_run.csv"
        }
    )