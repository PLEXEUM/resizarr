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
    from app.db.database import get_connection
    import io
    from datetime import datetime
    import logging
    
    logger = logging.getLogger("resizarr")
    
    conn = get_connection()
    
    # First, let's see what's in the run_history table
    count_cursor = conn.execute("SELECT COUNT(*) FROM run_history WHERE dry_run = 1")
    count = count_cursor.fetchone()[0]
    logger.info(f"Found {count} dry run entries in run_history")
    
    # Get the most recent dry run with CSV data
    cursor = conn.execute("""
        SELECT csv_data, started_at FROM run_history 
        WHERE dry_run = 1 
        ORDER BY started_at DESC 
        LIMIT 1
    """)
    
    row = cursor.fetchone()
    
    if row:
        logger.info(f"Row found! CSV data length: {len(row[0]) if row[0] else 0}")
    else:
        logger.info("No row found")
    
    conn.close()
    
    if not row or not row[0]:
        raise HTTPException(
            status_code=404,
            detail="No dry run CSV available. Please run a dry scan first."
        )
    
    csv_data = row[0]
    started_at = row[1]
    
    # Create filename with timestamp from the run
    timestamp = started_at.replace(":", "-").replace(".", "-")[:19] if started_at else datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"resizarr_dryrun_{timestamp}.csv"
    
    return StreamingResponse(
        io.StringIO(csv_data),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )