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
    
    conn = get_connection()
    
    # Get the most recent dry run
    cursor = conn.execute("""
        SELECT csv_data, started_at FROM run_history 
        WHERE dry_run = 1 AND csv_data IS NOT NULL
        ORDER BY started_at DESC 
        LIMIT 1
    """)
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(
            status_code=404,
            detail="No dry run CSV available. Please run a dry scan first."
        )
    
    csv_data = row[0]  # csv_data is the first column
    started_at = row[1]
    
    # Create filename with timestamp from the run
    timestamp = started_at.replace(":", "-").replace(".", "-") if started_at else datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"resizarr_dryrun_{timestamp}.csv"
    
    return StreamingResponse(
        io.StringIO(csv_data),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )