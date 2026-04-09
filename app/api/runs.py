from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.db.database import get_connection
from app.core.scheduler import execute_run, is_running, get_next_run_time, set_running
from app.utils.logger import get_logger
import io
from datetime import datetime

router = APIRouter()
logger = get_logger()


@router.post("/run")
async def trigger_run(dry_run: bool = False):
    """Manually trigger a scanner run."""
    import asyncio
    import uuid
    from app.core.scanner import run_resizarr
    
    if is_running():
        raise HTTPException(
            status_code=409,
            detail="A run is already in progress"
        )
    
    # Clear the most recent run history before starting new run
    conn = get_connection()
    # Check if there are any rows before trying to delete
    cursor = conn.execute("SELECT COUNT(*) FROM run_history")
    count = cursor.fetchone()[0]
    if count > 0:
        conn.execute("""
            DELETE FROM run_history 
            WHERE id = (SELECT id FROM run_history ORDER BY started_at DESC LIMIT 1)
        """)
        conn.commit()
        logger.info("Cleared previous run history before new run")
    else:
        logger.info("No previous run history to clear")
    conn.close()
    logger.info("Cleared previous run history before new run")
    
    logger.info(f"Manual run triggered (dry_run={dry_run})")
    
    # Generate unique run ID for cancellation
    run_id = str(uuid.uuid4())
    
    # Set running state
    set_running(True)
    
    # Set up progress tracking
    progress = {"current": 0, "total": 0, "movie": ""}
    
    async def progress_callback(current, total, movie_title):
        progress["current"] = current
        progress["total"] = total
        progress["movie"] = movie_title
        from app.api.runs import get_progress
        if not hasattr(get_progress, "progress_data"):
            get_progress.progress_data = {"current": 0, "total": 0, "movie": ""}
        get_progress.progress_data["current"] = current
        get_progress.progress_data["total"] = total
        get_progress.progress_data["movie"] = movie_title
        logger.debug(f"Progress: {current}/{total} - {movie_title}")
    
    # Run the scanner in background
    async def run_and_reset():
        try:
            await run_resizarr(dry_run=dry_run, progress_callback=progress_callback, run_id=run_id)
        finally:
            set_running(False)
            from app.api.runs import get_progress
            if hasattr(get_progress, "progress_data"):
                get_progress.progress_data = {"current": 0, "total": 0, "movie": ""}
    
    asyncio.create_task(run_and_reset())
    
    return {
        "success": True,
        "message": "Run started",
        "dry_run": dry_run,
        "run_id": run_id
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


@router.get("/run/progress")
async def get_progress():
    """Get current run progress including cancellation status."""
    from app.core.scheduler import is_running
    from app.core.scanner import get_run_progress_data
    
    # Get progress from scanner
    progress_data = get_run_progress_data()
    
    # Store progress in a global variable - keep for backward compatibility
    if not hasattr(get_progress, "progress_data"):
        get_progress.progress_data = {"current": 0, "total": 0, "movie": ""}
    
    current = progress_data.get("current", get_progress.progress_data.get("current", 0))
    total = progress_data.get("total", get_progress.progress_data.get("total", 0))
    movie = progress_data.get("movie", get_progress.progress_data.get("movie", ""))
    cancelled = progress_data.get("cancelled", False)
    completed = progress_data.get("completed", False)
    
    # Only calculate percent if total > 0 and run is running
    if is_running() and total > 0:
        percent = int((current / total) * 100)
    else:
        percent = 0
    
    return {
        "is_running": is_running(),
        "current": current,
        "total": total,
        "movie": movie,
        "percent": percent,
        "cancelled": cancelled,
        "completed": completed
    }


@router.get("/run/status")
async def get_run_status():
    """Get current run status including run_id for cancellation."""
    from app.core.scheduler import is_running
    from app.core.scanner import get_active_run_id
    
    return {
        "is_running": is_running(),
        "run_id": get_active_run_id() if is_running() else None
    }


@router.post("/run/{run_id}/cancel")
async def cancel_run_endpoint(run_id: str):
    """Cancel a running scan."""
    from app.core.scheduler import is_running
    from app.core.scanner import cancel_active_run
    
    if not is_running():
        raise HTTPException(status_code=400, detail="No active run in progress")
    
    success = await cancel_active_run(run_id)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to cancel run - run_id mismatch or already completing")
    
    logger.info(f"Run {run_id} cancellation requested")
    return {"success": True, "message": "Cancellation requested"}


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


# ========== CLEAR RUN HISTORY ENDPOINT ==========
@router.delete("/history/clear")
async def clear_run_history():
    """Clear all run history records."""
    conn = get_connection()
    
    # Count before deleting
    result = conn.execute("SELECT COUNT(*) FROM run_history")
    count = result.fetchone()[0]
    
    # Delete all history
    conn.execute("DELETE FROM run_history")
    conn.commit()
    conn.close()
    
    logger.info(f"Cleared {count} run history records")
    return {"success": True, "count": count}
# ========== END CLEAR RUN HISTORY ENDPOINT ==========