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
    
    # Clear ALL run history before starting new run
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) FROM run_history")
    count = cursor.fetchone()[0]
    if count > 0:
        conn.execute("DELETE FROM run_history")
        conn.commit()
        logger.info(f"Cleared {count} previous run history records before new run")
    else:
        logger.info("No previous run history to clear")
    conn.close()
    
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

    # Get live pending count (current, not historical)
    live_pending = conn.execute("""
        SELECT COUNT(*) FROM pending_replacements WHERE status = 'pending'
    """).fetchone()[0]

    # Get approved count since last run
    approved_count = 0
    if last_run:
        started_at = last_run["started_at"]
        approved_count = conn.execute("""
            SELECT COUNT(*) FROM completed_jobs 
            WHERE status IN ('queued', 'completed')
            AND completed_at > ?
        """, (started_at,)).fetchone()[0]

    # Get run history (last 10)
    history = conn.execute("""
        SELECT * FROM run_history
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    last_run_dict = dict(last_run) if last_run else None
    if last_run_dict:
        last_run_dict["approved"] = approved_count
        last_run_dict["live_pending"] = live_pending

    return {
        "is_running": is_running(),
        "next_run": get_next_run_time(),
        "last_run": last_run_dict,
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
    
    # Get the most recent dry run with CSV data
    cursor = conn.execute("""
        SELECT csv_data, started_at FROM run_history 
        WHERE dry_run = 1 AND csv_data IS NOT NULL
        ORDER BY started_at DESC 
        LIMIT 1
    """)
    
    row = cursor.fetchone()
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

@router.get("/run/details/{category}")
async def get_run_details(category: str, run_started: str = None):
    """Get detailed movie list for a specific category from the last run."""
    from app.db.database import get_connection
    
    # Validate category
    valid_categories = ['processed', 'queued', 'pending', 'quality_skipped', 'no_releases', 'failed', 'approved']
    if category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"Invalid category. Must be one of: {valid_categories}")
    
    conn = get_connection()
    
    # Get the most recent run (or specific run by started_at if provided)
    if run_started:
        run = conn.execute("""
            SELECT * FROM run_history 
            WHERE started_at = ?
            ORDER BY started_at DESC 
            LIMIT 1
        """, (run_started,)).fetchone()
    else:
        run = conn.execute("""
            SELECT * FROM run_history 
            ORDER BY started_at DESC 
            LIMIT 1
        """).fetchone()
    
    if not run:
        conn.close()
        return {"movies": [], "category": category, "run_id": None}
    
    run_dict = dict(run)
    run_id = run_dict.get('id')
    started_at = run_dict.get('started_at')
    
    movies = []
    
    # PENDING: Get from pending_replacements table (active approvals)
    if category == 'pending':
        rows = conn.execute("""
            SELECT id, movie_title as title, movie_year as year,
                   current_size_gb, current_quality,
                   found_size_gb, found_quality,
                   indexer, seeders, created_at, tmdb_rating
            FROM pending_replacements 
            WHERE status = 'pending'
            ORDER BY created_at DESC
            LIMIT 100
        """).fetchall()
        movies = [dict(row) for row in rows]
    
    # QUEUED: Get from pending_replacements with status 'queued'
    elif category == 'queued':
        rows = conn.execute("""
            SELECT movie_title as title, movie_year as year,
                   current_size_gb, current_quality,
                   found_size_gb, found_quality,
                   status, queued_at as created_at
            FROM pending_replacements 
            WHERE status = 'queued'
            ORDER BY queued_at DESC
            LIMIT 100
        """).fetchall()
        movies = [dict(row) for row in rows]
    
    # FAILED: Get from pending_replacements with status 'failed' for this run only
    elif category == 'failed':
        rows = conn.execute("""
            SELECT pr.movie_title as title, pr.movie_year as year,
                pr.current_size_gb, pr.current_quality,
                pr.found_size_gb, pr.found_quality,
               'Download/queue failed' as error_message
            FROM pending_replacements pr
            INNER JOIN run_history rh ON rh.id = pr.run_id
            WHERE pr.status = 'failed' AND rh.id = ?
            ORDER BY pr.completed_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
        movies = [dict(row) for row in rows]
    
    # QUALITY_SKIPPED: First try run_details table, then fallback to CSV
    elif category == 'quality_skipped':
        # First try to get from run_details table (non-dry runs)
        rows = conn.execute("""
            SELECT movie_title as title, movie_year as year,
                current_size_gb, current_quality,
                found_size_gb, found_quality, skip_reason
            FROM run_details 
            WHERE run_id = ? AND category = 'quality_skipped'
            ORDER BY created_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
    
        if rows:
            movies = [dict(row) for row in rows]
        else:
            # Fallback to CSV for dry runs
            csv_data = run_dict.get('csv_data')
            if csv_data:
                import csv
                import io
                reader = csv.DictReader(io.StringIO(csv_data))
                for row in reader:
                    if row.get('Would Trigger') == 'No':
                        movies.append({
                            'title': row.get('Movie', 'Unknown'),
                            'year': None,
                            'current_size_gb': float(row.get('Current Size (GB)', 0)),
                            'current_quality': row.get('Current Quality', 'Unknown'),
                            'found_size_gb': float(row.get('Found Size (GB)', 0)),
                            'found_quality': row.get('Found Quality', 'Unknown'),
                            'skip_reason': row.get('Quality Decision', 'Quality check failed')
                        })
            movies = movies[:100]

    # NO_RELEASES: Get from run_details table
    elif category == 'no_releases':
        rows = conn.execute("""
            SELECT movie_title as title, movie_year as year,
                current_size_gb, current_quality
            FROM run_details 
            WHERE run_id = ? AND category = 'no_releases'
            ORDER BY created_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
        movies = [dict(row) for row in rows]

    # APPROVED: Get from completed_jobs for the current run
    elif category == 'approved':
        rows = conn.execute("""
            SELECT cj.movie_title as title, cj.movie_year as year,
                   cj.current_size_gb, cj.current_quality,
                   cj.found_size_gb, cj.found_quality,
                   cj.status, cj.completed_at as created_at,
                   cj.indexer, cj.seeders, cj.tmdb_rating
            FROM completed_jobs cj
            INNER JOIN run_history rh ON rh.id = cj.run_id
            WHERE rh.id = ? AND cj.status IN ('queued', 'completed')
            ORDER BY cj.completed_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
        movies = [dict(row) for row in rows]

    # PROCESSED: Get from run_details table (limit to 100)
    elif category == 'processed':
        rows = conn.execute("""
            SELECT movie_title as title, movie_year as year,
                    current_size_gb, current_quality,
                    date_added, tmdb_rating
            FROM run_details 
            WHERE run_id = ? AND category = 'processed'
            ORDER BY created_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
    
        if rows:
            movies = [dict(row) for row in rows]
        else:
            # Fallback to CSV for dry runs
            csv_data = run_dict.get('csv_data')
            if csv_data:
                import csv
                import io
                reader = csv.DictReader(io.StringIO(csv_data))
                for row in reader:
                    movies.append({
                        'title': row.get('Movie', 'Unknown'),
                        'year': None,
                        'current_size_gb': float(row.get('Current Size (GB)', 0)),
                        'current_quality': row.get('Current Quality', 'Unknown'),
                    })
            movies = movies[:100]
    
    conn.close()
    
    return {"movies": movies, "category": category, "run_id": run_id, "started_at": started_at}

@router.delete("/run/details/{category}/clear")
async def clear_category(category: str):
    """Clear all items in a specific category."""
    valid_categories = ['queued', 'quality_skipped', 'no_releases', 'failed', 'processed']
    if category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"Cannot clear {category} category")
    
    conn = get_connection()
    
    if category == 'queued':
        # Reset queued to pending? Or just delete? Let's reset to pending
        result = conn.execute("""
            UPDATE pending_replacements 
            SET status = 'pending', queued_at = NULL 
            WHERE status = 'queued'
        """)
        count = result.rowcount
        logger.info(f"Reset {count} queued items to pending")
    
    elif category == 'failed':
        result = conn.execute("DELETE FROM pending_replacements WHERE status = 'failed'")
        count = result.rowcount
        logger.info(f"Deleted {count} failed items")
    
    elif category == 'quality_skipped':
        # Quality skipped isn't stored persistently yet
        count = 0
        logger.info("Quality skipped clear requested but not yet implemented")
    
    elif category == 'no_releases':
        count = 0
        logger.info("No releases clear requested but not yet implemented")
    
    elif category == 'processed':
        # Can't clear processed - it's derived from run history
        count = 0
        logger.info("Processed clear requested but not derived from stored data")
    
    conn.commit()
    conn.close()
    
    return {"success": True, "count": count, "category": category}

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

# ========== CLEAR DASHBOARD ENDPOINT ==========
@router.delete("/dashboard/clear")
async def clear_dashboard():
    """Clear all dashboard data: run history, run details, and pending approvals."""
    conn = get_connection()
    
    # Count records before deleting
    history_count = conn.execute("SELECT COUNT(*) FROM run_history").fetchone()[0]
    details_count = conn.execute("SELECT COUNT(*) FROM run_details").fetchone()[0]
    pending_count = conn.execute("SELECT COUNT(*) FROM pending_replacements WHERE status = 'pending'").fetchone()[0]
    
    # Clear all run history
    conn.execute("DELETE FROM run_history")
    
    # Clear all run details
    conn.execute("DELETE FROM run_details")
    
    # Clear pending approvals
    conn.execute("DELETE FROM pending_replacements")
    
    # Reset run state (so next run starts from beginning)
    conn.execute("DELETE FROM run_state")
    
    conn.commit()
    conn.close()
    
    logger.info(f"Cleared dashboard: {history_count} history records, {details_count} details, {pending_count} pending approvals")
    
    return {
        "success": True,
        "cleared": {
            "run_history": history_count,
            "run_details": details_count,
            "pending_approvals": pending_count
        }
    }
# ========== END CLEAR DASHBOARD ENDPOINT ==========