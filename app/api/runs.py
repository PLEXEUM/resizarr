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

    # Get approved count from current run only
    approved_count = 0
    if last_run:
        run_id = last_run["id"]
        approved_count = conn.execute("""
            SELECT COUNT(*) FROM completed_jobs 
            WHERE run_id = ? AND status IN ('queued', 'completed')
        """, (run_id,)).fetchone()[0]

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
    
    progress_data = get_run_progress_data()
    
    if not hasattr(get_progress, "progress_data"):
        get_progress.progress_data = {"current": 0, "total": 0, "movie": ""}
    
    current = progress_data.get("current", get_progress.progress_data.get("current", 0))
    total = progress_data.get("total", get_progress.progress_data.get("total", 0))
    movie = progress_data.get("movie", get_progress.progress_data.get("movie", ""))
    cancelled = progress_data.get("cancelled", False)
    completed = progress_data.get("completed", False)
    
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
    
    timestamp = started_at.replace(":", "-").replace(".", "-")[:19] if started_at else datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"resizarr_dryrun_{timestamp}.csv"
    
    return StreamingResponse(
        io.StringIO(csv_data),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/run/details/{category}")
async def get_run_details(category: str, run_started: str = None):
    """Get detailed movie list for a specific category from the last run.
    
    Categories:
    - processed: All movies scanned in the run (with status: Pending/Approved/Skipped)
    - pending: Awaiting user approval
    - approved: Approved in this run (queued/completed)
    - skipped: Quality Skipped + No Releases + Failed (unified)
    """
    from app.db.database import get_connection
    
    valid_categories = ['processed', 'pending', 'approved', 'skipped']
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
    
    # PROCESSED: Get from run_details with status determined by other tables
    if category == 'processed':
        rows = conn.execute("""
            SELECT rd.movie_title as title, rd.movie_year as year,
                   rd.current_size_gb, rd.current_quality,
                   rd.date_added, rd.tmdb_rating
            FROM run_details rd
            WHERE rd.run_id = ? AND rd.category = 'processed'
            ORDER BY rd.created_at DESC
            LIMIT 500
        """, (run_id,)).fetchall()
        
        # For each processed movie, determine its status
        for row in rows:
            movie = dict(row)
            movie_title = movie['title']
            
            # Check if pending
            pending = conn.execute("""
                SELECT 1 FROM pending_replacements 
                WHERE movie_title = ? AND status = 'pending' AND run_id = ?
                LIMIT 1
            """, (movie_title, run_id)).fetchone()
            
            if pending:
                movie['status'] = 'Pending'
            else:
                # Check if approved (in completed_jobs for this run)
                approved = conn.execute("""
                    SELECT 1 FROM completed_jobs 
                    WHERE movie_title = ? AND run_id = ? AND status IN ('queued', 'completed')
                    LIMIT 1
                """, (movie_title, run_id)).fetchone()
                
                if approved:
                    movie['status'] = 'Approved'
                else:
                    # Check if skipped
                    skipped = conn.execute("""
                        SELECT 1 FROM run_details rd2
                        WHERE rd2.run_id = ? AND rd2.movie_title = ? 
                        AND rd2.category IN ('quality_skipped', 'no_releases')
                        LIMIT 1
                    """, (run_id, movie_title)).fetchone()
                    
                    if skipped:
                        movie['status'] = 'Skipped'
                    else:
                        # Check failed
                        failed = conn.execute("""
                            SELECT 1 FROM pending_replacements 
                            WHERE movie_title = ? AND status = 'failed' AND run_id = ?
                            LIMIT 1
                        """, (movie_title, run_id)).fetchone()
                        
                        movie['status'] = 'Skipped' if failed else 'Processed'
            
            movies.append(movie)
    
    # PENDING: Get from pending_replacements
    elif category == 'pending':
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
    
    # APPROVED: Get from completed_jobs for this run only
    elif category == 'approved':
        rows = conn.execute("""
            SELECT cj.movie_title as title, cj.movie_year as year,
                   cj.current_size_gb, cj.current_quality,
                   cj.found_size_gb, cj.found_quality,
                   cj.status, cj.completed_at as created_at,
                   cj.indexer, cj.seeders, cj.tmdb_rating
            FROM completed_jobs cj
            WHERE cj.run_id = ? AND cj.status IN ('queued', 'completed')
            ORDER BY cj.completed_at DESC
            LIMIT 100
        """, (run_id,)).fetchall()
        movies = [dict(row) for row in rows]
    
    # SKIPPED: Unified table (Quality Skipped + No Releases + Failed)
    elif category == 'skipped':
        # Quality Skipped
        quality_skipped = conn.execute("""
            SELECT rd.movie_title as title, rd.movie_year as year,
                   rd.current_size_gb, rd.current_quality,
                   rd.found_size_gb, rd.found_quality,
                   rd.tmdb_rating,
                   rd.skip_reason as reason
            FROM run_details rd
            WHERE rd.run_id = ? AND rd.category = 'quality_skipped'
        """, (run_id,)).fetchall()
        
        for row in quality_skipped:
            movies.append(dict(row))
        
        # No Releases
        no_releases = conn.execute("""
            SELECT rd.movie_title as title, rd.movie_year as year,
                   rd.current_size_gb, rd.current_quality,
                   NULL as found_size_gb, NULL as found_quality,
                   rd.tmdb_rating,
                   '❌ no releases: none found' as reason
            FROM run_details rd
            WHERE rd.run_id = ? AND rd.category = 'no_releases'
        """, (run_id,)).fetchall()
        
        for row in no_releases:
            movies.append(dict(row))
        
        # Failed
        failed = conn.execute("""
            SELECT pr.movie_title as title, pr.movie_year as year,
                   pr.current_size_gb, pr.current_quality,
                   pr.found_size_gb, pr.found_quality,
                   pr.tmdb_rating,
                   '❌ failed: ' || COALESCE(pr.error_message, 'Unknown error') as reason
            FROM pending_replacements pr
            WHERE pr.run_id = ? AND pr.status = 'failed'
        """, (run_id,)).fetchall()
        
        for row in failed:
            movies.append(dict(row))
        
        # Sort by title for consistency
        movies.sort(key=lambda x: x.get('title', ''))
    
    conn.close()
    
    return {"movies": movies, "category": category, "run_id": run_id, "started_at": started_at}


@router.get("/total-space-saved")
async def get_total_space_saved():
    """Calculate total space saved from completed jobs (queued/completed status)."""
    conn = get_connection()
    
    result = conn.execute("""
        SELECT SUM(current_size_gb - found_size_gb) as total_saved
        FROM completed_jobs
        WHERE status IN ('queued', 'completed')
        AND found_size_gb IS NOT NULL
        AND found_size_gb > 0
    """).fetchone()
    
    conn.close()
    
    total_saved = result["total_saved"] if result and result["total_saved"] else 0
    
    return {"total_saved_gb": round(total_saved, 2)}


# ========== CLEAR DASHBOARD ENDPOINT ==========
@router.delete("/dashboard/clear")
async def clear_dashboard():
    """Clear all dashboard data: run history, run details, and pending approvals."""
    conn = get_connection()
    
    history_count = conn.execute("SELECT COUNT(*) FROM run_history").fetchone()[0]
    details_count = conn.execute("SELECT COUNT(*) FROM run_details").fetchone()[0]
    pending_count = conn.execute("SELECT COUNT(*) FROM pending_replacements WHERE status = 'pending'").fetchone()[0]
    
    conn.execute("DELETE FROM run_history")
    conn.execute("DELETE FROM run_details")
    conn.execute("DELETE FROM pending_replacements")
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


# ========== REMOVED: Per-category clear endpoints ==========
# The following endpoints have been removed as they are no longer needed:
# - DELETE /run/details/{category}/clear
# - DELETE /history/clear