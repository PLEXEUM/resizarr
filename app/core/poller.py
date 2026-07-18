import asyncio
from datetime import datetime, timedelta
from app.utils.logger import get_logger
from app.db.database import get_connection
from app.core.radarr_client import RadarrClient

logger = get_logger()

# Track if poller is running
_poller_in_progress = False
MAX_FAIL_COUNT = 5
MAX_AGE_HOURS = 24


async def poll_pending_replacements():
    """Check status of all pending replacements."""
    global _poller_in_progress

    if _poller_in_progress:
        logger.warning("Poller already running, skipping this interval")
        return

    _poller_in_progress = True

    try:
        conn = get_connection()

        # Load config
        config = conn.execute(
            "SELECT * FROM config WHERE id = 1"
        ).fetchone()

        if not config or not config["radarr_url"]:
            logger.warning("Radarr not configured, skipping poll")
            return

        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])

        # Get all queued replacements
        queued = conn.execute("""
            SELECT * FROM pending_replacements
            WHERE status = 'queued'
        """).fetchall()

        if not queued:
            # Check for any queued completed_jobs (with or without pending records)
            orphaned = conn.execute("""
                SELECT COUNT(*) FROM completed_jobs
                WHERE status = 'queued'
            """).fetchone()[0]
            
            if orphaned > 0:
                logger.info(f"Found {orphaned} queued jobs, checking directly")
                jobs = conn.execute("""
                    SELECT * FROM completed_jobs
                    WHERE status = 'queued'
                """).fetchall()
                
                for job in jobs:
                    await check_job_status(client, conn, job)
            else:
                logger.info("No queued replacements to poll")
            return

        logger.info(f"Polling {len(queued)} queued replacements...")

        for record in queued:
            await check_record_status(client, conn, record)

        # Also check any queued completed_jobs without pending records
        orphaned_jobs = conn.execute("""
            SELECT * FROM completed_jobs
            WHERE status = 'queued'
            AND NOT EXISTS (
                SELECT 1 FROM pending_replacements 
                WHERE pending_replacements.movie_id = completed_jobs.movie_id 
                AND pending_replacements.status = 'queued'
            )
        """).fetchall()
        
        for job in orphaned_jobs:
            await check_job_status(client, conn, job)

        # Purge old run history (keep last 100)
        conn.execute("""
            DELETE FROM run_history
            WHERE id NOT IN (
                SELECT id FROM run_history
                ORDER BY started_at DESC
                LIMIT 100
            )
        """)
        conn.commit()
        conn.close()

    except Exception as e:
        logger.error(f"Poller error: {e}")
    finally:
        _poller_in_progress = False


async def check_record_status(client, conn, record):
    """Check status of a pending replacement record."""
    record_id = record["id"]
    movie_id = record["movie_id"]
    movie_title = record["movie_title"]
    created_at = datetime.fromisoformat(record["created_at"])
    fail_count = record["fail_count"] or 0
    original_size_gb = record["current_size_gb"]
    original_quality = record["current_quality"]

    # Check if too old
    age = datetime.utcnow() - created_at
    if age > timedelta(hours=MAX_AGE_HOURS):
        logger.warning(f"Replacement for '{movie_title}' expired after 24h, marking failed")
        conn.execute("""
            UPDATE pending_replacements
            SET status = 'failed', completed_at = ?, error_message = 'Expired after 24 hours'
            WHERE id = ?
        """, (datetime.utcnow(), record_id))
        
        conn.execute("""
            UPDATE completed_jobs
            SET status = 'failed', completed_at = ?
            WHERE movie_id = ? AND status = 'queued'
        """, (datetime.utcnow(), movie_id))
        
        conn.commit()
        return

    # Check if too many failures
    if fail_count >= MAX_FAIL_COUNT:
        logger.warning(f"Replacement for '{movie_title}' failed {fail_count} times, cancelling")
        conn.execute("""
            UPDATE pending_replacements
            SET status = 'failed', completed_at = ?, error_message = 'Max failures exceeded'
            WHERE id = ?
        """, (datetime.utcnow(), record_id))
        
        conn.execute("""
            UPDATE completed_jobs
            SET status = 'failed', completed_at = ?
            WHERE movie_id = ? AND status = 'queued'
        """, (datetime.utcnow(), movie_id))
        
        conn.commit()
        return

    try:
        # Fetch current movie state from Radarr
        movie = await client.get_movie(movie_id)
        movie_file = movie.get("movieFile")

        if not movie_file:
            # No file yet - still downloading or deleted
            logger.debug(f"No file found for '{movie_title}', still downloading")
            conn.execute("""
                UPDATE pending_replacements
                SET fail_count = fail_count + 1
                WHERE id = ?
            """, (record_id,))
            conn.commit()
            return

        # Get current size and quality
        current_size_gb = movie_file.get("size", 0) / (1024 ** 3)
        
        # Get current quality
        current_quality = "Unknown"
        file_quality_wrapper = movie_file.get("quality", {})
        if isinstance(file_quality_wrapper, dict):
            file_quality_obj = file_quality_wrapper.get("quality", {})
            if isinstance(file_quality_obj, dict):
                current_quality = file_quality_obj.get("name", "Unknown")

        # Check if replacement completed
        size_changed = abs(current_size_gb - original_size_gb) > 0.01
        quality_changed = current_quality != original_quality and current_quality != "Unknown"
        
        # Mark as completed if ANY of these are true
        replacement_completed = (
            size_changed or 
            quality_changed or 
            current_size_gb < original_size_gb * 0.95
        )

        if replacement_completed:
            logger.info(
                f"✅ Replacement completed for '{movie_title}': "
                f"{original_size_gb:.2f}GB → {current_size_gb:.2f}GB "
                f"({current_quality})"
            )
            
            # Update pending_replacements
            conn.execute("""
                UPDATE pending_replacements
                SET status = 'completed', 
                    completed_at = ?, 
                    found_size_gb = ?,
                    found_quality = ?
                WHERE id = ?
            """, (datetime.utcnow(), current_size_gb, current_quality, record_id))

            # Update completed_jobs
            conn.execute("""
                UPDATE completed_jobs
                SET status = 'completed', 
                    completed_at = ?,
                    found_size_gb = ?,
                    found_quality = ?,
                    indexer = COALESCE(indexer, ?),
                    seeders = COALESCE(seeders, ?),
                    tmdb_rating = COALESCE(tmdb_rating, ?),
                    movie_year = COALESCE(movie_year, ?)
                WHERE movie_id = ? AND status = 'queued'
            """, (
                datetime.utcnow(), 
                current_size_gb, 
                current_quality,
                record.get("indexer"),
                record.get("seeders", 0),
                record.get("tmdb_rating"),
                record.get("movie_year", 0),
                movie_id
            ))
            
            conn.commit()
            logger.info(f"✅ Updated status to 'completed' for '{movie_title}'")
        else:
            # Still waiting
            logger.debug(
                f"⏳ No change yet for '{movie_title}' "
                f"({current_size_gb:.2f}GB, {current_quality})"
            )
            conn.execute("""
                UPDATE pending_replacements
                SET fail_count = fail_count + 1
                WHERE id = ?
            """, (record_id,))
            conn.commit()

    except Exception as e:
        logger.error(f"Poll failed for '{movie_title}': {e}")
        conn.execute("""
            UPDATE pending_replacements
            SET fail_count = fail_count + 1,
                error_message = ?
            WHERE id = ?
        """, (str(e)[:200], record_id))
        conn.commit()


async def check_job_status(client, conn, job):
    """Check status of a completed_job directly (no pending record)."""
    movie_id = job["movie_id"]
    movie_title = job["movie_title"]
    
    try:
        movie = await client.get_movie(movie_id)
        movie_file = movie.get("movieFile")
        
        if not movie_file:
            return
        
        current_size_gb = movie_file.get("size", 0) / (1024 ** 3)
        
        # Get current quality
        current_quality = "Unknown"
        file_quality_wrapper = movie_file.get("quality", {})
        if isinstance(file_quality_wrapper, dict):
            file_quality_obj = file_quality_wrapper.get("quality", {})
            if isinstance(file_quality_obj, dict):
                current_quality = file_quality_obj.get("name", "Unknown")
        
        # Check if size changed from the recorded found_size OR original size
        original_found = job["found_size_gb"]
        original_size = job["current_size_gb"]
        
        # If found_size is NULL or 0, use original_size as fallback
        size_to_compare = original_found if original_found and original_found > 0 else original_size
        
        # Also check if quality changed
        original_quality = job["current_quality"]
        quality_changed = current_quality != original_quality and current_quality != "Unknown"
        
        # Check if size changed significantly OR quality changed
        if (size_to_compare and abs(current_size_gb - size_to_compare) > 0.01) or quality_changed:
            logger.info(f"✅ Job completed for '{movie_title}': {current_size_gb:.2f}GB ({current_quality})")
            conn.execute("""
                UPDATE completed_jobs
                SET status = 'completed', 
                    completed_at = ?,
                    found_size_gb = ?,
                    found_quality = ?,
                    indexer = COALESCE(indexer, ?),
                    seeders = COALESCE(seeders, ?),
                    tmdb_rating = COALESCE(tmdb_rating, ?),
                    movie_year = COALESCE(movie_year, ?)
                WHERE id = ? AND status = 'queued'
            """, (
                datetime.utcnow(), 
                current_size_gb,
                current_quality,
                job.get("indexer"),
                job.get("seeders", 0),
                job.get("tmdb_rating"),
                job.get("movie_year", 0),
                job["id"]
            ))
            conn.commit()
            
    except Exception as e:
        logger.error(f"Failed to check job status for '{movie_title}': {e}")


async def start_poller(interval_minutes: int = 5):
    """Run the poller on a loop at the given interval."""
    logger.info(f"Poller started with {interval_minutes} minute interval")
    while True:
        await poll_pending_replacements()
        await asyncio.sleep(interval_minutes * 60)