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

        # Get all pending or queued replacements
        pending = conn.execute("""
            SELECT * FROM pending_replacements
            WHERE status IN ('pending', 'queued')
        """).fetchall()

        if not pending:
            logger.info("No pending replacements to poll")
            return

        logger.info(f"Polling {len(pending)} pending replacements...")

        for record in pending:
            record_id = record["id"]
            movie_id = record["movie_id"]
            movie_title = record["movie_title"]
            created_at = datetime.fromisoformat(record["created_at"])
            fail_count = record["fail_count"] or 0

            # Check if too old
            age = datetime.utcnow() - created_at
            if age > timedelta(hours=MAX_AGE_HOURS):
                logger.warning(
                    f"Replacement for '{movie_title}' expired after 24h, marking failed"
                )
                conn.execute("""
                    UPDATE pending_replacements
                    SET status = 'failed', completed_at = ?
                    WHERE id = ?
                """, (datetime.utcnow(), record_id))
                conn.commit()
                continue

            # Check if too many failures
            if fail_count >= MAX_FAIL_COUNT:
                logger.warning(
                    f"Replacement for '{movie_title}' failed {fail_count} times, cancelling"
                )
                conn.execute("""
                    UPDATE pending_replacements
                    SET status = 'failed', completed_at = ?
                    WHERE id = ?
                """, (datetime.utcnow(), record_id))
                conn.commit()
                continue

            try:
                # Fetch current movie state from Radarr
                movie = await client.get_movie(movie_id)
                movie_file = movie.get("movieFile")

                if not movie_file:
                    logger.info(
                        f"No file found for '{movie_title}', may still be downloading"
                    )
                    conn.execute("""
                        UPDATE pending_replacements
                        SET fail_count = fail_count + 1
                        WHERE id = ?
                    """, (record_id,))
                    conn.commit()
                    continue

                # Get current size
                current_size_gb = movie_file.get("size", 0) / (1024 ** 3)
                original_size_gb = record["current_size_gb"]

                # Check if size has changed meaningfully (> 1% difference)
                size_changed = abs(current_size_gb - original_size_gb) / max(original_size_gb, 0.001) > 0.01

                if size_changed:
                    logger.info(
                        f"Replacement completed for '{movie_title}': "
                        f"{original_size_gb:.2f}GB → {current_size_gb:.2f}GB"
                    )
                    conn.execute("""
                        UPDATE pending_replacements
                        SET status = 'completed', completed_at = ?, found_size_gb = ?
                        WHERE id = ?
                    """, (datetime.utcnow(), current_size_gb, record_id))
                else:
                    logger.info(
                        f"No change yet for '{movie_title}' "
                        f"({current_size_gb:.2f}GB)"
                    )
                    conn.execute("""
                        UPDATE pending_replacements
                        SET fail_count = fail_count + 1
                        WHERE id = ?
                    """, (record_id,))

                conn.commit()

            except Exception as e:
                logger.error(
                    f"Poll failed for '{movie_title}': {e}"
                )
                conn.execute("""
                    UPDATE pending_replacements
                    SET fail_count = fail_count + 1
                    WHERE id = ?
                """, (record_id,))
                conn.commit()

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


async def start_poller(interval_minutes: int = 5):
    """Run the poller on a loop at the given interval."""
    logger.info(f"Poller started with {interval_minutes} minute interval")
    while True:
        await poll_pending_replacements()
        await asyncio.sleep(interval_minutes * 60)