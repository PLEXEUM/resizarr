import asyncio
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.utils.logger import get_logger
from app.db.database import get_connection

logger = get_logger()

# Global scheduler instance
scheduler = AsyncIOScheduler()

# Track if a run is currently in progress
_run_in_progress = False
_run_started_at = None
RUN_TIMEOUT_SECONDS = 7200  # 2 hours


async def execute_run(dry_run: bool = False):
    """Execute a scanner run with concurrency and timeout protection."""
    global _run_in_progress, _run_started_at

    if _run_in_progress:
        logger.warning("Run already in progress, skipping scheduled run")
        return

    # Check for timeout on previous run
    if _run_started_at:
        elapsed = (datetime.utcnow() - _run_started_at).total_seconds()
        if elapsed > RUN_TIMEOUT_SECONDS:
            logger.error(f"Previous run timed out after {elapsed:.0f}s, resetting")
            _run_in_progress = False

    _run_in_progress = True
    _run_started_at = datetime.utcnow()

    try:
        # Import here to avoid circular imports
        from app.core.scanner import run_resizarr

        conn = get_connection()
        settings = conn.execute(
            "SELECT * FROM settings WHERE id = 1"
        ).fetchone()
        conn.close()

        batch_limit = settings["batch_size"] if settings else 10

        logger.info(
            f"Starting scheduled run "
            f"(batch_limit={batch_limit}, dry_run={dry_run})"
        )

        summary = await asyncio.wait_for(
            run_resizarr(dry_run=dry_run, batch_limit=batch_limit),
            timeout=RUN_TIMEOUT_SECONDS
        )

        logger.info(f"Scheduled run completed: {summary}")

    except asyncio.TimeoutError:
        logger.error("Run timed out after 2 hours, aborting")
    except Exception as e:
        logger.error(f"Scheduled run failed: {e}")
    finally:
        _run_in_progress = False
        _run_started_at = None


def update_schedule(cron_expression: str):
    """Update the cron schedule from DB settings."""
    try:
        # Remove existing job if present
        if scheduler.get_job("resizarr_run"):
            scheduler.remove_job("resizarr_run")

        # Validate and add new schedule
        trigger = CronTrigger.from_crontab(cron_expression)
        scheduler.add_job(
            execute_run,
            trigger=trigger,
            id="resizarr_run",
            name="Resizarr Scanner Run",
            misfire_grace_time=None  # skip missed runs
        )
        logger.info(f"Schedule updated: {cron_expression}")

    except Exception as e:
        logger.error(f"Invalid cron expression '{cron_expression}': {e}")
        raise ValueError(f"Invalid cron expression: {e}")


def get_next_run_time() -> str:
    """Get the next scheduled run time as a string."""
    job = scheduler.get_job("resizarr_run")
    if job and job.next_run_time:
        return job.next_run_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    return "Not scheduled"


def is_running() -> bool:
    """Check if a run is currently in progress."""
    return _run_in_progress

def set_running(state: bool):
    """Set the running state (called from API)."""
    global _run_in_progress, _run_started_at
    _run_in_progress = state
    if state:
        _run_started_at = datetime.utcnow()
    else:
        _run_started_at = None

def get_running_state() -> bool:
    """Get the current running state."""
    return _run_in_progress

def start_scheduler(cron_expression: str = "0 2 * * *"):
    """Start the scheduler with the given cron expression."""
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    update_schedule(cron_expression)

def stop_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")