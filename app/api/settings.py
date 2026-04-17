from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.db.database import get_connection
from app.utils.validators import validate_cron, validate_batch_size
from app.utils.logger import get_logger, setup_logger
from app.core.scheduler import update_schedule

router = APIRouter()
logger = get_logger()


class SettingsInput(BaseModel):
    batch_size: int = 10
    cron_schedule: str = "0 2 * * *"
    poller_interval: int = 5


@router.get("/settings")
async def get_settings():
    """Get current application settings."""
    conn = get_connection()
    settings = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
    conn.close()

    if not settings:
        return {
            "batch_size": 10,
            "cron_schedule": "0 2 * * *",
            "poller_interval": 5
        }

    return {
        "batch_size": settings["batch_size"],
        "cron_schedule": settings["cron_schedule"],
        "poller_interval": settings["poller_interval"]
    }


@router.post("/settings")
async def save_settings(data: SettingsInput):
    """Save application settings."""
    # Validate batch size
    is_valid, error = validate_batch_size(data.batch_size)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)

    # Validate cron expression
    is_valid, error = validate_cron(data.cron_schedule)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)

    # Validate poller interval
    if data.poller_interval < 1 or data.poller_interval > 60:
        raise HTTPException(
            status_code=400,
            detail="Poller interval must be between 1 and 60 minutes"
        )

    conn = get_connection()
    try:
        conn.execute("""
            UPDATE settings SET
                batch_size = ?,
                cron_schedule = ?,
                poller_interval = ?
            WHERE id = 1
        """, (
            data.batch_size,
            data.cron_schedule,
            data.poller_interval
        ))
        conn.commit()

        # Apply new cron schedule immediately
        update_schedule(data.cron_schedule)

        logger.info("Settings saved and applied successfully")
        return {"success": True, "message": "Settings saved"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to save settings")
    finally:
        conn.close()