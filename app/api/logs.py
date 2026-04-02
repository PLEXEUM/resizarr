from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path
from app.db.database import get_connection
from app.utils.logger import get_logger, setup_logger

router = APIRouter()
logger = get_logger()

LOG_PATH = Path("/app/logs/resizarr.log")


class LogSettingsInput(BaseModel):
    log_level: str = "INFO"
    log_max_size_mb: int = 10
    log_max_files: int = 5


@router.get("/logs")
async def get_logs(lines: int = 100):
    """Return the last N lines of the log file."""
    if not LOG_PATH.exists():
        return {"lines": [], "message": "No log file found yet"}

    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return {
            "lines": [line.rstrip("\n") for line in last_lines],
            "total_lines": len(all_lines),
            "showing": len(last_lines)
        }
    except Exception as e:
        logger.error(f"Failed to read logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to read log file")


@router.get("/logs/download")
async def download_logs():
    """Download the full log file."""
    if not LOG_PATH.exists():
        raise HTTPException(status_code=404, detail="No log file found")

    return FileResponse(
        path=LOG_PATH,
        media_type="text/plain",
        filename="resizarr.log"
    )


@router.delete("/logs")
async def clear_logs():
    """Clear the log file."""
    try:
        if LOG_PATH.exists():
            with open(LOG_PATH, "w") as f:
                f.write("")
            logger.info("Log file cleared by user")
        return {"success": True, "message": "Logs cleared"}
    except Exception as e:
        logger.error(f"Failed to clear logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear logs")


@router.get("/logs/settings")
async def get_log_settings():
    """Get current log settings."""
    conn = get_connection()
    settings = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
    conn.close()

    if not settings:
        return {
            "log_level": "INFO",
            "log_max_size_mb": 10,
            "log_max_files": 5
        }

    return {
        "log_level": settings["log_level"],
        "log_max_size_mb": settings["log_max_size_mb"],
        "log_max_files": settings["log_max_files"]
    }


@router.post("/logs/settings")
async def save_log_settings(data: LogSettingsInput):
    """Save log settings."""
    if data.log_level.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        raise HTTPException(status_code=400, detail="Invalid log level")

    if data.log_max_size_mb < 1 or data.log_max_size_mb > 100:
        raise HTTPException(
            status_code=400,
            detail="Log max size must be between 1 and 100 MB"
        )

    if data.log_max_files < 1 or data.log_max_files > 20:
        raise HTTPException(
            status_code=400,
            detail="Log max files must be between 1 and 20"
        )

    conn = get_connection()
    try:
        conn.execute("""
            UPDATE settings SET
                log_level = ?,
                log_max_size_mb = ?,
                log_max_files = ?
            WHERE id = 1
        """, (
            data.log_level.upper(),
            data.log_max_size_mb,
            data.log_max_files
        ))
        conn.commit()

        # Apply immediately
        setup_logger(
            log_level=data.log_level,
            log_max_size_mb=data.log_max_size_mb,
            log_max_files=data.log_max_files
        )

        logger.info("Log settings updated")
        return {"success": True, "message": "Log settings saved"}
    except Exception as e:
        logger.error(f"Failed to save log settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to save log settings")
    finally:
        conn.close()