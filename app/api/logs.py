from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime
from app.db.database import get_connection
from app.utils.logger import get_logger, setup_logger

router = APIRouter()
logger = get_logger()

# ========== ADD THIS MISSING CLASS ==========
class LogSettingsInput(BaseModel):
    log_level: str = "INFO"
    log_max_size_mb: int = 10
    log_max_files: int = 5
# ========== END ADDED CLASS ==========

# Log directory path
LOG_DIR = Path("/app/logs")

def get_todays_log_path() -> Path:
    """Get today's dated log file path."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    return LOG_DIR / f"resizarr_{date_str}.log"

def get_all_log_files() -> list:
    """Get all dated log files sorted by date (newest first)."""
    if not LOG_DIR.exists():
        return []
    
    log_files = list(LOG_DIR.glob("resizarr_*"))
    # Sort by date extracted from filename (newest first)
    log_files.sort(reverse=True)
    return log_files


@router.get("/logs")
async def get_logs(lines: int = 100):
    """Return the last N lines of today's log file."""
    log_path = get_todays_log_path()
    
    if not log_path.exists():
        # Check if there are any log files
        all_logs = get_all_log_files()
        if all_logs:
            # Use the most recent log file
            log_path = all_logs[0]
            logger.info(f"No log for today, using most recent: {log_path.name}")
        else:
            return {"lines": [], "message": "No log file found yet", "total_lines": 0, "showing": 0}

    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return {
            "lines": [line.rstrip("\n") for line in last_lines],
            "total_lines": len(all_lines),
            "showing": len(last_lines),
            "log_file": log_path.name
        }
    except Exception as e:
        logger.error(f"Failed to read logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to read log file")


@router.get("/logs/download")
async def download_logs():
    """Download today's log file (or most recent if today doesn't exist)."""
    log_path = get_todays_log_path()
    
    if not log_path.exists():
        all_logs = get_all_log_files()
        if all_logs:
            log_path = all_logs[0]
        else:
            raise HTTPException(status_code=404, detail="No log file found")

    return FileResponse(
        path=log_path,
        media_type="text/plain",
        filename=log_path.name + ".log"  # Add .log for download only
    )


@router.delete("/logs")
async def clear_logs():
    """Clear today's log file (or all log files if specified)."""
    log_path = get_todays_log_path()
    
    try:
        if log_path.exists():
            with open(log_path, "w") as f:
                f.write("")
            logger.info(f"Cleared log file: {log_path.name}")
            return {"success": True, "message": f"Cleared {log_path.name}"}
        else:
            # No log file exists
            return {"success": True, "message": "No log file to clear"}
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