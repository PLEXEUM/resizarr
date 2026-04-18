from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.db.database import get_connection
from app.utils.validators import validate_cron, validate_batch_size
from app.utils.logger import get_logger, setup_logger
from app.core.scheduler import update_schedule
from app.core.radarr_client import RadarrClient

router = APIRouter()
logger = get_logger()


class SettingsInput(BaseModel):
    batch_size: int = 10
    cron_schedule: str = "0 2 * * *"
    poller_interval: int = 5


class RadarrConfigInput(BaseModel):
    radarr_url: str
    radarr_api_key: str
    quality_profile_id: Optional[int] = None
    quality_profile_name: Optional[str] = None


@router.get("/settings")
async def get_settings():
    """Get current application settings (schedule only)."""
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
    """Save application settings (schedule only)."""
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


# ========== RADARR CONFIGURATION ENDPOINTS (moved from config.py) ==========

@router.get("/config")
async def get_config():
    """Get current Radarr configuration."""
    conn = get_connection()
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    conn.close()

    if not config:
        return {"configured": False}

    return {
        "configured": bool(config["radarr_url"]),
        "radarr_url": config["radarr_url"],
        "radarr_api_key": "[REDACTED]" if config["radarr_api_key"] else None,
        "quality_profile_id": config["quality_profile_id"],
        "quality_profile_name": config["quality_profile_name"]
    }


@router.post("/config")
async def save_config(data: RadarrConfigInput):
    """Save Radarr connection settings."""
    # Validate URL format
    if not data.radarr_url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")

    conn = get_connection()
    try:
        conn.execute("""
            UPDATE config SET
                radarr_url = ?,
                radarr_api_key = ?,
                quality_profile_id = ?,
                quality_profile_name = ?
            WHERE id = 1
        """, (
            data.radarr_url.rstrip("/"),
            data.radarr_api_key,
            data.quality_profile_id,
            data.quality_profile_name
        ))
        conn.commit()
        logger.info(f"Config saved for URL: {data.radarr_url}")
        return {"success": True, "message": "Configuration saved"}
    except Exception as e:
        logger.error(f"Failed to save config: {e}")
        raise HTTPException(status_code=500, detail="Failed to save configuration")
    finally:
        conn.close()


@router.post("/config/test")
async def test_connection(data: RadarrConfigInput):
    """Test connection to Radarr."""
    # Use the provided credentials or fetch from DB if not provided
    radarr_url = data.radarr_url
    radarr_api_key = data.radarr_api_key
    
    if not radarr_url or not radarr_api_key or radarr_api_key == "[REDACTED]":
        # Fetch from DB
        conn = get_connection()
        config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
        conn.close()
        if config and config["radarr_url"] and config["radarr_api_key"]:
            radarr_url = config["radarr_url"]
            radarr_api_key = config["radarr_api_key"]
        else:
            raise HTTPException(status_code=400, detail="No Radarr configuration found")
    
    client = RadarrClient(radarr_url, radarr_api_key)
    success, message = await client.test_connection(radarr_url, radarr_api_key)

    if not success:
        raise HTTPException(status_code=400, detail=message)

    return {"success": True, "message": message}


@router.get("/quality-profiles")
async def get_quality_profiles(refresh: bool = False):
    """Fetch quality profiles from Radarr."""
    conn = get_connection()
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    conn.close()
    
    if not config or not config["radarr_url"] or not config["radarr_api_key"]:
        raise HTTPException(status_code=400, detail="Radarr not configured. Please save your Radarr settings first.")
    
    client = RadarrClient(config["radarr_url"], config["radarr_api_key"])
    
    try:
        profiles = await client.get_quality_profiles(force_refresh=refresh)
        
        # Cache profiles in DB
        conn = get_connection()
        conn.execute("DELETE FROM quality_profiles_cache")
        for rank, profile in enumerate(profiles):
            conn.execute("""
                INSERT INTO quality_profiles_cache 
                (profile_id, profile_name, profile_rank, last_updated) 
                VALUES (?, ?, ?, datetime('now'))
            """, (profile.get("id"), profile.get("name"), rank))
        conn.commit()
        conn.close()
        
        return profiles
        
    except Exception as e:
        logger.error(f"Failed to fetch quality profiles: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch quality profiles: {str(e)}")