from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.db.database import get_connection
from app.core.radarr_client import RadarrClient
from app.utils.validators import validate_url
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger()


class ConfigInput(BaseModel):
    radarr_url: str
    radarr_api_key: str
    quality_profile_id: Optional[int] = None
    quality_profile_name: Optional[str] = None


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
async def save_config(data: ConfigInput):
    """Save Radarr connection settings."""
    # Validate URL
    is_valid, error = validate_url(data.radarr_url)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)

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
async def test_connection(data: ConfigInput):
    """Test connection to Radarr."""
    client = RadarrClient(data.radarr_url, data.radarr_api_key)
    success, message = await client.test_connection(data.radarr_url, data.radarr_api_key)

    if not success:
        raise HTTPException(status_code=400, detail=message)

    return {"success": True, "message": message}


@router.get("/quality-profiles")
async def get_quality_profiles(refresh: bool = False):
    """Fetch quality profiles from Radarr."""
    conn = get_connection()
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    conn.close()

    if not config or not config["radarr_url"]:
        raise HTTPException(status_code=400, detail="Radarr not configured")

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

        return {"profiles": profiles}
    except Exception as e:
        logger.error(f"Failed to fetch quality profiles: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch quality profiles")