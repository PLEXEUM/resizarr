from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import json
from app.db.database import get_connection
from app.utils.validators import validate_size_value
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger()


class RulesInput(BaseModel):
    current_operator: str
    current_size: float
    current_unit: str
    target_operator: str
    target_size: float
    target_unit: str
    min_size: Optional[float] = 100
    min_size_unit: Optional[str] = "MB"
    excluded_extensions: Optional[list] = []
    quality_rule: str = "equal_or_better"
    min_quality_threshold: Optional[str] = None  # ADD THIS NEW FIELD
    trigger_logic: str = "manual"
    min_peers: Optional[int] = 2
    language: Optional[str] = "English"
    # NEW FIELDS
    operation_delay_seconds: Optional[int] = 3
    folder_pattern: Optional[str] = None


@router.get("/rules")
async def get_rules():
    """Get current replacement rules."""
    conn = get_connection()
    rules = conn.execute("SELECT * FROM rules WHERE id = 1").fetchone()
    conn.close()

    if not rules:
        return {"configured": False}

     return {
        "configured": True,
        "current_operator": rules["current_operator"],
        "current_size": rules["current_size"],
        "current_unit": rules["current_unit"],
        "target_operator": rules["target_operator"],
        "target_size": rules["target_size"],
        "target_unit": rules["target_unit"],
        "min_size": rules["min_size"],
        "min_size_unit": rules["min_size_unit"],
        "excluded_extensions": json.loads(rules["excluded_extensions"] or "[]"),
        "quality_rule": rules["quality_rule"],
        "min_quality_threshold": rules["min_quality_threshold"],  # Changed
        "trigger_logic": rules["trigger_logic"],
        "min_peers": rules["min_peers"] if rules["min_peers"] is not None else 0,
        "language": rules["language"] if rules["language"] is not None else "Any",
        "operation_delay_seconds": rules["operation_delay_seconds"] or 3,
        "folder_pattern": rules["folder_pattern"] or ""
    }

# ========== ADD THIS NEW ENDPOINT HERE ==========
@router.get("/quality-types")
async def get_quality_types():
    """Get unique quality types from existing movie files."""
    from app.core.radarr_client import RadarrClient
    
    conn = get_connection()
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    conn.close()
    
    if not config or not config["radarr_url"] or not config["radarr_api_key"]:
        return {"quality_types": []}
    
    try:
        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])
        movies = await client.get_movies()
        
        quality_types = set()
        for movie in movies:
            movie_file = movie.get("movieFile")
            if movie_file:
                # Extract quality from file metadata (same logic as scanner.py)
                file_quality_wrapper = movie_file.get("quality", {})
                if isinstance(file_quality_wrapper, dict):
                    file_quality_obj = file_quality_wrapper.get("quality", {})
                    if isinstance(file_quality_obj, dict):
                        quality_name = file_quality_obj.get("name")
                        if quality_name:
                            quality_types.add(quality_name)
        
        # Add common quality types as fallbacks
        common_qualities = [
            "WEBDL-1080p", "WEBDL-720p", "WEBDL-480p",
            "BluRay-1080p", "BluRay-720p", "BluRay-480p",
            "WEBRip-1080p", "WEBRip-720p", "WEBRip-480p",
            "HDTV-1080p", "HDTV-720p", "HDTV-480p",
            "DVD", "SDTV"
        ]
        for q in common_qualities:
            quality_types.add(q)
        
        return {"quality_types": sorted(list(quality_types))}
        
    except Exception as e:
        logger.error(f"Failed to fetch quality types: {e}")
        return {"quality_types": []}
# ========== END NEW ENDPOINT ==========

@router.post("/rules")
async def save_rules(data: RulesInput):
    """Save replacement rules."""
    # Validate operators
    if data.current_operator not in (">", "<"):
        raise HTTPException(status_code=400, detail="Invalid current operator")
    if data.target_operator not in (">", "<"):
        raise HTTPException(status_code=400, detail="Invalid target operator")
    
    # Validate units
    if data.current_unit not in ("GB", "MB"):
        raise HTTPException(status_code=400, detail="Invalid current unit")
    if data.target_unit not in ("GB", "MB"):
        raise HTTPException(status_code=400, detail="Invalid target unit")
    
    # Validate quality rule
    if data.quality_rule not in ("equal_or_better", "any", "same_only"):
        raise HTTPException(status_code=400, detail="Invalid quality rule")
    
    # Validate trigger logic
    if data.trigger_logic not in ("auto", "manual", "quality_match"):
        raise HTTPException(status_code=400, detail="Invalid trigger logic")
    
    # Validate sizes
    is_valid, error = validate_size_value(data.current_size)
    if not is_valid:
        raise HTTPException(status_code=400, detail=f"Current size: {error}")
    is_valid, error = validate_size_value(data.target_size)
    if not is_valid:
        raise HTTPException(status_code=400, detail=f"Target size: {error}")
    
    # Validate peers
    if data.min_peers < 0 or data.min_peers > 100:
        raise HTTPException(status_code=400, detail="Min peers must be between 0 and 100")
    
    # Validate delay
    if data.operation_delay_seconds < 0 or data.operation_delay_seconds > 30:
        raise HTTPException(status_code=400, detail="Delay must be between 0 and 30 seconds")

    conn = get_connection()
    try:
        existing = conn.execute("SELECT id FROM rules WHERE id = 1").fetchone()
        
        if existing:
            conn.execute("""
                UPDATE rules SET
                    current_operator = ?,
                    current_size = ?,
                    current_unit = ?,
                    target_operator = ?,
                    target_size = ?,
                    target_unit = ?,
                    min_size = ?,
                    min_size_unit = ?,
                    excluded_extensions = ?,
                    quality_rule = ?,
                    min_quality_threshold = ?,
                    trigger_logic = ?,
                    min_peers = ?,
                    language = ?,
                    operation_delay_seconds = ?,
                    folder_pattern = ?
                WHERE id = 1
            """, (
                data.current_operator, data.current_size, data.current_unit,
                data.target_operator, data.target_size, data.target_unit,
                data.min_size, data.min_size_unit,
                json.dumps(data.excluded_extensions),
                data.quality_rule, data.min_quality_threshold,  # Changed
                data.trigger_logic, data.min_peers, data.language,
                data.operation_delay_seconds, data.folder_pattern
            ))
        else:
            conn.execute("""
                INSERT INTO rules
                (id, current_operator, current_size, current_unit,
                 target_operator, target_size, target_unit,
                 min_size, min_size_unit, excluded_extensions,
                 quality_rule, min_quality_threshold, trigger_logic,
                 min_peers, language, operation_delay_seconds, folder_pattern)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.current_operator, data.current_size, data.current_unit,
                data.target_operator, data.target_size, data.target_unit,
                data.min_size, data.min_size_unit,
                json.dumps(data.excluded_extensions),
                data.quality_rule, data.min_quality_threshold,  # Changed
                data.trigger_logic, data.min_peers, data.language,
                data.operation_delay_seconds, data.folder_pattern
            ))
        
        conn.commit()
        logger.info("Rules saved successfully")
        return {"success": True, "message": "Rules saved"}
        
    except Exception as e:
        logger.error(f"Failed to save rules: {e}")
        raise HTTPException(status_code=500, detail="Failed to save rules")
    finally:
        conn.close()