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
    min_quality_profile_id: Optional[int] = None
    trigger_logic: str = "manual"
    min_peers: Optional[int] = 2
    language: Optional[str] = "English"


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
        "min_quality_profile_id": rules["min_quality_profile_id"],
        "trigger_logic": rules["trigger_logic"]
    }


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
                    min_quality_profile_id = ?,
                    trigger_logic = ?,
                    min_peers = ?,
                    language = ?
                WHERE id = 1
            """, (
                data.current_operator, data.current_size, data.current_unit,
                data.target_operator, data.target_size, data.target_unit,
                data.min_size, data.min_size_unit,
                json.dumps(data.excluded_extensions),
                data.quality_rule, data.min_quality_profile_id,
                data.trigger_logic,
                data.min_peers,
                data.language
            ))
        else:
            conn.execute("""
                INSERT INTO rules
                (id, current_operator, current_size, current_unit,
                 target_operator, target_size, target_unit,
                 min_size, min_size_unit, excluded_extensions,
                 quality_rule, min_quality_profile_id, trigger_logic, min_peers, language)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.current_operator, data.current_size, data.current_unit,
                data.target_operator, data.target_size, data.target_unit,
                data.min_size, data.min_size_unit,
                json.dumps(data.excluded_extensions),
                data.quality_rule, data.min_quality_profile_id,
                data.trigger_logic,
                data.min_peers,
                data.language
            ))
        
        conn.commit()
        logger.info("Rules saved successfully")
        return {"success": True, "message": "Rules saved"}
        
    except Exception as e:
        logger.error(f"Failed to save rules: {e}")
        raise HTTPException(status_code=500, detail="Failed to save rules")
    finally:
        conn.close()