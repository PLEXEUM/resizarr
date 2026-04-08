from typing import Optional
from app.utils.logger import get_logger
from app.utils.quality_ranking import get_quality_score, get_source_score, is_quality_equal_or_better, is_quality_same

logger = get_logger()


def check_quality(
    current_quality: str,
    found_quality: str,
    quality_rule: str,
    min_quality_profile_id: Optional[int] = None,
    profiles_cache: list = None
) -> tuple:
    """
    Check if the found quality meets the quality requirements.
    
    Returns:
        tuple: (is_allowed, is_downgrade, reason)
    """
    # If quality rule is "any", always allow
    if quality_rule == "any":
        return True, False, "Any quality allowed"
    
    # Handle unknown qualities
    if current_quality == "Unknown" or found_quality == "Unknown":
        logger.warning(f"Could not compare qualities: current='{current_quality}' found='{found_quality}'")
        # In strict modes, unknown is not allowed
        if quality_rule == "same_only":
            return False, True, f"Unknown quality - cannot verify same quality"
        return False, True, f"Unknown quality - cannot verify equal or better"
    
    # Same quality only
    if quality_rule == "same_only":
        if is_quality_same(current_quality, found_quality):
            return True, False, f"Same quality: {current_quality} == {found_quality}"
        else:
            current_score = get_quality_score(current_quality)
            found_score = get_quality_score(found_quality)
            if current_score > found_score:
                return False, True, f"Quality downgrade: {current_quality} > {found_quality}"
            else:
                return False, True, f"Different quality: {current_quality} != {found_quality}"
    
    # Equal or better quality only
    if quality_rule == "equal_or_better":
        if is_quality_equal_or_better(current_quality, found_quality):
            return True, False, f"Quality OK: {found_quality} >= {current_quality}"
        else:
            return False, True, f"Quality downgrade: {found_quality} < {current_quality}"
    
    # Fallback
    return True, False, "No quality restriction"