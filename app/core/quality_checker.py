from typing import Optional
from app.utils.logger import get_logger

logger = get_logger()


def get_profile_rank(profile_id: int, profiles_cache: list) -> Optional[int]:
    """Get the rank of a quality profile by ID."""
    for profile in profiles_cache:
        if profile.get("id") == profile_id:
            return profile.get("profile_rank", 0)
    return None


def get_profile_by_name(name: str, profiles_cache: list) -> Optional[dict]:
    """Find a quality profile by name."""
    for profile in profiles_cache:
        if profile.get("profile_name", "").lower() == name.lower():
            return profile
    return None


def get_profile_by_id(profile_id: int, profiles_cache: list) -> Optional[dict]:
    """Find a quality profile by ID."""
    for profile in profiles_cache:
        if profile.get("profile_id") == profile_id:
            return profile
    return None


def check_quality(
    current_quality: str,
    found_quality: str,
    quality_rule: str,
    min_quality_profile_id: Optional[int],
    profiles_cache: list
) -> tuple[bool, bool, str]:
    """
    Check if a replacement meets quality requirements.

    Returns:
        (is_allowed, is_downgrade, reason)
    """
    # Find current and found profiles
    current_profile = get_profile_by_name(current_quality, profiles_cache)
    found_profile = get_profile_by_name(found_quality, profiles_cache)

    if not current_profile or not found_profile:
        logger.warning(
            f"Could not compare qualities: "
            f"current='{current_quality}' found='{found_quality}'"
        )
        # Allow if we can't determine - log warning
        return True, False, "Quality profiles not found in cache, allowing by default"

    current_rank = current_profile.get("profile_rank", 0)
    found_rank = found_profile.get("profile_rank", 0)
    is_downgrade = found_rank < current_rank

    # Check minimum quality threshold
    if min_quality_profile_id:
        min_profile = get_profile_by_id(min_quality_profile_id, profiles_cache)
        if min_profile:
            min_rank = min_profile.get("profile_rank", 0)
            if found_rank < min_rank:
                return (
                    False,
                    is_downgrade,
                    f"Found quality '{found_quality}' is below minimum threshold"
                )

    # Apply quality rule
    if quality_rule == "equal_or_better":
        if is_downgrade:
            return (
                False,
                True,
                f"Quality downgrade: '{current_quality}' → '{found_quality}'"
            )
        return True, False, "Quality is equal or better"

    elif quality_rule == "same_only":
        if current_rank != found_rank:
            return (
                False,
                is_downgrade,
                f"Quality mismatch: '{current_quality}' ≠ '{found_quality}'"
            )
        return True, False, "Quality matches exactly"

    elif quality_rule == "any":
        return True, is_downgrade, "Any quality allowed"

    return True, is_downgrade, "No quality rule matched"


def sort_profiles_by_rank(profiles: list) -> list:
    """Sort quality profiles by rank ascending."""
    return sorted(profiles, key=lambda p: p.get("profile_rank", 0))