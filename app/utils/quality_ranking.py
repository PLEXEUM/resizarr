"""Quality ranking system for media file comparisons."""

# Quality ranking scores (higher = better)
# Scores are based on resolution and source quality
QUALITY_RANKING = {
    # ========== 4K / 2160p ==========
    "4K": 100,
    "2160p": 100,
    "Bluray-2160p": 100,
    "BluRay-2160p": 100,
    "Bluray-4K": 100,
    "BluRay-4K": 100,
    "Remux-2160p": 100,
    "Remux-4K": 100,
    "WEBDL-2160p": 95,
    "WEB-DL-2160p": 95,
    "WEBRip-2160p": 90,
    "WEB-Rip-2160p": 90,
    "HDTV-2160p": 85,
    "HDTV-4K": 85,
    
    # ========== 1080p ==========
    "Remux-1080p": 85,
    "Bluray-1080p": 80,
    "BluRay-1080p": 80,
    "Bluray-1080i": 75,
    "WEBDL-1080p": 75,
    "WEB-DL-1080p": 75,
    "WEBRip-1080p": 70,
    "WEB-Rip-1080p": 70,
    "HDTV-1080p": 65,
    "HDTV-1080i": 65,
    
    # ========== 720p ==========
    "Remux-720p": 65,
    "Bluray-720p": 60,
    "BluRay-720p": 60,
    "WEBDL-720p": 55,
    "WEB-DL-720p": 55,
    "WEBRip-720p": 50,
    "WEB-Rip-720p": 50,
    "HDTV-720p": 45,
    
    # ========== 576p / PAL ==========
    "Bluray-576p": 40,
    "BluRay-576p": 40,
    "WEBDL-576p": 38,
    "WEB-DL-576p": 38,
    "WEBRip-576p": 35,
    "WEB-Rip-576p": 35,
    "DVD": 35,
    "DVD-Rip": 33,
    "DVDRip": 33,
    "DVD5": 33,
    "DVD9": 33,
    
    # ========== 480p / SD ==========
    "Bluray-480p": 30,
    "BluRay-480p": 30,
    "WEBDL-480p": 28,
    "WEB-DL-480p": 28,
    "WEBRip-480p": 25,
    "WEB-Rip-480p": 25,
    "SDTV": 20,
    "SD": 20,
    "480p": 25,
    
    # ========== Lower quality / unknown ==========
    "TELESYNC": 5,
    "TS": 5,
    "CAM": 5,
    "Cam": 5,
    "Unknown": 0,
}

# Source type ranking (for when resolution is the same)
SOURCE_RANKING = {
    "Remux": 11,
    "Bluray": 10,
    "BluRay": 10,
    "WEBDL": 8,
    "WEB-DL": 8,
    "WEBRip": 6,
    "WEB-Rip": 6,
    "HDTV": 5,
    "DVD": 4,
    "DVDRip": 3,
    "SDTV": 2,
    "TELESYNC": 1,
    "CAM": 0,
}

# Quality resolution mapping for threshold checking
# Maps quality names to their resolution value
QUALITY_RESOLUTION = {
    "4K": 2160,
    "2160p": 2160,
    "Bluray-2160p": 2160,
    "BluRay-2160p": 2160,
    "WEBDL-2160p": 2160,
    "WEB-DL-2160p": 2160,
    "WEBRip-2160p": 2160,
    "WEB-Rip-2160p": 2160,
    "HDTV-2160p": 2160,
    "1080p": 1080,
    "Bluray-1080p": 1080,
    "BluRay-1080p": 1080,
    "WEBDL-1080p": 1080,
    "WEB-DL-1080p": 1080,
    "WEBRip-1080p": 1080,
    "WEB-Rip-1080p": 1080,
    "HDTV-1080p": 1080,
    "720p": 720,
    "Bluray-720p": 720,
    "BluRay-720p": 720,
    "WEBDL-720p": 720,
    "WEB-DL-720p": 720,
    "WEBRip-720p": 720,
    "WEB-Rip-720p": 720,
    "HDTV-720p": 720,
    "576p": 576,
    "Bluray-576p": 576,
    "BluRay-576p": 576,
    "WEBDL-576p": 576,
    "WEB-DL-576p": 576,
    "WEBRip-576p": 576,
    "WEB-Rip-576p": 576,
    "DVD": 480,
    "DVD-Rip": 480,
    "DVDRip": 480,
    "480p": 480,
    "Bluray-480p": 480,
    "BluRay-480p": 480,
    "WEBDL-480p": 480,
    "WEB-DL-480p": 480,
    "WEBRip-480p": 480,
    "WEB-Rip-480p": 480,
    "SDTV": 480,
    "SD": 480,
    "TELESYNC": 0,
    "CAM": 0,
    "Unknown": 0,
}


def get_quality_score(quality_name: str) -> int:
    """Get the numerical score for a quality name."""
    if not quality_name or quality_name == "Unknown":
        return 0
    
    # Try exact match first
    if quality_name in QUALITY_RANKING:
        return QUALITY_RANKING[quality_name]
    
    # Try case-insensitive match
    for key, score in QUALITY_RANKING.items():
        if quality_name.lower() == key.lower():
            return score
    
    # Try partial match
    quality_lower = quality_name.lower()
    if "2160p" in quality_lower or "4k" in quality_lower:
        return 100
    elif "1080p" in quality_lower or "1080i" in quality_lower:
        return 80
    elif "720p" in quality_lower:
        return 60
    elif "576p" in quality_lower:
        return 40
    elif "480p" in quality_lower or "dvd" in quality_lower or "sdtv" in quality_lower:
        return 30
    elif "sd" in quality_lower:
        return 20
    
    return 0


def get_source_score(quality_name: str) -> int:
    """Get the source score for a quality name (e.g., Bluray > WEBDL > WEBRip)."""
    if not quality_name:
        return 0
    
    quality_lower = quality_name.lower()
    for source, score in SOURCE_RANKING.items():
        if source.lower() in quality_lower:
            return score
    
    return 0


def get_quality_resolution(quality_name: str) -> int:
    """Get the resolution value for a quality name."""
    if not quality_name:
        return 0
    
    # Try exact match first
    if quality_name in QUALITY_RESOLUTION:
        return QUALITY_RESOLUTION[quality_name]
    
    # Try case-insensitive match
    for key, value in QUALITY_RESOLUTION.items():
        if quality_name.lower() == key.lower():
            return value
    
    # Try to extract from the name
    quality_lower = quality_name.lower()
    if "2160p" in quality_lower or "4k" in quality_lower:
        return 2160
    elif "1080p" in quality_lower or "1080i" in quality_lower:
        return 1080
    elif "720p" in quality_lower:
        return 720
    elif "576p" in quality_lower:
        return 576
    elif "480p" in quality_lower or "dvd" in quality_lower or "sdtv" in quality_lower:
        return 480
    elif "sd" in quality_lower:
        return 480
    
    return 0


def is_quality_equal_or_better(current_quality: str, found_quality: str) -> bool:
    """Check if found quality is equal to or better than current quality."""
    current_score = get_quality_score(current_quality)
    found_score = get_quality_score(found_quality)
    
    if found_score > current_score:
        return True
    elif found_score < current_score:
        return False
    
    # Same resolution, compare source type
    current_source = get_source_score(current_quality)
    found_source = get_source_score(found_quality)
    
    return found_source >= current_source


def is_quality_same(current_quality: str, found_quality: str) -> bool:
    """Check if qualities are the same (ignoring minor differences)."""
    current_score = get_quality_score(current_quality)
    found_score = get_quality_score(found_quality)
    return current_score == found_score   # ✅ Inside the function


def get_quality_order_descending() -> list:
    """Return qualities sorted by rank (highest to lowest / best to worst)."""
    quality_scores = {}
    for quality, score in QUALITY_RANKING.items():
        quality_scores[quality] = score
    
    # Sort by score descending (best first)
    sorted_qualities = sorted(quality_scores.items(), key=lambda x: x[1], reverse=True)
    return [q[0] for q in sorted_qualities]


def get_quality_order_with_scores_descending() -> list:
    """Return qualities with their scores, sorted highest to lowest (best to worst)."""
    quality_scores = {}
    for quality, score in QUALITY_RANKING.items():
        quality_scores[quality] = score
    
    sorted_qualities = sorted(quality_scores.items(), key=lambda x: x[1], reverse=True)
    return [{"name": q[0], "score": q[1]} for q in sorted_qualities]