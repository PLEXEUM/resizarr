import json
import csv
import io
import asyncio
import re
from typing import Optional, Dict
from datetime import datetime

from app.utils.logger import get_logger
from app.core.radarr_client import RadarrClient
from app.core.quality_checker import check_quality
from app.db.database import get_connection

logger = get_logger()

# Extensions to ignore when checking file sizes
IGNORED_EXTENSIONS = {'.nfo', '.jpg', '.png', '.srt', '.idx', '.sub', '.iso', '.exe'}

# Global tracking for cancellation
_active_run: Dict = {
    "is_running": False,
    "run_id": None,
    "cancel_event": None,
    "cancelled": False,
    "completed": False,
    "current": 0,
    "total": 0,
    "current_movie": ""
}

def get_largest_file(movie: dict) -> Optional[dict]:
    """Get the largest movie file, ignoring excluded extensions."""
    movie_file = movie.get("movieFile")
    if not movie_file:
        return None
    path = movie_file.get("relativePath", "")
    ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
    if ext in IGNORED_EXTENSIONS:
        return None
    return movie_file

def size_to_gb(size: float, unit: str) -> float:
    """Convert size to GB."""
    if unit == "MB":
        return size / 1024
    return size

def matches_condition(file_size_gb: float, operator: str, threshold_gb: float) -> bool:
    """Check if a file size matches the condition."""
    if operator == ">":
        return file_size_gb > threshold_gb
    elif operator == "<":
        return file_size_gb < threshold_gb
    return False

def extract_proper_guid(release: dict) -> str:
    """Extract the proper GUID format that Radarr expects for downloads."""
    guid = release.get('guid', '')
    if ':' in guid and not guid.startswith('http'):
        return guid
    if guid.startswith('http'):
        match = re.search(r'/(\d+)(?:\.|$)', guid)
        if match and release.get('indexer'):
            return f"{release.get('indexer')}:{match.group(1)}"
    return guid

def extract_quality_value(quality_name: str) -> int:
    """Extract resolution from quality name (e.g., 'BluRay-1080p' -> 1080, '4K' -> 2160)"""
    if not quality_name:
        return 0
    quality_lower = str(quality_name).lower()
    
    # Handle 4K/2160p
    if '4k' in quality_lower or '2160p' in quality_lower:
        return 2160
    # Handle 1080p
    if '1080p' in quality_lower:
        return 1080
    # Handle 720p
    if '720p' in quality_lower:
        return 720
    # Handle 480p/DVD
    if '480p' in quality_lower or 'dvd' in quality_lower:
        return 480
    
    # Generic pattern matching
    import re
    match = re.search(r'(\d+)p', quality_lower)
    return int(match.group(1)) if match else 0

def check_quality_threshold(found_quality: str, threshold: str) -> tuple:
    """Check if found quality meets or exceeds the minimum threshold.
    Returns (passed: bool, reason: str)
    """
    if not threshold or threshold == "":
        return True, "No threshold set"
    
    found_value = extract_quality_value(found_quality)
    threshold_value = extract_quality_value(threshold)
    
    if found_value == 0 or threshold_value == 0:
        # If we can't parse, assume it passes (don't block)
        return True, f"Could not parse quality values (found: {found_quality}, threshold: {threshold})"
    
    if found_value >= threshold_value:
        return True, f"Quality {found_quality} ({found_value}p) meets threshold {threshold} ({threshold_value}p)"
    else:
        return False, f"Quality {found_quality} ({found_value}p) below threshold {threshold} ({threshold_value}p)"

# Cancellation helpers
def get_active_run_id() -> Optional[str]:
    return _active_run.get("run_id") if _active_run.get("is_running") else None

def get_run_progress_data() -> dict:
    return {
        "current": _active_run.get("current", 0),
        "total": _active_run.get("total", 0),
        "movie": _active_run.get("current_movie", ""),
        "cancelled": _active_run.get("cancelled", False),
        "completed": _active_run.get("completed", False)
    }

async def cancel_active_run(run_id: str) -> bool:
    global _active_run
    if not _active_run.get("is_running") or _active_run.get("run_id") != run_id:
        return False
    _active_run["cancelled"] = True
    cancel_event = _active_run.get("cancel_event")
    if cancel_event:
        cancel_event.set()
    return True

async def run_resizarr(
    dry_run: bool = False,
    batch_limit: int = 0,
    progress_callback=None,
    run_id: Optional[str] = None
) -> dict:
    started_at = datetime.utcnow()
    summary = {
        "started_at": started_at.isoformat(),
        "dry_run": dry_run,
        "total_movies_processed": 0,
        "candidates_found": 0,
        "replacements_queued": 0,
        "replacements_failed": 0,
        "quality_skipped": 0,
        "no_releases_found": 0,
        "pending_approval": 0,
        "csv_data": None
    }

    # ========== ADD THESE TRACKING LISTS ==========
    # Track movies for non-dry run details
    quality_skipped_movies = []   # Store (movie_title, year, current_size, current_quality, found_size, found_quality, skip_reason)
    no_releases_movies = []       # Store (movie_title, year, current_size, current_quality)
    processed_movies = []         # Store (movie_title, year, current_size, current_quality)
    # ========== END TRACKING LISTS ==========

    global _active_run
    cancel_event = asyncio.Event() if run_id else None
    if run_id:
        _active_run["is_running"] = True
        _active_run["run_id"] = run_id
        _active_run["cancel_event"] = cancel_event
        _active_run["cancelled"] = False
        _active_run["completed"] = False
        _active_run["current"] = 0
        _active_run["total"] = 0
        _active_run["current_movie"] = ""

    conn = get_connection()
    try:
        # --- CLEAR EXISTING PENDING APPROVALS ---
        # Clear existing pending approvals before new run
        deleted_count = conn.execute("DELETE FROM pending_replacements WHERE status = 'pending'")
        conn.commit()
        logger.info(f"Cleared {deleted_count.rowcount} existing pending approvals before new run")
        # --- END CLEAR BLOCK ---
        
        # Auto-create download_url column if missing
        cursor = conn.execute("PRAGMA table_info(pending_replacements)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'download_url' not in columns:
            conn.execute("ALTER TABLE pending_replacements ADD COLUMN download_url TEXT")
            conn.commit()
            logger.info("Added missing 'download_url' column to pending_replacements table")

        if 'mode' not in columns:
            conn.execute("ALTER TABLE pending_replacements ADD COLUMN mode TEXT DEFAULT 'manual'")
            conn.commit()
            logger.info("Added missing 'mode' column to pending_replacements table")

        # Load config
        config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
        if not config or not config["radarr_url"] or not config["radarr_api_key"]:
            logger.error("Radarr not configured. Aborting run.")
            summary["error"] = "Radarr not configured"
            return summary

        # Load rules
        rules_row = conn.execute("SELECT * FROM rules WHERE id = 1").fetchone()
        if not rules_row:
            logger.error("No rules configured. Aborting run.")
            summary["error"] = "No rules configured"
            return summary
        rules = dict(rules_row)

        # New settings (Node.js-inspired improvements)
        delay_seconds = int(rules.get("operation_delay_seconds", 3))
        folder_pattern = rules.get("folder_pattern")

        # Auto-cleanup of stale queued records
        conn.execute("""
            UPDATE pending_replacements 
            SET status = 'pending', queued_at = NULL 
            WHERE status = 'queued' AND queued_at < datetime('now', '-1 hour')
        """)
        conn.commit()

        # Load caches
        profiles_cache = [dict(p) for p in conn.execute("SELECT * FROM quality_profiles_cache").fetchall()]
        run_state = conn.execute("SELECT * FROM run_state WHERE id = 1").fetchone()
        last_processed_id = run_state["last_processed_movie_id"] if run_state else None

        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])
        movies = await client.get_movies()
        logger.info(f"Found {len(movies)} movies")

        # Parse rules
        current_threshold_gb = size_to_gb(rules["current_size"], rules["current_unit"])
        excluded_extensions = json.loads(rules["excluded_extensions"] or "[]")
        min_size_gb = size_to_gb(rules.get("min_size") or 0, rules.get("min_size_unit") or "GB")

        # Insert empty run history record to get an ID
        conn.execute("""
                    INSERT INTO run_history
                    (started_at, total_movies_processed, candidates_found,
                    replacements_queued, replacements_failed, quality_skipped, no_releases_found,
                    pending_approval, dry_run, mode, csv_data)
                    VALUES (?, 0, 0, 0, 0, 0, 0, 0, ?, ?, NULL)
                """, (
                started_at,
                1 if dry_run else 0,
                "shrink" if rules["current_operator"] == ">" else "upgrade"
            ))
        
        # Get the run_id for this run
        run_id_from_db = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        logger.info(f"Created run history record with ID: {run_id_from_db}")

        candidates = []
        resume_processing = last_processed_id is None

        for movie in movies:
            movie_id = movie.get("id")
            if not resume_processing:
                if movie_id == last_processed_id:
                    resume_processing = True
                continue

            # Folder pattern filter
            if folder_pattern:
                movie_path = movie.get("path", "")
                if not re.search(folder_pattern, movie_path, re.IGNORECASE):
                    continue

            movie_file = get_largest_file(movie)
            if not movie_file:
                continue

            size_gb = movie_file.get("size", 0) / (1024 ** 3)
            if size_gb < min_size_gb:
                continue

            path = movie_file.get("relativePath", "")
            ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
            if ext in excluded_extensions:
                continue

            if matches_condition(size_gb, rules["current_operator"], current_threshold_gb):
                candidates.append({
                    "movie": movie,
                    "movie_file": movie_file,
                    "size_gb": size_gb
                })

        logger.info(f"Found {len(candidates)} candidates matching condition")
        summary["candidates_found"] = len(candidates)

        # Largest-first sorting (biggest space savings first)
        candidates.sort(key=lambda x: x["size_gb"], reverse=True)

        if batch_limit > 0:
            candidates = candidates[:batch_limit]

        csv_rows = []

        for i, candidate in enumerate(candidates):
            if cancel_event and cancel_event.is_set():
                logger.info(f"Run cancelled by user after processing {i} movies")
                summary["cancelled"] = True
                if run_id:
                    _active_run["cancelled"] = True
                break

            if run_id:
                _active_run["current"] = i + 1
                _active_run["total"] = len(candidates)
                _active_run["current_movie"] = candidate["movie"].get("title", "Unknown")

            movie = candidate["movie"]
            movie_id = movie.get("id")
            movie_title = movie.get("title", "Unknown")
            size_gb = candidate["size_gb"]
            summary["total_movies_processed"] += 1

            if progress_callback:
                await progress_callback(i + 1, len(candidates), movie_title)

            logger.info(f"Processing ({i+1}/{len(candidates)}): {movie_title} ({size_gb:.2f} GB)")

            # Try to get quality from the actual movie file first
            current_quality = "Unknown"
            movie_file = movie.get("movieFile")
            if movie_file:
                file_quality_wrapper = movie_file.get("quality", {})
                if isinstance(file_quality_wrapper, dict):
                    file_quality_obj = file_quality_wrapper.get("quality", {})
                    if isinstance(file_quality_obj, dict):
                        current_quality = file_quality_obj.get("name", "Unknown")
    
            # If still unknown, try to infer from filename
            if current_quality == "Unknown" and movie_file:
                filename = movie_file.get("relativePath", "")
                if "1080p" in filename.lower():
                    current_quality = "1080p"
                elif "720p" in filename.lower():
                    current_quality = "720p"
                elif "4k" in filename.lower() or "2160p" in filename.lower():
                    current_quality = "4K"

            # Fallback to quality profile if file quality not found
            if current_quality == "Unknown":
                current_profile_id = movie.get("qualityProfileId")
                if current_profile_id:
                    for profile in profiles_cache:
                        if profile.get("profile_id") == current_profile_id:
                            current_quality = profile.get("profile_name", "Unknown")
                            break
            
            # Track processed movie for all runs
            processed_movies.append({
                'title': movie_title,
                'year': movie.get('year'),
                'current_size_gb': size_gb,
                'current_quality': current_quality
            })

            already_queued = await client.check_existing_replacement(movie_id)
            if already_queued and rules["trigger_logic"] == "auto":
                logger.info(f"Skipping {movie_title} - replacement actively in Radarr queue")
                continue

            target_threshold_gb = size_to_gb(rules["target_size"], rules["target_unit"])
            min_peers = rules.get("min_peers", 0)
            preferred_language = rules.get("language", "Any")

            releases = await client.search_for_releases(movie_id)

            # === SIMPLE TITLE + YEAR MATCHING ===
            filter_title = movie.get("title", "").lower()
            movie_year = str(movie.get("year", ""))
            original_count = len(releases)

            # Filter by title (regex word boundary for short titles)
            if len(filter_title) <= 3:
                pattern = r'\b' + re.escape(filter_title) + r'\b'
                releases = [r for r in releases if re.search(pattern, r.get('title', ''), re.IGNORECASE)]
            else:
                releases = [r for r in releases if filter_title in r.get('title', '').lower()]

            # Filter by year (if available)
            if movie_year:
                releases = [r for r in releases if movie_year in r.get('title', '')]

            if original_count != len(releases):
                logger.info(f"Title+year filter: kept {len(releases)} of {original_count} releases for '{filter_title} ({movie_year})'")
            
            # Filter for valid releases (has title AND size > 0)
            valid_releases = [r for r in releases if r.get('title') and r.get('size', 0) > 0]

            # Initialize variables for CSV logging
            candidate_releases = []
            found_size_gb = None
            found_quality = None
            is_downgrade = False
            reason = ""
            should_proceed = False

            if not valid_releases:
                summary["no_releases_found"] += 1
                no_releases_movies.append({
                    'title': movie_title,
                    'year': movie.get('year'),
                    'current_size_gb': size_gb,
                    'current_quality': current_quality
                })
                logger.info(f"No valid releases found for: {movie_title}")
            else:
                # Build candidate releases
                for release in valid_releases:
                    release_size_gb = release.get("size", 0) / (1024 ** 3)
                    peers = (release.get("seeders", 0) + release.get("leechers", 0) or
                        release.get("peers", 0) or release.get("peerCount", 0))

                    languages = release.get("languages", [])
                    release_language = (languages[0].get("name", "Unknown")
                                        if languages and isinstance(languages[0], dict) else "Unknown")

                    if matches_condition(release_size_gb, rules["target_operator"], target_threshold_gb):
                        if peers < min_peers:
                            continue
                        if preferred_language.lower() != "any" and preferred_language.lower() not in release_language.lower():
                            continue

                        release_quality = client.get_release_quality_name(release)
                        candidate_releases.append({
                            "release": release,
                            "size_gb": release_size_gb,
                            "quality": release_quality,
                            "guid": release.get("guid"),
                            "download_url": release.get("downloadUrl") or release.get("magnetUrl"),
                            "peers": peers,
                            "language": release_language
                        })

                if not candidate_releases:
                    summary["quality_skipped"] += 1
                    # Find the smallest release from valid_releases
                    smallest_release = min(valid_releases, key=lambda r: r.get('size', 0)) if valid_releases else None
                    smallest_release_size = (smallest_release.get("size", 0) / (1024 ** 3)) if smallest_release else None
                    smallest_release_quality = client.get_release_quality_name(smallest_release) if smallest_release else None

                    # Get closest release details
                    closest_peers = None
                    closest_language = None
                    if smallest_release:
                        closest_peers = (smallest_release.get("seeders", 0) + smallest_release.get("leechers", 0) or 
                                        smallest_release.get("peers", 0))
                        languages = smallest_release.get("languages", [])
                        closest_language = (languages[0].get("name", "Unknown") if languages and isinstance(languages[0], dict) else "Unknown")

                    # Build filter status - ONLY show failures (✗)
                    failed_filters = []
    
                    # Size check on closest release
                    if smallest_release_size:
                        size_passes = matches_condition(smallest_release_size, rules["target_operator"], target_threshold_gb)
                        if not size_passes:
                            size_needs = f"{rules['target_operator']}{rules['target_size']}{rules['target_unit']}"
                            failed_filters.append(f"size (needs {size_needs}) found {smallest_release_size:.1f}GB ✗")
    
                    # Peers check on closest release
                    if min_peers > 0 and closest_peers is not None:
                        peers_passes = closest_peers >= min_peers
                        if not peers_passes:
                            failed_filters.append(f"peers (needs ≥{min_peers}) found {closest_peers} ✗")
    
                    # Language check on closest release
                    if preferred_language.lower() != "any" and closest_language:
                        lang_passes = preferred_language.lower() in closest_language.lower()
                        if not lang_passes:
                            failed_filters.append(f"language (needs '{preferred_language}') found '{closest_language}' ✗")
    
                    # Build the skip reason
                    if failed_filters:
                        skip_reason = f"No release passed: {' | '.join(failed_filters)}"
                    else:
                        # All filters passed on the closest release? That means candidate_releases should not be empty
                        # This shouldn't happen, but fallback just in case
                        skip_reason = f"No release passed all filters (closest: {smallest_release_size:.1f}GB)" if smallest_release_size else "No releases found"

                    quality_skipped_movies.append({
                        'title': movie_title,
                        'year': movie.get('year'),
                        'current_size_gb': size_gb,
                        'current_quality': current_quality,
                        'found_size_gb': smallest_release_size,
                        'found_quality': smallest_release_quality,
                        'skip_reason': skip_reason
                    })
                    logger.info(f"No suitable releases found for: {movie_title} - {skip_reason}")
                else:
                    logger.info(f"[CANDIDATE FOUND] {movie_title} has {len(candidate_releases)} candidate releases")
                    candidate_releases.sort(key=lambda x: x["size_gb"])
                    best_candidate = candidate_releases[0]
                    found_size_gb = best_candidate["size_gb"]
                    found_quality = best_candidate["quality"]

                    is_allowed, is_downgrade, reason = check_quality(
                        current_quality, found_quality, rules["quality_rule"]
                    )

                    # === THRESHOLD CHECK FOR ALL MODES ===
                    threshold_passed = True
                    threshold_reason = "No threshold set"
                    min_quality_threshold = rules.get("min_quality_threshold")
                    
                    if min_quality_threshold and min_quality_threshold != "":
                        threshold_passed, threshold_reason = check_quality_threshold(found_quality, min_quality_threshold)
                    
                    # If threshold fails, skip regardless of mode
                    if not threshold_passed:
                        should_proceed = False
                        combined_reason = f"{reason} | {threshold_reason}"
                        logger.info(f"[{rules['trigger_logic'].upper()}] Quality threshold failed: {threshold_reason} - Skipping")
                        summary["quality_skipped"] += 1
                        quality_skipped_movies.append({
                            'title': movie_title,
                            'year': movie.get('year'),
                            'current_size_gb': size_gb,
                            'current_quality': current_quality,
                            'found_size_gb': found_size_gb,
                            'found_quality': found_quality,
                            'skip_reason': combined_reason
                        })
                        continue  # Skip this movie entirely
                    
                    # === DETERMINE MODE-SPECIFIC BEHAVIOR ===
                    if rules["trigger_logic"] == "auto":
                        # Auto mode: queue automatically if quality rule passes
                        if is_allowed:
                            should_proceed = True
                            logger.info(f"[AUTO MODE] Quality check passed: {reason} - Will queue automatically")
                        else:
                            should_proceed = False
                            logger.info(f"[AUTO MODE] Quality check failed: {reason} - Skipping")
                            summary["quality_skipped"] += 1
                            quality_skipped_movies.append({
                                'title': movie_title,
                                'year': movie.get('year'),
                                'current_size_gb': size_gb,
                                'current_quality': current_quality,
                                'found_size_gb': found_size_gb,
                                'found_quality': found_quality,
                                'skip_reason': reason
                            })
                    elif rules["trigger_logic"] == "quality_match":
                        # Quality match mode: queue only if quality rule passes
                        should_proceed = is_allowed
                        if not should_proceed:
                            logger.info(f"[QUALITY MATCH] Quality check failed: {reason} - Skipping")
                            summary["quality_skipped"] += 1
                            quality_skipped_movies.append({
                                'title': movie_title,
                                'year': movie.get('year'),
                                'current_size_gb': size_gb,
                                'current_quality': current_quality,
                                'found_size_gb': found_size_gb,
                                'found_quality': found_quality,
                                'skip_reason': reason
                            })
                    else:  # Manual mode
                        should_proceed = is_allowed
                        if not should_proceed:
                            summary["quality_skipped"] += 1
                            quality_skipped_movies.append({
                                'title': movie_title,
                                'year': movie.get('year'),
                                'current_size_gb': size_gb,
                                'current_quality': current_quality,
                                'found_size_gb': found_size_gb,
                                'found_quality': found_quality,
                                'skip_reason': reason
                            })

            # ========== DRY RUN CSV LOGGING ==========
            if dry_run:
                # Determine outcome for CSV
                if not valid_releases:
                    outcome = "No Releases Found"
                    quality_decision = "No releases available"
                    found_size_display = "N/A"
                    found_quality_display = "N/A"
                    is_downgrade_display = "N/A"
                elif not candidate_releases:
                    outcome = "Quality Skipped"
                    quality_decision = "No releases matched size/peers/language filters"
                    found_size_display = "N/A"
                    found_quality_display = "N/A"
                    is_downgrade_display = "N/A"
                else:
                    found_size_display = f"{found_size_gb:.2f}"
                    found_quality_display = found_quality
                    is_downgrade_display = "Yes" if is_downgrade else "No"
                    
                    if not should_proceed:
                        outcome = "Quality Skipped"
                        quality_decision = reason
                    else:
                        outcome = "Pending Approval"
                        quality_decision = reason
                        # In dry-run, insert into pending_replacements for preview
                        proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                        release = best_candidate.get("release", {})
                        tmdb_rating = movie.get("ratings", {}).get("tmdb", {}).get("value") or movie.get("tmdbRating")
                        
                        conn.execute("""
                            INSERT INTO pending_replacements
                            (movie_id, movie_title, movie_year, current_size_gb, current_quality,
                             found_size_gb, found_quality, quality_downgrade, status,
                             release_guid, download_url, mode, indexer, seeders, release_title, tmdb_rating, run_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, 'dry_run', ?, ?, ?, ?, ?)
                        """, (
                            movie_id, movie_title, movie.get("year"), size_gb, str(current_quality),
                            found_size_gb, str(found_quality), 1 if is_downgrade else 0,
                            proper_guid, best_candidate.get("download_url"),
                            release.get("indexer"), best_candidate.get("peers", 0), release.get("title"), tmdb_rating,
                            run_id_from_db
                        ))
                        conn.commit()
                        summary["pending_approval"] += 1
                
                csv_rows.append({
                    "Movie": movie_title,
                    "Year": movie.get('year', 'N/A'),
                    "Current Size (GB)": f"{size_gb:.2f}",
                    "Current Quality": current_quality,
                    "Found Size (GB)": found_size_display,
                    "Found Quality": found_quality_display,
                    "Outcome": outcome,
                    "Quality Decision": quality_decision,
                    "Is Downgrade": is_downgrade_display,
                    "Mode": rules["trigger_logic"]
                })
                
                # Skip all actual operations in dry run
                continue
            # ========== END DRY RUN CSV LOGGING ==========

            # ========== ACTUAL OPERATIONS (ONLY for non-dry runs) ==========
            if not should_proceed:
                continue

            if rules["trigger_logic"] == "manual":
                proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                release = best_candidate.get("release", {})

                tmdb_rating = movie.get("ratings", {}).get("tmdb", {}).get("value") or movie.get("tmdbRating")

                conn.execute("""
                    INSERT INTO pending_replacements
                    (movie_id, movie_title, movie_year, current_size_gb, current_quality,
                     found_size_gb, found_quality, quality_downgrade, status,
                     release_guid, download_url, mode, indexer, seeders, release_title, tmdb_rating, run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, 'manual', ?, ?, ?, ?, ?)
                """, (
                    movie_id, movie_title, movie.get("year"), size_gb, str(current_quality),
                    found_size_gb, str(found_quality), 1 if is_downgrade else 0,
                    proper_guid, best_candidate.get("download_url"),
                    release.get("indexer"), best_candidate.get("peers", 0), release.get("title"), tmdb_rating,
                    run_id_from_db
                ))
                conn.commit()
                summary["pending_approval"] += 1
                continue

            # Quality match mode - auto queue if quality matches
            if rules["trigger_logic"] == "quality_match" and should_proceed:
                try:
                    proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                    download_url = best_candidate.get("download_url", "")
                    original_guid = best_candidate.get("release", {}).get("guid", "")
                    
                    if original_guid.startswith("http"):
                        torrent_id = None
                        match = re.search(r'torrentid=(\d+)', original_guid)
                        if match:
                            torrent_id = match.group(1)
                            proper_guid = f"Prowlarr:{torrent_id}"
                        else:
                            match = re.search(r'/(\d+)(?:/|$)', original_guid)
                            if match:
                                torrent_id = match.group(1)
                                proper_guid = f"Prowlarr:{torrent_id}"
                    
                    logger.info(f"Deleting existing file for '{movie_title}' before replacement")
                    await client.delete_movie_file_only(movie_id)
                    
                    await client.download_release_by_guid(
                        movie_id=movie_id,
                        guid=proper_guid,
                        indexerId=1,
                        download_url=download_url,
                        title=f"{movie_title} 2025",
                        publish_date=datetime.utcnow().isoformat()
                    )
                    
                    summary["replacements_queued"] += 1
                    logger.info(f"[QUALITY MATCH MODE] Queued release for {movie_title}: {found_size_gb:.2f}GB, Quality: {found_quality}")
                except Exception as e:
                    logger.error(f"Failed to queue release for {movie_title}: {e}")
                    summary["replacements_failed"] += 1
                continue

            # Auto mode
            if rules["trigger_logic"] == "auto":
                proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                download_url = best_candidate.get("download_url", "")

                logger.info(f"Deleting existing file for '{movie_title}' before replacement")
                await client.delete_movie_file_only(movie_id)

                await client.download_release_by_guid(
                    movie_id=movie_id,
                    guid=proper_guid,
                    indexerId=1,
                    download_url=download_url,
                    title=f"{movie_title} 2025",
                    publish_date=datetime.utcnow().isoformat()
                )
                summary["replacements_queued"] += 1
                logger.info(f"[AUTO MODE] Queued release for {movie_title}: {found_size_gb:.2f} GB")
            
            # Save resume point
            conn.execute("""
                INSERT OR REPLACE INTO run_state (id, last_processed_movie_id, last_run_date)
                VALUES (1, ?, ?)
            """, (movie_id, datetime.utcnow()))
            conn.commit()

            # Configurable delay between movies
            await asyncio.sleep(delay_seconds)
            logger.debug(f"Applied {delay_seconds}s delay before next movie")

        # Dry-run CSV
        if dry_run and csv_rows:
            output = io.StringIO()
            fieldnames = ['Movie', 'Year', 'Current Size (GB)', 'Current Quality', 
                         'Found Size (GB)', 'Found Quality', 'Outcome', 
                         'Quality Decision', 'Is Downgrade', 'Mode']
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
            summary["csv_data"] = output.getvalue()
        elif dry_run and not csv_rows:
            output = io.StringIO()
            fieldnames = ['Movie', 'Year', 'Current Size (GB)', 'Current Quality', 
                         'Found Size (GB)', 'Found Quality', 'Outcome', 
                         'Quality Decision', 'Is Downgrade', 'Mode']
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            summary["csv_data"] = output.getvalue()
            logger.info("Created header-only CSV for dry run (no movies to report)")

         # ========== UPDATE run history with final stats ==========
        completed_at = datetime.utcnow()
        summary["completed_at"] = completed_at.isoformat()
        
        conn.execute("""
                    UPDATE run_history 
                    SET completed_at = ?,
                        total_movies_processed = ?,
                        candidates_found = ?,
                        replacements_queued = ?,
                        replacements_failed = ?,
                        quality_skipped = ?,
                        no_releases_found = ?,
                        pending_approval = ?,
                        csv_data = ?
                    WHERE id = ?
                """, (
                    completed_at,
                    summary["total_movies_processed"],
                    summary["candidates_found"],
                    summary["replacements_queued"],
                    summary["replacements_failed"],
                    summary["quality_skipped"],
                    summary["no_releases_found"],
                    summary["pending_approval"],
                    summary.get("csv_data"),
                    run_id_from_db
                ))
        # ========== END RUN HISTORY UPDATE ==========

        # ========== SAVE TRACKING DETAILS FOR ALL RUNS (dry and real) ==========
        # Save quality skipped movies
        for movie in quality_skipped_movies:
            conn.execute("""
                INSERT INTO run_details
                (run_id, movie_title, movie_year, category, current_size_gb, current_quality,
                 found_size_gb, found_quality, skip_reason)
                VALUES (?, ?, ?, 'quality_skipped', ?, ?, ?, ?, ?)
            """, (
                run_id_from_db,
                movie['title'],
                movie.get('year'),
                movie['current_size_gb'],
                movie['current_quality'],
                movie.get('found_size_gb'),
                movie.get('found_quality'),
                movie['skip_reason']
             ))

        # Save no releases movies
        for movie in no_releases_movies:
            conn.execute("""
                INSERT INTO run_details
                (run_id, movie_title, movie_year, category, current_size_gb, current_quality)
                VALUES (?, ?, ?, 'no_releases', ?, ?)
            """, (
                run_id_from_db,
                movie['title'],
                movie.get('year'),
                movie['current_size_gb'],
                movie['current_quality']
            ))

        # Save processed movies (optional - can be large, maybe limit to 500)
        for movie in processed_movies[:500]:  # Limit to 500 to avoid huge inserts
            conn.execute("""
                INSERT INTO run_details
                (run_id, movie_title, movie_year, category, current_size_gb, current_quality)
                VALUES (?, ?, ?, 'processed', ?, ?)
            """, (
                run_id_from_db,
                movie['title'],
                movie.get('year'),
                movie['current_size_gb'],
                movie['current_quality']
            ))

        logger.info(f"Saved {len(quality_skipped_movies)} quality_skipped, {len(no_releases_movies)} no_releases, {len(processed_movies[:500])} processed movies to run_details")
        # ========== END TRACKING SAVE ==========
        conn.commit()

        if batch_limit == 0 or len(candidates) < batch_limit:
            conn.execute("DELETE FROM run_state WHERE id = 1")
            conn.commit()

        logger.info(f"Run complete: {summary['total_movies_processed']} processed, "
                    f"{summary['replacements_queued']} queued, "
                    f"{summary['pending_approval']} pending")

    except Exception as e:
        logger.error(f"Unexpected error during run: {e}")
        summary["error"] = str(e)
    finally:
        if run_id:
            _active_run["completed"] = not _active_run.get("cancelled", False)
            _active_run["is_running"] = False
        conn.close()

    return summary