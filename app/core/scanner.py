import json
import csv
import io
import asyncio  # ADD THIS
from typing import Optional, Dict  # ADD Dict
from datetime import datetime
from app.utils.logger import get_logger
from app.core.radarr_client import RadarrClient
from app.core.quality_checker import check_quality
from app.db.database import get_connection

logger = get_logger()

# Extensions to ignore when checking file sizes
IGNORED_EXTENSIONS = {'.nfo', '.jpg', '.png', '.srt', '.idx', '.sub', '.iso', '.exe'}

# ========== ADD GLOBAL TRACKING FOR CANCELLATION ==========
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
# ========== END GLOBAL TRACKING ==========

def get_largest_file(movie: dict) -> Optional[dict]:
    """Get the largest movie file, ignoring excluded extensions."""
    movie_file = movie.get("movieFile")
    if not movie_file:
        return None
    
    # Check extension
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
    
    # If it's already in short format (contains colon but not http), return as-is
    if ':' in guid and not guid.startswith('http'):
        return guid
    
    # If it's a URL (starts with http), try to extract proper format
    if guid.startswith('http'):
        indexer = release.get('indexer', '')
        
        # Try to extract numeric ID from URL
        import re
        match = re.search(r'/(\d+)(?:\.|$)', guid)
        if match and indexer:
            torrent_id = match.group(1)
            proper_guid = f"{indexer}:{torrent_id}"
            logger.info(f"Converted URL GUID '{guid[:50]}...' to '{proper_guid}'")
            return proper_guid
    
    # Fallback to original GUID
    return guid

# ========== ADD TRACKING FUNCTIONS FOR CANCELLATION ==========
def get_active_run_id() -> Optional[str]:
    """Get the current active run ID"""
    return _active_run.get("run_id") if _active_run.get("is_running") else None

def get_run_progress_data() -> dict:
    """Get current run progress data"""
    return {
        "current": _active_run.get("current", 0),
        "total": _active_run.get("total", 0),
        "movie": _active_run.get("current_movie", ""),
        "cancelled": _active_run.get("cancelled", False),
        "completed": _active_run.get("completed", False)
    }

async def cancel_active_run(run_id: str) -> bool:
    """Cancel the currently active run"""
    global _active_run
    
    if not _active_run.get("is_running"):
        return False
    
    if _active_run.get("run_id") != run_id:
        return False
    
    # Set cancellation flag
    _active_run["cancelled"] = True
    
    # If there's a cancel event, set it
    cancel_event = _active_run.get("cancel_event")
    if cancel_event:
        cancel_event.set()
    
    return True
# ========== END TRACKING FUNCTIONS ==========

async def run_resizarr(
    dry_run: bool = False,
    batch_limit: int = 0,
    progress_callback=None,
    run_id: Optional[str] = None  # ADD THIS PARAMETER
) -> dict:
    """
    Main scanner function.
    Returns a summary dict of the run.
    """
    started_at = datetime.utcnow()
    summary = {
        "started_at": started_at.isoformat(),
        "dry_run": dry_run,
        "total_movies_processed": 0,
        "candidates_found": 0,
        "replacements_queued": 0,
        "replacements_failed": 0,
        "quality_skipped": 0,
        "pending_approval": 0,
        "csv_data": None
    }

     # ========== ADD INITIALIZATION FOR CANCELLATION ==========
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
    # ========== END INITIALIZATION ==========
    
    conn = get_connection()

    try:
        cursor = conn.execute("PRAGMA table_info(pending_replacements)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'download_url' not in columns:
            conn.execute("ALTER TABLE pending_replacements ADD COLUMN download_url TEXT")
            conn.commit()  # Make sure you have this line
            logger.info("Added missing 'download_url' column to pending_replacements table")
    except Exception as e:
        logger.warning(f"Could not verify/add download_url column: {e}")
    
    try:
        # Load config
        config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
        if not config or not config["radarr_url"] or not config["radarr_api_key"]:
            logger.error("Radarr not configured. Aborting run.")
            summary["error"] = "Radarr not configured"
            return summary
        
        # Load rules and convert to dict
        rules_row = conn.execute("SELECT * FROM rules WHERE id = 1").fetchone()
        if not rules_row:
            logger.error("No rules configured. Aborting run.")
            summary["error"] = "No rules configured"
            return summary
        
        # Convert rules to dict for easy access
        rules = dict(rules_row)
        
        # ========== ADD AUTO-CLEANUP OF STALE QUEUED RECORDS ==========
        try:
            result = conn.execute("""
                UPDATE pending_replacements 
                SET status = 'pending', queued_at = NULL 
                WHERE status = 'queued' AND queued_at < datetime('now', '-1 hour')
            """)
            if result.rowcount > 0:
                logger.info(f"Auto-cleaned {result.rowcount} stale queued records")
            conn.commit()
        except Exception as e:
            logger.warning(f"Could not auto-clean stale records: {e}")
        # ========== END AUTO-CLEANUP ==========
        
        # Load quality profiles cache
        profiles_cache = conn.execute(
            "SELECT * FROM quality_profiles_cache"
        ).fetchall()
        profiles_cache = [dict(p) for p in profiles_cache]
        
        # Load run state for batch resume
        run_state = conn.execute(
            "SELECT * FROM run_state WHERE id = 1"
        ).fetchone()
        last_processed_id = run_state["last_processed_movie_id"] if run_state else None
        
        # Initialize Radarr client
        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])
        
        # Fetch all movies
        logger.info("Fetching movies from Radarr...")
        movies = await client.get_movies()
        logger.info(f"Found {len(movies)} movies")
        
        # Parse rules
        current_threshold_gb = size_to_gb(rules["current_size"], rules["current_unit"])
        excluded_extensions = json.loads(rules["excluded_extensions"] or "[]")
        min_size_gb = size_to_gb(
            rules["min_size"] or 0,
            rules["min_size_unit"] or "GB"
        )
        
        # Filter candidates
        candidates = []
        resume_processing = last_processed_id is None
        
        # Get the selected quality profile from rules
        selected_quality_profile_id = rules.get("min_quality_profile_id")
        if selected_quality_profile_id:
            logger.info(f"Filtering movies by quality profile ID: {selected_quality_profile_id}")
        
        for movie in movies:
            movie_id = movie.get("id")
            
            # Handle batch resume
            if not resume_processing:
                if movie_id == last_processed_id:
                    resume_processing = True
                continue
            
            # Filter by quality profile if specified in rules
            if selected_quality_profile_id:
                movie_quality_id = movie.get("qualityProfileId")
                if movie_quality_id != selected_quality_profile_id:
                    logger.debug(f"Skipping movie '{movie.get('title')}' - quality profile ID {movie_quality_id} does not match selected {selected_quality_profile_id}")
                    continue
            
            movie_file = get_largest_file(movie)
            if not movie_file:
                continue
            
            # Get file size in GB
            size_bytes = movie_file.get("size", 0)
            size_gb = size_bytes / (1024 ** 3)
            
            # Apply minimum size filter
            if size_gb < min_size_gb:
                continue
            
            # Check extension
            path = movie_file.get("relativePath", "")
            ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
            if ext in excluded_extensions:
                continue
            
            # Check if matches condition
            if matches_condition(size_gb, rules["current_operator"], current_threshold_gb):
                candidates.append({
                    "movie": movie,
                    "movie_file": movie_file,
                    "size_gb": size_gb
                })
        
        logger.info(f"Found {len(candidates)} candidates matching condition")
        summary["candidates_found"] = len(candidates)
        
        # Apply batch limit
        if batch_limit > 0:
            candidates = candidates[:batch_limit]
            logger.info(f"Batch limit applied: processing {len(candidates)} candidates")
        
        # CSV rows for dry run
        csv_rows = []
        
        # Process each candidate
        for i, candidate in enumerate(candidates):
            # ========== ADD CANCELLATION CHECK ==========
            if cancel_event and cancel_event.is_set():
                logger.info(f"Run cancelled by user after processing {i} movies")
                summary["cancelled"] = True
                if run_id:
                    _active_run["cancelled"] = True
                break
            # ========== END CANCELLATION CHECK ==========

             # ========== UPDATE PROGRESS TRACKING ==========
            if run_id:
                _active_run["current"] = i + 1
                _active_run["total"] = len(candidates)
                _active_run["current_movie"] = candidate["movie"].get("title", "Unknown")
            # ========== END PROGRESS TRACKING ==========

            movie = candidate["movie"]
            movie_id = movie.get("id")
            movie_title = movie.get("title", "Unknown")
            size_gb = candidate["size_gb"]
            summary["total_movies_processed"] += 1
            
            # Emit progress
            if progress_callback:
                await progress_callback(i + 1, len(candidates), movie_title)
            
            logger.info(
                f"Processing ({i+1}/{len(candidates)}): "
                f"{movie_title} ({size_gb:.2f} GB)"
            )
            
            # Get current quality - fetch the actual profile name
            current_profile_id = movie.get("qualityProfileId")
            current_quality = "Unknown"
            if current_profile_id:
                for profile in profiles_cache:
                    if profile.get("profile_id") == current_profile_id:
                        current_quality = profile.get("profile_name", "Unknown")
                        break
            
            # Check for existing replacement - only skip if actively in Radarr queue
            already_queued = await client.check_existing_replacement(movie_id)
            
            # In auto mode, only skip if it's actively queued in Radarr
            if already_queued and rules["trigger_logic"] == "auto":
                logger.info(f"Skipping {movie_title} - replacement actively in Radarr queue")
                continue
            
            # If not actively queued, check for stale database record that needs cleanup
            if rules["trigger_logic"] == "auto" and not already_queued:
                conn.execute("""
                    UPDATE pending_replacements 
                    SET status = 'pending', queued_at = NULL 
                    WHERE movie_id = ? AND status = 'queued'
                """, (movie_id,))
                conn.commit()

            # Parse target size in GB
            target_threshold_gb = size_to_gb(rules["target_size"], rules["target_unit"])
            logger.info(f"DEBUG: target_threshold_gb = {target_threshold_gb}, type = {type(target_threshold_gb)}")
            
            # Get peer and language filters from rules
            min_peers = rules.get("min_peers", 0)
            preferred_language = rules.get("language", "Any")
            
            logger.info(f"DEBUG: rules['target_operator'] = '{rules['target_operator']}', rules['target_size'] = {rules['target_size']}, rules['target_unit'] = '{rules['target_unit']}'")
            logger.info(f"Target size threshold: {rules['target_operator']} {target_threshold_gb} GB")
            logger.info(f"Peer requirement: >= {min_peers} peers")
            logger.info(f"Language requirement: {preferred_language}")
            
            # Search for available releases
            logger.info(f"Searching for alternatives for: {movie_title}")
            releases = await client.search_for_releases(movie_id)

            # ========== FORCE DEBUG - Print first release GUID ==========
            if releases and len(releases) > 0:
                first_rel = releases[0]
                logger.info("=" * 80)
                logger.info("FIRST RELEASE RAW DATA:")
                logger.info(f"  GUID: {first_rel.get('guid')}")
                logger.info(f"  GUID raw: {first_rel.get('guid', 'NOT FOUND')}")
                logger.info(f"  Has 'guid' key: {'guid' in first_rel}")
                logger.info(f"  downloadUrl: {first_rel.get('downloadUrl', 'NOT FOUND')[:100] if first_rel.get('downloadUrl') else 'NOT FOUND'}")
                logger.info(f"  magnetUrl: {first_rel.get('magnetUrl', 'NOT FOUND')[:100] if first_rel.get('magnetUrl') else 'NOT FOUND'}")
                logger.info(f"  infoHash: {first_rel.get('infoHash', 'NOT FOUND')}")
                logger.info("=" * 80)
            # ========== END DEBUG ==========

            # ========== DEBUG: Dump entire first release ==========
            if releases and len(releases) > 0 and not hasattr(run_resizarr, '_dumped_release'):
                run_resizarr._dumped_release = True
                first_rel = releases[0]
                logger.info("=" * 80)
                logger.info("COMPLETE FIRST RELEASE OBJECT:")
                for key, value in first_rel.items():
                    if key in ['guid', 'downloadUrl', 'magnetUrl', 'infoHash']:
                        logger.info(f"  {key}: {value}")
                    elif key == 'size':
                        size_gb = value / (1024 ** 3)
                        logger.info(f"  {key}: {value} bytes ({size_gb:.2f} GB)")
                    else:
                        logger.info(f"  {key}: {value}")
                logger.info("=" * 80)
            # ========== END DEBUG ==========
            
            # ========== DEBUG: Log first release immediately ==========
            if releases and not hasattr(run_resizarr, '_releases_checked'):
                run_resizarr._releases_checked = True
                first_rel = releases[0]
                logger.info("=" * 80)
                logger.info("DEBUG - Raw Release Object (first):")
                logger.info(f"  All keys: {list(first_rel.keys())}")
                logger.info(f"  GUID value: {first_rel.get('guid')}")
                logger.info(f"  GUID exists: {'guid' in first_rel}")
                logger.info("=" * 80)
            # ========== END DEBUG ==========           
            
            logger.info(f"Found {len(releases)} total releases for {movie_title}")
            if releases:
                # Log ALL releases with their sizes
                all_sizes = []
                for rel in releases:
                    rel_size = rel.get("size", 0) / (1024 ** 3)
                    all_sizes.append(rel_size)
                    if rel_size < 5.0:  # Only log potentially interesting ones
                        logger.info(f"  POTENTIAL: {rel_size:.2f}GB - {rel.get('title', 'Unknown')[:80]}")
                logger.info(f"  Size stats: min={min(all_sizes):.2f}GB, max={max(all_sizes):.2f}GB, count={len(all_sizes)}")

            if not releases:
                logger.info(f"No releases found for: {movie_title}")
                continue

            # Log first few releases for debugging
            for idx, release in enumerate(releases[:5]):
                release_size_gb_debug = release.get("size", 0) / (1024 ** 3)
                peers_debug = release.get("seeders", 0) + release.get("leechers", 0)
                if peers_debug == 0:
                    peers_debug = release.get("peers", 0)
                release_lang = release.get("language", "Unknown")
                if isinstance(release_lang, dict):
                    release_lang = release_lang.get("name", "Unknown")
                logger.debug(f"Release {idx+1}: {release.get('title', 'Unknown')[:50]} - Size: {release_size_gb_debug:.2f}GB - Peers: {peers_debug} - Language: {release_lang}")
            
            # Filter releases by target size rule, peers, and language
            candidate_releases = []
            for release in releases:
                release_size_bytes = release.get("size", 0)
                release_size_gb = release_size_bytes / (1024 ** 3)
                
                # Get peer count (seeders + leechers)
                peers = release.get("seeders", 0) + release.get("leechers", 0)
                if peers == 0:
                    peers = release.get("peers", 0)
                if peers == 0:
                    peers = release.get("peerCount", 0)  # ADD THIS LINE
                
                # Get language - Radarr uses 'languages' array
                languages = release.get("languages", [])
                if languages and len(languages) > 0:
                    # Get the first language's name
                    first_lang = languages[0]
                    if isinstance(first_lang, dict):
                        release_language = first_lang.get("name", "Unknown")
                    else:
                        release_language = str(first_lang)
                else:
                    release_language = "Unknown"

                # ========== DEBUG: Log GUID for first release ==========
                if not hasattr(run_resizarr, '_guid_printed'):
                    run_resizarr._guid_printed = True
                    logger.info("=" * 80)
                    logger.info("DEBUG - First Release Details:")
                    logger.info(f"  GUID: {release.get('guid')}")
                    logger.info(f"  GUID type: {type(release.get('guid'))}")
                    logger.info(f"  downloadUrl: {release.get('downloadUrl')}")
                    logger.info(f"  infoHash: {release.get('infoHash')}")
                    logger.info(f"  magnetUrl: {release.get('magnetUrl')}")
                    logger.info(f"  indexerId: {release.get('indexerId')}")
                    logger.info(f"  indexer: {release.get('indexer')}")
                    logger.info("=" * 80)
                # ========== END DEBUG ==========

                # Check if release matches target size condition
                logger.info(f"COMPARE: {release_size_gb:.2f} {rules['target_operator']} {target_threshold_gb} = {matches_condition(release_size_gb, rules['target_operator'], target_threshold_gb)}")
                if matches_condition(release_size_gb, rules["target_operator"], target_threshold_gb):
                    logger.info(f"Size passed: {release_size_gb:.2f}GB, peers={peers}, lang={release_language}")
                    # Check peer requirement
                    if peers < min_peers:
                        logger.debug(f"Skipping release - insufficient peers ({peers} < {min_peers}) - {release.get('title')}")
                        continue
    
                    # Check language requirement (case-insensitive) - skip if language is "Any"
                    if preferred_language and preferred_language.lower() != "any" and preferred_language.lower() not in release_language.lower():
                        logger.debug(f"Skipping release - language mismatch ({release_language} != {preferred_language}) - {release.get('title')}") 
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
                    logger.debug(f"Found candidate release: {release.get('title')} ({release_size_gb:.2f} GB, {peers} peers, {release_language}) - {release_quality}")
            
            if not candidate_releases:
                logger.info(f"No releases matching size/peers/language criteria for: {movie_title}")
                continue

            # Sort candidates: prefer smaller size
            candidate_releases.sort(key=lambda x: x["size_gb"])

            # Get the best candidate (smallest)
            best_candidate = candidate_releases[0]
            found_size_gb = best_candidate["size_gb"]
            found_quality = best_candidate["quality"]

            logger.info(f"Best candidate for {movie_title}: {found_size_gb:.2f} GB, Quality: {found_quality}")

            # Get the minimum quality profile name if set
            min_profile_name = None
            if rules.get("min_quality_profile_id"):
                for profile in profiles_cache:
                    if profile.get("profile_id") == rules["min_quality_profile_id"]:
                        min_profile_name = profile.get("profile_name")
                        break

            # Check quality (for logging only)
            is_allowed, is_downgrade, reason = check_quality(
                current_quality,
                found_quality,
                rules["quality_rule"],
                min_profile_name,
                profiles_cache    
            )

            # Determine if we should proceed based on mode
            if rules["trigger_logic"] == "auto":
                # Auto mode: ignore quality, only care about size reduction
                should_proceed = True
                logger.info(f"[AUTO MODE] Quality check: {reason} - IGNORING for auto mode")
            else:
                # Manual mode: respect quality rules
                should_proceed = is_allowed

            # Dry run - just log and collect CSV data
            if dry_run:
                csv_rows.append({
                    "Movie": movie_title,
                    "Current Size (GB)": f"{size_gb:.2f}",
                    "Current Quality": current_quality,
                    "Found Size (GB)": f"{found_size_gb:.2f}",
                    "Found Quality": found_quality,
                    "Would Trigger": "Yes" if should_proceed else "No",
                    "Quality Decision": reason,
                    "Is Downgrade": "Yes" if is_downgrade else "No",
                    "Mode": rules["trigger_logic"]
                })
                logger.info(f"[DRY RUN] {movie_title}: {reason} (Mode: {rules['trigger_logic']}, Trigger: {should_proceed})")
                if not should_proceed:
                    continue

            # Manual mode - add ALL candidates to pending approvals for user review
            if rules["trigger_logic"] == "manual":
                try:
                    proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                    conn.execute("""
                        INSERT INTO pending_replacements
                        (movie_id, movie_title, current_size_gb, current_quality,
                        found_size_gb, found_quality, quality_downgrade, status, release_guid, download_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    """, (
                        movie_id, movie_title, size_gb,
                        str(current_quality), found_size_gb,
                        str(found_quality), 1 if is_downgrade else 0,
                        proper_guid,
                        best_candidate.get("download_url")
                    ))
                    conn.commit()
                    summary["pending_approval"] += 1
                    if is_allowed:
                        logger.info(f"Added to pending approvals: {movie_title} (quality allowed - {found_size_gb:.2f}GB, {found_quality})")
                    else:
                        logger.info(f"Added to pending approvals: {movie_title} (quality blocked - {reason})")
                except Exception as e:
                    logger.error(f"Failed to add pending approval for {movie_title}: {e}")
                    summary["replacements_failed"] += 1
                continue

            # Auto mode - trigger specific release immediately
            if rules["trigger_logic"] == "auto":
                try:
                    proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                    await client.trigger_specific_release(movie_id, proper_guid)
                    summary["replacements_queued"] += 1
                    logger.info(f"[AUTO MODE] Queued specific release for {movie_title}: {found_size_gb:.2f}GB, Quality: {found_quality}")
                except Exception as e:
                    logger.error(f"Failed to queue release for {movie_title}: {e}")
                    summary["replacements_failed"] += 1
                continue
            
            # Save last processed ID for resume
            conn.execute("""
                INSERT OR REPLACE INTO run_state (id, last_processed_movie_id, last_run_date)
                VALUES (1, ?, ?)
            """, (movie_id, datetime.utcnow()))
            conn.commit()
        
        # Generate CSV for dry run
        if dry_run and csv_rows:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
            csv_content = output.getvalue()
            summary["csv_data"] = csv_content
            # Don't save to disk automatically
        
        # Save run to history
        completed_at = datetime.utcnow()
        summary["completed_at"] = completed_at.isoformat()
        conn.execute("""
            INSERT INTO run_history
            (started_at, completed_at, total_movies_processed, candidates_found,
            replacements_queued, replacements_failed, quality_skipped, dry_run, mode, csv_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            started_at,
            completed_at,
            summary["total_movies_processed"],
            summary["candidates_found"],
            summary["replacements_queued"],
            summary["replacements_failed"],
            summary["quality_skipped"],
            1 if dry_run else 0,
            "shrink" if rules["current_operator"] == ">" else "upgrade",
            summary.get("csv_data")
        ))
        conn.commit()
        
        # Clear run state if completed fully
        if batch_limit == 0 or len(candidates) < batch_limit:
            conn.execute("DELETE FROM run_state WHERE id = 1")
            conn.commit()
        
        logger.info(
            f"Run complete: {summary['total_movies_processed']} processed, "
            f"{summary['replacements_queued']} queued, "
            f"{summary['quality_skipped']} quality skipped, "
            f"{summary['replacements_failed']} failed"
        )
        
    except ConnectionError as e:
        logger.error(f"Radarr connection lost during run: {e}")
        summary["error"] = str(e)
    except Exception as e:
        logger.error(f"Unexpected error during run: {e}")
        summary["error"] = str(e)
    finally:
        # ========== ADD CLEANUP FOR CANCELLATION ==========
        if run_id:
            _active_run["completed"] = not _active_run.get("cancelled", False)
            _active_run["is_running"] = False
        # ========== END CLEANUP ==========
        conn.close()
    
    return summary