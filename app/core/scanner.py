import json
import csv
import io
from typing import Optional
from datetime import datetime
from app.utils.logger import get_logger
from app.core.radarr_client import RadarrClient
from app.core.quality_checker import check_quality
from app.db.database import get_connection

logger = get_logger()

# Extensions to ignore when checking file sizes
IGNORED_EXTENSIONS = {'.nfo', '.jpg', '.png', '.srt', '.idx', '.sub', '.iso', '.exe'}

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

async def run_resizarr(
    dry_run: bool = False,
    batch_limit: int = 0,
    progress_callback=None
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
    
    conn = get_connection()
    
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
            
            # Check for existing replacement
            already_queued = await client.check_existing_replacement(movie_id)
            if already_queued and rules["trigger_logic"] == "auto":
                logger.info(f"Skipping {movie_title} - replacement already queued")
                continue

            # Search for available releases
            logger.info(f"Searching for alternatives for: {movie_title}")
            releases = await client.search_for_releases(movie_id)

            if not releases:
                logger.info(f"No releases found for: {movie_title}")
                continue

            # Parse target size in GB
            target_threshold_gb = size_to_gb(rules["target_size"], rules["target_unit"])

            # Filter releases by target size rule
            candidate_releases = []
            for release in releases:
                release_size_bytes = release.get("size", 0)
                release_size_gb = release_size_bytes / (1024 ** 3)
                
                # Check if release matches target size condition
                if matches_condition(release_size_gb, rules["target_operator"], target_threshold_gb):
                    release_quality = client.get_release_quality_name(release)
                    candidate_releases.append({
                        "release": release,
                        "size_gb": release_size_gb,
                        "quality": release_quality,
                        "guid": release.get("guid")
                    })
                    logger.debug(f"Found candidate release: {release.get('title')} ({release_size_gb:.2f} GB) - {release_quality}")

            if not candidate_releases:
                logger.info(f"No releases matching size criteria for: {movie_title}")
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

            is_allowed, is_downgrade, reason = check_quality(
                current_quality,
                found_quality,
                rules["quality_rule"],
                min_profile_name,
                profiles_cache    
            )
            
            # Dry run - just log and collect CSV data
            if dry_run:
                csv_rows.append({
                    "Movie": movie_title,
                    "Current Size (GB)": f"{size_gb:.2f}",
                    "Current Quality": current_quality,
                    "Found Size (GB)": f"{found_size_gb:.2f}",
                    "Found Quality": found_quality,
                    "Would Trigger": "Yes" if is_allowed else "No",
                    "Quality Decision": reason,
                    "Is Downgrade": "Yes" if is_downgrade else "No"
                })
                logger.info(f"[DRY RUN] {movie_title}: {reason} (Current: {size_gb:.2f}GB/{current_quality} → Found: {found_size_gb:.2f}GB/{found_quality})")
                continue
            
            # Quality blocked in auto mode
            if not is_allowed and rules["trigger_logic"] == "auto":
                logger.info(f"Quality skipped: {movie_title} - {reason}")
                summary["quality_skipped"] += 1
                continue
            
                        # Manual approval mode
            if rules["trigger_logic"] == "manual" or (is_downgrade and not is_allowed):
                try:
                    conn.execute("""
                        INSERT INTO pending_replacements
                        (movie_id, movie_title, current_size_gb, current_quality,
                         found_size_gb, found_quality, quality_downgrade, status, release_guid)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """, (
                        movie_id, movie_title, size_gb,
                        str(current_quality), found_size_gb,
                        str(found_quality), 1 if is_downgrade else 0,
                        best_candidate.get("guid")
                    ))
                    conn.commit()
                    summary["pending_approval"] += 1
                    logger.info(f"Added to pending approvals: {movie_title}")
                except Exception as e:
                    logger.error(f"Failed to add pending approval for {movie_title}: {e}")
                    summary["replacements_failed"] += 1
                continue
            
            # Auto queue mode - trigger search
            try:
                await client.trigger_movie_search([movie_id])
                summary["replacements_queued"] += 1
                logger.info(f"Triggered search for: {movie_title}")
            except Exception as e:
                logger.error(f"Failed to trigger search for {movie_title}: {e}")
                summary["replacements_failed"] += 1
            
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
            summary["csv_data"] = output.getvalue()
        
        # Save run to history
        completed_at = datetime.utcnow()
        summary["completed_at"] = completed_at.isoformat()
        conn.execute("""
            INSERT INTO run_history
            (started_at, completed_at, total_movies_processed, candidates_found,
             replacements_queued, replacements_failed, quality_skipped, dry_run, mode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            started_at, completed_at,
            summary["total_movies_processed"],
            summary["candidates_found"],
            summary["replacements_queued"],
            summary["replacements_failed"],
            summary["quality_skipped"],
            1 if dry_run else 0,
            "shrink" if rules["current_operator"] == ">" else "upgrade"
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
        conn.close()
    
    return summary