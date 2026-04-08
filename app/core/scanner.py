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
        "pending_approval": 0,
        "csv_data": None
    }

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
        # Auto-create download_url column if missing
        cursor = conn.execute("PRAGMA table_info(pending_replacements)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'download_url' not in columns:
            conn.execute("ALTER TABLE pending_replacements ADD COLUMN download_url TEXT")
            conn.commit()
            logger.info("Added missing 'download_url' column to pending_replacements table")

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
        selected_quality_profile_id = rules.get("min_quality_profile_id")

        candidates = []
        resume_processing = last_processed_id is None

        for movie in movies:
            movie_id = movie.get("id")
            if not resume_processing:
                if movie_id == last_processed_id:
                    resume_processing = True
                continue

            # Quality profile filter
            if selected_quality_profile_id and movie.get("qualityProfileId") != selected_quality_profile_id:
                continue

            # Folder pattern filter (new)
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

            current_quality = "Unknown"
            current_profile_id = movie.get("qualityProfileId")
            if current_profile_id:
                for profile in profiles_cache:
                    if profile.get("profile_id") == current_profile_id:
                        current_quality = profile.get("profile_name", "Unknown")
                        break

            already_queued = await client.check_existing_replacement(movie_id)
            if already_queued and rules["trigger_logic"] == "auto":
                logger.info(f"Skipping {movie_title} - replacement actively in Radarr queue")
                continue

            target_threshold_gb = size_to_gb(rules["target_size"], rules["target_unit"])
            min_peers = rules.get("min_peers", 0)
            preferred_language = rules.get("language", "Any")

            releases = await client.search_for_releases(movie_id)
            if not releases:
                logger.info(f"No releases found for: {movie_title}")
                continue

             # ========== DEBUG: Print quality info for first few releases ==========
            for idx, rel in enumerate(releases[:3]):
                logger.info(f"Release {idx+1} quality data: {rel.get('quality')}")
                logger.info(f"Release {idx+1} qualityId: {rel.get('qualityId')}")
                logger.info(f"Release {idx+1} title: {rel.get('title')}")
            # ========== END DEBUG ==========

            candidate_releases = []
            for release in releases:
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
                continue

            candidate_releases.sort(key=lambda x: x["size_gb"])
            best_candidate = candidate_releases[0]
            found_size_gb = best_candidate["size_gb"]
            found_quality = best_candidate["quality"]

            is_allowed, is_downgrade, reason = check_quality(
                current_quality, found_quality, rules["quality_rule"],
                None, profiles_cache
            )

            should_proceed = True if rules["trigger_logic"] == "auto" else is_allowed

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
                if not should_proceed:
                    continue

            if rules["trigger_logic"] == "manual":
                proper_guid = extract_proper_guid(best_candidate.get("release", {}))
                conn.execute("""
                    INSERT INTO pending_replacements
                    (movie_id, movie_title, current_size_gb, current_quality,
                     found_size_gb, found_quality, quality_downgrade, status,
                     release_guid, download_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """, (
                    movie_id, movie_title, size_gb, str(current_quality),
                    found_size_gb, str(found_quality), 1 if is_downgrade else 0,
                    proper_guid, best_candidate.get("download_url")
                ))
                conn.commit()
                summary["pending_approval"] += 1
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
            writer = csv.DictWriter(output, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
            summary["csv_data"] = output.getvalue()

        # Save run history
        completed_at = datetime.utcnow()
        summary["completed_at"] = completed_at.isoformat()
        conn.execute("""
            INSERT INTO run_history
            (started_at, completed_at, total_movies_processed, candidates_found,
             replacements_queued, replacements_failed, quality_skipped, pending_approval,
             dry_run, mode, csv_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            started_at, completed_at,
            summary["total_movies_processed"], summary["candidates_found"],
            summary["replacements_queued"], summary["replacements_failed"],
            summary["quality_skipped"], summary["pending_approval"],
            1 if dry_run else 0,
            "shrink" if rules["current_operator"] == ">" else "upgrade",
            summary.get("csv_data")
        ))
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