from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import asyncio  # ADD THIS
import re
from urllib.parse import urlparse

from app.db.database import get_connection
from app.core.radarr_client import RadarrClient
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger()


class ApproveInput(BaseModel):
    override_quality: Optional[bool] = False


class BatchApproveInput(BaseModel):
    ids: list[int]
    override_quality: Optional[bool] = False


@router.get("/pending")
async def get_pending(page: int = 1, per_page: int = 20):
    """Get paginated pending replacements."""
    offset = (page - 1) * per_page

    conn = get_connection()

    total = conn.execute("""
        SELECT COUNT(*) FROM pending_replacements
        WHERE status = 'pending'
    """).fetchone()[0]

    records = conn.execute("""
        SELECT id, movie_title, movie_year, current_size_gb, current_quality,
           found_size_gb, found_quality, created_at, indexer, seeders, release_title, tmdb_rating
        FROM pending_replacements
        WHERE status = 'pending'
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset)).fetchall()

    conn.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "records": [dict(r) for r in records]
    }


@router.post("/pending/{record_id}/approve")
async def approve_pending(record_id: int, data: ApproveInput):
    """Approve a single pending replacement."""
    conn = get_connection()
    record = conn.execute("""
        SELECT * FROM pending_replacements WHERE id = ?
    """, (record_id,)).fetchone()

    if record:
        record = dict(record)
    
    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="Record not found")
    
    if record["status"] != "pending":
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Record is not pending (status: {record['status']})"
        )
    
    if record["quality_downgrade"]:
        logger.info(f"Quality downgrade detected for '{record['movie_title']}', but proceeding anyway")
    
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    if not config or not config["radarr_url"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Radarr not configured")
    
    try:
        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])

        # Always delete existing file first
        logger.info(f"Deleting existing file for '{record['movie_title']}' before replacement")
        try:
            delete_result = await client.delete_movie_file_only(record["movie_id"])
            if delete_result["success"]:
                logger.info(f"Successfully deleted existing file")
                await asyncio.sleep(2)
            else:
                logger.warning(f"Could not delete file: {delete_result['message']}")
        except Exception as e:
            logger.warning(f"Error deleting file: {e}")
        
        # Download the specific release
        release_guid = record["release_guid"]
        if release_guid:
            logger.info(f"Downloading specific release for '{record['movie_title']}': {release_guid}")

            if release_guid.startswith("http"):
                # Universal torrent ID extraction (same as before)
                torrent_id = None
                proper_guid = None
                patterns = [r'torrentid=(\d+)', r'\.(\d+)$', r'/(\d+)(?:/|$)', r'id=(\d+)']
                for pattern in patterns:
                    match = re.search(pattern, release_guid)
                    if match:
                        torrent_id = match.group(1)
                        proper_guid = f"Prowlarr:{torrent_id}"
                        break

                if proper_guid:
                    stored_download_url = record.get("download_url")
                    await client.download_release_by_guid(
                        movie_id=record["movie_id"],
                        guid=proper_guid,
                        indexerId=1,
                        download_url=stored_download_url,
                        title=f"{record['movie_title']} 2025",
                        publish_date=datetime.utcnow().isoformat()
                    )
                else:
                    logger.info(f"Could not extract ID, falling back to generic search")
                    await client.trigger_movie_search([record["movie_id"]])
            else:
                # Already a proper GUID
                stored_download_url = record.get("download_url")
                await client.download_release_by_guid(
                    movie_id=record["movie_id"],
                    guid=release_guid,
                    indexerId=1,
                    download_url=stored_download_url,
                    title=f"{record['movie_title']} 2025",
                    publish_date=datetime.utcnow().isoformat()
                )
        else:
            await client.trigger_movie_search([record["movie_id"]])

        # Update status
        conn.execute("""
            UPDATE pending_replacements
            SET status = 'queued', queued_at = datetime('now')
            WHERE id = ?
        """, (record_id,))
        conn.commit()

         # Add to completed jobs
        await add_completed_job(
            movie_id=record["movie_id"],
            movie_title=record["movie_title"],
            movie_year=0,
            current_size_gb=record["current_size_gb"],
            current_quality=record["current_quality"],
            found_size_gb=record["found_size_gb"],
            found_quality=record["found_quality"],
            mode="manual",
            status="queued"
        )
        
        logger.info(f"Approved pending replacement for '{record['movie_title']}'")
        return {"success": True, "message": f"Approved: {record['movie_title']}"}
        
    except Exception as e:
        logger.error(f"Failed to approve replacement: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger replacement: {str(e)}")
    finally:
        conn.close()


@router.post("/pending/approve-batch")
async def approve_batch(data: BatchApproveInput):
    """Approve multiple pending replacements using the SAME logic as single approve."""
    if len(data.ids) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 approvals per batch")
    if not data.ids:
        raise HTTPException(status_code=400, detail="No IDs provided")

    conn = get_connection()
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    if not config or not config["radarr_url"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Radarr not configured")

    client = RadarrClient(config["radarr_url"], config["radarr_api_key"])

    approved = []
    failed = []

    for record_id in data.ids:
        record = conn.execute("""
            SELECT * FROM pending_replacements WHERE id = ?
        """, (record_id,)).fetchone()

        if not record or record["status"] != "pending":
            failed.append(record_id)
            continue

        record = dict(record)

        try:
            # Exact same safe workflow as single approve
            logger.info(f"Batch deleting existing file for '{record['movie_title']}'")
            await client.delete_movie_file_only(record["movie_id"])
            await asyncio.sleep(2)

            release_guid = record["release_guid"]
            download_url = record.get("download_url")

            if release_guid and release_guid.startswith("http"):
                torrent_id = None
                proper_guid = None
                patterns = [r'torrentid=(\d+)', r'\.(\d+)$', r'/(\d+)(?:/|$)', r'id=(\d+)']
                for pattern in patterns:
                    match = re.search(pattern, release_guid)
                    if match:
                        torrent_id = match.group(1)
                        proper_guid = f"Prowlarr:{torrent_id}"
                        break

                if proper_guid:
                    await client.download_release_by_guid(
                        movie_id=record["movie_id"],
                        guid=proper_guid,
                        indexerId=1,
                        download_url=download_url,
                        title=f"{record['movie_title']} 2025",
                        publish_date=datetime.utcnow().isoformat()
                    )
                else:
                    await client.trigger_movie_search([record["movie_id"]])
            else:
                await client.download_release_by_guid(
                    movie_id=record["movie_id"],
                    guid=release_guid,
                    indexerId=1,
                    download_url=download_url,
                    title=f"{record['movie_title']} 2025",
                    publish_date=datetime.utcnow().isoformat()
                )

            conn.execute("""
                UPDATE pending_replacements
                SET status = 'queued', queued_at = datetime('now')
                WHERE id = ?
            """, (record_id,))
            conn.commit()

             # Add to completed jobs
            await add_completed_job(
                movie_id=record["movie_id"],
                movie_title=record["movie_title"],
                movie_year=0,
                current_size_gb=record["current_size_gb"],
                current_quality=record["current_quality"],
                found_size_gb=record["found_size_gb"],
                found_quality=record["found_quality"],
                mode="manual",
                status="queued"
            )

            approved.append(record_id)
            logger.info(f"Batch approved: '{record['movie_title']}'")

        except Exception as e:
            logger.error(f"Batch approve failed for {record_id}: {e}")
            failed.append(record_id)

    conn.close()

    return {
        "success": True,
        "approved": len(approved),
        "failed": len(failed),
        "approved_ids": approved,
        "failed_ids": failed
    }


@router.delete("/pending/clear-list")
async def clear_pending_list():
    """Clear all pending replacement records from the list."""
    conn = get_connection()
    
    result = conn.execute("SELECT COUNT(*) FROM pending_replacements WHERE status = 'pending'")
    count = result.fetchone()[0]
    
    conn.execute("DELETE FROM pending_replacements WHERE status = 'pending'")
    
    # Optional: reset stuck queued records
    conn.execute("""
        UPDATE pending_replacements 
        SET status = 'pending', queued_at = NULL 
        WHERE status = 'queued'
    """)
    
    conn.commit()
    conn.close()
    
    logger.info(f"Cleared {count} pending records from list")
    return {"success": True, "count": count}


@router.delete("/pending/{record_id}")
async def delete_pending(record_id: int):
    """Delete/reject a pending replacement."""
    conn = get_connection()

    record = conn.execute("""
        SELECT * FROM pending_replacements WHERE id = ?
    """, (record_id,)).fetchone()

    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="Record not found")

    # Add to completed jobs as rejected
    await add_completed_job(
        movie_id=record["movie_id"],
        movie_title=record["movie_title"],
        movie_year=0,
        current_size_gb=record["current_size_gb"],
        current_quality=record["current_quality"],
        found_size_gb=record["found_size_gb"],
        found_quality=record["found_quality"],
        mode="rejected",
        status="rejected"
    )

    conn.execute(
        "DELETE FROM pending_replacements WHERE id = ?",
        (record_id,)
    )
    conn.commit()
    conn.close()

    logger.info(f"Deleted pending replacement: '{record['movie_title']}'")
    return {"success": True, "message": f"Deleted: {record['movie_title']}"}

# ========== COMPLETED JOBS ENDPOINTS ==========
@router.get("/completed")
async def get_completed(page: int = 1, per_page: int = 20):
    """Get paginated completed jobs."""
    offset = (page - 1) * per_page
    
    conn = get_connection()
    
    total = conn.execute("SELECT COUNT(*) FROM completed_jobs").fetchone()[0]
    
    records = conn.execute("""
        SELECT * FROM completed_jobs
        ORDER BY completed_at DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset)).fetchall()
    
    conn.close()
    
    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "records": [dict(r) for r in records]
    }

@router.delete("/completed/clear")
async def clear_completed():
    """Clear all completed jobs."""
    conn = get_connection()
    result = conn.execute("SELECT COUNT(*) FROM completed_jobs")
    count = result.fetchone()[0]
    conn.execute("DELETE FROM completed_jobs")
    conn.commit()
    conn.close()
    logger.info(f"Cleared {count} completed job records")
    return {"success": True, "count": count}


async def add_completed_job(movie_id: int, movie_title: str, movie_year: int,
                            current_size_gb: float, current_quality: str,
                            found_size_gb: float, found_quality: str,
                            mode: str, status: str):
    """Add a job to completed jobs table."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO completed_jobs
        (movie_id, movie_title, movie_year, current_size_gb, current_quality,
         found_size_gb, found_quality, mode, status, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (movie_id, movie_title, movie_year, current_size_gb, current_quality,
          found_size_gb, found_quality, mode, status))
    conn.commit()
    conn.close()
# ========== END COMPLETED JOBS ENDPOINTS ==========