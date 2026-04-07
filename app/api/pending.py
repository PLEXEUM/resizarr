from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime  # Add this line
from urllib.parse import urlparse  # ADD THIS LINE
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
        SELECT * FROM pending_replacements
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

    # Convert sqlite3.Row to dict for easier access
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
    
    # Allow all approvals (ignore quality downgrade)
    if record["quality_downgrade"]:
        logger.info(f"Quality downgrade detected for '{record['movie_title']}', but proceeding anyway")
    
    # Get Radarr config
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    if not config or not config["radarr_url"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Radarr not configured")
    
    try:
        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])

        # Always delete existing file before replacement
        logger.info(f"Deleting existing file for '{record['movie_title']}' before replacement")
        try:
            delete_result = await client.delete_movie_file_only(record["movie_id"])
            if delete_result["success"]:
                logger.info(f"Successfully deleted existing file")
                import asyncio
                await asyncio.sleep(2)
            else:
                logger.warning(f"Could not delete file: {delete_result['message']}")
        except Exception as e:
            logger.warning(f"Error deleting file: {e}")
        
        # If we have a specific release GUID, download it directly
        release_guid = record["release_guid"]
        if release_guid:
            logger.info(f"Downloading specific release for '{record['movie_title']}': {release_guid}")

            # Check if it's a URL or a GUID
            if release_guid.startswith("http"):
                # It's a torrent URL, extract the torrent ID
                import re
                from urllib.parse import urlparse
        
                # Universal torrent ID extraction - tries multiple patterns
                torrent_id = None
                proper_guid = None
                
                # Pattern 1: torrentid=12345
                match = re.search(r'torrentid=(\d+)', release_guid)
                if match:
                    torrent_id = match.group(1)
                    proper_guid = f"Prowlarr:{torrent_id}"
                
                # Pattern 2: .123456 at end of URL (Beyond-HD, etc.)
                if not torrent_id:
                    match = re.search(r'\.(\d+)$', release_guid)
                    if match:
                        torrent_id = match.group(1)
                        proper_guid = f"Prowlarr:{torrent_id}"
                
                # Pattern 3: /123456/ or /123456 in path
                if not torrent_id:
                    match = re.search(r'/(\d+)(?:/|$)', release_guid)
                    if match:
                        torrent_id = match.group(1)
                        proper_guid = f"Prowlarr:{torrent_id}"
                
                # Pattern 4: id=12345
                if not torrent_id:
                    match = re.search(r'id=(\d+)', release_guid)
                    if match:
                        torrent_id = match.group(1)
                        proper_guid = f"Prowlarr:{torrent_id}"
                
                if torrent_id:
                    # Use the stored download_url from database (Prowlarr URL) - most reliable
                    stored_download_url = record.get("download_url")
                    
                    if stored_download_url:
                        # Use the Prowlarr URL from the database
                        final_download_url = stored_download_url
                        logger.info(f"Using stored Prowlarr URL")
                    else:
                        # Fallback: construct a download URL (less reliable)
                        parsed_url = urlparse(release_guid)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        
                        if 'torrents.php' in release_guid:
                            final_download_url = f"{base_url}/download.php?torrent={torrent_id}"
                        elif 'download' in release_guid:
                            final_download_url = release_guid
                        else:
                            final_download_url = release_guid.replace('torrents.php', 'download.php')
                            if 'id=' in final_download_url:
                                final_download_url = final_download_url.replace(f'id={torrent_id}', f'torrent={torrent_id}')
                        logger.info(f"No stored URL, using constructed URL")
                    
                    logger.info(f"Extracted torrent ID: {torrent_id}, using GUID: {proper_guid}")
                    logger.info(f"Final download URL: {final_download_url[:100]}...")
                    
                    await client.download_release_by_guid(
                        movie_id=record["movie_id"],
                        guid=proper_guid,
                        indexerId=1,
                        download_url=final_download_url,
                        title=f"{record['movie_title']} 2025",
                        publish_date=datetime.utcnow().isoformat()
                    )
                else:
                    logger.info(f"Could not extract ID from URL, falling back to generic search")
                    await client.trigger_movie_search([record["movie_id"]])
            else:
                # It's already a GUID, use the GUID method
                stored_download_url = record.get("download_url")
                await client.download_release_by_guid(
                    movie_id=record["movie_id"],
                    guid=release_guid,
                    indexerId=1,
                    download_url=stored_download_url,
                    title=f"{record['movie_title']} 2025",
                    publish_date=datetime.utcnow().isoformat()
                )
        # Update status
        conn.execute("""
            UPDATE pending_replacements
            SET status = 'queued', queued_at = datetime('now')
            WHERE id = ?
        """, (record_id,))
        conn.commit()
        
        logger.info(f"Approved pending replacement for '{record['movie_title']}'")
        return {"success": True, "message": f"Approved: {record['movie_title']}"}
        
    except Exception as e:
        logger.error(f"Failed to approve replacement: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger replacement: {str(e)}")
    finally:
        conn.close()

@router.post("/pending/approve-batch")
async def approve_batch(data: BatchApproveInput):
    """Approve multiple pending replacements (max 50)."""
    if len(data.ids) > 50:
        raise HTTPException(
            status_code=400,
            detail="Maximum 50 approvals per batch"
        )

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

        if record["quality_downgrade"] and not data.override_quality:
            failed.append(record_id)
            continue

        try:
            await client.trigger_movie_search([record["movie_id"]])
            conn.execute("""
                UPDATE pending_replacements
                SET status = 'queued', queued_at = datetime('now')
                WHERE id = ?
            """, (record_id,))
            conn.commit()
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

# ========== NEW CLEAR LIST ENDPOINT ==========
@router.delete("/pending/clear-list")
async def clear_pending_list():
    """Clear all pending replacement records from the list."""
    conn = get_connection()
    
    # Count before deleting
    result = conn.execute("SELECT COUNT(*) FROM pending_replacements WHERE status = 'pending'")
    count = result.fetchone()[0]
    
    # Delete all pending records
    conn.execute("DELETE FROM pending_replacements WHERE status = 'pending'")
    
    # OPTIONAL: Also reset stuck queued records (uncomment if desired)
    conn.execute("""
        UPDATE pending_replacements 
        SET status = 'pending', queued_at = NULL 
        WHERE status = 'queued'
    """)
    
    conn.commit()
    conn.close()
    
    logger.info(f"Cleared {count} pending records from list")
    return {"success": True, "count": count}
# ========== END CLEAR LIST ENDPOINT ==========

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

    conn.execute(
        "DELETE FROM pending_replacements WHERE id = ?",
        (record_id,)
    )
    conn.commit()
    conn.close()

    logger.info(f"Deleted pending replacement: '{record['movie_title']}'")
    return {"success": True, "message": f"Deleted: {record['movie_title']}"}