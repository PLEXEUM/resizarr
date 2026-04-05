from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime  # Add this line
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
    
    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="Record not found")
    
    if record["status"] != "pending":
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Record is not pending (status: {record['status']})"
        )
    
    # Check quality downgrade without override
    if record["quality_downgrade"] and not data.override_quality:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Quality downgrade detected. Set override_quality=true to approve anyway."
        )
    
    # Get Radarr config
    config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
    if not config or not config["radarr_url"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Radarr not configured")
    
    try:
        client = RadarrClient(config["radarr_url"], config["radarr_api_key"])
        
        # If we have a specific release GUID, download it directly
        release_guid = record["release_guid"]
        if release_guid:
            logger.info(f"Downloading specific release for '{record['movie_title']}': {release_guid}")
    
            # Check if it's a URL or a GUID
            if release_guid.startswith("http"):
                # It's a torrent URL, extract the torrent ID
                import re
                match = re.search(r'torrentid=(\d+)', release_guid)
                if match:
                    torrent_id = match.group(1)
                    proper_guid = f"Prowlarr:{torrent_id}"
                    
                    logger.info(f"Extracted torrent ID: {torrent_id}, using GUID: {proper_guid}")
                    
                    # Let Radarr handle the download using GUID and indexerId (no download_url needed)
                    await client.download_release_by_guid(
                        movie_id=record["movie_id"],
                        guid=proper_guid,
                        indexerId=1,
                        title=record["movie_title"],
                        publish_date=datetime.utcnow().isoformat()
                    )
                else:
                    logger.info(f"Could not extract ID from URL, falling back to generic search")
                    await client.trigger_movie_search([record["movie_id"]])
            else:
                # It's already a GUID, use the GUID method
                await client.download_release_by_guid(
                    movie_id=record["movie_id"],
                    guid=release_guid,
                    indexerId=1,
                    title=record["movie_title"],
                    publish_date=datetime.utcnow().isoformat()
                )
        else:
            # Fall back to generic search
            logger.info(f"No specific release GUID, triggering generic search for '{record['movie_title']}'")
            await client.trigger_movie_search([record["movie_id"]])
        
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