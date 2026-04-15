from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
from app.db.database import get_connection
from app.utils.logger import get_logger
import json

router = APIRouter()
logger = get_logger()


@router.get("/backup")
async def export_backup():
    """Export all config and pending approvals as JSON."""
    conn = get_connection()

    try:
        config = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
        rules = conn.execute("SELECT * FROM rules WHERE id = 1").fetchone()
        settings = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
        pending = conn.execute("""
            SELECT * FROM pending_replacements
            WHERE status IN ('pending', 'queued')
        """).fetchall()

        backup_data = {
            "backup_version": "1.0",
            "exported_at": datetime.utcnow().isoformat(),
            "config": {
                "radarr_url": config["radarr_url"] if config else None,
                "quality_profile_id": config["quality_profile_id"] if config else None,
                "quality_profile_name": config["quality_profile_name"] if config else None
                # NOTE: API keys are intentionally excluded from backup
            },
            "rules": dict(rules) if rules else None,
            "settings": dict(settings) if settings else None,
            "pending_replacements": [dict(p) for p in pending]
        }

        return JSONResponse(
            content=backup_data,
            headers={
                "Content-Disposition": f"attachment; filename=resizarr_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            }
        )

    except Exception as e:
        logger.error(f"Failed to export backup: {e}")
        raise HTTPException(status_code=500, detail="Failed to export backup")
    finally:
        conn.close()


class RestoreInput(BaseModel):
    backup_version: str
    config: dict
    rules: dict = None
    settings: dict = None
    pending_replacements: list = []


@router.post("/restore")
async def import_backup(data: RestoreInput):
    """Import a JSON backup."""
    if data.backup_version != "1.0":
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported backup version: {data.backup_version}"
        )

    conn = get_connection()

    try:
        # Restore config (URL only, not API keys)
        if data.config:
            conn.execute("""
                UPDATE config SET
                    radarr_url = ?,
                    quality_profile_id = ?,
                    quality_profile_name = ?
                WHERE id = 1
            """, (
                data.config.get("radarr_url"),
                data.config.get("quality_profile_id"),
                data.config.get("quality_profile_name")
            ))

        # Restore rules
        if data.rules:
            conn.execute("""
                INSERT OR REPLACE INTO rules
                (id, current_operator, current_size, current_unit,
                 target_operator, target_size, target_unit,
                 min_size, min_size_unit, excluded_extensions,
                 quality_rule, min_quality_threshold, trigger_logic)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.rules.get("current_operator"),
                data.rules.get("current_size"),
                data.rules.get("current_unit"),
                data.rules.get("target_operator"),
                data.rules.get("target_size"),
                data.rules.get("target_unit"),
                data.rules.get("min_size"),
                data.rules.get("min_size_unit"),
                data.rules.get("excluded_extensions"),
                data.rules.get("quality_rule"),
                data.rules.get("min_quality_threshold"),
                data.rules.get("trigger_logic")
            ))

        # Restore settings
        if data.settings:
            conn.execute("""
                UPDATE settings SET
                    batch_size = ?,
                    cron_schedule = ?,
                    poller_interval = ?,
                    log_level = ?,
                    log_max_size_mb = ?,
                    log_max_files = ?
                WHERE id = 1
            """, (
                data.settings.get("batch_size", 10),
                data.settings.get("cron_schedule", "0 2 * * *"),
                data.settings.get("poller_interval", 5),
                data.settings.get("log_level", "INFO"),
                data.settings.get("log_max_size_mb", 10),
                data.settings.get("log_max_files", 5)
            ))

        # Restore pending replacements
        if data.pending_replacements:
            for record in data.pending_replacements:
                conn.execute("""
                    INSERT OR IGNORE INTO pending_replacements
                    (movie_id, movie_title, current_size_gb, current_quality,
                     found_size_gb, found_quality, quality_downgrade, status,
                     created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get("movie_id"),
                    record.get("movie_title"),
                    record.get("current_size_gb"),
                    record.get("current_quality"),
                    record.get("found_size_gb"),
                    record.get("found_quality"),
                    record.get("quality_downgrade", 0),
                    record.get("status", "pending"),
                    record.get("created_at")
                ))

        conn.commit()
        logger.info("Backup restored successfully")
        return {"success": True, "message": "Backup restored successfully"}

    except Exception as e:
        logger.error(f"Failed to restore backup: {e}")
        raise HTTPException(status_code=500, detail="Failed to restore backup")
    finally:
        conn.close()