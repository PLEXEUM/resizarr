from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Config:
    id: Optional[int]
    radarr_url: Optional[str]
    radarr_api_key: Optional[str]
    api_key: Optional[str]
    quality_profile_id: Optional[int]
    quality_profile_name: Optional[str]

@dataclass
class Rules:
    id: Optional[int]
    current_operator: Optional[str]
    current_size: Optional[float]
    current_unit: Optional[str]
    target_operator: Optional[str]
    target_size: Optional[float]
    target_unit: Optional[str]
    min_size: Optional[float]
    min_size_unit: Optional[str]
    excluded_extensions: Optional[str]  # JSON array as string
    quality_rule: Optional[str]
    min_quality_profile_id: Optional[int]
    trigger_logic: Optional[str]

@dataclass
class Settings:
    id: Optional[int]
    batch_size: int = 10
    cron_schedule: str = '0 2 * * *'
    poller_interval: int = 5
    log_level: str = 'Info'
    log_max_size_mb: int = 10
    log_max_files: int = 5

@dataclass
class PendingReplacement:
    id: Optional[int]
    movie_id: int
    movie_title: str
    current_size_gb: float
    current_quality: str
    found_size_gb: float
    found_quality: str
    quality_downgrade: bool = False
    status: str = 'pending'
    created_at: Optional[datetime] = None
    queued_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    fail_count: int = 0

@dataclass
class RunHistory:
    id: Optional[int]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    total_movies_processed: int
    candidates_found: int
    replacements_queued: int
    replacements_failed: int
    quality_skipped: int
    dry_run: bool
    mode: str

@dataclass
class RunState:
    id: Optional[int]
    last_processed_movie_id: Optional[int]
    last_run_date: Optional[datetime]
    remaining_candidates: Optional[int]

@dataclass
class QualityProfileCache:
    id: Optional[int]
    profile_id: int
    profile_name: str
    profile_rank: int
    last_updated: Optional[datetime]