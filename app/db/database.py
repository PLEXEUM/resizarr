import sqlite3
import os
from pathlib import Path

# Path to the database file inside the config volume
DB_PATH = Path("/app/config/resizarr.db")

def get_connection():
    """Get a database connection with WAL mode enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets us access columns by name
    conn.execute("PRAGMA journal_mode=WAL")  # enables WAL mode for concurrent access
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    """Create all tables if they don't exist yet."""
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS config (
            id INTEGER PRIMARY KEY,
            radarr_url TEXT,
            radarr_api_key TEXT,
            api_key TEXT,
            quality_profile_id INTEGER,
            quality_profile_name TEXT
        );
                         
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY,
            current_operator TEXT CHECK(current_operator IN ('>', '<')),
            current_size REAL,
            current_unit TEXT CHECK(current_unit IN ('GB', 'MB')),
            target_operator TEXT CHECK(target_operator IN ('>', '<')),
            target_size REAL,
            target_unit TEXT CHECK(target_unit IN ('GB', 'MB')),
            min_size REAL,
            min_size_unit TEXT,
            excluded_extensions TEXT,
            quality_rule TEXT CHECK(quality_rule IN ('equal_or_better', 'any', 'same_only')),
            min_quality_profile_id INTEGER,
            trigger_logic TEXT CHECK(trigger_logic IN ('auto', 'manual', 'quality_match')),
            min_peers INTEGER DEFAULT 0,
            language TEXT DEFAULT 'Any'
        );

        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY,
            batch_size INTEGER DEFAULT 10,
            cron_schedule TEXT DEFAULT '0 2 * * *',
            poller_interval INTEGER DEFAULT 5,
            log_level TEXT DEFAULT 'Info',
            log_max_size_mb INTEGER DEFAULT 10,
            log_max_files INTEGER DEFAULT 5
        );

        CREATE TABLE IF NOT EXISTS pending_replacements (
            id INTEGER PRIMARY KEY,
            movie_id INTEGER,
            movie_title TEXT,
            current_size_gb REAL,
            current_quality TEXT,
            found_size_gb REAL,
            found_quality TEXT,
            quality_downgrade BOOLEAN DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            queued_at DATETIME,
            completed_at DATETIME,
            fail_count INTEGER DEFAULT 0,
            release_guid TEXT
        );

        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY,
            started_at DATETIME,
            completed_at DATETIME,
            total_movies_processed INTEGER,
            candidates_found INTEGER,
            replacements_queued INTEGER,
            replacements_failed INTEGER,
            quality_skipped INTEGER,
            pending_approval INTEGER DEFAULT 0,
            dry_run BOOLEAN,
            mode TEXT,
            csv_data TEXT
        );

        CREATE TABLE IF NOT EXISTS run_state (
            id INTEGER PRIMARY KEY,
            last_processed_movie_id INTEGER,
            last_run_date DATETIME,
            remaining_candidates INTEGER
        );

        CREATE TABLE IF NOT EXISTS quality_profiles_cache (
            id INTEGER PRIMARY KEY,
            profile_id INTEGER,
            profile_name TEXT,
            profile_rank INTEGER,
            last_updated DATETIME
        );
    """)

    # Migration: Add pending_approval column to run_history for existing databases
    cursor.execute("PRAGMA table_info(run_history)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'pending_approval' not in columns:
        cursor.execute("ALTER TABLE run_history ADD COLUMN pending_approval INTEGER DEFAULT 0")
        print("Added 'pending_approval' column to existing run_history table")

    conn.commit()
    conn.close()
    print("Database initialized successfully.")