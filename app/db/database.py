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
    """Create all tables if they don't exist yet + run migrations."""
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS completed_jobs (
            id INTEGER PRIMARY KEY,
            movie_id INTEGER,
            movie_title TEXT,
            movie_year INTEGER,
            current_size_gb REAL,
            current_quality TEXT,
            found_size_gb REAL,
            found_quality TEXT,
            mode TEXT,
            status TEXT,
            completed_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
            min_quality_threshold TEXT,
            trigger_logic TEXT CHECK(trigger_logic IN ('auto', 'manual', 'quality_match')),
            min_peers INTEGER DEFAULT 0,
            language TEXT DEFAULT 'Any',
            operation_delay_seconds INTEGER DEFAULT 3,   -- NEW
            folder_pattern TEXT                          -- NEW
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
            release_guid TEXT,
            download_url TEXT
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

    # === MIGRATIONS (run every startup - safe if columns already exist) ===

    # Migration: Add pending_approval column to run_history (your existing one)
    cursor.execute("PRAGMA table_info(run_history)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'pending_approval' not in columns:
        cursor.execute("ALTER TABLE run_history ADD COLUMN pending_approval INTEGER DEFAULT 0")
        print("Added 'pending_approval' column to existing run_history table")

    # NEW MIGRATIONS - these will run automatically
    cursor.execute("PRAGMA table_info(rules)")
    columns = [row[1] for row in cursor.fetchall()]

    if 'operation_delay_seconds' not in columns:
        cursor.execute("ALTER TABLE rules ADD COLUMN operation_delay_seconds INTEGER DEFAULT 3")
        print("Added 'operation_delay_seconds' column to rules table (default: 3 seconds)")

    if 'folder_pattern' not in columns:
        cursor.execute("ALTER TABLE rules ADD COLUMN folder_pattern TEXT")
        print("Added 'folder_pattern' column to rules table")

    # ========== ADD THIS MIGRATION HERE ==========
    # Migration: Add min_quality_threshold column to rules (replaces min_quality_profile_id)
    if 'min_quality_threshold' not in columns:
        cursor.execute("ALTER TABLE rules ADD COLUMN min_quality_threshold TEXT")
        print("Added 'min_quality_threshold' column to rules table")
        
        # If there was existing min_quality_profile_id data, we could migrate it
        # For now, just set to NULL
        cursor.execute("UPDATE rules SET min_quality_threshold = NULL")
    # ========== END MIGRATION ==========

    # Migration for pending_replacements missing columns
    cursor.execute("PRAGMA table_info(pending_replacements)")
    pending_columns = [row[1] for row in cursor.fetchall()]
    
    if 'download_url' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN download_url TEXT")
        print("Added 'download_url' column to pending_replacements table")
    
    if 'mode' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN mode TEXT DEFAULT 'manual'")
        print("Added 'mode' column to pending_replacements table")

    conn.commit()
    conn.close()
    print("Database initialized successfully.")