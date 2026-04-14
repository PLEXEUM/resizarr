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
            completed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            indexer TEXT,
            seeders INTEGER DEFAULT 0,
            tmdb_rating REAL
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
            operation_delay_seconds INTEGER DEFAULT 3,
            folder_pattern TEXT
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
            download_url TEXT,
            movie_year INTEGER,
            indexer TEXT,
            seeders INTEGER DEFAULT 0,
            release_title TEXT,
            tmdb_rating REAL,
            mode TEXT DEFAULT 'manual'
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
            no_releases_found INTEGER DEFAULT 0,
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

        -- ========== NEW TABLE FOR TRACKING RUN DETAILS ==========
        CREATE TABLE IF NOT EXISTS run_details (
            id INTEGER PRIMARY KEY,
            run_id INTEGER,
            movie_id INTEGER,
            movie_title TEXT,
            movie_year INTEGER,
            category TEXT CHECK(category IN ('processed', 'quality_skipped', 'no_releases')),
            current_size_gb REAL,
            current_quality TEXT,
            found_size_gb REAL,
            found_quality TEXT,
            skip_reason TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES run_history(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_run_details_run_id ON run_details(run_id);
        CREATE INDEX IF NOT EXISTS idx_run_details_category ON run_details(category);
        -- ========== END NEW TABLE ==========
    """)

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

    # Migration: Add min_quality_threshold column to rules (replaces min_quality_profile_id)
    if 'min_quality_threshold' not in columns:
        cursor.execute("ALTER TABLE rules ADD COLUMN min_quality_threshold TEXT")
        print("Added 'min_quality_threshold' column to rules table")
        
        # If there was existing min_quality_profile_id data, we could migrate it
        # For now, just set to NULL
        cursor.execute("UPDATE rules SET min_quality_threshold = NULL")

    # Migration for pending_replacements missing columns
    cursor.execute("PRAGMA table_info(pending_replacements)")
    pending_columns = [row[1] for row in cursor.fetchall()]
    
    if 'download_url' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN download_url TEXT")
        print("Added 'download_url' column to pending_replacements table")
    
    if 'mode' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN mode TEXT DEFAULT 'manual'")
        print("Added 'mode' column to pending_replacements table")

    if 'movie_year' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN movie_year INTEGER")
        print("Added 'movie_year' column to pending_replacements table")
    
    if 'indexer' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN indexer TEXT")
        print("Added 'indexer' column to pending_replacements table")
    
    if 'seeders' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN seeders INTEGER DEFAULT 0")
        print("Added 'seeders' column to pending_replacements table")
    
    if 'release_title' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN release_title TEXT")
        print("Added 'release_title' column to pending_replacements table")

    if 'tmdb_rating' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN tmdb_rating REAL")
        print("Added 'tmdb_rating' column to pending_replacements table")

    # === NEW: no_releases_found column for run_history ===
    cursor.execute("PRAGMA table_info(run_history)")
    history_columns = [row[1] for row in cursor.fetchall()]
    if 'no_releases_found' not in history_columns:
        cursor.execute("ALTER TABLE run_history ADD COLUMN no_releases_found INTEGER DEFAULT 0")
        print("Added 'no_releases_found' column to run_history table")

    # Migration: Add run_id column to pending_replacements
    cursor.execute("PRAGMA table_info(pending_replacements)")
    pending_columns = [row[1] for row in cursor.fetchall()]
    if 'run_id' not in pending_columns:
        cursor.execute("ALTER TABLE pending_replacements ADD COLUMN run_id INTEGER")
        print("Added 'run_id' column to pending_replacements table")

    # Migration: Add run_details table for tracking movie details per run
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='run_details'")
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE run_details (
                id INTEGER PRIMARY KEY,
                run_id INTEGER,
                movie_id INTEGER,
                movie_title TEXT,
                movie_year INTEGER,
                category TEXT CHECK(category IN ('processed', 'quality_skipped', 'no_releases')),
                current_size_gb REAL,
                current_quality TEXT,
                found_size_gb REAL,
                found_quality TEXT,
                skip_reason TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES run_history(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX idx_run_details_run_id ON run_details(run_id)")
        cursor.execute("CREATE INDEX idx_run_details_category ON run_details(category)")
        print("Added run_details table for per-run movie tracking")

    # Migration: Add indexer, seeders, tmdb_rating to completed_jobs
    cursor.execute("PRAGMA table_info(completed_jobs)")
    completed_columns = [row[1] for row in cursor.fetchall()]
    
    if 'indexer' not in completed_columns:
        cursor.execute("ALTER TABLE completed_jobs ADD COLUMN indexer TEXT")
        print("Added 'indexer' column to completed_jobs table")
    
    if 'seeders' not in completed_columns:
        cursor.execute("ALTER TABLE completed_jobs ADD COLUMN seeders INTEGER DEFAULT 0")
        print("Added 'seeders' column to completed_jobs table")
    
    if 'tmdb_rating' not in completed_columns:
        cursor.execute("ALTER TABLE completed_jobs ADD COLUMN tmdb_rating REAL")
        print("Added 'tmdb_rating' column to completed_jobs table")

    # Migration: Add date_added and tmdb_rating to run_details
    cursor.execute("PRAGMA table_info(run_details)")
    run_details_columns = [row[1] for row in cursor.fetchall()]
    
    if 'date_added' not in run_details_columns:
        cursor.execute("ALTER TABLE run_details ADD COLUMN date_added DATETIME")
        print("Added 'date_added' column to run_details table")
    
    if 'tmdb_rating' not in run_details_columns:
        cursor.execute("ALTER TABLE run_details ADD COLUMN tmdb_rating REAL")
        print("Added 'tmdb_rating' column to run_details table")
    
    # Migration: Add run_id column to completed_jobs
    cursor.execute("PRAGMA table_info(completed_jobs)")
    completed_columns = [row[1] for row in cursor.fetchall()]
    
    if 'run_id' not in completed_columns:
        cursor.execute("ALTER TABLE completed_jobs ADD COLUMN run_id INTEGER")
        print("Added 'run_id' column to completed_jobs table")

    conn.commit()
    conn.close()
    print("Database initialized successfully.")