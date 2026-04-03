import logging
import os
import re
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime

LOG_PATH = Path("/app/logs/resizarr.log")

def redact_api_keys(message: str) -> str:
    """Replace API keys in log messages with [REDACTED]."""
    # Redact anything that looks like an API key (32+ alphanumeric chars)
    message = re.sub(r'[a-zA-Z0-9]{32,}', '[REDACTED]', message)
    # Redact radarr API key patterns
    message = re.sub(r'(apikey=)[^&\s]+', r'\1[REDACTED]', message)
    message = re.sub(r'(X-Api-Key:\s*)\S+', r'\1[REDACTED]', message)
    return message

class RedactingFormatter(logging.Formatter):
    """Custom log formatter that redacts API keys."""
    def format(self, record):
        msg = super().format(record)
        return redact_api_keys(msg)

def get_dated_log_path() -> Path:
    """Generate a log file path with today's date."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    return Path(f"/app/logs/resizarr_{date_str}.log")

def setup_logger(
    log_level: str = "INFO",
    log_max_size_mb: int = 10,
    log_max_files: int = 5
) -> logging.Logger:
    """Set up and return the application logger with dated log files."""
    os.makedirs(LOG_PATH.parent, exist_ok=True)
    
    logger = logging.getLogger("resizarr")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Remove existing handlers to avoid duplicates on reload
    logger.handlers.clear()
    
    # Use TimedRotatingFileHandler for date-based rotation
    dated_log_path = get_dated_log_path()
    file_handler = TimedRotatingFileHandler(
        str(dated_log_path).replace(".log", ""),  # base name without .log
        when="midnight",  # rotate at midnight
        interval=1,       # every day
        backupCount=log_max_files,  # keep this many days
        encoding="utf-8"
    )
    # Set the suffix for rotated files
    file_handler.suffix = "%Y-%m-%d.log"
    
    file_handler.setFormatter(RedactingFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    # Also add a console handler for Docker logs (always shows current date)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(RedactingFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log the log file location
    logger.info(f"Logging to dated file: {get_dated_log_path()}")
    
    return logger

def get_logger() -> logging.Logger:
    """Get the application logger."""
    return logging.getLogger("resizarr")