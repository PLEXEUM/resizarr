import logging
import os
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path

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

def setup_logger(
    log_level: str = "INFO",
    log_max_size_mb: int = 10,
    log_max_files: int = 5
) -> logging.Logger:
    """Set up and return the application logger."""
    os.makedirs(LOG_PATH.parent, exist_ok=True)

    logger = logging.getLogger("resizarr")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates on reload
    logger.handlers.clear()

    # File handler with rotation by size
    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=log_max_size_mb * 1024 * 1024,
        backupCount=log_max_files
    )
    file_handler.setFormatter(RedactingFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    # Console handler for Docker logs
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(RedactingFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def get_logger() -> logging.Logger:
    """Get the application logger."""
    return logging.getLogger("resizarr")