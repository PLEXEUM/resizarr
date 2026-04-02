import re

# Allowed characters for user inputs
ALLOWED_PATTERN = re.compile(r'^[a-zA-Z0-9/\-_.:]+$')

# Dangerous characters to reject
DANGEROUS_PATTERN = re.compile(r'[;|&$`>]')

def sanitize_input(value: str) -> tuple[bool, str]:
    """
    Validate user input against allowed characters.
    Returns (is_valid, error_message)
    """
    if not value:
        return True, ""

    if DANGEROUS_PATTERN.search(value):
        return False, "Input contains invalid characters (; | & $ ` >)"

    return True, ""

def validate_url(url: str) -> tuple[bool, str]:
    """Validate a URL format."""
    if not url:
        return False, "URL cannot be empty"

    if not url.startswith(("http://", "https://")):
        return False, "URL must start with http:// or https://"

    if DANGEROUS_PATTERN.search(url):
        return False, "URL contains invalid characters"

    return True, ""

def validate_cron(expression: str) -> tuple[bool, str]:
    """Basic cron expression validator (5 fields)."""
    if not expression:
        return False, "Cron expression cannot be empty"

    parts = expression.strip().split()
    if len(parts) != 5:
        return False, "Cron expression must have exactly 5 fields (e.g. 0 2 * * *)"

    return True, ""

def validate_batch_size(value: int) -> tuple[bool, str]:
    """Validate batch size is between 0 and 20."""
    if value < 0 or value > 20:
        return False, "Batch size must be between 0 and 20"
    return True, ""

def validate_size_value(value: float) -> tuple[bool, str]:
    """Validate a file size value is positive."""
    if value <= 0:
        return False, "Size value must be greater than 0"
    return True, ""