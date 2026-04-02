import re

# Patterns to detect and redact sensitive data
REDACT_PATTERNS = [
    # API keys in query strings
    (re.compile(r'(apikey=)[^&\s]+', re.IGNORECASE), r'\1[REDACTED]'),
    # X-Api-Key header values
    (re.compile(r'(X-Api-Key:\s*)\S+', re.IGNORECASE), r'\1[REDACTED]'),
    # X-API-Key header values
    (re.compile(r'(X-API-Key:\s*)\S+', re.IGNORECASE), r'\1[REDACTED]'),
    # Generic long alphanumeric strings that look like API keys
    (re.compile(r'(["\']?api[_-]?key["\']?\s*[:=]\s*["\']?)([a-zA-Z0-9]{32,})(["\']?)', re.IGNORECASE), r'\1[REDACTED]\3'),
    # Radarr API key in URLs
    (re.compile(r'(/api/v3/[^\s]*apikey=)[^\s&]+', re.IGNORECASE), r'\1[REDACTED]'),
]

def redact(text: str) -> str:
    """Redact all sensitive information from a string."""
    if not text:
        return text

    for pattern, replacement in REDACT_PATTERNS:
        text = pattern.sub(replacement, text)

    return text

def redact_dict(data: dict) -> dict:
    """Redact sensitive keys from a dictionary."""
    sensitive_keys = {
        'api_key', 'apikey', 'radarr_api_key',
        'x-api-key', 'x_api_key', 'password', 'secret'
    }

    redacted = {}
    for key, value in data.items():
        if key.lower() in sensitive_keys:
            redacted[key] = '[REDACTED]'
        elif isinstance(value, str):
            redacted[key] = redact(value)
        elif isinstance(value, dict):
            redacted[key] = redact_dict(value)
        else:
            redacted[key] = value

    return redacted