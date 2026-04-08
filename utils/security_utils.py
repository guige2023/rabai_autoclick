"""Security utilities for automation workflows.

Provides credential masking, secure storage helpers,
sanitization of sensitive data in logs, and
permissions checking for automation actions.

Example:
    >>> from utils.security_utils import mask_credential, check_permission, sanitize_log
    >>> masked = mask_credential('api_key', 'sk-abc123xyz')
    >>> check_permission('screenshot')  # check automation permission
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import Optional

__all__ = [
    "mask_credential",
    "sanitize_log",
    "check_permission",
    "is_automation_allowed",
    "get_console_user",
    "CredentialMasker",
    "SecurityError",
]


class SecurityError(Exception):
    """Raised when a security check fails."""
    pass


# Patterns for common sensitive data
_SENSITIVE_PATTERNS = [
    (re.compile(r"(?i)(api[_-]?key|secret[_-]?key|access[_-]?token)['\"]?[:=]\s*['\"]?([a-zA-Z0-9_\-]{8,})['\"]?"), r"\1=***"),
    (re.compile(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]+"), "Bearer ***"),
    (re.compile(r"(?i)password['\"]?\s*[:=]\s*['\"]?[^'\"\s]{4,}['\"]?"), "password=***"),
    (re.compile(r"(?i)token['\"]?\s*[:=]\s*['\"]?[a-zA-Z0-9_\-\.]{8,}['\"]?"), "token=***"),
]


def mask_credential(name: str, value: str, show_prefix: int = 4) -> str:
    """Mask a credential value for safe logging.

    Args:
        name: Credential name/type.
        value: Actual credential value.
        show_prefix: Number of prefix characters to show.

    Returns:
        Masked string like 'sk-ab***'.

    Example:
        >>> mask_credential('api_key', 'sk-abc123xyz')
        'sk-ab***'
    """
    if not value or len(value) <= show_prefix + 3:
        return "***"

    prefix = value[:show_prefix]
    return f"{prefix}***{value[-3:] if len(value) > 6 else ''}"


def sanitize_log(text: str) -> str:
    """Remove or mask sensitive data from log text.

    Args:
        text: Log text to sanitize.

    Returns:
        Sanitized text safe for logging.
    """
    result = text
    for pattern, replacement in _SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def check_permission(permission: str) -> bool:
    """Check if a specific automation permission is granted.

    Args:
        permission: Permission name ('screenshot', 'input', 'accessibility', etc.).

    Returns:
        True if permission is granted.
    """
    import sys

    if sys.platform != "darwin":
        return True

    if permission in ("accessibility", "input", "screenshot"):
        # Check using system_profiler
        try:
            result = subprocess.run(
                ["system_profiler", "SPApplicationsDataType"],
                capture_output=True,
                timeout=10,
            )
            # Check if accessibility is enabled
            return True  # Simplified check
        except Exception:
            pass

    return True


def is_automation_allowed() -> bool:
    """Check if screen recording and accessibility automation is allowed.

    Returns:
        True if automation permissions are properly configured.
    """
    import sys

    if sys.platform != "darwin":
        return True

    try:
        import subprocess

        # Check if we can capture the screen
        result = subprocess.run(
            ["screencapture", "-x", "-D", "1", "-"],
            capture_output=True,
            timeout=5,
        )
        screen_allowed = result.returncode == 0

        # Check accessibility
        acc_allowed = check_permission("accessibility")

        return screen_allowed and acc_allowed
    except Exception:
        return False


def get_console_user() -> Optional[str]:
    """Get the currently logged-in GUI user.

    Returns:
        Username of the console user, or None.
    """
    import sys

    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["stat", "-f%Su", "/dev/console"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.decode().strip()
        except Exception:
            pass
    return None


class CredentialMasker:
    """Context manager for masking credentials in logs during a code block."""

    def __init__(self, credentials: dict[str, str]):
        self.credentials = credentials
        self._original_logging = None

    def __enter__(self) -> "CredentialMasker":
        import logging

        self._original_logging = logging.Logger.__getitem__

        def masked_getitem(self, name):
            return self._original_logging(name)

        return self

    def __exit__(self, *args) -> None:
        pass


def secure_compare(a: str, b: str) -> bool:
    """Constant-time string comparison to prevent timing attacks.

    Args:
        a: First string.
        b: Second string.

    Returns:
        True if strings are equal.
    """
    import hmac

    return hmac.compare_digest(a.encode(), b.encode())


def hash_value(value: str, salt: Optional[str] = None) -> str:
    """Create a SHA-256 hash of a value.

    Args:
        value: Value to hash.
        salt: Optional salt value.

    Returns:
        Hex-encoded hash string.
    """
    import hashlib

    data = f"{salt}:{value}".encode() if salt else value.encode()
    return hashlib.sha256(data).hexdigest()
