"""Time formatting utilities for RabAI AutoClick.

Provides:
- Duration formatting
- Timestamp utilities
- Time parsing
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Optional


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string like "1h 23m 45s".
    """
    if seconds < 0:
        return f"-{format_duration(-seconds)}"

    parts = []
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def format_timestamp(ts: float, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format timestamp as string.

    Args:
        ts: Unix timestamp.
        fmt: strftime format.

    Returns:
        Formatted string.
    """
    return datetime.fromtimestamp(ts).strftime(fmt)


def parse_duration(duration_str: str) -> float:
    """Parse duration string to seconds.

    Args:
        duration_str: Duration like "1h30m", "30s", "1.5h".

    Returns:
        Duration in seconds.
    """
    import re

    total_seconds = 0.0
    patterns = [
        (r"(\d+(?:\.\d+)?)h", 3600),
        (r"(\d+(?:\.\d+)?)m", 60),
        (r"(\d+(?:\.\d+)?)s", 1),
        (r"(\d+(?:\.\d+)?)", 1),
    ]

    remaining = duration_str
    for pattern, multiplier in patterns:
        match = re.search(pattern, remaining)
        if match:
            value = float(match.group(1))
            total_seconds += value * multiplier

    return total_seconds


def time_ago(timestamp: float) -> str:
    """Get human-readable time ago string.

    Args:
        timestamp: Unix timestamp.

    Returns:
        String like "5 minutes ago".
    """
    seconds = time.time() - timestamp

    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if seconds < 604800:
        days = int(seconds / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"
    if seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    return format_timestamp(timestamp, "%Y-%m-%d")
