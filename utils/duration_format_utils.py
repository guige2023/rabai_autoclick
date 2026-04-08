"""
Duration formatting utilities.

Provides human-readable duration formatting,
parsing, and calculations.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Literal


def format_duration(
    seconds: float,
    precision: int = 2,
    format_type: Literal["full", "abbreviated", "compact"] = "full",
) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds
        precision: Number of significant units to show
        format_type: Format style

    Returns:
        Formatted duration string
    """
    if seconds < 0:
        return f"negative {format_duration(-seconds, precision, format_type)}"

    units_full = [
        (86400 * 365, "year"),
        (86400 * 30, "month"),
        (86400, "day"),
        (3600, "hour"),
        (60, "minute"),
        (1, "second"),
    ]

    units_abbr = [
        (86400 * 365, "yr"),
        (86400 * 30, "mo"),
        (86400, "day"),
        (3600, "hr"),
        (60, "min"),
        (1, "sec"),
    ]

    units_compact = [
        (86400 * 365, "y"),
        (86400 * 30, "mo"),
        (86400, "d"),
        (3600, "h"),
        (60, "m"),
        (1, "s"),
    ]

    units = {"full": units_full, "abbreviated": units_abbr, "compact": units_compact}

    parts = []
    remaining = seconds

    for threshold, name in units[format_type]:
        count = int(remaining // threshold)
        if count > 0:
            if format_type == "full":
                parts.append(f"{count} {name}{'s' if count != 1 else ''}")
            elif format_type == "abbreviated":
                parts.append(f"{count}{name}")
            else:
                parts.append(f"{count}{name}")
            remaining -= count * threshold
            if len(parts) >= precision:
                break

    if not parts:
        if format_type == "compact":
            return f"{int(seconds * 1000)}ms"
        return "0 seconds"

    return " ".join(parts)


def format_duration_timedelta(delta: timedelta) -> str:
    """Format timedelta to human-readable string."""
    total_seconds = delta.total_seconds()
    return format_duration(total_seconds)


def parse_duration(duration_str: str) -> float:
    """
    Parse duration string to seconds.

    Supported formats:
      - "1h30m"
      - "2 hours 30 minutes"
      - "3600"
      - "1.5h"
      - "1d 2h 3m 4s"

    Args:
        duration_str: Duration string

    Returns:
        Duration in seconds
    """
    duration_str = duration_str.strip().lower()

    numeric = float(duration_str)
    if numeric > 1000:
        return numeric
    elif numeric > 0:
        return numeric
    return 0


def parse_duration_with_regex(duration_str: str) -> float:
    """Parse complex duration string with units."""
    duration_str = duration_str.strip()
    total_seconds = 0.0

    unit_patterns = [
        (r"(\d+(?:\.\d+)?)\s*y(?:ears?)?", 365.25 * 86400),
        (r"(\d+(?:\.\d+)?)\s*mo(?:nths?)?", 30.44 * 86400),
        (r"(\d+(?:\.\d+)?)\s*d(?:ays?)?", 86400),
        (r"(\d+(?:\.\d+)?)\s*h(?:(?:ou)?rs?)?", 3600),
        (r"(\d+(?:\.\d+)?)\s*m(?:in(?:utes?)?)?", 60),
        (r"(\d+(?:\.\d+)?)\s*s(?:ec(?:onds?)?)?", 1),
        (r"(\d+(?:\.\d+)?)\s*ms", 0.001),
    ]

    remaining = duration_str
    for pattern, multiplier in unit_patterns:
        match = re.search(pattern, remaining)
        if match:
            value = float(match.group(1))
            total_seconds += value * multiplier
            remaining = remaining[:match.start()] + remaining[match.end():]

    if total_seconds == 0:
        try:
            return float(duration_str)
        except ValueError:
            return 0.0

    return total_seconds


def format_duration_ago(seconds: float) -> str:
    """
    Format duration as 'time ago' string.

    Args:
        seconds: Duration in seconds

    Returns:
        String like "5 minutes ago"
    """
    if seconds < 0:
        return "in the future"

    units = [
        (365.25 * 86400, "year"),
        (30.44 * 86400, "month"),
        (86400, "day"),
        (3600, "hour"),
        (60, "minute"),
        (1, "second"),
    ]

    for threshold, name in units:
        count = int(seconds // threshold)
        if count >= 1:
            return f"{count} {name}{'s' if count != 1 else ''} ago"

    return "just now"


def duration_buckets(
    durations: list[float],
    bucket_count: int = 10,
) -> list[tuple[float, float]]:
    """
    Create duration buckets for histogram.

    Args:
        durations: List of durations
        bucket_count: Number of buckets

    Returns:
        List of (min, max) bucket boundaries
    """
    if not durations:
        return []
    min_dur = min(durations)
    max_dur = max(durations)
    if min_dur == max_dur:
        return [(min_dur, min_dur * 1.1 if min_dur > 0 else 1.0)]
    step = (max_dur - min_dur) / bucket_count
    buckets = [(min_dur + i * step, min_dur + (i + 1) * step) for i in range(bucket_count)]
    return buckets
