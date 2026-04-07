"""Date and time utilities for RabAI AutoClick.

Provides:
- Datetime formatting and parsing
- Timezone handling
- Duration utilities
"""

import datetime
import time
from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class TimeRange:
    """Represents a time range."""
    start: datetime.datetime
    end: datetime.datetime

    @property
    def duration(self) -> datetime.timedelta:
        """Get duration of time range."""
        return self.end - self.start

    def contains(self, dt: datetime.datetime) -> bool:
        """Check if datetime is within range."""
        return self.start <= dt <= self.end

    def overlaps(self, other: 'TimeRange') -> bool:
        """Check if two ranges overlap."""
        return self.start < other.end and other.start < self.end


def now() -> datetime.datetime:
    """Get current datetime."""
    return datetime.datetime.now()


def today() -> datetime.datetime:
    """Get current date at midnight."""
    return datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def parse_datetime(
    date_string: str,
    formats: Optional[list] = None,
) -> Optional[datetime.datetime]:
    """Parse datetime string with multiple format attempts.

    Args:
        date_string: String to parse.
        formats: List of format strings to try.

    Returns:
        Parsed datetime or None.
    """
    default_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ]

    for fmt in (formats or default_formats):
        try:
            return datetime.datetime.strptime(date_string, fmt)
        except ValueError:
            continue

    return None


def format_datetime(
    dt: Optional[datetime.datetime] = None,
    format_str: str = "%Y-%m-%d %H:%M:%S",
) -> str:
    """Format datetime as string.

    Args:
        dt: Datetime to format (defaults to now).
        format_str: Format string.

    Returns:
        Formatted string.
    """
    if dt is None:
        dt = datetime.datetime.now()
    return dt.strftime(format_str)


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "1h 23m 45s").
    """
    if seconds < 0:
        return "-" + format_duration(-seconds)

    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def parse_duration(duration_str: str) -> float:
    """Parse duration string to seconds.

    Args:
        duration_str: Duration string (e.g., "1h30m", "45s").

    Returns:
        Duration in seconds.
    """
    seconds = 0.0
    current = ""

    for char in duration_str:
        if char.isdigit() or char == ".":
            current += char
        elif char in "hms":
            if current:
                value = float(current)
                if char == "h":
                    seconds += value * 3600
                elif char == "m":
                    seconds += value * 60
                else:
                    seconds += value
                current = ""

    if current:
        seconds += float(current)

    return seconds


def timestamp() -> float:
    """Get current Unix timestamp."""
    return time.time()


def timestamp_ms() -> int:
    """Get current Unix timestamp in milliseconds."""
    return int(time.time() * 1000)


def from_timestamp(ts: float) -> datetime.datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.datetime.fromtimestamp(ts)


def utcnow() -> datetime.datetime:
    """Get current UTC datetime."""
    return datetime.datetime.utcnow()


def is_weekend(dt: Optional[datetime.datetime] = None) -> bool:
    """Check if date is weekend."""
    if dt is None:
        dt = datetime.datetime.now()
    return dt.weekday() >= 5


def is_weekday(dt: Optional[datetime.datetime] = None) -> bool:
    """Check if date is weekday."""
    return not is_weekend(dt)


def start_of_week(dt: Optional[datetime.datetime] = None) -> datetime.datetime:
    """Get start of week (Monday)."""
    if dt is None:
        dt = datetime.datetime.now()
    return dt - datetime.timedelta(days=dt.weekday())


def start_of_month(dt: Optional[datetime.datetime] = None) -> datetime.datetime:
    """Get start of month."""
    if dt is None:
        dt = datetime.datetime.now()
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(dt: Optional[datetime.datetime] = None) -> datetime.datetime:
    """Get end of month."""
    if dt is None:
        dt = datetime.datetime.now()
    next_month = dt.replace(day=28) + datetime.timedelta(days=4)
    return next_month.replace(day=1, hour=23, minute=59, second=59) - datetime.timedelta(days=1)


def add_business_days(
    dt: datetime.datetime,
    days: int,
) -> datetime.datetime:
    """Add business days to date.

    Args:
        dt: Starting date.
        days: Number of business days to add.

    Returns:
        Resulting date.
    """
    current = dt
    delta = 1 if days >= 0 else -1

    while days != 0:
        current += datetime.timedelta(days=delta)
        if current.weekday() < 5:  # Mon-Fri
            days -= delta

    return current


def age_string(dt: datetime.datetime) -> str:
    """Get human-readable age string.

    Args:
        dt: Past datetime.

    Returns:
        Age string (e.g., "5 minutes ago").
    """
    now = datetime.datetime.now()
    delta = now - dt

    if delta.total_seconds() < 60:
        return "just now"
    elif delta.total_seconds() < 3600:
        minutes = int(delta.total_seconds() / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif delta.total_seconds() < 86400:
        hours = int(delta.total_seconds() / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif delta.days < 30:
        return f"{delta.days} day{'s' if delta.days != 1 else ''} ago"
    elif delta.days < 365:
        months = int(delta.days / 30)
        return f"{months} month{'s' if months != 1 else ''} ago"
    else:
        years = int(delta.days / 365)
        return f"{years} year{'s' if years != 1 else ''} ago"