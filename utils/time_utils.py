"""Time utilities for RabAI AutoClick.

Provides:
- Time and date helpers
- Duration formatting
- Timestamp utilities
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Union


def now() -> datetime:
    """Get current datetime.

    Returns:
        Current datetime.
    """
    return datetime.now()


def utc_now() -> datetime:
    """Get current UTC datetime.

    Returns:
        Current UTC datetime.
    """
    return datetime.now(timezone.utc)


def timestamp() -> float:
    """Get current Unix timestamp.

    Returns:
        Current timestamp.
    """
    return time.time()


def timestamp_ms() -> int:
    """Get current timestamp in milliseconds.

    Returns:
        Current timestamp in milliseconds.
    """
    return int(time.time() * 1000)


def from_timestamp(ts: float) -> datetime:
    """Convert timestamp to datetime.

    Args:
        ts: Unix timestamp.

    Returns:
        Datetime object.
    """
    return datetime.fromtimestamp(ts)


def from_timestamp_ms(ts: int) -> datetime:
    """Convert milliseconds timestamp to datetime.

    Args:
        ts: Timestamp in milliseconds.

    Returns:
        Datetime object.
    """
    return datetime.fromtimestamp(ts / 1000.0)


def to_timestamp(dt: datetime) -> float:
    """Convert datetime to timestamp.

    Args:
        dt: Datetime object.

    Returns:
        Unix timestamp.
    """
    return dt.timestamp()


def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime as string.

    Args:
        dt: Datetime to format.
        fmt: Format string.

    Returns:
        Formatted string.
    """
    return dt.strftime(fmt)


def format_date(dt: datetime) -> str:
    """Format datetime as date string.

    Args:
        dt: Datetime to format.

    Returns:
        Date string (YYYY-MM-DD).
    """
    return dt.strftime("%Y-%m-%d")


def format_time(dt: datetime) -> str:
    """Format datetime as time string.

    Args:
        dt: Datetime to format.

    Returns:
        Time string (HH:MM:SS).
    """
    return dt.strftime("%H:%M:%S")


def parse_datetime(date_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> Optional[datetime]:
    """Parse datetime from string.

    Args:
        date_str: Date string to parse.
        fmt: Format string.

    Returns:
        Datetime object or None if parsing fails.
    """
    try:
        return datetime.strptime(date_str, fmt)
    except ValueError:
        return None


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string (YYYY-MM-DD).

    Args:
        date_str: Date string.

    Returns:
        Datetime object or None if parsing fails.
    """
    return parse_datetime(date_str, "%Y-%m-%d")


def add_days(dt: datetime, days: int) -> datetime:
    """Add days to datetime.

    Args:
        dt: Datetime.
        days: Number of days to add (can be negative).

    Returns:
        New datetime.
    """
    return dt + timedelta(days=days)


def add_hours(dt: datetime, hours: int) -> datetime:
    """Add hours to datetime.

    Args:
        dt: Datetime.
        hours: Number of hours to add (can be negative).

    Returns:
        New datetime.
    """
    return dt + timedelta(hours=hours)


def add_minutes(dt: datetime, minutes: int) -> datetime:
    """Add minutes to datetime.

    Args:
        dt: Datetime.
        minutes: Number of minutes to add (can be negative).

    Returns:
        New datetime.
    """
    return dt + timedelta(minutes=minutes)


def add_seconds(dt: datetime, seconds: float) -> datetime:
    """Add seconds to datetime.

    Args:
        dt: Datetime.
        seconds: Number of seconds to add (can be negative).

    Returns:
        New datetime.
    """
    return dt + timedelta(seconds=seconds)


def days_between(dt1: datetime, dt2: datetime) -> int:
    """Get number of days between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of days (can be negative).
    """
    return (dt2 - dt1).days


def hours_between(dt1: datetime, dt2: datetime) -> float:
    """Get number of hours between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of hours (can be negative).
    """
    return (dt2 - dt1).total_seconds() / 3600


def minutes_between(dt1: datetime, dt2: datetime) -> float:
    """Get number of minutes between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of minutes (can be negative).
    """
    return (dt2 - dt1).total_seconds() / 60


def seconds_between(dt1: datetime, dt2: datetime) -> float:
    """Get number of seconds between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of seconds (can be negative).
    """
    return (dt2 - dt1).total_seconds()


def is_weekend(dt: datetime) -> bool:
    """Check if datetime is on weekend.

    Args:
        dt: Datetime to check.

    Returns:
        True if weekend.
    """
    return dt.weekday() >= 5


def is_weekday(dt: datetime) -> bool:
    """Check if datetime is on weekday.

    Args:
        dt: Datetime to check.

    Returns:
        True if weekday.
    """
    return dt.weekday() < 5


def start_of_day(dt: datetime) -> datetime:
    """Get start of day (midnight).

    Args:
        dt: Datetime.

    Returns:
        Datetime at start of day.
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime) -> datetime:
    """Get end of day (23:59:59.999999).

    Args:
        dt: Datetime.

    Returns:
        Datetime at end of day.
    """
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def start_of_week(dt: datetime) -> datetime:
    """Get start of week (Monday).

    Args:
        dt: Datetime.

    Returns:
        Datetime at start of week.
    """
    days_since_monday = dt.weekday()
    return start_of_day(dt - timedelta(days=days_since_monday))


def start_of_month(dt: datetime) -> datetime:
    """Get start of month.

    Args:
        dt: Datetime.

    Returns:
        Datetime at start of month.
    """
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(dt: datetime) -> datetime:
    """Get end of month.

    Args:
        dt: Datetime.

    Returns:
        Datetime at end of month.
    """
    if dt.month == 12:
        return dt.replace(day=31, hour=23, minute=59, second=59, microsecond=999999)
    next_month = dt.replace(month=dt.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return next_month - timedelta(seconds=1)


def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """Check if two datetimes are on the same day.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        True if same day.
    """
    return dt1.year == dt2.year and dt1.month == dt2.month and dt1.day == dt2.day


def is_today(dt: datetime) -> bool:
    """Check if datetime is today.

    Args:
        dt: Datetime to check.

    Returns:
        True if today.
    """
    return is_same_day(dt, now())


def is_past(dt: datetime) -> bool:
    """Check if datetime is in the past.

    Args:
        dt: Datetime to check.

    Returns:
        True if in the past.
    """
    return dt < now()


def is_future(dt: datetime) -> bool:
    """Check if datetime is in the future.

    Args:
        dt: Datetime to check.

    Returns:
        True if in the future.
    """
    return dt > now()


def sleep(seconds: float) -> None:
    """Sleep for specified seconds.

    Args:
        seconds: Number of seconds to sleep.
    """
    time.sleep(seconds)


def sleep_ms(milliseconds: int) -> None:
    """Sleep for specified milliseconds.

    Args:
        milliseconds: Number of milliseconds to sleep.
    """
    time.sleep(milliseconds / 1000.0)


def format_duration_seconds(seconds: float) -> str:
    """Format duration in seconds as human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "1h 30m").
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {seconds:.0f}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def format_duration_ms(milliseconds: float) -> str:
    """Format duration in milliseconds as human-readable string.

    Args:
        milliseconds: Duration in milliseconds.

    Returns:
        Formatted string.
    """
    return format_duration_seconds(milliseconds / 1000.0)
