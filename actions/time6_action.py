"""
Time and date utilities and helpers.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_current_timestamp() -> int:
    """Get current Unix timestamp."""
    return int(datetime.now().timestamp())


def get_current_timestamp_ms() -> int:
    """Get current Unix timestamp in milliseconds."""
    return int(datetime.now().timestamp() * 1000)


def get_current_datetime() -> datetime:
    """Get current datetime."""
    return datetime.now()


def get_current_utc_datetime() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def format_datetime_iso(dt: Optional[datetime] = None) -> str:
    """
    Format datetime as ISO string.

    Args:
        dt: Datetime (defaults to now).

    Returns:
        ISO formatted string.
    """
    if dt is None:
        dt = datetime.now()
    return dt.isoformat()


def format_datetime_readable(dt: Optional[datetime] = None) -> str:
    """
    Format datetime in readable format.

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Readable formatted string.
    """
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%B %d, %Y at %I:%M %p')


def format_datetime_short(dt: Optional[datetime] = None) -> str:
    """
    Format datetime in short format.

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Short formatted string.
    """
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%Y-%m-%d')


def get_timestamp_components(timestamp: float) -> Dict[str, int]:
    """
    Get timestamp as datetime components.

    Args:
        timestamp: Unix timestamp.

    Returns:
        Dictionary with datetime components.
    """
    dt = datetime.fromtimestamp(timestamp)
    return {
        'year': dt.year,
        'month': dt.month,
        'day': dt.day,
        'hour': dt.hour,
        'minute': dt.minute,
        'second': dt.second,
    }


def create_datetime(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0
) -> datetime:
    """
    Create datetime from components.

    Args:
        year, month, day: Date components.
        hour, minute, second: Time components.

    Returns:
        Datetime object.
    """
    return datetime(year, month, day, hour, minute, second)


def add_days(dt: Union[datetime, str], days: int) -> datetime:
    """
    Add days to datetime.

    Args:
        dt: Datetime or ISO string.
        days: Days to add.

    Returns:
        Result datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt + timedelta(days=days)


def subtract_days(dt: Union[datetime, str], days: int) -> datetime:
    """
    Subtract days from datetime.

    Args:
        dt: Datetime or ISO string.
        days: Days to subtract.

    Returns:
        Result datetime.
    """
    return add_days(dt, -days)


def add_hours(dt: Union[datetime, str], hours: int) -> datetime:
    """
    Add hours to datetime.

    Args:
        dt: Datetime or ISO string.
        hours: Hours to add.

    Returns:
        Result datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt + timedelta(hours=hours)


def add_minutes(dt: Union[datetime, str], minutes: int) -> datetime:
    """
    Add minutes to datetime.

    Args:
        dt: Datetime or ISO string.
        minutes: Minutes to add.

    Returns:
        Result datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt + timedelta(minutes=minutes)


def get_date_from_now(days: int = 0, hours: int = 0) -> datetime:
    """
    Get datetime from now plus offset.

    Args:
        days: Days to add.
        hours: Hours to add.

    Returns:
        Result datetime.
    """
    return datetime.now() + timedelta(days=days, hours=hours)


def get_date_ago(days: int = 0, hours: int = 0) -> datetime:
    """
    Get datetime from now minus offset.

    Args:
        days: Days to subtract.
        hours: Hours to subtract.

    Returns:
        Result datetime.
    """
    return datetime.now() - timedelta(days=days, hours=hours)


def is_past_datetime(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is in the past.

    Args:
        dt: Datetime or ISO string.

    Returns:
        True if past.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt < datetime.now()


def is_future_datetime(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is in the future.

    Args:
        dt: Datetime or ISO string.

    Returns:
        True if future.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt > datetime.now()


def is_weekend_day(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is on weekend.

    Args:
        dt: Datetime or ISO string.

    Returns:
        True if weekend.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt.weekday() >= 5


def get_weekday_name(dt: Union[datetime, str]) -> str:
    """
    Get weekday name.

    Args:
        dt: Datetime or ISO string.

    Returns:
        Weekday name.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt.strftime('%A')


def get_month_name(dt: Union[datetime, str]) -> str:
    """
    Get month name.

    Args:
        dt: Datetime or ISO string.

    Returns:
        Month name.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt.strftime('%B')


def get_time_now_formatted(use_12h: bool = False) -> str:
    """
    Get current time formatted.

    Args:
        use_12h: Use 12-hour format.

    Returns:
        Formatted time string.
    """
    now = datetime.now()
    if use_12h:
        return now.strftime('%I:%M %p')
    return now.strftime('%H:%M:%S')


def get_date_now_formatted() -> str:
    """
    Get current date formatted.

    Returns:
        Formatted date string.
    """
    return datetime.now().strftime('%Y-%m-%d')


def get_datetime_now_formatted(use_12h: bool = False) -> str:
    """
    Get current datetime formatted.

    Args:
        use_12h: Use 12-hour format.

    Returns:
        Formatted datetime string.
    """
    now = datetime.now()
    if use_12h:
        return now.strftime('%Y-%m-%d %I:%M %p')
    return now.strftime('%Y-%m-%d %H:%M:%S')


def get_year(dt: Optional[datetime] = None) -> int:
    """Get year from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.year


def get_month(dt: Optional[datetime] = None) -> int:
    """Get month from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.month


def get_day(dt: Optional[datetime] = None) -> int:
    """Get day from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.day


def get_hour(dt: Optional[datetime] = None) -> int:
    """Get hour from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.hour


def get_minute(dt: Optional[datetime] = None) -> int:
    """Get minute from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.minute


def get_second(dt: Optional[datetime] = None) -> int:
    """Get second from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.second


def get_weekday(dt: Optional[datetime] = None) -> int:
    """Get weekday (0=Mon, 6=Sun) from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.weekday()


def get_day_of_year(dt: Optional[datetime] = None) -> int:
    """Get day of year from datetime."""
    if dt is None:
        dt = datetime.now()
    return dt.timetuple().tm_yday


def get_quarter(dt: Optional[datetime] = None) -> int:
    """Get quarter from datetime."""
    if dt is None:
        dt = datetime.now()
    return (dt.month - 1) // 3 + 1
