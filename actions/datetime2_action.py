"""
Datetime manipulation and formatting actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def parse_datetime(
    date_string: str,
    format_string: Optional[str] = None,
    timezone_str: Optional[str] = None
) -> datetime:
    """
    Parse a datetime string.

    Args:
        date_string: Date string to parse.
        format_string: Optional format string.
        timezone_str: Optional timezone name.

    Returns:
        Parsed datetime.

    Raises:
        ValueError: If parsing fails.
    """
    if format_string:
        dt = datetime.strptime(date_string, format_string)
    else:
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y %H:%M',
            '%d/%m/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%m/%d/%Y',
            '%Y/%m/%d %H:%M:%S',
            '%Y/%m/%d',
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_string, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Could not parse date string: {date_string}")

    if timezone_str:
        from dateutil import tz
        tz_obj = tz.gettz(timezone_str)
        if tz_obj:
            dt = dt.replace(tzinfo=tz_obj)

    return dt


def format_datetime(
    dt: Union[datetime, str],
    format_string: str = '%Y-%m-%d %H:%M:%S'
) -> str:
    """
    Format a datetime.

    Args:
        dt: Datetime object or ISO string.
        format_string: Output format.

    Returns:
        Formatted datetime string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.strftime(format_string)


def add_timedelta(
    dt: Union[datetime, str],
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Add a timedelta to a datetime.

    Args:
        dt: Datetime object or ISO string.
        days: Days to add.
        hours: Hours to add.
        minutes: Minutes to add.
        seconds: Seconds to add.

    Returns:
        New datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return dt + delta


def subtract_timedelta(
    dt: Union[datetime, str],
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Subtract a timedelta from a datetime.

    Args:
        dt: Datetime object or ISO string.
        days: Days to subtract.
        hours: Hours to subtract.
        minutes: Minutes to subtract.
        seconds: Seconds to subtract.

    Returns:
        New datetime.
    """
    return add_timedelta(dt, days=-days, hours=-hours, minutes=-minutes, seconds=-seconds)


def get_time_difference(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> Dict[str, int]:
    """
    Get the difference between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Dictionary with difference components.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    delta = dt2 - dt1

    total_seconds = int(delta.total_seconds())
    days = delta.days
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return {
        'total_seconds': total_seconds,
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'is_future': total_seconds > 0,
    }


def get_current_timestamp() -> int:
    """
    Get current Unix timestamp.

    Returns:
        Unix timestamp in seconds.
    """
    return int(datetime.now().timestamp())


def timestamp_to_datetime(timestamp: Union[int, float]) -> datetime:
    """
    Convert Unix timestamp to datetime.

    Args:
        timestamp: Unix timestamp.

    Returns:
        Datetime object.
    """
    return datetime.fromtimestamp(timestamp)


def datetime_to_timestamp(dt: Union[datetime, str]) -> int:
    """
    Convert datetime to Unix timestamp.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Unix timestamp.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return int(dt.timestamp())


def get_date_range(
    start: Union[datetime, str],
    end: Union[datetime, str],
    step_days: int = 1
) -> List[datetime]:
    """
    Generate a list of dates between start and end.

    Args:
        start: Start datetime.
        end: End datetime.
        step_days: Step size in days.

    Returns:
        List of datetimes.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    dates = []
    current = start

    while current <= end:
        dates.append(current)
        current += timedelta(days=step_days)

    return dates


def is_weekend(dt: Union[datetime, str]) -> bool:
    """
    Check if date is weekend.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        True if weekend.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.weekday() >= 5


def get_week_number(dt: Union[datetime, str]) -> int:
    """
    Get ISO week number.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        ISO week number.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.isocalendar()[1]


def get_quarter(dt: Union[datetime, str]) -> int:
    """
    Get quarter of the year.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Quarter (1-4).
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return (dt.month - 1) // 3 + 1


def start_of_day(dt: Union[datetime, str]) -> datetime:
    """
    Get start of day.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of day datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: Union[datetime, str]) -> datetime:
    """
    Get end of day.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of day datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def start_of_week(dt: Union[datetime, str]) -> datetime:
    """
    Get start of week (Monday).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of week datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    days_since_monday = dt.weekday()
    return start_of_day(dt - timedelta(days=days_since_monday))


def end_of_week(dt: Union[datetime, str]) -> datetime:
    """
    Get end of week (Sunday).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of week datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    days_until_sunday = 6 - dt.weekday()
    return end_of_day(dt + timedelta(days=days_until_sunday))


def start_of_month(dt: Union[datetime, str]) -> datetime:
    """
    Get start of month.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of month datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(dt: Union[datetime, str]) -> datetime:
    """
    Get end of month.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of month datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    next_month = dt.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)

    return last_day.replace(hour=23, minute=59, second=59, microsecond=999999)


def days_in_month(dt: Union[datetime, str]) -> int:
    """
    Get number of days in month.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Number of days.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    next_month = dt.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    return last_day.day


def is_business_day(dt: Union[datetime, str]) -> bool:
    """
    Check if date is a business day (Mon-Fri, not holiday).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        True if business day.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.weekday() < 5


def add_business_days(
    dt: Union[datetime, str],
    days: int
) -> datetime:
    """
    Add business days to a date.

    Args:
        dt: Starting datetime.
        days: Number of business days to add.

    Returns:
        Resulting datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt
    direction = 1 if days > 0 else -1
    remaining = abs(days)

    while remaining > 0:
        current += timedelta(days=direction)
        if is_business_day(current):
            remaining -= 1

    return current


def format_duration(seconds: int) -> str:
    """
    Format seconds as human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted duration string.
    """
    if seconds < 60:
        return f'{seconds}s'

    minutes = seconds // 60
    seconds = seconds % 60

    if minutes < 60:
        if seconds:
            return f'{minutes}m {seconds}s'
        return f'{minutes}m'

    hours = minutes // 60
    minutes = minutes % 60

    if hours < 24:
        parts = [f'{hours}h']
        if minutes:
            parts.append(f'{minutes}m')
        return ' '.join(parts)

    days = hours // 24
    hours = hours % 24

    parts = [f'{days}d']
    if hours:
        parts.append(f'{hours}h')
    return ' '.join(parts)
