"""
Time and datetime advanced utilities.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_elapsed_seconds(start: Union[datetime, str]) -> float:
    """
    Get elapsed seconds since start.

    Args:
        start: Start datetime.

    Returns:
        Elapsed seconds.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))

    return (datetime.now() - start).total_seconds()


def get_remaining_seconds(end: Union[datetime, str]) -> float:
    """
    Get remaining seconds until end.

    Args:
        end: End datetime.

    Returns:
        Remaining seconds (negative if past).
    """
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    return (end - datetime.now()).total_seconds()


def format_elapsed(seconds: float) -> str:
    """
    Format elapsed seconds as readable string.

    Args:
        seconds: Seconds to format.

    Returns:
        Human-readable string.
    """
    if seconds < 0:
        return 'negative'

    if seconds < 60:
        return f'{int(seconds)}s'

    minutes = int(seconds // 60)
    seconds = int(seconds % 60)

    if minutes < 60:
        return f'{minutes}m {seconds}s'

    hours = minutes // 60
    minutes = minutes % 60

    if hours < 24:
        return f'{hours}h {minutes}m'

    days = hours // 24
    hours = hours % 24
    return f'{days}d {hours}h'


def get_current_time_ms() -> int:
    """Get current time in milliseconds."""
    return int(datetime.now().timestamp() * 1000)


def get_timezone_offset(tz_name: str) -> float:
    """
    Get UTC offset for timezone in hours.

    Args:
        tz_name: Timezone name.

    Returns:
        Offset in hours.
    """
    try:
        from datetime import timezone as tz_module
        import zoneinfo

        tz = zoneinfo.ZoneInfo(tz_name)
        now = datetime.now()
        offset = tz.utcoffset(now)
        return offset.total_seconds() / 3600 if offset else 0.0
    except Exception:
        return 0.0


def get_timestamp_for_date(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0
) -> float:
    """
    Get timestamp for specific date.

    Args:
        year, month, day: Date components.
        hour, minute, second: Time components.

    Returns:
        Unix timestamp.
    """
    dt = datetime(year, month, day, hour, minute, second)
    return dt.timestamp()


def get_datetime_now() -> Dict[str, int]:
    """
    Get current datetime as components.

    Returns:
        Dictionary with datetime components.
    """
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day,
        'hour': now.hour,
        'minute': now.minute,
        'second': now.second,
        'microsecond': now.microsecond,
    }


def format_datetime_custom(
    dt: Union[datetime, str],
    format_str: str
) -> str:
    """
    Format datetime with custom format.

    Args:
        dt: Datetime or ISO string.
        format_str: strftime format string.

    Returns:
        Formatted string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.strftime(format_str)


def get_start_of_day(dt: Optional[datetime] = None) -> datetime:
    """
    Get start of day (midnight).

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Start of day.
    """
    if dt is None:
        dt = datetime.now()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def get_end_of_day(dt: Optional[datetime] = None) -> datetime:
    """
    Get end of day (23:59:59).

    Args:
        dt: Datetime (defaults to now).

    Returns:
        End of day.
    """
    if dt is None:
        dt = datetime.now()
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def get_start_of_week(dt: Optional[datetime] = None) -> datetime:
    """
    Get start of week (Monday).

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Start of week.
    """
    if dt is None:
        dt = datetime.now()
    days_since_monday = dt.weekday()
    return get_start_of_day(dt - timedelta(days=days_since_monday))


def get_start_of_month(dt: Optional[datetime] = None) -> datetime:
    """
    Get start of month.

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Start of month.
    """
    if dt is None:
        dt = datetime.now()
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_start_of_year(dt: Optional[datetime] = None) -> datetime:
    """
    Get start of year.

    Args:
        dt: Datetime (defaults to now).

    Returns:
        Start of year.
    """
    if dt is None:
        dt = datetime.now()
    return datetime(dt.year, 1, 1)


def get_time_until_hour(target_hour: int) -> Dict[str, int]:
    """
    Get time until target hour.

    Args:
        target_hour: Target hour (0-23).

    Returns:
        Hours, minutes, seconds until target.
    """
    now = datetime.now()
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)

    if target <= now:
        target += timedelta(days=1)

    diff = target - now
    total_seconds = int(diff.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
    }


def get_timezone_now(tz_name: str) -> datetime:
    """
    Get current datetime in timezone.

    Args:
        tz_name: Timezone name.

    Returns:
        Datetime in timezone.
    """
    try:
        from dateutil import tz
        return datetime.now(tz.gettz(tz_name))
    except ImportError:
        return datetime.now()


def get_utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def get_local_now() -> datetime:
    """Get current local datetime."""
    return datetime.now()


def get_timestamp_utc() -> int:
    """Get current UTC timestamp."""
    return int(datetime.now(timezone.utc).timestamp())


def get_timestamp_local() -> int:
    """Get current local timestamp."""
    return int(datetime.now().timestamp())


def get_date_range(
    start: Union[datetime, str],
    end: Union[datetime, str],
    step_days: int = 1
) -> List[datetime]:
    """
    Generate list of dates between start and end.

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
    current = get_start_of_day(start)

    while current <= end:
        dates.append(current)
        current += timedelta(days=step_days)

    return dates


def get_week_dates(year: int, week: int) -> List[datetime]:
    """
    Get all dates in a week.

    Args:
        year: Year.
        week: Week number (1-53).

    Returns:
        List of 7 datetimes for the week.
    """
    jan4 = datetime(year, 1, 4)
    week1_monday = jan4 - timedelta(days=jan4.weekday())

    week_start = week1_monday + timedelta(weeks=week - 1)

    return [week_start + timedelta(days=i) for i in range(7)]


def get_month_dates(year: int, month: int) -> List[datetime]:
    """
    Get all dates in a month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        List of datetimes for the month.
    """
    start = datetime(year, month, 1)

    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(days=1)

    return get_date_range(start, end, step_days=1)


def is_same_date(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> bool:
    """
    Check if two datetimes are same date.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        True if same date.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    return dt1.date() == dt2.date()


def get_days_between(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> int:
    """
    Get number of days between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of days.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    return abs((dt2 - dt1).days)


def get_hours_between(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> float:
    """
    Get number of hours between two datetimes.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Number of hours.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    return abs((dt2 - dt1).total_seconds() / 3600)
