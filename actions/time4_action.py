"""
Time operations and utilities actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_timestamp_utc() -> int:
    """
    Get current UTC timestamp.

    Returns:
        UTC Unix timestamp.
    """
    return int(datetime.now(timezone.utc).timestamp())


def get_timestamp_local() -> int:
    """
    Get current local timestamp.

    Returns:
        Local Unix timestamp.
    """
    return int(datetime.now().timestamp())


def is_time_between(
    dt: Union[datetime, str],
    start: str,
    end: str
) -> bool:
    """
    Check if time is between two times.

    Args:
        dt: Datetime to check.
        start: Start time string.
        end: End time string.

    Returns:
        True if between.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current_minutes = dt.hour * 60 + dt.minute

    import re
    def parse_to_minutes(t: str) -> int:
        match = re.match(r'^(\d{1,2}):(\d{2})', t)
        if match:
            return int(match.group(1)) * 60 + int(match.group(2))
        return 0

    start_minutes = parse_to_minutes(start)
    end_minutes = parse_to_minutes(end)

    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes <= end_minutes
    else:
        return current_minutes >= start_minutes or current_minutes <= end_minutes


def add_time_components(
    hours: int,
    minutes: int,
    seconds: int,
    add_hours: int = 0,
    add_minutes: int = 0,
    add_seconds: int = 0
) -> Dict[str, int]:
    """
    Add time components.

    Args:
        hours, minutes, seconds: Base time.
        add_hours, add_minutes, add_seconds: Values to add.

    Returns:
        Result time components.
    """
    total_seconds = (
        hours * 3600 + minutes * 60 + seconds +
        add_hours * 3600 + add_minutes * 60 + add_seconds
    )

    result_hours = (total_seconds // 3600) % 24
    remaining = total_seconds % 3600
    result_minutes = remaining // 60
    result_seconds = remaining % 60

    return {
        'hours': result_hours,
        'minutes': result_minutes,
        'seconds': result_seconds,
    }


def get_time_of_day(
    hours: int,
    minutes: int = 0
) -> str:
    """
    Get time of day description.

    Args:
        hours: Hour.
        minutes: Minutes.

    Returns:
        Time of day description.
    """
    if 5 <= hours < 12:
        period = 'morning'
    elif hours == 12 and minutes == 0:
        period = 'noon'
    elif 12 <= hours < 17:
        period = 'afternoon'
    elif 17 <= hours < 21:
        period = 'evening'
    else:
        period = 'night'

    return period


def get_business_hours_status(
    work_start: int = 9,
    work_end: int = 17
) -> Dict[str, Any]:
    """
    Get current business hours status.

    Args:
        work_start: Work start hour.
        work_end: Work end hour.

    Returns:
        Status dictionary.
    """
    now = datetime.now()

    is_workday = now.weekday() < 5
    is_during_hours = work_start <= now.hour < work_end
    is_business_time = is_workday and is_during_hours

    if is_business_time:
        remaining_seconds = (
            (work_end - now.hour) * 3600 +
            (0 - now.minute) * 60
        )
    elif is_workday and now.hour < work_start:
        until_start_seconds = (
            (work_start - now.hour) * 3600 +
            (0 - now.minute) * 60
        )
        remaining_seconds = until_start_seconds
    elif is_workday and now.hour >= work_end:
        tomorrow = now + timedelta(days=1)
        while tomorrow.weekday() >= 5:
            tomorrow += timedelta(days=1)

        remaining_seconds = (
            (24 - now.hour + work_start) * 3600 +
            (0 - now.minute) * 60
        )
    else:
        until_workday = 1
        if now.weekday() >= 5:
            until_workday = 7 - now.weekday()

        remaining_seconds = (
            until_workday * 24 * 3600 +
            (work_start - now.hour) * 3600
        )

    return {
        'is_business_time': is_business_time,
        'is_workday': is_workday,
        'is_during_hours': is_during_hours,
        'remaining_seconds': remaining_seconds,
        'current_time': now.isoformat(),
    }


def get_time_ago(seconds: int) -> str:
    """
    Get human-readable "time ago" string.

    Args:
        seconds: Seconds ago.

    Returns:
        String like "2 hours ago".
    """
    if seconds < 60:
        return f'{seconds} second{"s" if seconds != 1 else ""} ago'

    minutes = seconds // 60
    if minutes < 60:
        return f'{minutes} minute{"s" if minutes != 1 else ""} ago'

    hours = minutes // 60
    if hours < 24:
        remaining_minutes = minutes % 60
        if remaining_minutes:
            return f'{hours} hour{"s" if hours != 1 else ""} {remaining_minutes} minute{"s" if remaining_minutes != 1 else ""} ago'
        return f'{hours} hour{"s" if hours != 1 else ""} ago'

    days = hours // 24
    remaining_hours = hours % 24
    if remaining_hours:
        return f'{days} day{"s" if days != 1 else ""} {remaining_hours} hour{"s" if remaining_hours != 1 else ""} ago'
    return f'{days} day{"s" if days != 1 else ""} ago'


def get_time_until(seconds: int) -> str:
    """
    Get human-readable "time until" string.

    Args:
        seconds: Seconds until.

    Returns:
        String like "in 2 hours".
    """
    if seconds < 60:
        return f'in {seconds} second{"s" if seconds != 1 else ""}'

    minutes = seconds // 60
    if minutes < 60:
        return f'in {minutes} minute{"s" if minutes != 1 else ""}'

    hours = minutes // 60
    if hours < 24:
        remaining_minutes = minutes % 60
        if remaining_minutes:
            return f'in {hours} hour{"s" if hours != 1 else ""} {remaining_minutes} minute{"s" if remaining_minutes != 1 else ""}'
        return f'in {hours} hour{"s" if hours != 1 else ""}'

    days = hours // 24
    remaining_hours = hours % 24
    if remaining_hours:
        return f'in {days} day{"s" if days != 1 else ""} {remaining_hours} hour{"s" if remaining_hours != 1 else ""}'
    return f'in {days} day{"s" if days != 1 else ""}'


def get_elapsed_since(start: Union[datetime, str]) -> Dict[str, int]:
    """
    Get elapsed time since a datetime.

    Args:
        start: Starting datetime.

    Returns:
        Elapsed components.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))

    now = datetime.now()
    diff = now - start

    total_seconds = int(diff.total_seconds())

    days = diff.days
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return {
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
    }


def get_future_from_now(
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Get future datetime from now.

    Args:
        days, hours, minutes, seconds: Time to add.

    Returns:
        Future datetime.
    """
    return datetime.now() + timedelta(
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


def get_past_from_now(
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Get past datetime from now.

    Args:
        days, hours, minutes, seconds: Time to subtract.

    Returns:
        Past datetime.
    """
    return datetime.now() - timedelta(
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


def get_timestamps_for_range(
    start: Union[datetime, str],
    end: Union[datetime, str],
    interval_minutes: int = 60
) -> List[int]:
    """
    Generate timestamps for a time range.

    Args:
        start: Start datetime.
        end: End datetime.
        interval_minutes: Interval in minutes.

    Returns:
        List of timestamps.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    timestamps = []
    current = start

    while current <= end:
        timestamps.append(int(current.timestamp()))
        current += timedelta(minutes=interval_minutes)

    return timestamps


def get_current_iso_timestamp() -> str:
    """
    Get current ISO timestamp string.

    Returns:
        ISO formatted timestamp.
    """
    return datetime.now().isoformat()


def get_utc_iso_timestamp() -> str:
    """
    Get current UTC ISO timestamp string.

    Returns:
        UTC ISO formatted timestamp.
    """
    return datetime.now(timezone.utc).isoformat()


def parse_any_date(date_str: str) -> Optional[datetime]:
    """
    Parse any date string format.

    Args:
        date_str: Date string to parse.

    Returns:
        Datetime or None.
    """
    import re

    date_str = date_str.strip()

    common_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%m-%d-%Y',
        '%m/%d/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
    ]

    for fmt in common_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    natural_patterns = [
        (r'yesterday', lambda: datetime.now() - timedelta(days=1)),
        (r'today', lambda: datetime.now()),
        (r'tomorrow', lambda: datetime.now() + timedelta(days=1)),
    ]

    for pattern, parser in natural_patterns:
        if re.match(pattern, date_str, re.IGNORECASE):
            result = parser()
            return result.replace(hour=0, minute=0, second=0, microsecond=0)

    return None
