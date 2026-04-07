"""
Time formatting and parsing advanced actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def format_duration_long(seconds: int) -> str:
    """
    Format seconds as long duration string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Human-readable duration string.
    """
    if seconds < 0:
        return 'negative duration'

    parts = []

    days = seconds // 86400
    seconds %= 86400

    hours = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    if days > 0:
        parts.append(f'{days} day{"s" if days != 1 else ""}')

    if hours > 0:
        parts.append(f'{hours} hour{"s" if hours != 1 else ""}')

    if minutes > 0:
        parts.append(f'{minutes} minute{"s" if minutes != 1 else ""}')

    if seconds > 0 or not parts:
        parts.append(f'{seconds} second{"s" if seconds != 1 else ""}')

    return ', '.join(parts)


def format_duration_compact(seconds: int) -> str:
    """
    Format seconds as compact duration string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Compact string like "1d2h3m4s".
    """
    if seconds < 0:
        return '-'

    parts = []

    days = seconds // 86400
    seconds %= 86400

    hours = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    if days > 0:
        parts.append(f'{days}d')
    if hours > 0:
        parts.append(f'{hours}h')
    if minutes > 0:
        parts.append(f'{minutes}m')
    if seconds > 0 or not parts:
        parts.append(f'{seconds}s')

    return ''.join(parts)


def parse_duration(duration_str: str) -> int:
    """
    Parse duration string to seconds.

    Args:
        duration_str: Duration string (e.g., "1h30m", "2 days").

    Returns:
        Duration in seconds.
    """
    import re

    duration_str = duration_str.lower().strip()

    total_seconds = 0

    patterns = [
        (r'(\d+)\s*d(?:ays?)?', 86400),
        (r'(\d+)\s*h(?:ours?)?', 3600),
        (r'(\d+)\s*m(?:in(?:utes?)?)?', 60),
        (r'(\d+)\s*s(?:ec(?:onds?)?)?', 1),
        (r'(\d+)h', 3600),
        (r'(\d+)m', 60),
        (r'(\d+)s', 1),
    ]

    for pattern, multiplier in patterns:
        matches = re.findall(pattern, duration_str)
        for match in matches:
            total_seconds += int(match) * multiplier

    return total_seconds


def get_time_until_datetime(dt: Union[datetime, str]) -> Dict[str, int]:
    """
    Get time until a future datetime.

    Args:
        dt: Future datetime.

    Returns:
        Dictionary with time components.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    now = datetime.now()

    if dt <= now:
        return {
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'total_seconds': 0,
            'is_past': True,
        }

    diff = dt - now
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
        'is_past': False,
    }


def get_time_since_datetime(dt: Union[datetime, str]) -> Dict[str, int]:
    """
    Get time since a past datetime.

    Args:
        dt: Past datetime.

    Returns:
        Dictionary with time components.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    now = datetime.now()

    if dt >= now:
        return {
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'total_seconds': 0,
            'is_future': True,
        }

    diff = now - dt
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
        'is_future': False,
    }


def get_timezone_conversion(
    dt: Union[datetime, str],
    from_tz: str,
    to_tz: str
) -> Dict[str, Any]:
    """
    Convert time between timezones.

    Args:
        dt: Datetime to convert.
        from_tz: Source timezone.
        to_tz: Target timezone.

    Returns:
        Conversion result.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    try:
        from dateutil import tz

        from_timezone = tz.gettz(from_tz)
        to_timezone = tz.gettz(to_tz)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=from_timezone)

        converted = dt.astimezone(to_timezone)

        return {
            'original': dt.isoformat(),
            'converted': converted.isoformat(),
            'from_timezone': from_tz,
            'to_timezone': to_tz,
            'success': True,
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
        }


def is_workday(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is a workday.

    Args:
        dt: Datetime to check.

    Returns:
        True if workday (Mon-Fri).
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.weekday() < 5


def get_next_workday(dt: Union[datetime, str]) -> datetime:
    """
    Get next workday after datetime.

    Args:
        dt: Starting datetime.

    Returns:
        Next workday.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt + timedelta(days=1)

    while current.weekday() >= 5:
        current += timedelta(days=1)

    return current


def get_previous_workday(dt: Union[datetime, str]) -> datetime:
    """
    Get previous workday before datetime.

    Args:
        dt: Starting datetime.

    Returns:
        Previous workday.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt - timedelta(days=1)

    while current.weekday() >= 5:
        current -= timedelta(days=1)

    return current


def get_business_hours_difference(
    start: Union[datetime, str],
    end: Union[datetime, str],
    work_start: int = 9,
    work_end: int = 17
) -> float:
    """
    Calculate business hours between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.
        work_start: Work day start hour.
        work_end: Work day end hour.

    Returns:
        Business hours.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    total_hours = 0.0
    current = start

    while current < end:
        if current.weekday() < 5:
            hour = current.hour

            if work_start <= hour < work_end:
                end_of_work = min(
                    current.replace(minute=59, second=59),
                    current.replace(hour=work_end, minute=0, second=0)
                )

                hours = (end_of_work - current).total_seconds() / 3600
                total_hours += hours

        current = (current + timedelta(days=1)).replace(hour=work_start, minute=0, second=0)

    return round(total_hours, 2)


def get_nearest_time_slot(
    minutes_interval: int = 30
) -> Dict[str, int]:
    """
    Get nearest time slot (rounded time).

    Args:
        minutes_interval: Slot interval in minutes.

    Returns:
        Rounded time components.
    """
    now = datetime.now()

    rounded_minute = (now.minute // minutes_interval) * minutes_interval

    if rounded_minute == 60:
        next_hour = now.replace(hour=now.hour + 1, minute=0, second=0, microsecond=0)
    else:
        next_hour = now.replace(minute=rounded_minute, second=0, microsecond=0)

    return {
        'hour': next_hour.hour,
        'minute': next_hour.minute,
        'datetime': next_hour,
    }


def get_week_bounds(
    year: int,
    week: int
) -> Dict[str, datetime]:
    """
    Get start and end of ISO week.

    Args:
        year: Year.
        week: Week number (1-53).

    Returns:
        Dictionary with 'start' and 'end'.
    """
    jan4 = datetime(year, 1, 4)

    week1_start = jan4 - timedelta(days=jan4.weekday())

    week_start = week1_start + timedelta(weeks=week - 1)
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    return {
        'start': week_start,
        'end': week_end,
    }


def get_month_bounds(
    year: int,
    month: int
) -> Dict[str, datetime]:
    """
    Get start and end of month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        Dictionary with 'start' and 'end'.
    """
    start = datetime(year, month, 1)

    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(seconds=1)

    return {
        'start': start,
        'end': end,
    }


def timestamp_to_datetime_str(
    timestamp: float,
    timezone_name: str = 'UTC'
) -> str:
    """
    Convert Unix timestamp to timezone-aware datetime string.

    Args:
        timestamp: Unix timestamp.
        timezone_name: Target timezone.

    Returns:
        ISO formatted datetime string.
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

    if timezone_name != 'UTC':
        try:
            from dateutil import tz
            local_tz = tz.gettz(timezone_name)
            dt = dt.astimezone(local_tz)
        except Exception:
            pass

    return dt.isoformat()


def datetime_to_timestamp_ms(dt: Union[datetime, str]) -> int:
    """
    Convert datetime to milliseconds timestamp.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Timestamp in milliseconds.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return int(dt.timestamp() * 1000)


def parse_natural_time(time_str: str) -> Dict[str, Any]:
    """
    Parse natural language time expressions.

    Args:
        time_str: Natural time string.

    Returns:
        Parsed time information.
    """
    import re

    time_str = time_str.lower().strip()

    patterns = [
        (r'(\d+)\s*(?:hour|hr)s?\s*(?:and\s*)?(\d+)\s*(?:minute|min)s?', 'hours_minutes'),
        (r'(\d+)\s*(?:hour|hr)s?', 'hours'),
        (r'(\d+)\s*(?:minute|min)s?', 'minutes'),
        (r'(\d+)\s*(?:second|sec)s?', 'seconds'),
    ]

    for pattern, time_type in patterns:
        match = re.search(pattern, time_str)
        if match:
            value = int(match.group(1))

            if time_type == 'hours_minutes':
                value = value * 3600 + int(match.group(2)) * 60
            elif time_type == 'hours':
                value *= 3600
            elif time_type == 'minutes':
                value *= 60

            return {
                'seconds': value,
                'type': time_type,
            }

    return {'seconds': 0, 'type': 'unknown'}


def get_timestamp_components(timestamp: float) -> Dict[str, int]:
    """
    Get timestamp as datetime components.

    Args:
        timestamp: Unix timestamp.

    Returns:
        Datetime components.
    """
    dt = datetime.fromtimestamp(timestamp)

    return {
        'year': dt.year,
        'month': dt.month,
        'day': dt.day,
        'hour': dt.hour,
        'minute': dt.minute,
        'second': dt.second,
        'microsecond': dt.microsecond,
        'weekday': dt.weekday(),
        'day_of_year': dt.timetuple().tm_yday,
    }


def combine_date_and_time(
    date_str: str,
    time_str: str
) -> datetime:
    """
    Combine date and time strings.

    Args:
        date_str: Date string.
        time_str: Time string.

    Returns:
        Combined datetime.
    """
    date_dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))

    import re

    match = re.match(
        r'^(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s*(AM|PM))?$',
        time_str.strip(),
        re.IGNORECASE
    )

    if not match:
        raise ValueError(f"Invalid time format: {time_str}")

    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3)) if match.group(3) else 0
    meridiem = match.group(4)

    if meridiem:
        if meridiem.upper() == 'PM' and hour != 12:
            hour += 12
        elif meridiem.upper() == 'AM' and hour == 12:
            hour = 0

    return date_dt.replace(hour=hour, minute=minute, second=second)
