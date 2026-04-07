"""
Time and date operations advanced actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_time_until_next_hour() -> Dict[str, int]:
    """
    Get time until next hour.

    Returns:
        Minutes and seconds until next hour.
    """
    now = datetime.now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    diff = next_hour - now

    total_seconds = int(diff.total_seconds())
    minutes = total_seconds // 60
    seconds = total_seconds % 60

    return {
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
    }


def get_time_until_next_day() -> Dict[str, int]:
    """
    Get time until midnight.

    Returns:
        Hours, minutes, seconds until midnight.
    """
    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    diff = midnight - now

    total_seconds = int(diff.total_seconds())
    hours = total_seconds // 3600
    remaining = total_seconds % 3600
    minutes = remaining // 60
    seconds = remaining % 60

    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
    }


def get_weekday_index(day_name: str) -> int:
    """
    Get weekday index from name.

    Args:
        day_name: Day name (e.g., 'Monday').

    Returns:
        Weekday index (0=Monday, 6=Sunday).
    """
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    day_lower = day_name.lower()

    if day_lower in days:
        return days.index(day_lower)

    return -1


def get_month_index(month_name: str) -> int:
    """
    Get month index from name.

    Args:
        month_name: Month name (e.g., 'January').

    Returns:
        Month index (1-12) or 0 if not found.
    """
    months = [
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december'
    ]

    month_lower = month_name.lower()

    if month_lower in months:
        return months.index(month_lower) + 1

    return 0


def get_days_in_month(year: int, month: int) -> int:
    """
    Get number of days in a month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        Number of days.
    """
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    last_day = next_month - timedelta(days=1)

    return last_day.day


def is_valid_date(year: int, month: int, day: int) -> bool:
    """
    Validate a date.

    Args:
        year: Year.
        month: Month.
        day: Day.

    Returns:
        True if valid date.
    """
    try:
        datetime(year, month, day)
        return True
    except ValueError:
        return False


def get_quarter_dates(year: int, quarter: int) -> Dict[str, datetime]:
    """
    Get start and end of quarter.

    Args:
        year: Year.
        quarter: Quarter (1-4).

    Returns:
        Dictionary with 'start' and 'end'.
    """
    quarters = {
        1: (1, 1, 3, 31),
        2: (4, 1, 6, 30),
        3: (7, 1, 9, 30),
        4: (10, 1, 12, 31),
    }

    if quarter not in quarters:
        raise ValueError(f"Invalid quarter: {quarter}")

    start_month, start_day, end_month, end_day = quarters[quarter]

    return {
        'start': datetime(year, start_month, start_day),
        'end': datetime(year, end_month, end_day, 23, 59, 59),
    }


def get_week_number(dt: Union[datetime, str]) -> int:
    """
    Get ISO week number.

    Args:
        dt: Datetime or ISO string.

    Returns:
        Week number.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.isocalendar()[1]


def get_day_of_year(dt: Union[datetime, str]) -> int:
    """
    Get day of year.

    Args:
        dt: Datetime or ISO string.

    Returns:
        Day of year (1-366).
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.timetuple().tm_yday


def get_timezone_abbreviation(tz_name: str) -> str:
    """
    Get timezone abbreviation.

    Args:
        tz_name: Timezone name.

    Returns:
        Abbreviation (e.g., 'PST', 'EST').
    """
    try:
        from dateutil import tz
        import datetime as dt_module

        tz_obj = tz.gettz(tz_name)
        now = dt_module.datetime.now(tz_obj)

        abbreviations = {
            'America/Los_Angeles': 'PST',
            'America/New_York': 'EST',
            'America/Chicago': 'CST',
            'America/Denver': 'MST',
            'Europe/London': 'GMT',
            'Europe/Paris': 'CET',
            'Asia/Tokyo': 'JST',
            'Asia/Shanghai': 'CST',
            'Asia/Kolkata': 'IST',
            'Australia/Sydney': 'AEST',
            'UTC': 'UTC',
        }

        return abbreviations.get(tz_name, tz_name.split('/')[-1])
    except Exception:
        return tz_name


def convert_timestamp_to_iso(timestamp: float) -> str:
    """
    Convert Unix timestamp to ISO string.

    Args:
        timestamp: Unix timestamp.

    Returns:
        ISO formatted string.
    """
    return datetime.utcfromtimestamp(timestamp).isoformat()


def get_current_unix_timestamp() -> int:
    """
    Get current Unix timestamp.

    Returns:
        Unix timestamp.
    """
    return int(datetime.now().timestamp())


def get_current_unix_timestamp_ms() -> int:
    """
    Get current Unix timestamp in milliseconds.

    Returns:
        Unix timestamp in ms.
    """
    return int(datetime.now().timestamp() * 1000)


def format_timestamp(timestamp: float, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    Format Unix timestamp.

    Args:
        timestamp: Unix timestamp.
        format_str: strftime format string.

    Returns:
        Formatted string.
    """
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime(format_str)


def parse_timestamp_utc(timestamp: float) -> datetime:
    """
    Parse timestamp as UTC.

    Args:
        timestamp: Unix timestamp.

    Returns:
        UTC datetime.
    """
    return datetime.utcfromtimestamp(timestamp)


def get_time_until_midnight() -> Dict[str, int]:
    """
    Get time until midnight.

    Returns:
        Hours, minutes, seconds until midnight.
    """
    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    diff = midnight - now

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


def get_time_from_now(
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Get datetime from now plus offset.

    Args:
        hours: Hours to add.
        minutes: Minutes to add.
        seconds: Seconds to add.

    Returns:
        Future datetime.
    """
    return datetime.now() + timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


def get_time_ago(
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    Get datetime from now minus offset.

    Args:
        hours: Hours to subtract.
        minutes: Minutes to subtract.
        seconds: Seconds to subtract.

    Returns:
        Past datetime.
    """
    return datetime.now() - timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


def format_time_12h(hours: int, minutes: int, seconds: int = 0) -> str:
    """
    Format time in 12-hour format.

    Args:
        hours: Hour (0-23).
        minutes: Minutes.
        seconds: Seconds.

    Returns:
        Formatted time string.
    """
    if hours == 0:
        h = 12
        meridiem = 'AM'
    elif hours < 12:
        h = hours
        meridiem = 'AM'
    elif hours == 12:
        h = 12
        meridiem = 'PM'
    else:
        h = hours - 12
        meridiem = 'PM'

    if seconds:
        return f'{h}:{minutes:02d}:{seconds:02d} {meridiem}'

    return f'{h}:{minutes:02d} {meridiem}'


def format_time_24h(hours: int, minutes: int, seconds: int = 0) -> str:
    """
    Format time in 24-hour format.

    Args:
        hours: Hour.
        minutes: Minutes.
        seconds: Seconds.

    Returns:
        Formatted time string.
    """
    if seconds:
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'

    return f'{hours:02d}:{minutes:02d}'


def compare_times(
    time1: str,
    time2: str
) -> Dict[str, Any]:
    """
    Compare two time strings.

    Args:
        time1: First time.
        time2: Second time.

    Returns:
        Comparison result.
    """
    import re

    def parse_time(t: str) -> int:
        match = re.match(r'^(\d{1,2}):(\d{2})(?::(\d{2}))?', t)
        if match:
            h = int(match.group(1))
            m = int(match.group(2))
            s = int(match.group(3)) if match.group(3) else 0
            return h * 3600 + m * 60 + s
        return 0

    t1_seconds = parse_time(time1)
    t2_seconds = parse_time(time2)

    return {
        'time1': time1,
        'time2': time2,
        'time1_seconds': t1_seconds,
        'time2_seconds': t2_seconds,
        'time1_before_time2': t1_seconds < t2_seconds,
        'time1_after_time2': t1_seconds > t2_seconds,
        'equal': t1_seconds == t2_seconds,
    }
