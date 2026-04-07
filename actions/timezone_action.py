"""
Timezone conversion and formatting actions.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, List, Any, Union


# Common timezone mappings
TIMEZONE_MAP: Dict[str, str] = {
    'UTC': 'UTC',
    'GMT': 'UTC',
    'EST': 'America/New_York',
    'EDT': 'America/New_York',
    'CST': 'America/Chicago',
    'CDT': 'America/Chicago',
    'MST': 'America/Denver',
    'MDT': 'America/Denver',
    'PST': 'America/Los_Angeles',
    'PDT': 'America/Los_Angeles',
    'JST': 'Asia/Tokyo',
    'CCT': 'Asia/Shanghai',
    'SGT': 'Asia/Singapore',
    'HKT': 'Asia/Hong_Kong',
    'KST': 'Asia/Seoul',
    'IST': 'Asia/Kolkata',
    'BST': 'Europe/London',
    'CET': 'Europe/Paris',
    'CEST': 'Europe/Paris',
    'EET': 'Europe/Helsinki',
    'EEST': 'Europe/Helsinki',
    'AEST': 'Australia/Sydney',
    'AEDT': 'Australia/Sydney',
    'NZST': 'Pacific/Auckland',
    'NZDT': 'Pacific/Auckland',
}

CITY_TIMEZONES: Dict[str, str] = {
    'new_york': 'America/New_York',
    'los_angeles': 'America/Los_Angeles',
    'chicago': 'America/Chicago',
    'denver': 'America/Denver',
    'london': 'Europe/London',
    'paris': 'Europe/Paris',
    'berlin': 'Europe/Berlin',
    'tokyo': 'Asia/Tokyo',
    'shanghai': 'Asia/Shanghai',
    'beijing': 'Asia/Shanghai',
    'hong_kong': 'Asia/Hong_Kong',
    'singapore': 'Asia/Singapore',
    'sydney': 'Australia/Sydney',
    'dubai': 'Asia/Dubai',
    'mumbai': 'Asia/Kolkata',
    'delhi': 'Asia/Kolkata',
    'seoul': 'Asia/Seoul',
    'toronto': 'America/Toronto',
    'vancouver': 'America/Vancouver',
    'amsterdam': 'Europe/Amsterdam',
    'moscow': 'Europe/Moscow',
}


def get_current_time(timezone_str: str = 'UTC') -> datetime:
    """
    Get the current time in a specific timezone.

    Args:
        timezone_str: Timezone name or abbreviation.

    Returns:
        Current datetime in the specified timezone.

    Raises:
        ValueError: If timezone is not recognized.
    """
    tz = _get_timezone(timezone_str)
    return datetime.now(tz)


def convert_timezone(
    dt: Union[datetime, str],
    from_tz: str,
    to_tz: str
) -> datetime:
    """
    Convert a datetime from one timezone to another.

    Args:
        dt: The datetime to convert (or ISO string).
        from_tz: Source timezone.
        to_tz: Target timezone.

    Returns:
        Converted datetime in the target timezone.

    Raises:
        ValueError: If timezone is not recognized.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    from_timezone = _get_timezone(from_tz)
    to_timezone = _get_timezone(to_tz)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=from_timezone)

    return dt.astimezone(to_timezone)


def _get_timezone(tz_str: str) -> timezone:
    """
    Resolve a timezone string to a timezone object.

    Args:
        tz_str: Timezone name or abbreviation.

    Returns:
        timezone object.

    Raises:
        ValueError: If timezone is not recognized.
    """
    tz_str = tz_str.upper()

    if tz_str in TIMEZONE_MAP:
        tz_str = TIMEZONE_MAP[tz_str]

    if tz_str.lower() in CITY_TIMEZONES:
        tz_str = CITY_TIMEZONES[tz_str.lower()]

    try:
        from datetime import timezone as tz_module
        import zoneinfo
        return tz_module.ZoneInfo(tz_str)
    except (ImportError, KeyError):
        try:
            import pytz
            return pytz.timezone(tz_str).zone
        except Exception:
            pass

    offset_map = {
        'UTC+0': 0, 'UTC+1': 1, 'UTC+2': 2, 'UTC+3': 3,
        'UTC+4': 4, 'UTC+5': 5, 'UTC+6': 6, 'UTC+7': 7,
        'UTC+8': 8, 'UTC+9': 9, 'UTC+10': 10, 'UTC+11': 11,
        'UTC+12': 12, 'UTC-1': -1, 'UTC-2': -2, 'UTC-3': -3,
        'UTC-4': -4, 'UTC-5': -5, 'UTC-6': -6, 'UTC-7': -7,
        'UTC-8': -8, 'UTC-9': -9, 'UTC-10': -10, 'UTC-11': -11,
    }

    if tz_str in offset_map:
        from datetime import timezone as tz_module, timedelta
        return tz_module(timedelta(hours=offset_map[tz_str]))

    raise ValueError(f"Unrecognized timezone: {tz_str}")


def format_time(
    dt: Union[datetime, str],
    timezone_str: str,
    format_string: str = '%Y-%m-%d %H:%M:%S %Z'
) -> str:
    """
    Format a datetime in a specific timezone.

    Args:
        dt: The datetime to format (or ISO string).
        timezone_str: Target timezone.
        format_string: strftime format string.

    Returns:
        Formatted time string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    converted = convert_timezone(dt, 'UTC', timezone_str)
    return converted.strftime(format_string)


def get_timezone_offset(timezone_str: str, date: Optional[str] = None) -> int:
    """
    Get the UTC offset in hours for a timezone.

    Args:
        timezone_str: Timezone name.
        date: Optional date for DST-aware offset.

    Returns:
        UTC offset in hours.
    """
    try:
        from datetime import timezone as tz_module
        import zoneinfo
        tz = tz_module.ZoneInfo(timezone_str)
    except Exception:
        try:
            import pytz
            tz = pytz.timezone(timezone_str)
        except Exception:
            raise ValueError(f"Unrecognized timezone: {timezone_str}")

    now = datetime.now()
    if date:
        now = datetime.fromisoformat(date)

    return tz.utcoffset(now).total_seconds() / 3600


def list_common_timezones() -> List[Dict[str, Any]]:
    """
    List common timezone abbreviations with their full names and offsets.

    Returns:
        List of timezone information dictionaries.
    """
    common = [
        ('UTC', 'UTC', 0),
        ('EST', 'America/New_York', -5),
        ('EDT', 'America/New_York', -4),
        ('CST', 'America/Chicago', -6),
        ('CDT', 'America/Chicago', -5),
        ('MST', 'America/Denver', -7),
        ('MDT', 'America/Denver', -6),
        ('PST', 'America/Los_Angeles', -8),
        ('PDT', 'America/Los_Angeles', -7),
        ('JST', 'Asia/Tokyo', 9),
        ('CCT', 'Asia/Shanghai', 8),
        ('SGT', 'Asia/Singapore', 8),
        ('HKT', 'Asia/Hong_Kong', 8),
        ('KST', 'Asia/Seoul', 9),
        ('IST', 'Asia/Kolkata', 5.5),
        ('BST', 'Europe/London', 1),
        ('CET', 'Europe/Paris', 1),
        ('CEST', 'Europe/Paris', 2),
        ('AEST', 'Australia/Sydney', 10),
        ('AEDT', 'Australia/Sydney', 11),
    ]

    return [
        {'abbrev': abbrev, 'name': name, 'offset_hours': offset}
        for abbrev, name, offset in common
    ]


def calculate_time_difference(
    timezone1: str,
    timezone2: str,
    reference_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate the time difference between two timezones.

    Args:
        timezone1: First timezone.
        timezone2: Second timezone.
        reference_date: Optional reference date.

    Returns:
        Dictionary with time difference information.
    """
    now = get_current_time(timezone1)
    now2 = get_current_time(timezone2)

    delta = now2.hour - now.hour
    if now2.day != now.day:
        delta += 24 if now2.day > now.day else -24

    return {
        'timezone1': timezone1,
        'timezone2': timezone2,
        'time1': now.strftime('%H:%M'),
        'time2': now2.strftime('%H:%M'),
        'difference_hours': delta,
        'difference_minutes': delta * 60,
        'same_time': delta == 0,
    }


def get_next_occurrence(
    hour: int,
    minute: int,
    timezone_str: str,
    from_time: Optional[datetime] = None
) -> datetime:
    """
    Get the next occurrence of a specific time in a timezone.

    Args:
        hour: Hour (0-23).
        minute: Minute (0-59).
        timezone_str: Target timezone.
        from_time: Starting point (defaults to current time).

    Returns:
        Next occurrence datetime.
    """
    current = from_time or get_current_time(timezone_str)

    target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if target <= current:
        target = target.replace(day=target.day + 1)

    return target


def is_dst(timezone_str: str, date: Optional[str] = None) -> bool:
    """
    Check if a timezone is in Daylight Saving Time.

    Args:
        timezone_str: Timezone name.
        date: Optional date to check.

    Returns:
        True if DST is active.
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(timezone_str)
    except Exception:
        try:
            import pytz
            tz = pytz.timezone(timezone_str)
        except Exception:
            return False

    check_date = datetime.now()
    if date:
        check_date = datetime.fromisoformat(date)

    if hasattr(tz, 'localize'):
        aware_date = tz.localize(check_date)
    else:
        aware_date = check_date.replace(tzinfo=tz)

    return bool(aware_date.dst())


def convert_between_zones(
    utc_datetime: Union[datetime, str],
    zones: List[str]
) -> Dict[str, str]:
    """
    Convert a UTC datetime to multiple timezones at once.

    Args:
        utc_datetime: UTC datetime (or ISO string).
        zones: List of target timezone names.

    Returns:
        Dictionary mapping timezone names to formatted time strings.
    """
    if isinstance(utc_datetime, str):
        utc_datetime = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))

    results = {}
    for zone in zones:
        converted = convert_timezone(utc_datetime, 'UTC', zone)
        results[zone] = converted.strftime('%Y-%m-%d %H:%M:%S %Z')

    return results


def parse_time_string(time_str: str, timezone_str: str = 'UTC') -> datetime:
    """
    Parse a time string into a timezone-aware datetime.

    Args:
        time_str: Time string (various formats supported).
        timezone_str: Timezone for the parsed time.

    Returns:
        Parsed datetime in the specified timezone.

    Raises:
        ValueError: If time string cannot be parsed.
    """
    import re

    time_str = time_str.strip()

    time_patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})', '%Y-%m-%d %H:%M:%S'),
        (r'(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2}):(\d{2})', '%m/%d/%Y %H:%M:%S'),
        (r'(\d{2})-(\d{2})-(\d{4}) (\d{2}):(\d{2}):(\d{2})', '%d-%m-%Y %H:%M:%S'),
        (r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})', '%Y-%m-%d %H:%M'),
        (r'(\d{2}):(\d{2}):(\d{2})', '%H:%M:%S'),
        (r'(\d{2}):(\d{2})', '%H:%M'),
    ]

    for pattern, fmt in time_patterns:
        match = re.match(pattern, time_str)
        if match:
            base_date = datetime.now()
            try:
                return datetime.strptime(time_str[:len(fmt)], fmt).replace(
                    year=base_date.year, month=base_date.month, day=base_date.day
                )
            except ValueError:
                return datetime.strptime(time_str, fmt)

    raise ValueError(f"Could not parse time string: {time_str}")


def get_timezone_name(timezone_str: str) -> str:
    """
    Get the full IANA timezone name from an abbreviation.

    Args:
        timezone_str: Timezone abbreviation or city name.

    Returns:
        Full IANA timezone name.
    """
    tz_str = timezone_str.upper()

    if tz_str in TIMEZONE_MAP:
        return TIMEZONE_MAP[tz_str]

    if timezone_str.lower() in CITY_TIMEZONES:
        return CITY_TIMEZONES[timezone_str.lower()]

    return timezone_str
