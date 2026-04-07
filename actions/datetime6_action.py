"""
Datetime validation and conversion actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def validate_date_format(
    date_string: str,
    format_string: str
) -> bool:
    """
    Validate a date string against a format.

    Args:
        date_string: Date string to validate.
        format_string: Expected format string.

    Returns:
        True if valid.
    """
    try:
        datetime.strptime(date_string, format_string)
        return True
    except ValueError:
        return False


def validate_iso_date(date_string: str) -> bool:
    """
    Validate an ISO format date string.

    Args:
        date_string: ISO date string.

    Returns:
        True if valid ISO date.
    """
    try:
        datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


def validate_date_range(
    dt: Union[datetime, str],
    min_date: Optional[Union[datetime, str]] = None,
    max_date: Optional[Union[datetime, str]] = None
) -> Dict[str, Any]:
    """
    Validate if date is within a range.

    Args:
        dt: Datetime to validate.
        min_date: Minimum allowed date.
        max_date: Maximum allowed date.

    Returns:
        Validation result.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    result = {'valid': True, 'errors': []}

    if min_date:
        if isinstance(min_date, str):
            min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))

        if dt < min_date:
            result['valid'] = False
            result['errors'].append(f'Date is before minimum: {min_date}')

    if max_date:
        if isinstance(max_date, str):
            max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))

        if dt > max_date:
            result['valid'] = False
            result['errors'].append(f'Date is after maximum: {max_date}')

    return result


def parse_multi_date(date_string: str) -> List[datetime]:
    """
    Parse multiple dates from a string.

    Args:
        date_string: String with dates separated by comma or 'and'.

    Returns:
        List of parsed datetimes.
    """
    import re

    parts = re.split(r'[,;]+| and ', date_string)

    dates = []
    for part in parts:
        part = part.strip()
        try:
            dt = datetime.fromisoformat(part.replace('Z', '+00:00'))
            dates.append(dt)
        except ValueError:
            pass

    return dates


def convert_to_utc(
    dt: Union[datetime, str],
    source_tz: Optional[str] = None
) -> datetime:
    """
    Convert datetime to UTC.

    Args:
        dt: Datetime to convert.
        source_tz: Source timezone name.

    Returns:
        UTC datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if dt.tzinfo is None:
        if source_tz:
            from dateutil import tz
            source = tz.gettz(source_tz)
            dt = dt.replace(tzinfo=source)

    return dt.astimezone(timezone.utc)


def convert_from_utc(
    dt: Union[datetime, str],
    target_tz: str
) -> datetime:
    """
    Convert UTC datetime to target timezone.

    Args:
        dt: UTC datetime.
        target_tz: Target timezone name.

    Returns:
        Target timezone datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    from dateutil import tz
    target = tz.gettz(target_tz)

    return dt.astimezone(target)


def get_timezone_name_list() -> List[str]:
    """
    Get list of common timezone names.

    Returns:
        List of timezone names.
    """
    return [
        'UTC',
        'America/New_York',
        'America/Chicago',
        'America/Denver',
        'America/Los_Angeles',
        'America/Toronto',
        'America/Vancouver',
        'America/Sao_Paulo',
        'Europe/London',
        'Europe/Paris',
        'Europe/Berlin',
        'Europe/Moscow',
        'Asia/Tokyo',
        'Asia/Shanghai',
        'Asia/Hong_Kong',
        'Asia/Singapore',
        'Asia/Seoul',
        'Asia/Kolkata',
        'Asia/Dubai',
        'Australia/Sydney',
        'Australia/Melbourne',
        'Pacific/Auckland',
        'Pacific/Honolulu',
    ]


def is_dst_time(dt: Union[datetime, str], tz_name: str) -> bool:
    """
    Check if datetime is in DST for timezone.

    Args:
        dt: Datetime to check.
        tz_name: Timezone name.

    Returns:
        True if in DST.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    try:
        from dateutil import tz
        tz_obj = tz.gettz(tz_name)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz_obj)

        return bool(dt.dst())
    except ImportError:
        return False


def get_dst_transition_dates(year: int, tz_name: str) -> Dict[str, Any]:
    """
    Get DST transition dates for a timezone in a year.

    Args:
        year: Year.
        tz_name: Timezone name.

    Returns:
        Dictionary with DST start and end dates.
    """
    try:
        from dateutil import tz
        import datetime as dt_module

        tz_obj = tz.gettz(tz_name)

        jan1 = dt_module.datetime(year, 1, 1, tzinfo=tz_obj)
        jul1 = dt_module.datetime(year, 7, 1, tzinfo=tz_obj)

        jan_dst = jan1.dst()
        jul_dst = jul1.dst()

        if jan_dst is None or jul_dst is None:
            return {'has_dst': False}

        if jan_dst == jul_dst:
            return {'has_dst': False}

        if jan_dst > jul_dst:
            dst_start = jul1
            dst_end = jan1
        else:
            dst_start = jan1
            dst_end = jul1

        return {
            'has_dst': True,
            'dst_start': dst_start,
            'dst_end': dst_end,
        }
    except ImportError:
        return {'has_dst': False}


def get_age_in_days(birth_date: Union[datetime, str]) -> int:
    """
    Get age in days from birth date.

    Args:
        birth_date: Birth date.

    Returns:
        Age in days.
    """
    if isinstance(birth_date, str):
        birth_date = datetime.fromisoformat(birth_date.replace('Z', '+00:00'))

    today = datetime.now()
    delta = today - birth_date
    return delta.days


def get_age_in_years(birth_date: Union[datetime, str]) -> int:
    """
    Get age in years from birth date.

    Args:
        birth_date: Birth date.

    Returns:
        Age in years.
    """
    if isinstance(birth_date, str):
        birth_date = datetime.fromisoformat(birth_date.replace('Z', '+00:00'))

    today = datetime.now()
    age = today.year - birth_date.year

    if (today.month, today.day) < (birth_date.month, birth_date.day):
        age -= 1

    return age


def is_past(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is in the past.

    Args:
        dt: Datetime to check.

    Returns:
        True if past.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt < datetime.now()


def is_future(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is in the future.

    Args:
        dt: Datetime to check.

    Returns:
        True if future.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt > datetime.now()


def is_today(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is today.

    Args:
        dt: Datetime to check.

    Returns:
        True if today.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    today = datetime.now()
    return (dt.year, dt.month, dt.day) == (today.year, today.month, today.day)


def is_tomorrow(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is tomorrow.

    Args:
        dt: Datetime to check.

    Returns:
        True if tomorrow.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    tomorrow = datetime.now() + timedelta(days=1)
    return (dt.year, dt.month, dt.day) == (tomorrow.year, tomorrow.month, tomorrow.day)


def is_yesterday(dt: Union[datetime, str]) -> bool:
    """
    Check if datetime is yesterday.

    Args:
        dt: Datetime to check.

    Returns:
        True if yesterday.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    yesterday = datetime.now() - timedelta(days=1)
    return (dt.year, dt.month, dt.day) == (yesterday.year, yesterday.month, yesterday.day)


def format_date_custom(
    dt: Union[datetime, str],
    format_type: str = 'default'
) -> str:
    """
    Format datetime with custom style presets.

    Args:
        dt: Datetime object or ISO string.
        format_type: Format type ('default', 'us', 'eu', 'iso', 'friendly').

    Returns:
        Formatted date string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if format_type == 'us':
        return dt.strftime('%m/%d/%Y')

    if format_type == 'eu':
        return dt.strftime('%d/%m/%Y')

    if format_type == 'iso':
        return dt.isoformat()

    if format_type == 'friendly':
        return dt.strftime('%B %d, %Y')

    if format_type == 'short':
        return dt.strftime('%b %d, %y')

    return dt.strftime('%Y-%m-%d %H:%M:%S')


def parse_american_date(date_string: str) -> Optional[datetime]:
    """
    Parse American date format (MM/DD/YYYY).

    Args:
        date_string: American format date string.

    Returns:
        Datetime or None.
    """
    try:
        return datetime.strptime(date_string, '%m/%d/%Y')
    except ValueError:
        pass

    try:
        return datetime.strptime(date_string, '%m/%d/%y')
    except ValueError:
        return None


def parse_european_date(date_string: str) -> Optional[datetime]:
    """
    Parse European date format (DD/MM/YYYY).

    Args:
        date_string: European format date string.

    Returns:
        Datetime or None.
    """
    try:
        return datetime.strptime(date_string, '%d/%m/%Y')
    except ValueError:
        pass

    try:
        return datetime.strptime(date_string, '%d/%m/%y')
    except ValueError:
        return None


def get_time_until(dt: Union[datetime, str]) -> Dict[str, int]:
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

    if dt < now:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'is_past': True}

    delta = dt - now
    total_seconds = int(delta.total_seconds())

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return {
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'is_past': False,
    }


def get_time_since(dt: Union[datetime, str]) -> Dict[str, int]:
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

    if dt > now:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'is_future': True}

    delta = now - dt
    total_seconds = int(delta.total_seconds())

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return {
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'is_future': False,
    }
