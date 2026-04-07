"""
Datetime advanced operations and business day calculations.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_business_days_remaining(
    end_date: Union[datetime, str],
    holidays: Optional[List[Union[datetime, str]]] = None
) -> int:
    """
    Get number of business days remaining until end date.

    Args:
        end_date: End date.
        holidays: Optional list of holiday dates.

    Returns:
        Number of business days.
    """
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

    today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if end_date <= today:
        return 0

    holiday_set = set()
    if holidays:
        for h in holidays:
            if isinstance(h, str):
                h = datetime.fromisoformat(h.replace('Z', '+00:00'))
            holiday_set.add(h.date())

    count = 0
    current = today

    while current <= end_date:
        if current.weekday() < 5 and current.date() not in holiday_set:
            count += 1
        current += timedelta(days=1)

    return count


def get_business_days_elapsed(
    start_date: Union[datetime, str],
    holidays: Optional[List[Union[datetime, str]]] = None
) -> int:
    """
    Get number of business days elapsed since start date.

    Args:
        start_date: Start date.
        holidays: Optional list of holiday dates.

    Returns:
        Number of business days.
    """
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))

    today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if today <= start_date:
        return 0

    holiday_set = set()
    if holidays:
        for h in holidays:
            if isinstance(h, str):
                h = datetime.fromisoformat(h.replace('Z', '+00:00'))
            holiday_set.add(h.date())

    count = 0
    current = start_date

    while current < today:
        if current.weekday() < 5 and current.date() not in holiday_set:
            count += 1
        current += timedelta(days=1)

    return count


def add_working_days(
    start_date: Union[datetime, str],
    days: int,
    holidays: Optional[List[Union[datetime, str]]] = None
) -> datetime:
    """
    Add working days to a date.

    Args:
        start_date: Starting date.
        days: Number of working days to add.
        holidays: Optional list of holidays.

    Returns:
        Result date.
    """
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))

    holiday_set = set()
    if holidays:
        for h in holidays:
            if isinstance(h, str):
                h = datetime.fromisoformat(h.replace('Z', '+00:00'))
            holiday_set.add(h.date())

    direction = 1 if days >= 0 else -1
    remaining = abs(days)
    current = start_date

    while remaining > 0:
        current += timedelta(days=direction)

        if current.weekday() < 5 and current.date() not in holiday_set:
            remaining -= 1

    return current


def get_working_days_between(
    start_date: Union[datetime, str],
    end_date: Union[datetime, str],
    holidays: Optional[List[Union[datetime, str]]] = None
) -> int:
    """
    Get number of working days between two dates.

    Args:
        start_date: Start date.
        end_date: End date.
        holidays: Optional list of holidays.

    Returns:
        Number of working days.
    """
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

    holiday_set = set()
    if holidays:
        for h in holidays:
            if isinstance(h, str):
                h = datetime.fromisoformat(h.replace('Z', '+00:00'))
            holiday_set.add(h.date())

    count = 0
    current = start_date

    while current <= end_date:
        if current.weekday() < 5 and current.date() not in holiday_set:
            count += 1
        current += timedelta(days=1)

    return count


def get_quarter_start_end(
    year: int,
    quarter: int
) -> Dict[str, datetime]:
    """
    Get start and end of fiscal quarter.

    Args:
        year: Year.
        quarter: Quarter (1-4).

    Returns:
        Dictionary with 'start' and 'end'.
    """
    quarter_starts = {
        1: (1, 1),
        2: (4, 1),
        3: (7, 1),
        4: (10, 1),
    }

    if quarter not in quarter_starts:
        raise ValueError(f"Invalid quarter: {quarter}")

    month, day = quarter_starts[quarter]
    start = datetime(year, month, day)

    if quarter == 4:
        end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
    else:
        next_quarter_month = month + 3
        end = datetime(year, next_quarter_month, day) - timedelta(seconds=1)

    return {'start': start, 'end': end}


def get_week_start_end(
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

    week1_monday = jan4 - timedelta(days=jan4.weekday())

    week_start = week1_monday + timedelta(weeks=week - 1)
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    return {'start': week_start, 'end': week_end}


def is_valid_time_string(time_str: str) -> bool:
    """
    Validate a time string.

    Args:
        time_str: Time string to validate.

    Returns:
        True if valid.
    """
    import re

    patterns = [
        r'^\d{1,2}:\d{2}$',
        r'^\d{1,2}:\d{2}:\d{2}$',
        r'^\d{1,2}:\d{2}\s*(AM|PM)$',
        r'^\d{1,2}:\d{2}:\d{2}\s*(AM|PM)$',
    ]

    for pattern in patterns:
        if re.match(pattern, time_str.strip(), re.IGNORECASE):
            return True

    return False


def parse_time_string(time_str: str) -> Dict[str, int]:
    """
    Parse time string to components.

    Args:
        time_str: Time string.

    Returns:
        Dictionary with hour, minute, second.
    """
    import re

    match = re.match(
        r'^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$',
        time_str.strip(),
        re.IGNORECASE
    )

    if not match:
        raise ValueError(f"Invalid time string: {time_str}")

    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3)) if match.group(3) else 0
    meridiem = match.group(4)

    if meridiem:
        if meridiem.upper() == 'PM' and hour != 12:
            hour += 12
        elif meridiem.upper() == 'AM' and hour == 12:
            hour = 0

    return {'hour': hour, 'minute': minute, 'second': second}


def get_time_components(dt: Union[datetime, str]) -> Dict[str, int]:
    """
    Get time components from datetime.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Dictionary with time components.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return {
        'year': dt.year,
        'month': dt.month,
        'day': dt.day,
        'hour': dt.hour,
        'minute': dt.minute,
        'second': dt.second,
        'microsecond': dt.microsecond,
        'weekday': dt.weekday(),
        'day_name': dt.strftime('%A'),
        'month_name': dt.strftime('%B'),
        'day_of_year': dt.timetuple().tm_yday,
        'week_of_year': dt.isocalendar()[1],
        'quarter': (dt.month - 1) // 3 + 1,
    }


def get_day_start(dt: Union[datetime, str]) -> datetime:
    """
    Get start of day (midnight).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of day.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def get_day_end(dt: Union[datetime, str]) -> datetime:
    """
    Get end of day (23:59:59).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of day.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def get_week_start(dt: Union[datetime, str]) -> datetime:
    """
    Get start of week (Monday).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of week.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    days_since_monday = dt.weekday()
    return get_day_start(dt - timedelta(days=days_since_monday))


def get_week_end(dt: Union[datetime, str]) -> datetime:
    """
    Get end of week (Sunday).

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of week.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    days_until_sunday = 6 - dt.weekday()
    return get_day_end(dt + timedelta(days=days_until_sunday))


def get_month_start(dt: Union[datetime, str]) -> datetime:
    """
    Get start of month.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of month.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_month_end(dt: Union[datetime, str]) -> datetime:
    """
    Get end of month.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of month.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    next_month = dt.replace(day=28) + timedelta(days=4)
    last_day = next_month.replace(day=1) - timedelta(days=1)

    return last_day.replace(hour=23, minute=59, second=59, microsecond=999999)


def get_year_start(dt: Union[datetime, str]) -> datetime:
    """
    Get start of year.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Start of year.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return datetime(dt.year, 1, 1)


def get_year_end(dt: Union[datetime, str]) -> datetime:
    """
    Get end of year.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        End of year.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return datetime(dt.year, 12, 31, 23, 59, 59, 999999)


def get_quarter_of(dt: Union[datetime, str]) -> int:
    """
    Get quarter of year for datetime.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Quarter (1-4).
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return (dt.month - 1) // 3 + 1
