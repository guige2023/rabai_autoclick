"""
Datetime calculation and comparison actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def compare_dates(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> Dict[str, Any]:
    """
    Compare two dates and return detailed comparison.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        Comparison results.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    diff = dt2 - dt1

    return {
        'dt1': dt1.isoformat(),
        'dt2': dt2.isoformat(),
        'dt1_before_dt2': dt1 < dt2,
        'dt1_after_dt2': dt1 > dt2,
        'equal': dt1 == dt2,
        'difference_seconds': diff.total_seconds(),
        'difference_days': diff.days,
    }


def is_same_day(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> bool:
    """
    Check if two datetimes are on the same day.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        True if same day.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    return (dt1.year == dt2.year and
            dt1.month == dt2.month and
            dt1.day == dt2.day)


def is_same_month(
    dt1: Union[datetime, str],
    dt2: Union[datetime, str]
) -> bool:
    """
    Check if two datetimes are in the same month.

    Args:
        dt1: First datetime.
        dt2: Second datetime.

    Returns:
        True if same month.
    """
    if isinstance(dt1, str):
        dt1 = datetime.fromisoformat(dt1.replace('Z', '+00:00'))
    if isinstance(dt2, str):
        dt2 = datetime.fromisoformat(dt2.replace('Z', '+00:00'))

    return dt1.year == dt2.year and dt1.month == dt2.month


def get_month_start_end(year: int, month: int) -> Dict[str, datetime]:
    """
    Get start and end of a month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        Dictionary with 'start' and 'end' datetimes.
    """
    start = datetime(year, month, 1)

    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(seconds=1)

    return {'start': start, 'end': end}


def get_week_start_end(
    year: int,
    week: int
) -> Dict[str, datetime]:
    """
    Get start and end of an ISO week.

    Args:
        year: Year.
        week: ISO week number.

    Returns:
        Dictionary with 'start' and 'end' datetimes.
    """
    jan4 = datetime(year, 1, 4)

    week1_start = jan4 - timedelta(days=jan4.weekday())

    week_start = week1_start + timedelta(weeks=week - 1)
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    return {'start': week_start, 'end': week_end}


def iterate_days(
    start: Union[datetime, str],
    end: Union[datetime, str],
    step_days: int = 1
) -> List[datetime]:
    """
    Iterate through days between start and end.

    Args:
        start: Start datetime.
        end: End datetime.
        step_days: Number of days between each iteration.

    Returns:
        List of datetimes.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    days: List[datetime] = []
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)

    while current <= end:
        days.append(current)
        current += timedelta(days=step_days)

    return days


def iterate_months(
    start: Union[datetime, str],
    end: Union[datetime, str]
) -> List[Dict[str, Any]]:
    """
    Iterate through months between start and end.

    Args:
        start: Start datetime.
        end: End datetime.

    Returns:
        List of month dictionaries.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    months: List[Dict[str, Any]] = []
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end:
        month_start = current
        if current.month == 12:
            month_end = datetime(current.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            month_end = datetime(current.year, current.month + 1, 1) - timedelta(seconds=1)

        if month_end > end:
            month_end = end

        months.append({
            'year': current.year,
            'month': current.month,
            'start': month_start,
            'end': month_end,
        })

        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    return months


def get_next_business_day(dt: Union[datetime, str]) -> datetime:
    """
    Get the next business day after a date.

    Args:
        dt: Starting datetime.

    Returns:
        Next business day.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt + timedelta(days=1)

    while current.weekday() >= 5:
        current += timedelta(days=1)

    return current


def get_previous_business_day(dt: Union[datetime, str]) -> datetime:
    """
    Get the previous business day before a date.

    Args:
        dt: Starting datetime.

    Returns:
        Previous business day.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt - timedelta(days=1)

    while current.weekday() >= 5:
        current -= timedelta(days=1)

    return current


def is_leap_year(year: int) -> bool:
    """
    Check if a year is a leap year.

    Args:
        year: Year to check.

    Returns:
        True if leap year.
    """
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def get_days_in_year(year: int) -> int:
    """
    Get number of days in a year.

    Args:
        year: Year.

    Returns:
        Number of days.
    """
    return 366 if is_leap_year(year) else 365


def get_weekday_name(weekday: int) -> str:
    """
    Get name of weekday from number.

    Args:
        weekday: Weekday number (0=Mon, 6=Sun).

    Returns:
        Weekday name.
    """
    names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return names[weekday % 7]


def get_month_name(month: int) -> str:
    """
    Get name of month from number.

    Args:
        month: Month number (1-12).

    Returns:
        Month name.
    """
    names = [
        '', 'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    return names[month % 12]


def combine_date_and_time(
    date: Union[datetime, str],
    time_str: str
) -> datetime:
    """
    Combine a date with a time string.

    Args:
        date: Date object or string.
        time_str: Time string (e.g., '14:30' or '2:30 PM').

    Returns:
        Combined datetime.
    """
    if isinstance(date, str):
        date = datetime.fromisoformat(date.replace('Z', '+00:00'))

    import re

    match = re.match(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?', time_str.strip(), re.IGNORECASE)

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

    return date.replace(hour=hour, minute=minute, second=second)


def get_fiscal_quarter(dt: Union[datetime, str], fiscal_start_month: int = 1) -> int:
    """
    Get fiscal quarter for a date.

    Args:
        dt: Datetime object or ISO string.
        fiscal_start_month: Month where fiscal year starts (1-12).

    Returns:
        Fiscal quarter (1-4).
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    month = dt.month

    adjusted_month = month - fiscal_start_month + 1
    if adjusted_month <= 0:
        adjusted_month += 12

    return (adjusted_month - 1) // 3 + 1


def get_time_ago_dict(seconds_ago: int) -> Dict[str, int]:
    """
    Convert seconds to time units dict.

    Args:
        seconds_ago: Number of seconds ago.

    Returns:
        Dictionary with days, hours, minutes, seconds.
    """
    days = seconds_ago // 86400
    seconds_ago %= 86400

    hours = seconds_ago // 3600
    seconds_ago %= 3600

    minutes = seconds_ago // 60
    seconds = seconds_ago % 60

    return {
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
    }


def round_datetime(
    dt: Union[datetime, str],
    round_to: str = 'hour'
) -> datetime:
    """
    Round datetime to nearest interval.

    Args:
        dt: Datetime object or ISO string.
        round_to: What to round to ('second', 'minute', 'hour', 'day').

    Returns:
        Rounded datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if round_to == 'second':
        return dt.replace(microsecond=0)

    if round_to == 'minute':
        return dt.replace(second=0, microsecond=0)

    if round_to == 'hour':
        return dt.replace(minute=0, second=0, microsecond=0)

    if round_to == 'day':
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    return dt


def floor_datetime(
    dt: Union[datetime, str],
    floor_to: str = 'hour'
) -> datetime:
    """
    Floor datetime to nearest interval.

    Args:
        dt: Datetime object or ISO string.
        floor_to: What to floor to ('minute', 'hour', 'day').

    Returns:
        Floored datetime.
    """
    return round_datetime(dt, floor_to)
