"""
Datetime utilities and calendar actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def get_time_slots(
    start: Union[datetime, str],
    end: Union[datetime, str],
    slot_duration_minutes: int = 60
) -> List[Dict[str, Any]]:
    """
    Generate time slots between start and end.

    Args:
        start: Start datetime.
        end: End datetime.
        slot_duration_minutes: Duration of each slot in minutes.

    Returns:
        List of slot dictionaries.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    slots = []
    current = start

    while current < end:
        slot_end = current + timedelta(minutes=slot_duration_minutes)

        if slot_end > end:
            slot_end = end

        slots.append({
            'start': current,
            'end': slot_end,
            'duration_minutes': int((slot_end - current).total_seconds() / 60),
        })

        current = slot_end

    return slots


def get_holidays(
    year: int,
    country: str = 'US'
) -> List[Dict[str, Any]]:
    """
    Get major holidays for a year (simplified).

    Args:
        year: Year.
        country: Country code.

    Returns:
        List of holiday dictionaries.
    """
    holidays = []

    holidays.append({
        'name': "New Year's Day",
        'date': datetime(year, 1, 1).date(),
        'month': 1,
        'day': 1,
    })

    if country == 'US':
        holidays.append({
            'name': "Independence Day",
            'date': datetime(year, 7, 4).date(),
            'month': 7,
            'day': 4,
        })

        holidays.append({
            'name': "Christmas Day",
            'date': datetime(year, 12, 25).date(),
            'month': 12,
            'day': 25,
        })

        jan1 = datetime(year, 1, 1)
        mlk_day = jan1 + timedelta(days=(7 - jan1.weekday()) % 7 + 14)
        holidays.append({
            'name': "Martin Luther King Jr. Day",
            'date': mlk_day.date(),
            'month': 1,
            'day': mlk_day.day,
        })

        feb1 = datetime(year, 2, 1)
        presidents_day = feb1 + timedelta(days=(7 - feb1.weekday()) % 7 + 14)
        holidays.append({
            'name': "Presidents' Day",
            'date': presidents_day.date(),
            'month': 2,
            'day': presidents_day.day,
        })

        may31 = datetime(year, 5, 31)
        memorial_day = may31 - timedelta(days=(may31.weekday() - calendar.MONDAY) % 7 + 1)
        holidays.append({
            'name': "Memorial Day",
            'date': memorial_day.date(),
            'month': 5,
            'day': memorial_day.day,
        })

        sep1 = datetime(year, 9, 1)
        labor_day = sep1 + timedelta(days=(7 - sep1.weekday()) % 7 + 1)
        holidays.append({
            'name': "Labor Day",
            'date': labor_day.date(),
            'month': 9,
            'day': labor_day.day,
        })

        nov1 = datetime(year, 11, 1)
        thanksgiving = nov1 + timedelta(days=(3 - nov1.weekday()) % 7 + 21)
        holidays.append({
            'name': "Thanksgiving",
            'date': thanksgiving.date(),
            'month': 11,
            'day': thanksgiving.day,
        })

    return sorted(holidays, key=lambda h: h['date'])


def is_holiday(
    dt: Union[datetime, str],
    country: str = 'US'
) -> bool:
    """
    Check if a date is a holiday.

    Args:
        dt: Datetime object or ISO string.
        country: Country code.

    Returns:
        True if holiday.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    date = dt.date()
    holidays = get_holidays(dt.year, country)

    return any(h['date'] == date for h in holidays)


def get_working_hours(
    start: Union[datetime, str],
    end: Union[datetime, str],
    working_hours: Dict[str, Any] = None
) -> float:
    """
    Calculate working hours between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.
        working_hours: Dict with 'start_hour', 'end_hour', 'weekends'.

    Returns:
        Working hours.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    if working_hours is None:
        working_hours = {
            'start_hour': 9,
            'end_hour': 17,
            'weekends': False,
        }

    total_hours = 0.0
    current = start

    while current < end:
        if working_hours.get('weekends', False) or current.weekday() < 5:
            hour = current.hour
            start_hour = working_hours.get('start_hour', 9)
            end_hour = working_hours.get('end_hour', 17)

            if hour < start_hour:
                current = current.replace(hour=start_hour, minute=0, second=0)

            if current.hour < end_hour:
                end_of_hour = min(
                    current + timedelta(hours=1),
                    end,
                    current.replace(hour=end_hour, minute=0, second=0)
                )
                total_hours += (end_of_hour - current).total_seconds() / 3600

        current += timedelta(hours=1)
        if current.minute != 0:
            current = current.replace(minute=0, second=0)

    return round(total_hours, 2)


def get_quarter_date_range(
    year: int,
    quarter: int
) -> Dict[str, datetime]:
    """
    Get start and end of a quarter.

    Args:
        year: Year.
        quarter: Quarter (1-4).

    Returns:
        Dictionary with 'start' and 'end'.
    """
    if quarter == 1:
        start = datetime(year, 1, 1)
        end = datetime(year, 4, 1) - timedelta(seconds=1)
    elif quarter == 2:
        start = datetime(year, 4, 1)
        end = datetime(year, 7, 1) - timedelta(seconds=1)
    elif quarter == 3:
        start = datetime(year, 7, 1)
        end = datetime(year, 10, 1) - timedelta(seconds=1)
    else:
        start = datetime(year, 10, 1)
        end = datetime(year + 1, 1, 1) - timedelta(seconds=1)

    return {'start': start, 'end': end}


def get_year_start_end(year: int) -> Dict[str, datetime]:
    """
    Get start and end of a year.

    Args:
        year: Year.

    Returns:
        Dictionary with 'start' and 'end'.
    """
    return {
        'start': datetime(year, 1, 1),
        'end': datetime(year, 12, 31, 23, 59, 59),
    }


def is_business_hours(
    dt: Union[datetime, str],
    start_hour: int = 9,
    end_hour: int = 17
) -> bool:
    """
    Check if datetime is within business hours.

    Args:
        dt: Datetime object or ISO string.
        start_hour: Business start hour.
        end_hour: Business end hour.

    Returns:
        True if within business hours.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if dt.weekday() >= 5:
        return False

    return start_hour <= dt.hour < end_hour


def get_utc_now() -> datetime:
    """
    Get current UTC datetime.

    Returns:
        UTC datetime.
    """
    return datetime.now(timezone.utc)


def get_local_now(timezone_str: Optional[str] = None) -> datetime:
    """
    Get current datetime in specific timezone.

    Args:
        timezone_str: Timezone name (e.g., 'America/New_York').

    Returns:
        Local datetime.
    """
    utc_now = get_utc_now()

    if timezone_str:
        try:
            from dateutil import tz
            local_tz = tz.gettz(timezone_str)
            return utc_now.astimezone(local_tz)
        except ImportError:
            pass

    return datetime.now()


def timestamp_to_iso(timestamp: Union[int, float]) -> str:
    """
    Convert Unix timestamp to ISO string.

    Args:
        timestamp: Unix timestamp.

    Returns:
        ISO datetime string.
    """
    return datetime.utcfromtimestamp(timestamp).isoformat()


def iso_to_timestamp(iso_string: str) -> int:
    """
    Convert ISO datetime string to Unix timestamp.

    Args:
        iso_string: ISO datetime string.

    Returns:
        Unix timestamp.
    """
    dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    return int(dt.timestamp())


def get_time_between(
    start: Union[datetime, str],
    end: Union[datetime, str]
) -> Dict[str, Any]:
    """
    Get detailed time between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.

    Returns:
        Dictionary with detailed time breakdown.
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace('Z', '+00:00'))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))

    delta = end - start
    total_seconds = int(delta.total_seconds())

    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    return {
        'total_seconds': total_seconds,
        'total_minutes': total_seconds // 60,
        'total_hours': total_seconds // 3600,
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'formatted': f'{days}d {hours}h {minutes}m',
    }


def truncate_to_business_hours(
    dt: Union[datetime, str],
    start_hour: int = 9,
    end_hour: int = 17
) -> datetime:
    """
    Truncate datetime to business hours boundaries.

    Args:
        dt: Datetime object or ISO string.
        start_hour: Business start hour.
        end_hour: Business end hour.

    Returns:
        Truncated datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if dt.weekday() >= 5:
        return get_next_business_day(dt).replace(hour=start_hour, minute=0, second=0)

    if dt.hour < start_hour:
        return dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if dt.hour >= end_hour:
        return get_next_business_day(dt).replace(hour=start_hour, minute=0, second=0, microsecond=0)

    return dt


def get_calendar_weeks(
    year: int,
    month: int
) -> List[List[Optional[datetime]]]:
    """
    Get calendar weeks for a month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        List of weeks, each week is list of day datetimes or None.
    """
    first_day = datetime(year, month, 1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    weeks = []
    current = first_day - timedelta(days=first_day.weekday())

    while current <= last_day or len(weeks) < 6:
        week = []
        for _ in range(7):
            if first_day.month == current.month:
                week.append(current)
            else:
                week.append(None)
            current += timedelta(days=1)
        weeks.append(week)

        if current > last_day and current.weekday() == 0:
            break

    return weeks


def parse_natural_date(date_text: str) -> Optional[datetime]:
    """
    Parse natural language date expressions.

    Args:
        date_text: Natural language date (e.g., "next Monday", "last Friday").

    Returns:
        Parsed datetime or None.
    """
    import re

    date_text = date_text.lower().strip()
    now = datetime.now()

    patterns = [
        (r'next\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', 'next'),
        (r'last\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', 'last'),
        (r'this\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', 'this'),
    ]

    day_names = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }

    for pattern, direction in patterns:
        match = re.search(pattern, date_text)
        if match:
            day_name = match.group(1).lower()
            target_weekday = day_names[day_name]

            current_weekday = now.weekday()
            days_ahead = target_weekday - current_weekday

            if direction == 'next':
                if days_ahead <= 0:
                    days_ahead += 7
            elif direction == 'last':
                if days_ahead >= 0:
                    days_ahead -= 7
            else:
                if days_ahead < 0:
                    days_ahead += 7

            result = now + timedelta(days=days_ahead)
            return result.replace(hour=0, minute=0, second=0, microsecond=0)

    return None


import calendar
from datetime import datetime as datetime_local


def get_next_business_day(dt: Union[datetime, str]) -> datetime:
    """Get next business day."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    current = dt + timedelta(days=1)

    while current.weekday() >= 5:
        current += timedelta(days=1)

    return current
