"""
Datetime parsing and formatting advanced actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union
import re


def parse_flexible_date(date_string: str) -> Optional[datetime]:
    """
    Parse a date string with flexible format detection.

    Args:
        date_string: Date string to parse.

    Returns:
        Datetime object or None.
    """
    date_string = date_string.strip()

    now = datetime.now()

    relative_patterns = [
        (r'^now$', lambda: now),
        (r'^today$', lambda: now.replace(hour=0, minute=0, second=0, microsecond=0)),
        (r'^tomorrow$', lambda: (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)),
        (r'^yesterday$', lambda: (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)),
    ]

    for pattern, parser in relative_patterns:
        if re.match(pattern, date_string, re.IGNORECASE):
            return parser()

    offset_patterns = [
        (r'^(\d+) days? ago$', lambda m: now - timedelta(days=int(m.group(1)))),
        (r'^in (\d+) days?$', lambda m: now + timedelta(days=int(m.group(1)))),
        (r'^(\d+) hours? ago$', lambda m: now - timedelta(hours=int(m.group(1)))),
        (r'^in (\d+) hours?$', lambda m: now + timedelta(hours=int(m.group(1)))),
        (r'^(\d+) minutes? ago$', lambda m: now - timedelta(minutes=int(m.group(1)))),
        (r'^in (\d+) minutes?$', lambda m: now + timedelta(minutes=int(m.group(1)))),
    ]

    for pattern, parser in offset_patterns:
        match = re.match(pattern, date_string, re.IGNORECASE)
        if match:
            return parser(match)

    formats = [
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
        '%d %B %Y',
        '%B %d, %Y',
        '%d %b %Y',
        '%b %d, %Y',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue

    return None


def format_iso(dt: Union[datetime, str]) -> str:
    """
    Format datetime as ISO 8601 string.

    Args:
        dt: Datetime object or ISO string.

    Returns:
        ISO formatted string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    return dt.isoformat()


def format_relative(dt: Union[datetime, str]) -> str:
    """
    Format datetime as relative string (e.g., "2 hours ago").

    Args:
        dt: Datetime object or ISO string.

    Returns:
        Relative time string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    now = datetime.now()
    diff = now - dt

    if diff.total_seconds() < 0:
        future = True
        diff = -diff
    else:
        future = False

    seconds = diff.total_seconds()

    if seconds < 60:
        return 'just now' if not future else 'in a moment'

    minutes = seconds / 60
    if minutes < 60:
        return f'{int(minutes)} minute{"s" if int(minutes) != 1 else ""} {"ago" if not future else "from now"}'

    hours = minutes / 60
    if hours < 24:
        return f'{int(hours)} hour{"s" if int(hours) != 1 else ""} {"ago" if not future else "from now"}'

    days = hours / 24
    if days < 30:
        return f'{int(days)} day{"s" if int(days) != 1 else ""} {"ago" if not future else "from now"}'

    weeks = days / 7
    if weeks < 4:
        return f'{int(weeks)} week{"s" if int(weeks) != 1 else ""} {"ago" if not future else "from now"}'

    months = days / 30
    if months < 12:
        return f'{int(months)} month{"s" if int(months) != 1 else ""} {"ago" if not future else "from now"}'

    years = days / 365
    return f'{int(years)} year{"s" if int(years) != 1 else ""} {"ago" if not future else "from now"}'


def convert_timezone(
    dt: Union[datetime, str],
    from_tz: str,
    to_tz: str
) -> datetime:
    """
    Convert datetime between timezones.

    Args:
        dt: Datetime object or ISO string.
        from_tz: Source timezone.
        to_tz: Target timezone.

    Returns:
        Converted datetime.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    try:
        from dateutil import tz
        from_tz_obj = tz.gettz(from_tz)
        to_tz_obj = tz.gettz(to_tz)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=from_tz_obj)

        return dt.astimezone(to_tz_obj)
    except ImportError:
        return dt


def get_timezone_offset(tz_name: str) -> float:
    """
    Get UTC offset for a timezone in hours.

    Args:
        tz_name: Timezone name.

    Returns:
        UTC offset in hours.
    """
    try:
        from dateutil import tz
        import datetime as dt_module

        tz_obj = tz.gettz(tz_name)
        if tz_obj is None:
            return 0.0

        now = dt_module.datetime.now()
        return tz_obj.utcoffset(now).total_seconds() / 3600
    except ImportError:
        return 0.0


def is_valid_date_string(date_string: str) -> bool:
    """
    Check if a string is a valid date.

    Args:
        date_string: String to check.

    Returns:
        True if valid date.
    """
    return parse_flexible_date(date_string) is not None


def get_business_days_between(
    start: Union[datetime, str],
    end: Union[datetime, str]
) -> int:
    """
    Count business days between two dates.

    Args:
        start: Start date.
        end: End date.

    Returns:
        Number of business days.
    """
    if isinstance(start, str):
        start = parse_flexible_date(start) or datetime.now()
    if isinstance(end, str):
        end = parse_flexible_date(end) or datetime.now()

    count = 0
    current = start

    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)

    return count


def get_nth_weekday(
    year: int,
    month: int,
    weekday: int,
    n: int = 1
) -> datetime:
    """
    Get the nth occurrence of a weekday in a month.

    Args:
        year: Year.
        month: Month (1-12).
        weekday: Weekday (0=Mon, 6=Sun).
        n: Which occurrence (1=first, -1=last).

    Returns:
        Datetime of nth weekday.
    """
    if n == -1:
        last_day = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1)
        current = last_day - timedelta(days=1)
        while current.weekday() != weekday:
            current -= timedelta(days=1)
        return current

    current = datetime(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)

    for _ in range(n - 1):
        current += timedelta(days=7)

    return current


def get_month_calendar(year: int, month: int) -> List[List[Optional[datetime]]]:
    """
    Get calendar grid for a month.

    Args:
        year: Year.
        month: Month (1-12).

    Returns:
        List of weeks, each week is list of day datetimes or None.
    """
    first_day = datetime(year, month, 1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    calendar: List[List[Optional[datetime]]] = []

    current = first_day - timedelta(days=first_day.weekday())

    while current <= last_day or current.weekday() != 0:
        week = []
        for _ in range(7):
            if first_day.month == current.month:
                week.append(current)
            else:
                week.append(None)
            current += timedelta(days=1)
        calendar.append(week)

        if current > last_day and current.weekday() == 0:
            break

    return calendar


def parse_duration_string(duration: str) -> int:
    """
    Parse duration string to seconds.

    Args:
        duration: Duration string (e.g., "1h30m", "2 days").

    Returns:
        Duration in seconds.
    """
    duration = duration.lower().strip()

    seconds = 0

    patterns = [
        (r'(\d+)s', 1),
        (r'(\d+)sec', 1),
        (r'(\d+)seconds?', 1),
        (r'(\d+)m', 60),
        (r'(\d+)min', 60),
        (r'(\d+)minutes?', 60),
        (r'(\d+)h', 3600),
        (r'(\d+)hours?', 3600),
        (r'(\d+)d', 86400),
        (r'(\d+)days?', 86400),
        (r'(\d+)w', 604800),
        (r'(\d+)weeks?', 604800),
    ]

    for pattern, multiplier in patterns:
        match = re.search(pattern, duration)
        if match:
            seconds += int(match.group(1)) * multiplier

    return seconds


def format_date_human(
    dt: Union[datetime, str],
    style: str = 'medium'
) -> str:
    """
    Format date in human-readable style.

    Args:
        dt: Datetime object or ISO string.
        style: Style ('short', 'medium', 'long').

    Returns:
        Formatted date string.
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))

    if style == 'short':
        return dt.strftime('%m/%d/%y')

    if style == 'medium':
        return dt.strftime('%b %d, %Y')

    if style == 'long':
        return dt.strftime('%B %d, %Y')

    return dt.strftime('%Y-%m-%d')


def get_age(birth_date: Union[datetime, str]) -> Dict[str, int]:
    """
    Calculate age from birth date.

    Args:
        birth_date: Birth date.

    Returns:
        Age in years, months, days.
    """
    if isinstance(birth_date, str):
        birth_date = parse_flexible_date(birth_date) or datetime.now()

    today = datetime.now()

    years = today.year - birth_date.year
    months = today.month - birth_date.month
    days = today.day - birth_date.day

    if days < 0:
        months -= 1
        days += 30

    if months < 0:
        years -= 1
        months += 12

    return {
        'years': years,
        'months': months,
        'days': days,
    }
