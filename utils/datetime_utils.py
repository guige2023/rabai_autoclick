"""
Datetime and time utilities.

Provides datetime parsing, formatting, timezone conversion,
time difference calculations, and business day utilities.
"""

from __future__ import annotations

import calendar
import math
from datetime import datetime, timedelta, timezone


def parse_datetime(
    date_str: str,
    formats: list[str] | None = None,
) -> datetime | None:
    """
    Parse datetime string with multiple format attempts.

    Args:
        date_str: Date string
        formats: List of formats to try (default: common formats)

    Returns:
        Parsed datetime or None.
    """
    if formats is None:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d %B %Y",
            "%d %b %Y",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def format_datetime(
    dt: datetime,
    format_str: str = "%Y-%m-%d %H:%M:%S",
    timezone_str: str | None = None,
) -> str:
    """
    Format datetime with optional timezone.

    Args:
        dt: Datetime object
        format_str: Format string
        timezone_str: Timezone name (e.g., 'UTC', 'America/New_York')

    Returns:
        Formatted datetime string.
    """
    if timezone_str:
        try:
            tz = timezone.utc if timezone_str == "UTC" else None
            if tz is None:
                import pytz
                tz = pytz.timezone(timezone_str)
                dt = dt.astimezone(tz)
        except Exception:
            pass
    return dt.strftime(format_str)


def timestamp_to_datetime(ts: int | float, utc: bool = True) -> datetime:
    """
    Convert Unix timestamp to datetime.

    Args:
        ts: Unix timestamp (seconds)
        utc: If True, treat as UTC

    Returns:
        Datetime object.
    """
    if utc:
        return datetime.utcfromtimestamp(ts)
    return datetime.fromtimestamp(ts)


def datetime_to_timestamp(dt: datetime) -> int:
    """Convert datetime to Unix timestamp."""
    return int(dt.timestamp())


def datetime_diff(dt1: datetime, dt2: datetime) -> timedelta:
    """Compute difference between two datetimes."""
    return dt1 - dt2


def add_business_days(start_date: datetime, days: int) -> datetime:
    """
    Add business days to a date (skipping weekends).

    Args:
        start_date: Starting date
        days: Number of business days to add (can be negative)

    Returns:
        Resulting date.
    """
    current = start_date
    delta = 1 if days >= 0 else -1
    remaining = abs(days)
    while remaining > 0:
        current += timedelta(days=delta)
        if current.weekday() < 5:  # Mon-Fri
            remaining -= 1
    return current


def is_business_day(date: datetime) -> bool:
    """Check if date is a business day (Mon-Fri)."""
    return date.weekday() < 5


def is_weekend(date: datetime) -> bool:
    """Check if date is a weekend."""
    return date.weekday() >= 5


def days_in_month(year: int, month: int) -> int:
    """Get number of days in month."""
    return calendar.monthrange(year, month)[1]


def start_of_month(date: datetime) -> datetime:
    """Get first day of month."""
    return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(date: datetime) -> datetime:
    """Get last day of month."""
    days = days_in_month(date.year, date.month)
    return date.replace(day=days, hour=23, minute=59, second=59, microsecond=999999)


def start_of_week(date: datetime, week_start: int = 0) -> datetime:
    """Get start of week (week_start: 0=Monday, 6=Sunday)."""
    days_since_start = (date.weekday() - week_start) % 7
    return (date - timedelta(days=days_since_start)).replace(hour=0, minute=0, second=0, microsecond=0)


def week_number(date: datetime) -> int:
    """Get ISO week number."""
    return date.isocalendar()[1]


def quarter(date: datetime) -> int:
    """Get quarter (1-4)."""
    return (date.month - 1) // 3 + 1


def start_of_quarter(date: datetime) -> datetime:
    """Get first day of quarter."""
    q = quarter(date)
    month = (q - 1) * 3 + 1
    return date.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_quarter(date: datetime) -> datetime:
    """Get last day of quarter."""
    q = quarter(date)
    month = q * 3
    year = date.year
    if month == 12:
        month = 12
    else:
        year = date.year + 1
        month = (q % 4) * 3 + 1 if q % 4 != 0 else 12
    day = days_in_month(year, month if month != 0 else 12)
    return date.replace(year=year if month != 12 else date.year, month=month if month != 0 else 12, day=day, hour=23, minute=59, second=59)


def age_from_birthdate(birthdate: datetime, reference_date: datetime | None = None) -> int:
    """Calculate age in years."""
    if reference_date is None:
        reference_date = datetime.now()
    age = reference_date.year - birthdate.year
    if (reference_date.month, reference_date.day) < (birthdate.month, birthdate.day):
        age -= 1
    return age


def time_ago(dt: datetime) -> str:
    """
    Format datetime as 'time ago' string.

    Args:
        dt: Datetime to format

    Returns:
        Human-readable time difference.
    """
    now = datetime.now()
    diff = now - dt
    seconds = diff.total_seconds()
    if seconds < 60:
        return f"{int(seconds)}s ago"
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m ago"
    hours = minutes / 60
    if hours < 24:
        return f"{int(hours)}h ago"
    days = hours / 24
    if days < 30:
        return f"{int(days)}d ago"
    months = days / 30
    if months < 12:
        return f"{int(months)}mo ago"
    years = days / 365
    return f"{int(years)}y ago"


def date_range(start: datetime, end: datetime, step_days: int = 1) -> list[datetime]:
    """
    Generate date range.

    Args:
        start: Start date
        end: End date (exclusive)
        step_days: Number of days between each date

    Returns:
        List of datetime objects.
    """
    result: list[datetime] = []
    current = start
    while current < end:
        result.append(current)
        current += timedelta(days=step_days)
    return result


def business_days_between(start: datetime, end: datetime) -> int:
    """
    Count business days between two dates.

    Args:
        start: Start date
        end: End date

    Returns:
        Number of business days.
    """
    count = 0
    current = start
    while current < end:
        if is_business_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def timezone_convert(dt: datetime, from_tz: str, to_tz: str) -> datetime:
    """
    Convert datetime between timezones.

    Args:
        dt: Datetime to convert
        from_tz: Source timezone
        to_tz: Target timezone

    Returns:
        Converted datetime.
    """
    try:
        from zoneinfo import ZoneInfo
        aware_dt = dt.replace(tzinfo=ZoneInfo(from_tz))
        return aware_dt.astimezone(ZoneInfo(to_tz))
    except Exception:
        return dt


def is_leap_year(year: int) -> bool:
    """Check if year is a leap year."""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def iso_calendar_str(date: datetime) -> str:
    """Format date as ISO calendar string (e.g., 2025-W05-3)."""
    return date.strftime("%G-W%V-%u")


def parse_duration(duration_str: str) -> timedelta | None:
    """
    Parse duration string like '1d', '2h30m', '45s'.

    Args:
        duration_str: Duration string

    Returns:
        Timedelta or None.
    """
    import re
    pattern = r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
    match = re.match(pattern, duration_str)
    if not match:
        return None
    days, hours, minutes, seconds = match.groups()
    return timedelta(
        days=int(days or 0),
        hours=int(hours or 0),
        minutes=int(minutes or 0),
        seconds=int(seconds or 0),
    )
