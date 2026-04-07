"""Datetime utilities v2 for RabAI AutoClick.

Provides:
- Advanced datetime formatting and parsing
- Timezone conversion helpers
- Date arithmetic
- Duration formatting
"""

import calendar
import datetime
import time
from typing import (
    Optional,
    Union,
)


def now(tz: Optional[datetime.timezone] = None) -> datetime.datetime:
    """Get current datetime.

    Args:
        tz: Optional timezone.

    Returns:
        Current datetime.
    """
    if tz is None:
        return datetime.datetime.now()
    return datetime.datetime.now(tz)


def today(tz: Optional[datetime.timezone] = None) -> datetime.date:
    """Get today's date.

    Args:
        tz: Optional timezone.

    Returns:
        Today's date.
    """
    return now(tz).date()


def timestamp() -> float:
    """Get current Unix timestamp.

    Returns:
        Current timestamp.
    """
    return time.time()


def timestamp_ms() -> int:
    """Get current timestamp in milliseconds.

    Returns:
        Current timestamp in ms.
    """
    return int(time.time() * 1000)


def from_timestamp(ts: float, tz: Optional[datetime.timezone] = None) -> datetime.datetime:
    """Convert Unix timestamp to datetime.

    Args:
        ts: Unix timestamp.
        tz: Optional timezone.

    Returns:
        Datetime object.
    """
    return datetime.datetime.fromtimestamp(ts, tz=tz)


def from_timestamp_ms(ms: int, tz: Optional[datetime.timezone] = None) -> datetime.datetime:
    """Convert millisecond timestamp to datetime.

    Args:
        ms: Millisecond timestamp.
        tz: Optional timezone.

    Returns:
        Datetime object.
    """
    return datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz)


def parse_date(
    date_str: str,
    formats: Optional[list[str]] = None,
) -> Optional[datetime.date]:
    """Parse a date string.

    Args:
        date_str: Date string to parse.
        formats: List of format strings to try.

    Returns:
        Parsed date or None.
    """
    if formats is None:
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%b %d, %Y",
            "%B %d, %Y",
        ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def parse_datetime(
    dt_str: str,
    formats: Optional[list[str]] = None,
) -> Optional[datetime.datetime]:
    """Parse a datetime string.

    Args:
        dt_str: Datetime string to parse.
        formats: List of format strings to try.

    Returns:
        Parsed datetime or None.
    """
    if formats is None:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
        ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


def format_date(
    dt: Union[datetime.date, datetime.datetime],
    format_str: str = "%Y-%m-%d",
) -> str:
    """Format a date/datetime as string.

    Args:
        dt: Date or datetime to format.
        format_str: Format string.

    Returns:
        Formatted date string.
    """
    return dt.strftime(format_str)


def format_datetime(
    dt: datetime.datetime,
    format_str: str = "%Y-%m-%d %H:%M:%S",
) -> str:
    """Format a datetime as string.

    Args:
        dt: Datetime to format.
        format_str: Format string.

    Returns:
        Formatted datetime string.
    """
    return dt.strftime(format_str)


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted duration (e.g., "2h 30m 15s").
    """
    if seconds < 0:
        return "-" + format_duration(-seconds)

    parts = []

    hours = int(seconds // 3600)
    if hours > 0:
        parts.append(f"{hours}h")
        seconds -= hours * 3600

    minutes = int(seconds // 60)
    if minutes > 0:
        parts.append(f"{minutes}m")
        seconds -= minutes * 60

    if seconds > 0 or not parts:
        parts.append(f"{seconds:.1f}s")

    return " ".join(parts)


def format_duration_long(seconds: float) -> str:
    """Format duration as long human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted duration (e.g., "2 hours, 30 minutes, 15 seconds").
    """
    parts = []

    hours = int(seconds // 3600)
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        seconds -= hours * 3600

    minutes = int(seconds // 60)
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        seconds -= minutes * 60

    seconds = round(seconds, 2)
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    return ", ".join(parts)


def add_days(
    dt: Union[datetime.date, datetime.datetime],
    days: int,
) -> datetime.date:
    """Add days to a date.

    Args:
        dt: Date or datetime.
        days: Number of days to add (can be negative).

    Returns:
        New date.
    """
    return dt + datetime.timedelta(days=days)


def add_hours(
    dt: datetime.datetime,
    hours: int,
) -> datetime.datetime:
    """Add hours to a datetime.

    Args:
        dt: Datetime.
        hours: Number of hours to add.

    Returns:
        New datetime.
    """
    return dt + datetime.timedelta(hours=hours)


def add_minutes(
    dt: datetime.datetime,
    minutes: int,
) -> datetime.datetime:
    """Add minutes to a datetime.

    Args:
        dt: Datetime.
        minutes: Number of minutes to add.

    Returns:
        New datetime.
    """
    return dt + datetime.timedelta(minutes=minutes)


def days_between(
    start: Union[datetime.date, datetime.datetime],
    end: Union[datetime.date, datetime.datetime],
) -> int:
    """Get number of days between two dates.

    Args:
        start: Start date.
        end: End date.

    Returns:
        Number of days.
    """
    delta = end - start
    return abs(delta.days)


def is_business_day(
    date: datetime.date,
    holidays: Optional[list[datetime.date]] = None,
) -> bool:
    """Check if a date is a business day.

    Args:
        date: Date to check.
        holidays: Optional list of holiday dates.

    Returns:
        True if business day.
    """
    if date.weekday() >= 5:  # Saturday or Sunday
        return False
    if holidays and date in holidays:
        return False
    return True


def next_business_day(
    date: datetime.date,
    holidays: Optional[list[datetime.date]] = None,
) -> datetime.date:
    """Get the next business day.

    Args:
        date: Starting date.
        holidays: Optional list of holidays.

    Returns:
        Next business day.
    """
    next_day = date + datetime.timedelta(days=1)
    while not is_business_day(next_day, holidays):
        next_day += datetime.timedelta(days=1)
    return next_day


def start_of_day(dt: datetime.datetime) -> datetime.datetime:
    """Get start of day (00:00:00).

    Args:
        dt: Datetime.

    Returns:
        Start of day.
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime.datetime) -> datetime.datetime:
    """Get end of day (23:59:59.999999).

    Args:
        dt: Datetime.

    Returns:
        End of day.
    """
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def start_of_week(dt: datetime.datetime, weekday: int = 0) -> datetime.datetime:
    """Get start of week.

    Args:
        dt: Datetime.
        weekday: Start of week day (0=Monday, 6=Sunday).

    Returns:
        Start of week.
    """
    days_since = dt.weekday() - weekday
    if days_since < 0:
        days_since += 7
    return start_of_day(dt - datetime.timedelta(days=days_since))


def start_of_month(dt: datetime.datetime) -> datetime.datetime:
    """Get start of month.

    Args:
        dt: Datetime.

    Returns:
        Start of month.
    """
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(dt: datetime.datetime) -> datetime.datetime:
    """Get end of month.

    Args:
        dt: Datetime.

    Returns:
        End of month (last day, 23:59:59.999999).
    """
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)


def is_same_day(
    dt1: Union[datetime.date, datetime.datetime],
    dt2: Union[datetime.date, datetime.datetime],
) -> bool:
    """Check if two dates are the same day.

    Args:
        dt1: First date.
        dt2: Second date.

    Returns:
        True if same day.
    """
    return dt1.date() == dt2.date()


def age_in_years(birth_date: datetime.date, ref_date: Optional[datetime.date] = None) -> int:
    """Calculate age in years.

    Args:
        birth_date: Birth date.
        ref_date: Reference date (default: today).

    Returns:
        Age in years.
    """
    if ref_date is None:
        ref_date = today()

    age = ref_date.year - birth_date.year
    if (ref_date.month, ref_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age


def timestamp_to_iso(
    ts: float,
    tz: Optional[datetime.timezone] = None,
) -> str:
    """Convert timestamp to ISO format string.

    Args:
        ts: Unix timestamp.
        tz: Optional timezone.

    Returns:
        ISO format string.
    """
    dt = from_timestamp(ts, tz)
    return dt.isoformat()


def iso_to_datetime(iso_str: str) -> datetime.datetime:
    """Parse ISO format datetime string.

    Args:
        iso_str: ISO format string.

    Returns:
        Datetime object.
    """
    return datetime.datetime.fromisoformat(iso_str)
