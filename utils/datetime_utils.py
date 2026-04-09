"""Date and time utilities.

Provides timezone handling, date parsing, formatting,
and time-based operations for automation.
"""

import calendar
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Union


@dataclass
class TimeRange:
    """Represents a time range."""
    start: datetime
    end: datetime

    def contains(self, dt: datetime) -> bool:
        """Check if datetime is within range."""
        return self.start <= dt <= self.end

    def duration(self) -> timedelta:
        """Get duration of range."""
        return self.end - self.start

    def overlaps(self, other: "TimeRange") -> bool:
        """Check if ranges overlap."""
        return self.start < other.end and other.start < self.end


def now(tz: Optional[timezone] = None) -> datetime:
    """Get current datetime.

    Example:
        now()  # naive datetime
        now(timezone.utc)  # UTC aware
    """
    dt = datetime.now()
    if tz:
        dt = dt.astimezone(tz)
    return dt


def today(tz: Optional[timezone] = None) -> datetime:
    """Get current date at midnight."""
    n = now(tz)
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_datetime(
    date_str: str,
    formats: Optional[List[str]] = None,
    tz: Optional[timezone] = None,
) -> Optional[datetime]:
    """Parse datetime string with fallback formats.

    Example:
        parse_datetime("2024-01-15 10:30:00")
        parse_datetime("Jan 15, 2024", ["%b %d, %Y"])
    """
    if formats is None:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%b %d, %Y",
            "%B %d, %Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if tz:
                dt = dt.astimezone(tz)
            return dt
        except ValueError:
            continue

    return None


def format_datetime(
    dt: datetime,
    format_str: str = "%Y-%m-%d %H:%M:%S",
) -> str:
    """Format datetime as string.

    Example:
        format_datetime(now())  # "2024-01-15 10:30:00"
    """
    return dt.strftime(format_str)


def format_relative(dt: datetime) -> str:
    """Format datetime as relative string.

    Example:
        format_relative(now() - timedelta(hours=1))  # "1 hour ago"
    """
    now_dt = now(dt.tzinfo)
    diff = now_dt - dt

    seconds = diff.total_seconds()

    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    if seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    if seconds < 604800:
        days = int(seconds / 86400)
        return f"{days} day{'s' if days > 1 else ''} ago"
    if seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks} week{'s' if weeks > 1 else ''} ago"
    if seconds < 31536000:
        months = int(seconds / 2592000)
        return f"{months} month{'s' if months > 1 else ''} ago"
    years = int(seconds / 31536000)
    return f"{years} year{'s' if years > 1 else ''} ago"


def get_week_range(
    dt: Optional[datetime] = None,
    week_start: int = 0,
) -> TimeRange:
    """Get start and end of week containing datetime.

    Args:
        dt: Reference datetime (defaults to now).
        week_start: 0=Monday, 6=Sunday.

    Example:
        get_week_range()  # Monday to Sunday of current week
    """
    if dt is None:
        dt = now()

    days_since_weekstart = (dt.weekday() - week_start) % 7
    start = dt - timedelta(days=days_since_weekstart)
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    return TimeRange(start=start, end=end)


def get_month_range(
    dt: Optional[datetime] = None,
) -> TimeRange:
    """Get start and end of month containing datetime.

    Example:
        get_month_range()  # First to last day of current month
    """
    if dt is None:
        dt = now()

    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    _, last_day = calendar.monthrange(dt.year, dt.month)
    end = dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)

    return TimeRange(start=start, end=end)


def add_business_days(
    dt: datetime,
    days: int,
) -> datetime:
    """Add business days to datetime.

    Example:
        add_business_days(date(2024, 1, 5), 1)  # Monday + 1 = Tuesday
        add_business_days(date(2024, 1, 5), 3)  # Friday + 3 = Tuesday
    """
    current = dt
    delta = 1 if days >= 0 else -1
    remaining = abs(days)

    while remaining > 0:
        current += timedelta(days=delta)
        if current.weekday() < 5:
            remaining -= 1

    return current


def is_business_day(dt: datetime) -> bool:
    """Check if datetime is a business day (Mon-Fri)."""
    return dt.weekday() < 5


def get_quarter(dt: Optional[datetime] = None) -> Tuple[int, int]:
    """Get quarter and year for datetime.

    Returns:
        Tuple of (year, quarter).

    Example:
        get_quarter(date(2024, 3, 15))  # (2024, 1)
    """
    if dt is None:
        dt = now()
    quarter = (dt.month - 1) // 3 + 1
    return (dt.year, quarter)


def get_quarter_range(
    year: int,
    quarter: int,
) -> TimeRange:
    """Get time range for a quarter.

    Example:
        get_quarter_range(2024, 1)  # Jan 1 - Mar 31, 2024
    """
    start_month = (quarter - 1) * 3 + 1
    start = datetime(year, start_month, 1)

    end_month = start_month + 2
    _, last_day = calendar.monthrange(year, end_month)
    end = datetime(year, end_month, last_day, 23, 59, 59)

    return TimeRange(start=start, end=end)


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string to timedelta.

    Example:
        parse_duration("1h30m")  # timedelta(hours=1, minutes=30)
        parse_duration("2 days")  # timedelta(days=2)
    """
    import re

    patterns = [
        (r"(\d+)d", "days"),
        (r"(\d+)h", "hours"),
        (r"(\d+)m", "minutes"),
        (r"(\d+)s", "seconds"),
    ]

    kwargs = {}
    for pattern, unit in patterns:
        match = re.search(pattern, duration_str)
        if match:
            kwargs[unit] = int(match.group(1))

    return timedelta(**kwargs)


def timestamp_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def timestamp_seconds() -> int:
    """Get current timestamp in seconds."""
    return int(time.time())


def from_timestamp(timestamp: int, tz: Optional[timezone] = None) -> datetime:
    """Convert timestamp to datetime.

    Args:
        timestamp: Unix timestamp (seconds or milliseconds).
        tz: Timezone for result.

    Example:
        from_timestamp(1704067200)
    """
    if timestamp > 10**12:
        timestamp = timestamp // 1000

    dt = datetime.fromtimestamp(timestamp, tz=tz)
    return dt


def ensure_timezone(dt: datetime, tz: timezone) -> datetime:
    """Ensure datetime is in specified timezone."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """Check if two datetimes are on the same day."""
    return dt1.year == dt2.year and dt1.month == dt2.month and dt1.day == dt2.day


def get_age(birth_date: datetime, ref_date: Optional[datetime] = None) -> int:
    """Calculate age in years from birth date.

    Example:
        get_age(date(1990, 5, 15))  # ~33 (in 2024)
    """
    if ref_date is None:
        ref_date = now()

    age = ref_date.year - birth_date.year
    if (ref_date.month, ref_date.day) < (birth_date.month, birth_date.day):
        age -= 1

    return age
