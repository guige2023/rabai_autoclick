"""datetime action extensions for rabai_autoclick.

Provides utilities for datetime manipulation, parsing,
formatting, and timezone handling.
"""

from __future__ import annotations

import calendar
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
    tzinfo,
)
from typing import Any, Callable

__all__ = [
    "now",
    "utcnow",
    "today",
    "timestamp",
    "from_timestamp",
    "parse_date",
    "parse_time",
    "format_date",
    "format_time",
    "format_datetime",
    "format_relative",
    "add_days",
    "add_hours",
    "add_minutes",
    "add_seconds",
    "date_range",
    "datetime_range",
    "is_weekend",
    "is_weekday",
    "day_of_week",
    "week_number",
    "days_between",
    "hours_between",
    "minutes_between",
    "seconds_between",
    "start_of_day",
    "end_of_day",
    "start_of_week",
    "end_of_week",
    "start_of_month",
    "end_of_month",
    "start_of_year",
    "end_of_year",
    "is_same_day",
    "is_same_week",
    "is_same_month",
    "is_same_year",
    "to_utc",
    "from_utc",
    "to_timezone",
    "timezone_offset",
    "is_dst",
    "DateTimeRange",
    "TimeSlot",
    "FuzzyDateParser",
]


def now(tz: tzinfo | None = None) -> datetime:
    """Get current datetime.

    Args:
        tz: Optional timezone.

    Returns:
        Current datetime.
    """
    if tz:
        return datetime.now(tz)
    return datetime.now()


def utcnow() -> datetime:
    """Get current UTC datetime.

    Returns:
        Current UTC datetime.
    """
    return datetime.utcnow()


def today() -> date:
    """Get today's date.

    Returns:
        Today's date.
    """
    return date.today()


def timestamp(dt: datetime | None = None) -> float:
    """Get Unix timestamp.

    Args:
        dt: Datetime (now if None).

    Returns:
        Unix timestamp.
    """
    if dt is None:
        dt = datetime.now()
    return dt.timestamp()


def from_timestamp(ts: float, tz: tzinfo | None = None) -> datetime:
    """Create datetime from timestamp.

    Args:
        ts: Unix timestamp.
        tz: Optional timezone.

    Returns:
        Datetime object.
    """
    dt = datetime.fromtimestamp(ts)
    if tz:
        dt = dt.astimezone(tz)
    return dt


def parse_date(date_str: str, fmt: str | None = None) -> date:
    """Parse date string.

    Args:
        date_str: Date string.
        fmt: Format string (tries multiple if None).

    Returns:
        Date object.

    Raises:
        ValueError: If parsing fails.
    """
    if fmt:
        return datetime.strptime(date_str, fmt).date()

    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Cannot parse date: {date_str}")


def parse_time(time_str: str, fmt: str | None = None) -> time:
    """Parse time string.

    Args:
        time_str: Time string.
        fmt: Format string.

    Returns:
        Time object.

    Raises:
        ValueError: If parsing fails.
    """
    if fmt:
        return datetime.strptime(time_str, fmt).time()

    formats = [
        "%H:%M:%S",
        "%H:%M",
        "%I:%M:%S %p",
        "%I:%M %p",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt).time()
        except ValueError:
            continue

    raise ValueError(f"Cannot parse time: {time_str}")


def format_date(dt: date | datetime, fmt: str = "%Y-%m-%d") -> str:
    """Format date as string.

    Args:
        dt: Date or datetime.
        fmt: Format string.

    Returns:
        Formatted string.
    """
    return dt.strftime(fmt)


def format_time(t: time, fmt: str = "%H:%M:%S") -> str:
    """Format time as string.

    Args:
        t: Time object.
        fmt: Format string.

    Returns:
        Formatted string.
    """
    return t.strftime(fmt)


def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime as string.

    Args:
        dt: Datetime object.
        fmt: Format string.

    Returns:
        Formatted string.
    """
    return dt.strftime(fmt)


def format_relative(dt: datetime | None = None) -> str:
    """Format datetime as relative string.

    Args:
        dt: Datetime (now if None).

    Returns:
        Relative string like "2 hours ago".
    """
    from datetime import datetime

    if dt is None:
        dt = datetime.now()

    now_dt = datetime.now()
    delta = now_dt - dt

    if delta.total_seconds() < 0:
        return "in the future"

    seconds = int(delta.total_seconds())

    if seconds < 60:
        return f"{seconds} seconds ago"
    if seconds < 3600:
        return f"{seconds // 60} minutes ago"
    if seconds < 86400:
        return f"{seconds // 3600} hours ago"
    if seconds < 604800:
        return f"{seconds // 86400} days ago"
    if seconds < 2592000:
        return f"{seconds // 604800} weeks ago"
    if seconds < 31536000:
        return f"{seconds // 2592000} months ago"
    return f"{seconds // 31536000} years ago"


def add_days(dt: datetime, days: int) -> datetime:
    """Add days to datetime.

    Args:
        dt: Starting datetime.
        days: Days to add (can be negative).

    Returns:
        New datetime.
    """
    return dt + timedelta(days=days)


def add_hours(dt: datetime, hours: int) -> datetime:
    """Add hours to datetime.

    Args:
        dt: Starting datetime.
        hours: Hours to add.

    Returns:
        New datetime.
    """
    return dt + timedelta(hours=hours)


def add_minutes(dt: datetime, minutes: int) -> datetime:
    """Add minutes to datetime.

    Args:
        dt: Starting datetime.
        minutes: Minutes to add.

    Returns:
        New datetime.
    """
    return dt + timedelta(minutes=minutes)


def add_seconds(dt: datetime, seconds: int) -> datetime:
    """Add seconds to datetime.

    Args:
        dt: Starting datetime.
        seconds: Seconds to add.

    Returns:
        New datetime.
    """
    return dt + timedelta(seconds=seconds)


def date_range(
    start: date,
    end: date,
    step: timedelta = timedelta(days=1),
) -> list[date]:
    """Generate date range.

    Args:
        start: Start date.
        end: End date (inclusive).
        step: Step between dates.

    Returns:
        List of dates.
    """
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += step
    return dates


def datetime_range(
    start: datetime,
    end: datetime,
    step: timedelta = timedelta(hours=1),
) -> list[datetime]:
    """Generate datetime range.

    Args:
        start: Start datetime.
        end: End datetime.
        step: Step between datetimes.

    Returns:
        List of datetimes.
    """
    datetimes = []
    current = start
    while current <= end:
        datetimes.append(current)
        current += step
    return datetimes


def is_weekend(d: date | datetime) -> bool:
    """Check if date is weekend.

    Args:
        d: Date to check.

    Returns:
        True if Saturday or Sunday.
    """
    return d.weekday() in (5, 6)


def is_weekday(d: date | datetime) -> bool:
    """Check if date is weekday.

    Args:
        d: Date to check.

    Returns:
        True if Monday-Friday.
    """
    return not is_weekend(d)


def day_of_week(d: date | datetime) -> str:
    """Get day of week name.

    Args:
        d: Date to check.

    Returns:
        Day name (Monday, etc).
    """
    return d.strftime("%A")


def week_number(d: date | datetime) -> int:
    """Get ISO week number.

    Args:
        d: Date to check.

    Returns:
        Week number (1-53).
    """
    return d.isocalendar()[1]


def days_between(start: date | datetime, end: date | datetime) -> int:
    """Get days between two dates.

    Args:
        start: Start date.
        end: End date.

    Returns:
        Number of days (always positive).
    """
    return abs((end.date() if isinstance(end, datetime) else end) -
               (start.date() if isinstance(start, datetime) else start)).days


def hours_between(start: datetime, end: datetime) -> float:
    """Get hours between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.

    Returns:
        Number of hours.
    """
    return abs((end - start).total_seconds()) / 3600


def minutes_between(start: datetime, end: datetime) -> float:
    """Get minutes between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.

    Returns:
        Number of minutes.
    """
    return abs((end - start).total_seconds()) / 60


def seconds_between(start: datetime, end: datetime) -> float:
    """Get seconds between two datetimes.

    Args:
        start: Start datetime.
        end: End datetime.

    Returns:
        Number of seconds.
    """
    return abs((end - start).total_seconds())


def start_of_day(d: date | datetime) -> datetime:
    """Get start of day (00:00:00).

    Args:
        d: Date or datetime.

    Returns:
        Datetime at start of day.
    """
    dt = d if isinstance(d, datetime) else datetime.combine(d, time())
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(d: date | datetime) -> datetime:
    """Get end of day (23:59:59.999999).

    Args:
        d: Date or datetime.

    Returns:
        Datetime at end of day.
    """
    dt = d if isinstance(d, datetime) else datetime.combine(d, time())
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def start_of_week(d: date | datetime, week_start: int = 0) -> datetime:
    """Get start of week.

    Args:
        d: Date or datetime.
        week_start: Week start day (0=Monday).

    Returns:
        Datetime at start of week.
    """
    dt = start_of_day(d)
    days_since_week_start = (dt.weekday() - week_start) % 7
    return dt - timedelta(days=days_since_week_start)


def end_of_week(d: date | datetime, week_start: int = 0) -> datetime:
    """Get end of week.

    Args:
        d: Date or datetime.
        week_start: Week start day.

    Returns:
        Datetime at end of week.
    """
    start = start_of_week(d, week_start)
    return start + timedelta(days=6, hours=23, minutes=59, seconds=59)


def start_of_month(d: date | datetime) -> datetime:
    """Get start of month.

    Args:
        d: Date or datetime.

    Returns:
        Datetime at start of month.
    """
    dt = start_of_day(d)
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def end_of_month(d: date | datetime) -> datetime:
    """Get end of month.

    Args:
        d: Date or datetime.

    Returns:
        Datetime at end of month.
    """
    dt = start_of_day(d)
    last_day = calendar.monthrange(d.year, d.month)[1]
    return dt.replace(day=last_day, hour=23, minute=59, second=59)


def start_of_year(d: date | datetime) -> datetime:
    """Get start of year.

    Args:
        d: Date or datetime.

    Returns:
        Datetime at start of year.
    """
    return start_of_day(d).replace(month=1, day=1)


def end_of_year(d: date | datetime) -> datetime:
    """Get end of year.

    Args:
        d: Date or datetime.

    Returns:
        Datetime at end of year.
    """
    return start_of_day(d).replace(month=12, day=31, hour=23, minute=59, second=59)


def is_same_day(a: date | datetime, b: date | datetime) -> bool:
    """Check if two dates are the same day.

    Args:
        a: First date.
        b: Second date.

    Returns:
        True if same day.
    """
    a_date = a.date() if isinstance(a, datetime) else a
    b_date = b.date() if isinstance(b, datetime) else b
    return a_date == b_date


def is_same_week(a: date | datetime, b: date | datetime) -> bool:
    """Check if two dates are in same week.

    Args:
        a: First date.
        b: Second date.

    Returns:
        True if same week.
    """
    return a.isocalendar()[:2] == b.isocalendar()[:2]


def is_same_month(a: date | datetime, b: date | datetime) -> bool:
    """Check if two dates are in same month.

    Args:
        a: First date.
        b: Second date.

    Returns:
        True if same month.
    """
    return a.year == b.year and a.month == b.month


def is_same_year(a: date | datetime, b: date | datetime) -> bool:
    """Check if two dates are in same year.

    Args:
        a: First date.
        b: Second date.

    Returns:
        True if same year.
    """
    return a.year == b.year


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC.

    Args:
        dt: Datetime to convert.

    Returns:
        UTC datetime.
    """
    return dt.astimezone(timezone.utc)


def from_utc(dt_str: str) -> datetime:
    """Parse UTC datetime string.

    Args:
        dt_str: UTC datetime string.

    Returns:
        Datetime object.
    """
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def to_timezone(dt: datetime, tz: tzinfo) -> datetime:
    """Convert datetime to timezone.

    Args:
        dt: Datetime to convert.
        tz: Target timezone.

    Returns:
        Converted datetime.
    """
    return dt.astimezone(tz)


def timezone_offset(tz: tzinfo) -> int:
    """Get timezone offset in hours.

    Args:
        tz: Timezone.

    Returns:
        Offset in hours.
    """
    now_dt = datetime.now(tz)
    offset = now_dt.utcoffset()
    if offset is None:
        return 0
    return int(offset.total_seconds() / 3600)


def is_dst(dt: datetime) -> bool:
    """Check if datetime is in DST.

    Args:
        dt: Datetime to check.

    Returns:
        True if in DST.
    """
    return bool(dt.dst())


class DateTimeRange:
    """Range of datetime with iteration."""

    def __init__(
        self,
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
    ) -> None:
        self.start = start
        self.end = end
        self.step = step

    def __iter__(self) -> Iterator[datetime]:
        current = self.start
        while current <= self.end:
            yield current
            current += self.step

    def __contains__(self, dt: datetime) -> bool:
        return self.start <= dt <= self.end

    def __len__(self) -> int:
        delta = self.end - self.start
        return int(delta.total_seconds() / self.step.total_seconds()) + 1

    def split(self, count: int) -> list[DateTimeRange]:
        """Split range into count equal parts.

        Args:
            count: Number of parts.

        Returns:
            List of datetime ranges.
        """
        total = self.end - self.start
        part_size = total / count
        ranges = []
        for i in range(count):
            part_start = self.start + part_size * i
            part_end = self.start + part_size * (i + 1) - self.step
            if i == count - 1:
                part_end = self.end
            ranges.append(DateTimeRange(part_start, part_end, self.step))
        return ranges


class TimeSlot:
    """A time slot with start and end."""

    def __init__(self, start: datetime, end: datetime) -> None:
        self.start = start
        self.end = end

    def __repr__(self) -> str:
        return f"TimeSlot({self.start} - {self.end})"

    def __contains__(self, dt: datetime) -> bool:
        return self.start <= dt <= self.end

    def duration(self) -> timedelta:
        """Get duration of slot."""
        return self.end - self.start

    def overlaps(self, other: TimeSlot) -> bool:
        """Check if overlaps with another slot."""
        return self.start < other.end and self.end > other.start


class FuzzyDateParser:
    """Parse natural language date expressions."""

    RELATIVE_PATTERNS = {
        r"yesterday": lambda: today() - timedelta(days=1),
        r"today": lambda: today(),
        r"tomorrow": lambda: today() + timedelta(days=1),
        r"next week": lambda: today() + timedelta(weeks=1),
        r"last week": lambda: today() - timedelta(weeks=1),
        r"next month": lambda: today() + timedelta(days=30),
        r"last month": lambda: today() - timedelta(days=30),
    }

    @classmethod
    def parse(cls, text: str) -> date | None:
        """Parse fuzzy date string.

        Args:
            text: Date string like "yesterday", "next week".

        Returns:
            Date or None if cannot parse.
        """
        text = text.lower().strip()
        for pattern, func in cls.RELATIVE_PATTERNS.items():
            if pattern in text:
                return func()
        return None
