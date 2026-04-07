"""Date utilities for RabAI AutoClick.

Provides:
- Date-specific helpers
- Date parsing and formatting
- Date calculations
"""

from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple, Dict


def today() -> date:
    """Get today's date.

    Returns:
        Today's date.
    """
    return date.today()


def yesterday() -> date:
    """Get yesterday's date.

    Returns:
        Yesterday's date.
    """
    return today() - timedelta(days=1)


def tomorrow() -> date:
    """Get tomorrow's date.

    Returns:
        Tomorrow's date.
    """
    return today() + timedelta(days=1)


def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> Optional[date]:
    """Parse date string to date object.

    Args:
        date_str: Date string.
        fmt: Format string.

    Returns:
        Date object or None if parsing fails.
    """
    try:
        return datetime.strptime(date_str, fmt).date()
    except ValueError:
        return None


def format_date(d: date, fmt: str = "%Y-%m-%d") -> str:
    """Format date as string.

    Args:
        d: Date to format.
        fmt: Format string.

    Returns:
        Formatted date string.
    """
    return d.strftime(fmt)


def is_weekend(d: date) -> bool:
    """Check if date is weekend.

    Args:
        d: Date to check.

    Returns:
        True if weekend.
    """
    return d.weekday() >= 5


def is_weekday(d: date) -> bool:
    """Check if date is weekday.

    Args:
        d: Date to check.

    Returns:
        True if weekday.
    """
    return d.weekday() < 5


def is_same_day(d1: date, d2: date) -> bool:
    """Check if two dates are the same.

    Args:
        d1: First date.
        d2: Second date.

    Returns:
        True if same day.
    """
    return d1 == d2


def is_today(d: date) -> bool:
    """Check if date is today.

    Args:
        d: Date to check.

    Returns:
        True if today.
    """
    return d == today()


def is_past(d: date) -> bool:
    """Check if date is in the past.

    Args:
        d: Date to check.

    Returns:
        True if in the past.
    """
    return d < today()


def is_future(d: date) -> bool:
    """Check if date is in the future.

    Args:
        d: Date to check.

    Returns:
        True if in the future.
    """
    return d > today()


def add_days_to_date(d: date, days: int) -> date:
    """Add days to date.

    Args:
        d: Date.
        days: Number of days to add.

    Returns:
        New date.
    """
    return d + timedelta(days=days)


def subtract_days_from_date(d: date, days: int) -> date:
    """Subtract days from date.

    Args:
        d: Date.
        days: Number of days to subtract.

    Returns:
        New date.
    """
    return d - timedelta(days=days)


def days_between_dates(d1: date, d2: date) -> int:
    """Get number of days between dates.

    Args:
        d1: First date.
        d2: Second date.

    Returns:
        Number of days.
    """
    return abs((d2 - d1).days)


def weeks_between_dates(d1: date, d2: date) -> int:
    """Get number of weeks between dates.

    Args:
        d1: First date.
        d2: Second date.

    Returns:
        Number of weeks.
    """
    return abs((d2 - d1).days) // 7


def months_between_dates(d1: date, d2: date) -> int:
    """Get approximate number of months between dates.

    Args:
        d1: First date.
        d2: Second date.

    Returns:
        Number of months.
    """
    return abs((d2.year - d1.year) * 12 + (d2.month - d1.month))


def years_between_dates(d1: date, d2: date) -> int:
    """Get number of years between dates.

    Args:
        d1: First date.
        d2: Second date.

    Returns:
        Number of years.
    """
    return abs(d2.year - d1.year)


def start_of_week(d: date) -> date:
    """Get start of week (Monday).

    Args:
        d: Date.

    Returns:
        Start of week date.
    """
    days_since_monday = d.weekday()
    return d - timedelta(days=days_since_monday)


def end_of_week(d: date) -> date:
    """Get end of week (Sunday).

    Args:
        d: Date.

    Returns:
        End of week date.
    """
    days_until_sunday = 6 - d.weekday()
    return d + timedelta(days=days_until_sunday)


def start_of_month(d: date) -> date:
    """Get start of month.

    Args:
        d: Date.

    Returns:
        Start of month date.
    """
    return d.replace(day=1)


def end_of_month(d: date) -> date:
    """Get end of month.

    Args:
        d: Date.

    Returns:
        End of month date.
    """
    if d.month == 12:
        return d.replace(day=31)
    next_month = d.replace(month=d.month + 1, day=1)
    return next_month - timedelta(days=1)


def start_of_year(d: date) -> date:
    """Get start of year.

    Args:
        d: Date.

    Returns:
        Start of year date.
    """
    return d.replace(month=1, day=1)


def end_of_year(d: date) -> date:
    """Get end of year.

    Args:
        d: Date.

    Returns:
        End of year date.
    """
    return d.replace(month=12, day=31)


def get_week_number(d: date) -> int:
    """Get ISO week number.

    Args:
        d: Date.

    Returns:
        Week number.
    """
    return d.isocalendar()[1]


def get_quarter(d: date) -> int:
    """Get quarter of year.

    Args:
        d: Date.

    Returns:
        Quarter (1-4).
    """
    return (d.month - 1) // 3 + 1


def is_leap_year(year: int) -> bool:
    """Check if year is leap year.

    Args:
        year: Year to check.

    Returns:
        True if leap year.
    """
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def get_days_in_month(year: int, month: int) -> int:
    """Get number of days in month.

    Args:
        year: Year.
        month: Month.

    Returns:
        Number of days.
    """
    if month == 2:
        return 29 if is_leap_year(year) else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


def date_range(start: date, end: date) -> List[date]:
    """Generate list of dates in range.

    Args:
        start: Start date.
        end: End date (inclusive).

    Returns:
        List of dates.
    """
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def get_weekdays(start: date, end: date) -> List[date]:
    """Get list of weekdays in range.

    Args:
        start: Start date.
        end: End date.

    Returns:
        List of weekday dates.
    """
    return [d for d in date_range(start, end) if is_weekday(d)]


def get_weekends(start: date, end: date) -> List[date]:
    """Get list of weekend days in range.

    Args:
        start: Start date.
        end: End date.

    Returns:
        List of weekend dates.
    """
    return [d for d in date_range(start, end) if is_weekend(d)]


def parse_multiple_dates(date_str: str, separator: str = ",") -> List[date]:
    """Parse multiple dates from string.

    Args:
        date_str: String with multiple dates.
        separator: Separator between dates.

    Returns:
        List of date objects.
    """
    dates = []
    for part in date_str.split(separator):
        part = part.strip()
        parsed = parse_date(part)
        if parsed:
            dates.append(parsed)
    return dates


def format_date_range(start: date, end: date, fmt: str = "%Y-%m-%d") -> str:
    """Format date range as string.

    Args:
        start: Start date.
        end: End date.
        fmt: Format string.

    Returns:
        Formatted range string.
    """
    return f"{format_date(start, fmt)} to {format_date(end, fmt)}"


def get_age(birth_date: date) -> int:
    """Calculate age from birth date.

    Args:
        birth_date: Birth date.

    Returns:
        Age in years.
    """
    today_date = today()
    age = today_date.year - birth_date.year
    if (today_date.month, today_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age


def is_valid_date(year: int, month: int, day: int) -> bool:
    """Check if date is valid.

    Args:
        year: Year.
        month: Month.
        day: Day.

    Returns:
        True if valid date.
    """
    try:
        date(year, month, day)
        return True
    except ValueError:
        return False


def date_to_tuple(d: date) -> Tuple[int, int, int]:
    """Convert date to tuple.

    Args:
        d: Date.

    Returns:
        Tuple of (year, month, day).
    """
    return (d.year, d.month, d.day)


def tuple_to_date(t: Tuple[int, int, int]) -> date:
    """Convert tuple to date.

    Args:
        t: Tuple of (year, month, day).

    Returns:
        Date object.
    """
    return date(t[0], t[1], t[2])


def date_to_ordinal(d: date) -> int:
    """Convert date to ordinal.

    Args:
        d: Date.

    Returns:
        Ordinal number.
    """
    return d.toordinal()


def ordinal_to_date(ordinal: int) -> date:
    """Convert ordinal to date.

    Args:
        ordinal: Ordinal number.

    Returns:
        Date object.
    """
    return date.fromordinal(ordinal)


def get_nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> Optional[date]:
    """Get the nth occurrence of a weekday in a month.

    Args:
        year: Year.
        month: Month.
        weekday: Weekday (0=Monday, 6=Sunday).
        n: Which occurrence (1=first, -1=last).

    Returns:
        Date of nth weekday or None.
    """
    if n == -1:
        d = date(year, month, get_days_in_month(year, month))
        while d.weekday() != weekday:
            d -= timedelta(days=1)
        return d

    first_day = date(year, month, 1)
    days_until_weekday = (weekday - first_day.weekday()) % 7
    nth_day = first_day + timedelta(days=days_until_weekday + (n - 1) * 7)
    if nth_day.month == month:
        return nth_day
    return None


def get_next_weekday(d: date, weekday: int) -> date:
    """Get next occurrence of weekday after date.

    Args:
        d: Starting date.
        weekday: Target weekday (0=Monday, 6=Sunday).

    Returns:
        Next weekday date.
    """
    days_until = (weekday - d.weekday()) % 7
    if days_until == 0:
        days_until = 7
    return d + timedelta(days=days_until)
