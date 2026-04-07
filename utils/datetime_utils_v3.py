"""
Calendar utilities for date and calendar computations.

Provides holiday calculations, business day arithmetic,
week number computations, and calendar generation.
"""

from __future__ import annotations

import datetime


def day_of_week(year: int, month: int, day: int) -> int:
    """
    Get day of week (0=Monday, 6=Sunday).

    Example:
        >>> day_of_week(2024, 1, 1)
        0
    """
    return datetime.date(year, month, day).weekday()


def week_number(year: int, month: int, day: int) -> int:
    """Get ISO week number."""
    return datetime.date(year, month, day).isocalendar()[1]


def days_in_month(year: int, month: int) -> int:
    """Return number of days in given month."""
    return (datetime.date(year, month, 28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)


def is_leap_year(year: int) -> bool:
    """Check if year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def easter_date(year: int) -> tuple[int, int, int]:
    """
    Calculate Easter Sunday date for given year (Gregorian calendar).

    Returns:
        Tuple of (month, day)

    Example:
        >>> easter_date(2024)
        (3, 31)
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return month, day


def add_business_days(start: datetime.date, days: int) -> datetime.date:
    """
    Add business days (skipping weekends).

    Args:
        start: Starting date
        days: Number of business days to add

    Returns:
        Resulting date
    """
    current = start
    delta = 1 if days > 0 else -1
    remaining = abs(days)
    while remaining > 0:
        current += datetime.timedelta(days=delta)
        if current.weekday() < 5:
            remaining -= 1
    return current


def business_days_between(start: datetime.date, end: datetime.date) -> int:
    """
    Count business days between two dates (exclusive of start).

    Example:
        >>> start = datetime.date(2024, 1, 1)
        >>> end = datetime.date(2024, 1, 10)
        >>> business_days_between(start, end)
        7
    """
    if start > end:
        start, end = end, start
    days = 0
    current = start + datetime.timedelta(days=1)
    while current < end:
        if current.weekday() < 5:
            days += 1
        current += datetime.timedelta(days=1)
    return days


def us_holidays(year: int) -> dict[str, str]:
    """
    Get US federal holidays for given year.

    Returns:
        Dict mapping holiday name to date string
    """
    holidays = {}
    # New Year's Day
    nyd = datetime.date(year, 1, 1)
    holidays["New Year's Day"] = nyd.strftime("%Y-%m-%d")
    # MLK Day (3rd Monday of January)
    jan1 = datetime.date(year, 1, 1)
    mlk = jan1 + datetime.timedelta(days=(7 - jan1.weekday()) % 7 + 14)
    holidays["MLK Day"] = mlk.strftime("%Y-%m-%d")
    # Presidents Day (3rd Monday of February)
    feb1 = datetime.date(year, 2, 1)
    pres = feb1 + datetime.timedelta(days=(7 - feb1.weekday()) % 7 + 14)
    holidays["Presidents Day"] = pres.strftime("%Y-%m-%d")
    # Memorial Day (last Monday of May)
    may31 = datetime.date(year, 5, 31)
    memorial = may31 - datetime.timedelta(days=(may31.weekday() + 7) % 7 - 7)
    holidays["Memorial Day"] = memorial.strftime("%Y-%m-%d")
    # Independence Day
    holidays["Independence Day"] = datetime.date(year, 7, 4).strftime("%Y-%m-%d")
    # Labor Day (1st Monday of September)
    sep1 = datetime.date(year, 9, 1)
    labor = sep1 + datetime.timedelta(days=(7 - sep1.weekday()) % 7)
    holidays["Labor Day"] = labor.strftime("%Y-%m-%d")
    # Columbus Day (2nd Monday of October)
    oct1 = datetime.date(year, 10, 1)
    columbus = oct1 + datetime.timedelta(days=(7 - oct1.weekday()) % 7 + 7)
    holidays["Columbus Day"] = columbus.strftime("%Y-%m-%d")
    # Veterans Day
    holidays["Veterans Day"] = datetime.date(year, 11, 11).strftime("%Y-%m-%d")
    # Thanksgiving (4th Thursday of November)
    nov1 = datetime.date(year, 11, 1)
    thanksgiving = nov1 + datetime.timedelta(days=(3 - nov1.weekday() + 7) % 7 + 21)
    holidays["Thanksgiving"] = thanksgiving.strftime("%Y-%m-%d")
    # Christmas
    holidays["Christmas"] = datetime.date(year, 12, 25).strftime("%Y-%m-%d")
    return holidays


def quarter_of_year(date: datetime.date) -> int:
    """Return quarter number (1-4) for given date."""
    return (date.month - 1) // 3 + 1


def fiscal_year(date: datetime.date, fiscal_start_month: int = 10) -> int:
    """Get fiscal year for given date."""
    if date.month >= fiscal_start_month:
        return date.year + 1
    return date.year
