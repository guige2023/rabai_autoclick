"""
Human-readable formatting utilities.

Provides:
- Number formatting (bytes, percentage, ordinal)
- Date/time formatting (relative, duration)
- Text truncation and abbreviation
- List joining with Oxford comma
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta
from typing import Any


def humanize_bytes(num_bytes: int, precision: int = 1) -> str:
    """
    Format bytes as human-readable string.

    Args:
        num_bytes: Number of bytes
        precision: Decimal places

    Returns:
        Human-readable string like "1.5 GB"

    Example:
        >>> humanize_bytes(1536)
        '1.5 KB'
        >>> humanize_bytes(1073741824)
        '1.0 GB'
    """
    if num_bytes < 0:
        return f"-{humanize_bytes(-num_bytes, precision)}"

    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    unit_idx = 0
    value = float(num_bytes)

    while value >= 1024 and unit_idx < len(units) - 1:
        value /= 1024
        unit_idx += 1

    if unit_idx == 0:
        return f"{int(value)} {units[unit_idx]}"

    return f"{value:.{precision}f} {units[unit_idx]}"


def humanize_number(num: float | int, precision: int = 1) -> str:
    """
    Format a number with thousands separators.

    Args:
        num: Number to format
        precision: Decimal places

    Returns:
        Formatted string like "1,234,567.0"

    Example:
        >>> humanize_number(1234567)
        '1,234,567'
    """
    if isinstance(num, int):
        return f"{num:,}"
    return f"{num:,.{precision}f}"


def humanize_percentage(value: float, total: float | None = None, precision: int = 1) -> str:
    """
    Format a value as a percentage.

    Args:
        value: The value (or ratio if total is None)
        total: Optional total to calculate percentage from
        precision: Decimal places

    Returns:
        Formatted percentage string

    Example:
        >>> humanize_percentage(0.5)
        '50.0%'
        >>> humanize_percentage(50, 200)
        '25.0%'
    """
    if total is not None:
        if total == 0:
            return "0%"
        value = value / total
    return f"{value * 100:.{precision}f}%"


def humanize_duration(seconds: float, precision: int = 1) -> str:
    """
    Format a duration in seconds as human-readable string.

    Args:
        seconds: Duration in seconds
        precision: Decimal places for sub-unit

    Returns:
        Human-readable duration string

    Example:
        >>> humanize_duration(3665)
        '1h 1m 5s'
        >>> humanize_duration(0.5)
        '500ms'
    """
    if seconds < 0:
        return f"-{humanize_duration(-seconds, precision)}"

    if seconds < 1:
        ms = seconds * 1000
        if ms < 1:
            return f"{ms:.{precision}f}ms"
        return f"{int(ms)}ms"

    units = [
        ("y", 31536000),
        ("mo", 2592000),
        ("w", 604800),
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
        ("s", 1),
    ]

    parts = []
    remaining = seconds

    for unit_name, unit_seconds in units:
        if remaining >= unit_seconds:
            count = int(remaining // unit_seconds)
            remaining -= count * unit_seconds
            parts.append(f"{count}{unit_name}")

    if not parts:
        return f"{seconds:.{precision}f}s"

    return " ".join(parts)


def humanize_timedelta(delta: timedelta) -> str:
    """Format a timedelta as human-readable string."""
    return humanize_duration(delta.total_seconds())


def humanize_relative_time(dt: datetime | float | int) -> str:
    """
    Format a datetime as relative time (e.g., "2 hours ago").

    Args:
        dt: datetime object or Unix timestamp

    Returns:
        Relative time string

    Example:
        >>> import datetime
        >>> humanize_relative_time(datetime.datetime.now() - timedelta(hours=2))
        '2 hours ago'
    """
    if isinstance(dt, (int, float)):
        dt = datetime.fromtimestamp(dt)

    now = datetime.now(dt.tzinfo)
    delta = now - dt
    seconds = delta.total_seconds()

    if seconds < 0:
        return "in the future"

    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = int(seconds // 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if seconds < 86400:
        hours = int(seconds // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if seconds < 604800:
        days = int(seconds // 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"
    if seconds < 2592000:
        weeks = int(seconds // 604800)
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    if seconds < 31536000:
        months = int(seconds // 2592000)
        return f"{months} month{'s' if months != 1 else ''} ago"

    years = int(seconds // 31536000)
    return f"{years} year{'s' if years != 1 else ''} ago"


def humanize_list(items: list[str], conjunction: str = "and") -> str:
    """
    Format a list with proper Oxford comma.

    Args:
        items: List of strings
        conjunction: Conjunction to use ('and', 'or', 'but')

    Returns:
        Formatted list string

    Example:
        >>> humanize_list(["apple", "banana", "cherry"])
        'apple, banana, and cherry'
        >>> humanize_list(["apple", "banana"])
        'apple and banana'
    """
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} {conjunction} {items[1]}"

    return f"{', '.join(items[:-1])}, {conjunction} {items[-1]}"


def humanize_plural(count: int, singular: str, plural: str | None = None) -> str:
    """
    Return singular or plural form based on count.

    Args:
        count: Number of items
        singular: Singular form
        plural: Plural form (auto-generated if None)

    Returns:
        Formatted string with count

    Example:
        >>> humanize_plural(1, "apple")
        '1 apple'
        >>> humanize_plural(5, "apple")
        '5 apples'
    """
    if plural is None:
        if singular.endswith("y"):
            plural = singular[:-1] + "ies"
        elif singular.endswith(("s", "x", "z", "ch", "sh")):
            plural = singular + "es"
        else:
            plural = singular + "s"
    return f"{count} {singular if count == 1 else plural}"


def truncate(text: str, length: int, suffix: str = "...") -> str:
    """
    Truncate text to a maximum length.

    Args:
        text: Text to truncate
        length: Maximum length (including suffix)
        suffix: String to append if truncated

    Returns:
        Truncated text

    Example:
        >>> truncate("Hello World", 8)
        'Hello...'
    """
    if len(text) <= length:
        return text
    if length <= len(suffix):
        return suffix[:length]
    return text[: length - len(suffix)] + suffix


def truncate_words(text: str, max_words: int, suffix: str = "...") -> str:
    """
    Truncate text to a maximum number of words.

    Args:
        text: Text to truncate
        max_words: Maximum number of words
        suffix: String to append if truncated

    Returns:
        Truncated text
    """
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + suffix


def abbreviate(text: str, max_length: int) -> str:
    """
    Abbreviate text to first letters.

    Args:
        text: Text to abbreviate
        max_length: Maximum length of abbreviation

    Returns:
        Abbreviated string

    Example:
        >>> abbreviate("Hyper Text Markup Language", 4)
        'HTML'
    """
    words = text.split()
    abbrev = "".join(w[0].upper() for w in words if w)
    if len(abbrev) <= max_length:
        return abbrev
    return abbrev[:max_length]


def ordinal(n: int) -> str:
    """
    Convert integer to ordinal string.

    Args:
        n: Integer

    Returns:
        Ordinal string

    Example:
        >>> ordinal(1)
        '1st'
        >>> ordinal(22)
        '22nd'
    """
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def humanize_boolean(value: bool, true_text: str = "Yes", false_text: str = "No") -> str:
    """Format a boolean as human-readable text."""
    return true_text if value else false_text


def humanize_dict(data: dict[str, Any], separator: str = ": ", item_sep: str = ", ") -> str:
    """
    Format a dictionary as human-readable string.

    Args:
        data: Dictionary to format
        separator: Separator between key and value
        item_sep: Separator between items

    Returns:
        Formatted string
    """
    return item_sep.join(f"{k}{separator}{v}" for k, v in data.items())


def pluralize(word: str, count: int) -> str:
    """
    Return plural form of a word based on count.

    Args:
        word: Singular form
        count: Count

    Returns:
        Plural or singular form
    """
    if count == 1:
        return word

    if word.endswith("y"):
        return word[:-1] + "ies"
    elif word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    else:
        return word + "s"
