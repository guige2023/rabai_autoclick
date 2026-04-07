"""
Human-readable formatting utilities (file sizes, time deltas, numbers).
"""

from datetime import datetime, timezone
from typing import Union, Optional


def format_bytes(
    size: int,
    binary: bool = False,
    precision: int = 2,
    separator: str = " "
) -> str:
    """
    Format bytes as human-readable string.

    Args:
        size: Size in bytes
        binary: Use binary (1024) vs decimal (1000) units
        precision: Decimal precision
        separator: Separator between number and unit
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    if binary:
        units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
        divisor = 1024.0
    else:
        divisor = 1000.0

    if size < 0:
        return f"-{format_bytes(-size, binary, precision, separator)}"

    unit_idx = 0
    size_float = float(size)
    while size_float >= divisor and unit_idx < len(units) - 1:
        size_float /= divisor
        unit_idx += 1

    if unit_idx == 0:
        return f"{int(size_float)}{separator}{units[unit_idx]}"
    return f"{size_float:.{precision}f}{separator}{units[unit_idx]}"


def parse_bytes(size_str: str) -> int:
    """Parse human-readable byte string to integer."""
    size_str = size_str.strip().upper()
    units = {
        "B": 1, "KB": 1000, "KIB": 1024,
        "MB": 1000**2, "MIB": 1024**2,
        "GB": 1000**3, "GIB": 1024**3,
        "TB": 1000**4, "TIB": 1024**4,
    }
    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            num_str = size_str[: -len(unit)].strip()
            try:
                return int(float(num_str) * multiplier)
            except ValueError:
                pass
    try:
        return int(float(size_str))
    except ValueError:
        raise ValueError(f"Cannot parse byte size: {size_str!r}")


def format_duration(
    seconds: float,
    format_type: str = "long",
    max_units: int = 2
) -> str:
    """Format duration in seconds as human-readable string."""
    if seconds < 0:
        return f"-{format_duration(-seconds, format_type, max_units)}"

    units = [
        ("year", 365 * 24 * 3600), ("month", 30 * 24 * 3600),
        ("week", 7 * 24 * 3600), ("day", 24 * 3600),
        ("hour", 3600), ("minute", 60), ("second", 1),
    ]

    parts = []
    remaining = seconds
    for name, secs_in_unit in units:
        if remaining >= secs_in_unit:
            count = int(remaining // secs_in_unit)
            remaining %= secs_in_unit
            if format_type == "long":
                parts.append(f"{count} {name}{'s' if count != 1 else ''}")
            else:
                abbr = {"year": "yr", "month": "mo", "week": "wk",
                        "day": "d", "hour": "hr", "minute": "min", "second": "sec"}
                parts.append(f"{count}{abbr.get(name, name[0])}")
            if len(parts) >= max_units:
                break

    if not parts:
        return "just now"

    if format_type == "long" and len(parts) > 1:
        return ", ".join(parts[:-1]) + " and " + parts[-1]
    return " ".join(parts)


def time_ago(
    dt: Union[datetime, int, float],
    now: Optional[datetime] = None
) -> str:
    """Format a datetime as a human-readable 'time ago' string."""
    if now is None:
        now = datetime.now(timezone.utc)
    if isinstance(dt, (int, float)):
        dt = datetime.fromtimestamp(dt, tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    total_seconds = diff.total_seconds()
    if total_seconds < 0:
        return "in the future"
    if total_seconds < 60:
        return "just now"
    return format_duration(total_seconds, "long", 2) + " ago"


import math


def format_number(
    num: float,
    precision: int = 2,
    thousand_sep: str = ",",
    decimal_sep: str = "."
) -> str:
    """Format a number with thousand separators."""
    if math.isnan(num) or math.isinf(num):
        return str(num)
    negative = num < 0
    num = abs(num)
    int_part = int(num)
    dec_part = num - int_part
    int_str = f"{int_part:,}".replace(",", thousand_sep)
    if precision > 0:
        dec_str = f"{dec_part:.{precision}f}"[2:]
        return f"{'-' if negative else ''}{int_str}{decimal_sep}{dec_str}"
    return f"{'-' if negative else ''}{int_str}"


def format_currency(
    amount: float,
    currency: str = "USD",
    show_symbol: bool = True,
    precision: int = 2
) -> str:
    """Format a number as currency."""
    symbols = {
        "USD": "$", "EUR": "€", "GBP": "£",
        "CNY": "¥", "JPY": "¥", "KRW": "₩",
    }
    symbol = symbols.get(currency.upper(), currency + " ")
    formatted = format_number(amount, precision)
    if not show_symbol:
        return formatted
    after_symbol = currency.upper() in ("EUR", "GBP")
    return f"{formatted} {symbol}" if after_symbol else f"{symbol}{formatted}"


def pluralize(
    count: int,
    singular: str,
    plural: Optional[str] = None,
    include_count: bool = True
) -> str:
    """Pluralize a word based on count."""
    if plural is None:
        if singular.endswith("y") and singular[-2] not in "aeiou":
            plural = singular[:-1] + "ies"
        elif singular.endswith(("s", "x", "z", "ch", "sh")):
            plural = singular + "es"
        else:
            plural = singular + "s"
    word = singular if count == 1 else plural
    return f"{count} {word}" if include_count else word


def ordinal(n: int) -> str:
    """Return ordinal string (1st, 2nd, 3rd, etc.)."""
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def abbreviate_number(num: float, precision: int = 1) -> str:
    """Abbreviate large numbers (1.2K, 3.5M, etc.)."""
    if num < 1000:
        return str(int(num)) if num == int(num) else f"{num:.{precision}f}"
    elif num < 1_000_000:
        return f"{num / 1000:.{precision}f}K"
    elif num < 1_000_000_000:
        return f"{num / 1_000_000:.{precision}f}M"
    elif num < 1_000_000_000_000:
        return f"{num / 1_000_000_000:.{precision}f}B"
    else:
        return f"{num / 1_000_000_000_000:.{precision}f}T"


def natural_join(items: list, separator: str = ", ", last_separator: str = " and ") -> str:
    """Join items with natural language separators."""
    if len(items) == 0:
        return ""
    if len(items) == 1:
        return str(items[0])
    if len(items) == 2:
        return f"{items[0]}{last_separator}{items[1]}"
    return separator.join(str(i) for i in items[:-1]) + f"{last_separator}{items[-1]}"
