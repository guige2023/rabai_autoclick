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
