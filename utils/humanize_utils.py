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
