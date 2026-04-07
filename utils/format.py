"""Format utilities for RabAI AutoClick.

Provides:
- Data formatting
- Number formatting
- String formatting
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union


def format_number(n: Union[int, float], decimals: int = 2) -> str:
    """Format number with thousand separators.

    Args:
        n: Number to format.
        decimals: Decimal places.

    Returns:
        Formatted string.
    """
    if isinstance(n, float):
        return f"{n:,.{decimals}f}"
    return f"{n:,}"


def format_bytes(size: int, precision: int = 2) -> str:
    """Format bytes as human-readable string.

    Args:
        size: Size in bytes.
        precision: Decimal precision.

    Returns:
        Formatted string (e.g., "1.5 MB").
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size)
    unit_idx = 0

    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024
        unit_idx += 1

    return f"{size:.{precision}f} {units[unit_idx]}"


def format_percent(value: float, total: float, decimals: int = 1) -> str:
    """Format percentage.

    Args:
        value: Numerator.
        total: Denominator.
        decimals: Decimal places.

    Returns:
        Formatted percentage string.
    """
    if total == 0:
        return "0%"

    pct = (value / total) * 100
    return f"{pct:.{decimals}f}%"


def format_currency(
    amount: float,
    currency: str = "USD",
    locale: str = "en_US",
) -> str:
    """Format currency.

    Args:
        amount: Amount to format.
        currency: Currency code.
        locale: Locale for formatting.

    Returns:
        Formatted currency string.
    """
    try:
        from babel.numbers import format_currency as babel_format
        return babel_format(amount, currency, locale=locale)
    except ImportError:
        symbols = {
            "USD": "$",
            "EUR": "€",
            "GBP": "£",
            "JPY": "¥",
            "CNY": "¥",
        }
        symbol = symbols.get(currency, currency)
        return f"{symbol}{amount:,.2f}"


def format_duration_long(seconds: float) -> str:
    """Format duration as human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "2 hours, 15 minutes").
    """
    if seconds < 60:
        return f"{seconds:.0f} seconds"

    minutes = int(seconds // 60)
    seconds = seconds % 60

    if minutes < 60:
        parts = []
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if seconds > 0:
            parts.append(f"{seconds:.0f} second{'s' if seconds != 1 else ''}")
        return ", ".join(parts)

    hours = minutes // 60
    minutes = minutes % 60

    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")

    return ", ".join(parts)


def format_timestamp(ts: float, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format Unix timestamp.

    Args:
        ts: Unix timestamp.
        fmt: Format string.

    Returns:
        Formatted datetime string.
    """
    dt = datetime.fromtimestamp(ts)
    return dt.strftime(fmt)


def format_relative_time(dt: datetime) -> str:
    """Format datetime as relative time.

    Args:
        dt: Datetime to format.

    Returns:
        Relative time string.
    """
    now = datetime.now()
    delta = now - dt

    if delta.total_seconds() < 60:
        return "just now"

    if delta.total_seconds() < 3600:
        minutes = int(delta.total_seconds() / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"

    if delta.days < 1:
        hours = int(delta.total_seconds() / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"

    if delta.days < 7:
        return f"{delta.days} day{'s' if delta.days != 1 else ''} ago"

    if delta.days < 30:
        weeks = delta.days // 7
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"

    if delta.days < 365:
        months = delta.days // 30
        return f"{months} month{'s' if months != 1 else ''} ago"

    years = delta.days // 365
    return f"{years} year{'s' if years != 1 else ''} ago"


def format_json(data: Any, indent: int = 2, sort_keys: bool = False) -> str:
    """Format data as JSON.

    Args:
        data: Data to format.
        indent: Indentation level.
        sort_keys: Sort dictionary keys.

    Returns:
        JSON string.
    """
    return json.dumps(data, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def truncate_middle(text: str, max_length: int, placeholder: str = "...") -> str:
    """Truncate text in the middle.

    Args:
        text: Text to truncate.
        max_length: Maximum length.
        placeholder: Replacement for middle.

    Returns:
        Truncated string.
    """
    if len(text) <= max_length:
        return text

    placeholder_len = len(placeholder)
    if placeholder_len >= max_length:
        return placeholder[:max_length]

    available = max_length - placeholder_len
    start_len = (available + 1) // 2
    end_len = available - start_len

    return text[:start_len] + placeholder + text[-end_len:]


def format_list(items: list, conjunction: str = "and") -> str:
    """Format list as readable string.

    Args:
        items: Items to format.
        conjunction: Joining word.

    Returns:
        Formatted string (e.g., "A, B and C").
    """
    if not items:
        return ""
    if len(items) == 1:
        return str(items[0])
    if len(items) == 2:
        return f"{items[0]} {conjunction} {items[1]}"

    return ", ".join(str(i) for i in items[:-1]) + f", {conjunction} {items[-1]}"


def format_dict_table(data: Dict[str, Any], headers: Optional[tuple] = None) -> str:
    """Format dictionary as ASCII table.

    Args:
        data: Dictionary to format.
        headers: Optional (key_header, value_header) tuple.

    Returns:
        Formatted table string.
    """
    if not data:
        return ""

    if headers is None:
        headers = ("Key", "Value")

    key_header, value_header = headers
    max_key_len = max(len(str(k)) for k in data.keys())
    max_val_len = max(len(str(v)) for v in data.values())
    max_key_len = max(max_key_len, len(key_header))
    max_val_len = max(max_val_len, len(value_header))

    total_width = max_key_len + max_val_len + 7

    lines = []
    lines.append("+" + "-" * (total_width - 2) + "+")
    lines.append(f"| {key_header:<{max_key_len}} | {value_header:<{max_val_len}} |")
    lines.append("+" + "=" * (max_key_len + 2) + "+" + "=" * (max_val_len + 2) + "+")

    for key, value in sorted(data.items()):
        lines.append(f"| {str(key):<{max_key_len}} | {str(value):<{max_val_len}} |")

    lines.append("+" + "-" * (total_width - 2) + "+")

    return "\n".join(lines)