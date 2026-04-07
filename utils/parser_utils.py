"""
Generic parser utilities for various data formats.
"""

import re
import json
import csv
import io
from typing import Any, Callable, Dict, List, Optional, Tuple, Pattern


def parse_bool(value: str, default: bool = False) -> bool:
    """Parse a string to boolean."""
    truthy = {"true", "1", "yes", "on", "t", "y", "✅", "有"}
    falsy = {"false", "0", "no", "off", "f", "n", "❌", "无", ""}
    normalized = value.strip().lower()
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    return default


def parse_int(
    value: str,
    default: int = 0,
    base: int = 10,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None
) -> int:
    """Parse a string to integer with validation."""
    try:
        result = int(value.strip(), base)
        if min_val is not None:
            result = max(result, min_val)
        if max_val is not None:
            result = min(result, max_val)
        return result
    except ValueError:
        return default


def parse_float(
    value: str,
    default: float = 0.0,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None
) -> float:
    """Parse a string to float with validation."""
    try:
        result = float(value.strip())
        if min_val is not None:
            result = max(result, min_val)
        if max_val is not None:
            result = min(result, max_val)
        return result
    except ValueError:
        return default


def parse_json(
    text: str,
    default: Any = None,
    strict: bool = False
) -> Any:
    """Parse JSON string with error handling."""
    try:
        return json.loads(text, strict=strict)
    except (json.JSONDecodeError, ValueError):
        return default


def parse_csv_line(
    line: str,
    delimiter: str = ",",
    quotechar: str = '"'
) -> List[str]:
    """Parse a single CSV line."""
    reader = csv.reader(io.StringIO(line), delimiter=delimiter, quotechar=quotechar)
    return next(reader)


def parse_key_value(
    text: str,
    delimiter: str = "=",
) -> Dict[str, str]:
    """Parse key=value delimited text into a dict."""
    result = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(delimiter, 1)
        if len(parts) == 2:
            result[parts[0].strip()] = parts[1].strip()
    return result


def parse_url_params(url: str) -> Dict[str, str]:
    """Extract query parameters from URL."""
    from urllib.parse import parse_qs, urlparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    return {k: v[0] if len(v) == 1 else v for k, v in params.items()}
