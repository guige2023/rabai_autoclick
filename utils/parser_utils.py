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


def parse_list(
    text: str,
    delimiter: str = None,
    strip: bool = True,
    filter_empty: bool = True
) -> List[str]:
    """Parse comma/semicolon/line-separated list."""
    if delimiter is None:
        for d in [",", ";", "，", "；", "\n"]:
            if d in text:
                delimiter = d
                break
        else:
            delimiter = ","
    items = text.split(delimiter)
    if strip:
        items = [i.strip() for i in items]
    if filter_empty:
        items = [i for i in items if i]
    return items


def parse_number_range(text: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse text like '1-10' or '5.5~7.5' into (min, max)."""
    text = text.strip()
    for sep in ["-", "~", " to ", ".."]:
        if sep in text:
            parts = text.split(sep, 1)
            if len(parts) == 2:
                min_val = parse_float(parts[0]) if parts[0].strip() else None
                max_val = parse_float(parts[1]) if parts[1].strip() else None
                return (min_val, max_val)
    val = parse_float(text)
    return (val, val)


def parse_version(text: str) -> Tuple[int, ...]:
    """Parse version string like '1.2.3' into tuple of integers."""
    parts = re.findall(r"\d+", text)
    return tuple(int(p) for p in parts)


class RegexParser:
    """Regex-based parser with named groups."""

    def __init__(self, pattern: str, flags: int = 0):
        self.pattern = re.compile(pattern, flags)

    def parse(self, text: str) -> Optional[Dict[str, str]]:
        """Parse text using regex pattern with named groups."""
        match = self.pattern.search(text)
        if match:
            return match.groupdict()
        return None

    def parse_all(self, text: str) -> List[Dict[str, str]]:
        """Parse all occurrences."""
        return [m.groupdict() for m in self.pattern.finditer(text)]
