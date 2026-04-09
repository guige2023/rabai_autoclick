"""Parser utilities for extracting structured data from text.

Provides parsing functions for common text formats
used in automation: key-value pairs, CSV, INI, DSLs,
and coordinate specifications.

Example:
    >>> from utils.parser_utils import parse_kv, parse_csv_line, parse_coords
    >>> parse_kv("x=100,y=200")
    {"x": "100", "y": "200"}
    >>> parse_coords("100, 200")
    (100, 200)
    >>> parse_csv_line('"Alice",30,"NYC"')
    ["Alice", "30", "NYC"]
"""

from __future__ import annotations

import ast
import csv
import io
import json
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union


def parse_kv(
    text: str,
    separator: str = ",",
    kv_sep: str = "=",
    strip_chars: str = " \t",
) -> Dict[str, str]:
    """Parse key-value pairs from text.

    Args:
        text: Text like "x=100,y=200" or "x:100 y:200".
        separator: Separator between pairs.
        kv_sep: Separator between key and value.
        strip_chars: Characters to strip from keys/values.

    Returns:
        Dict of parsed key-value pairs.

    Example:
        >>> parse_kv("x=100, y=200")
        {"x": "100", "y": "200"}
    """
    result: Dict[str, str] = {}
    for part in text.split(separator):
        if kv_sep not in part:
            continue
        key, value = part.split(kv_sep, 1)
        result[key.strip(strip_chars)] = value.strip(strip_chars)
    return result


def parse_csv_line(
    line: str,
    delimiter: str = ",",
    quote_char: str = '"',
) -> List[str]:
    """Parse a single CSV line with proper quoting.

    Args:
        line: CSV line string.
        delimiter: Field delimiter.
        quote_char: Quote character.

    Returns:
        List of parsed fields.
    """
    reader = csv.reader(
        io.StringIO(line),
        delimiter=delimiter,
        quotechar=quote_char,
        skipinitialspace=True,
    )
    return next(reader)


def parse_coords(
    text: str,
    sep: str = ",",
) -> Tuple[int, int]:
    """Parse coordinate string to (x, y) tuple.

    Args:
        text: Coordinate string like "100,200" or "100 200".
        sep: Separator between x and y.

    Returns:
        (x, y) tuple of integers.

    Raises:
        ValueError: If parsing fails.
    """
    text = text.strip()
    for char in [sep, " ", "x", "y"]:
        if char in text:
            parts = text.replace("x", "").replace("y", "").split(char)
            if len(parts) >= 2:
                x = int(float(parts[0].strip()))
                y = int(float(parts[1].strip()))
                return x, y
    raise ValueError(f"Cannot parse coordinates from: {text}")


def parse_bounds(
    text: str,
) -> Tuple[int, int, int, int]:
    """Parse bounds string like 'x,y,w,h' or 'x1,y1,x2,y2'.

    Args:
        text: Bounds string.

    Returns:
        (x, y, width, height) tuple.
    """
    text = text.strip("[](){}")
    parts = re.split(r"[,x y]+", text.strip())
    nums = [int(float(p.strip())) for p in parts if p.strip()]

    if len(nums) == 4:
        if nums[2] > nums[0] and nums[3] > nums[1]:
            return nums[0], nums[1], nums[2] - nums[0], nums[3] - nums[1]
        return nums[0], nums[1], nums[2], nums[3]
    elif len(nums) == 2:
        return 0, 0, nums[0], nums[1]

    raise ValueError(f"Cannot parse bounds from: {text}")


def parse_json_safe(text: str) -> Optional[Any]:
    """Safely parse JSON, returning None on failure.

    Args:
        text: JSON string.

    Returns:
        Parsed object or None.
    """
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def parse_python_literal(text: str) -> Optional[Any]:
    """Safely parse a Python literal expression.

    Args:
        text: Python literal string.

    Returns:
        Parsed Python object or None.
    """
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError, TypeError):
        return None


def parse_range(text: str) -> Tuple[int, int]:
    """Parse a range specification like '10-20' or '10:20'.

    Args:
        text: Range string.

    Returns:
        (start, end) tuple.
    """
    for sep in ["-", ":", ".."]:
        if sep in text:
            parts = text.split(sep)
            if len(parts) == 2:
                return int(parts[0].strip()), int(parts[1].strip())
    raise ValueError(f"Cannot parse range from: {text}")


def parse_percentage(text: str) -> float:
    """Parse a percentage string like '50%'.

    Args:
        text: Percentage string.

    Returns:
        Value as float (e.g., 0.5 for '50%').
    """
    text = text.strip()
    if text.endswith("%"):
        return float(text[:-1]) / 100.0
    return float(text)


def parse_flags(text: str, known_flags: List[str]) -> Dict[str, bool]:
    """Parse command-line style flags.

    Args:
        text: Flag string like '--verbose -x --output=file.txt'.
        known_flags: List of recognized flags.

    Returns:
        Dict mapping flag name -> True if present.
    """
    result = {f: False for f in known_flags}

    tokens = re.findall(r"--?[\w\-]+(?:=\S+)?", text)
    for token in tokens:
        name = token.lstrip("-").split("=")[0]
        for known in known_flags:
            if known.lstrip("-") == name or name in known:
                result[known] = True
                break

    return result


def parse_repeated(
    text: str,
    pattern: str,
    *,
    group: int = 0,
) -> List[str]:
    """Find all occurrences of a pattern.

    Args:
        text: Text to search.
        pattern: Regex pattern.
        group: Which capture group to return (0 = full match).

    Returns:
        List of matched strings.
    """
    regex = re.compile(pattern)
    return [m.group(group) if group <= len(m.groups()) else m.group(0) for m in regex.finditer(text)]


def parse_table(
    lines: List[str],
    *,
    delimiter: str = ",",
    has_header: bool = True,
    skip_empty: bool = True,
) -> Tuple[List[str], List[List[str]]]:
    """Parse a text table into rows.

    Args:
        lines: Text lines.
        delimiter: Column delimiter.
        has_header: First line is header.
        skip_empty: Skip empty lines.

    Returns:
        (header_row, data_rows) tuple.
    """
    if skip_empty:
        lines = [l for l in lines if l.strip()]

    if not lines:
        return [], []

    data = [parse_csv_line(line, delimiter=delimiter) for line in lines]

    if has_header:
        return data[0], data[1:]
    return [], data


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text.

    Args:
        text: Text to parse.

    Returns:
        List of numbers found (as floats).
    """
    return [float(n) for n in re.findall(r"-?\d+(?:\.\d+)?", text)]


def parse_size(text: str) -> Tuple[int, int]:
    """Parse a size specification like '800x600' or '800, 600'.

    Args:
        text: Size string.

    Returns:
        (width, height) tuple.
    """
    return parse_coords(text.replace("x", ","))
