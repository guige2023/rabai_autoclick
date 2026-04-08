"""URL query utilities for RabAI AutoClick.

Provides:
- URL query string parsing
- Query parameter manipulation
- Query building
"""

from __future__ import annotations

import urllib.parse
from typing import (
    Any,
    Dict,
    List,
    Optional,
)


def parse_query(query_string: str) -> Dict[str, str]:
    """Parse URL query string to dict.

    Args:
        query_string: Query string (without leading ?).

    Returns:
        Dict of parameter names to values.
    """
    parsed = urllib.parse.parse_qs(query_string)
    return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}


def build_query(params: Dict[str, Any]) -> str:
    """Build query string from dict.

    Args:
        params: Parameter dict.

    Returns:
        Query string.
    """
    return urllib.parse.urlencode(params, doseq=True)


def add_param(url: str, key: str, value: Any) -> str:
    """Add a parameter to URL.

    Args:
        url: Base URL.
        key: Parameter name.
        value: Parameter value.

    Returns:
        URL with added parameter.
    """
    parsed = urllib.parse.urlsplit(url)
    params = parse_query(parsed.query)
    params[key] = str(value)
    new_query = build_query(params)
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        new_query,
        parsed.fragment,
    ))


def remove_param(url: str, key: str) -> str:
    """Remove a parameter from URL.

    Args:
        url: Base URL.
        key: Parameter name to remove.

    Returns:
        URL with parameter removed.
    """
    parsed = urllib.parse.urlsplit(url)
    params = parse_query(parsed.query)
    params.pop(key, None)
    new_query = build_query(params)
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        new_query,
        parsed.fragment,
    ))


def get_param(url: str, key: str, default: Optional[str] = None) -> Optional[str]:
    """Get a parameter value from URL.

    Args:
        url: URL to parse.
        key: Parameter name.
        default: Default if not found.

    Returns:
        Parameter value or default.
    """
    parsed = urllib.parse.urlsplit(url)
    params = parse_query(parsed.query)
    return params.get(key, default)


__all__ = [
    "parse_query",
    "build_query",
    "add_param",
    "remove_param",
    "get_param",
]
