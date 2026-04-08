"""URL utilities for RabAI AutoClick.

Provides:
- URL parsing and manipulation
- URL validation
- URL component extraction
"""

from __future__ import annotations

import urllib.parse
from typing import (
    Any,
    Dict,
    Optional,
)


def parse(url: str) -> Dict[str, str]:
    """Parse URL into components.

    Args:
        url: URL to parse.

    Returns:
        Dict with scheme, netloc, path, params, query, fragment.
    """
    parsed = urllib.parse.urlsplit(url)
    return {
        "scheme": parsed.scheme,
        "netloc": parsed.netloc,
        "path": parsed.path,
        "params": parsed.params,
        "query": parsed.query,
        "fragment": parsed.fragment,
    }


def build(
    scheme: str = "https",
    netloc: str = "",
    path: str = "",
    params: str = "",
    query: str = "",
    fragment: str = "",
) -> str:
    """Build URL from components.

    Args:
        scheme: URL scheme.
        netloc: Network location.
        path: Path component.
        params: Parameters.
        query: Query string.
        fragment: Fragment.

    Returns:
        Complete URL string.
    """
    return urllib.parse.urlunsplit((scheme, netloc, path, params, query, fragment))


def get_domain(url: str) -> Optional[str]:
    """Extract domain from URL.

    Args:
        url: URL to parse.

    Returns:
        Domain or None.
    """
    parsed = urllib.parse.urlsplit(url)
    return parsed.netloc


def get_path(url: str) -> str:
    """Extract path from URL.

    Args:
        url: URL to parse.

    Returns:
        Path component.
    """
    parsed = urllib.parse.urlsplit(url)
    return parsed.path


def is_valid_url(url: str) -> bool:
    """Check if URL is valid.

    Args:
        url: URL to validate.

    Returns:
        True if valid URL.
    """
    try:
        parsed = urllib.parse.urlsplit(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False


def normalize(url: str) -> str:
    """Normalize URL (remove trailing slashes, etc).

    Args:
        url: URL to normalize.

    Returns:
        Normalized URL.
    """
    parsed = urllib.parse.urlsplit(url)
    path = parsed.path.rstrip("/")
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        path,
        parsed.params,
        parsed.fragment,
    ))


__all__ = [
    "parse",
    "build",
    "get_domain",
    "get_path",
    "is_valid_url",
    "normalize",
]
