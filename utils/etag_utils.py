"""
ETag generation and validation utilities.

Provides ETag creation, comparison, and conditional request handling.
"""

from __future__ import annotations

import hashlib
import os
from typing import Literal


def generate_etag(
    content: bytes | str | None = None,
    *,
    file_path: str | None = None,
    weak: bool = False,
) -> str:
    """
    Generate ETag for content or file.

    Args:
        content: Content bytes or string
        file_path: Alternative: compute from file
        weak: Generate weak ETag (prefixed with W/)

    Returns:
        ETag string (with or without W/ prefix)
    """
    if file_path:
        content = _etag_from_file(file_path)
    if content is None:
        return ""

    if isinstance(content, str):
        content = content.encode("utf-8")

    digest = hashlib.sha256(content).hexdigest()[:16]
    prefix = 'W/"' if weak else '"'
    return f'{prefix}{digest}"'


def _etag_from_file(path: str) -> bytes:
    """Compute content hash from file."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.digest()


def generate_etag_from_stat(
    size: int,
    mtime: float,
) -> str:
    """
    Generate ETag from file stat values.

    Args:
        size: File size in bytes
        mtime: Modified time (Unix timestamp)

    Returns:
        ETag string
    """
    digest = hashlib.sha256(f"{size}-{mtime}".encode()).hexdigest()[:16]
    return f'"{digest}"'


def weak_etag(content: bytes | str) -> str:
    """Generate weak ETag (for content that is semantically equivalent)."""
    return generate_etag(content, weak=True)


def strong_etag(content: bytes | str) -> str:
    """Generate strong ETag (for byte-identical content)."""
    return generate_etag(content, weak=False)


def parse_etag(tag: str) -> tuple[bool, str]:
    """
    Parse ETag string.

    Args:
        tag: Raw ETag header value

    Returns:
        Tuple of (is_weak, tag_without_quotes)
    """
    tag = tag.strip()
    weak = tag.startswith('W/"') or tag.startswith("W/'")
    if weak:
        tag = tag[3:-1]
    else:
        if (tag.startswith('"') and tag.endswith('"')) or (tag.startswith("'") and tag.endswith("'")):
            tag = tag[1:-1]
    return weak, tag


def etag_matches(
    if_none_match: str,
    current_etag: str,
) -> bool:
    """
    Check if current ETag matches If-None-Match header.

    Args:
        if_none_match: Raw If-None-Match header value
        current_etag: Current ETag

    Returns:
        True if ETag matches (should return 304)
    """
    if not if_none_match or if_none_match == "*":
        return False
    current_weak, current_val = parse_etag(current_etag)
    for tag in if_none_match.split(","):
        tag = tag.strip()
        if not tag:
            continue
        weak, val = parse_etag(tag)
        if weak:
            continue
        if current_val == val:
            return True
    return False


def etag_matches_any(
    etags: list[str],
    current_etag: str,
) -> bool:
    """Check if current ETag matches any in a list."""
    current_weak, current_val = parse_etag(current_etag)
    for tag in etags:
        weak, val = parse_etag(tag)
        if weak:
            continue
        if current_val == val:
            return True
    return False


def build_etag_header(etag: str) -> dict[str, str]:
    """Build response headers dict for ETag."""
    return {"ETag": etag}


def build_conditional_headers(
    etag: str,
    last_modified: float | None = None,
) -> dict[str, str]:
    """Build all conditional request headers."""
    headers = {"ETag": etag}
    if last_modified is not None:
        import time
        headers["Last-Modified"] = time.strftime(
            "%a, %d %b %Y %H:%M:%S GMT", time.gmtime(last_modified)
        )
    return headers
