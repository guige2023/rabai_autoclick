"""
Content negotiation utilities.

Provides Accept header parsing, quality factor handling,
and content type/encoding negotiation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass
class MediaRange:
    """Parsed media range with quality factor."""
    type: str
    subtype: str
    params: dict[str, str]
    quality: float
    raw: str


def parse_accept_header(header: str) -> list[MediaRange]:
    """
    Parse Accept header into media ranges with quality factors.

    Args:
        header: Raw Accept header value

    Returns:
        List of MediaRange sorted by quality (highest first)
    """
    parts = header.split(",")
    results = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        tokens = part.split(";")
        media_type = tokens[0].strip()
        quality = 1.0
        params: dict[str, str] = {}
        for token in tokens[1:]:
            token = token.strip()
            if token.startswith("q="):
                try:
                    quality = float(token[2:])
                except ValueError:
                    quality = 1.0
            elif "=" in token:
                k, v = token.split("=", 1)
                params[k.strip()] = v.strip()
        if "/" in media_type:
            mtype, subtype = media_type.split("/", 1)
            results.append(MediaRange(
                type=mtype.lower(),
                subtype=subtype.lower(),
                params=params,
                quality=quality,
                raw=part,
            ))
    return sorted(results, key=lambda m: m.quality, reverse=True)


def parse_accept_encoding(header: str) -> list[tuple[str, float]]:
    """
    Parse Accept-Encoding header.

    Args:
        header: Accept-Encoding header value

    Returns:
        List of (encoding, qvalue) sorted by quality
    """
    parts = header.split(",")
    encodings = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        tokens = part.split(";")
        encoding = tokens[0].strip().lower()
        q = 1.0
        for token in tokens[1:]:
            token = token.strip()
            if token.startswith("q="):
                try:
                    q = float(token[2:])
                except ValueError:
                    q = 1.0
        encodings.append((encoding, q))
    return sorted(encodings, key=lambda x: x[1], reverse=True)


def parse_accept_language(header: str) -> list[tuple[str, float]]:
    """
    Parse Accept-Language header.

    Args:
        header: Accept-Language header value

    Returns:
        List of (language-tag, qvalue) sorted by quality
    """
    parts = header.split(",")
    languages = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        tokens = part.split(";")
        lang = tokens[0].strip().lower()
        q = 1.0
        for token in tokens[1:]:
            token = token.strip()
            if token.startswith("q="):
                try:
                    q = float(token[2:])
                except ValueError:
                    q = 1.0
        languages.append((lang, q))
    return sorted(languages, key=lambda x: x[1], reverse=True)


def match_content_type(
    accepted: list[MediaRange],
    available: list[str],
) -> str | None:
    """
    Match best content type using media range negotiation.

    Args:
        accepted: Parsed Accept header media ranges
        available: Available content types

    Returns:
        Best matching content type or None
    """
    available_set = {t.strip().lower() for t in available}
    for media in accepted:
        if media.quality == 0:
            continue
        if media.type == "*" and media.subtype == "*":
            return available[0] if available else None
        if media.subtype == "*":
            if f"{media.type}/*" in available_set:
                return f"{media.type}/*"
        else:
            full = f"{media.type}/{media.subtype}"
            if full in available_set:
                return full
    return None


def match_accept_encoding(
    accepted: list[tuple[str, float]],
    available: list[str],
) -> str | None:
    """
    Match best encoding using Accept-Encoding negotiation.

    Args:
        accepted: Parsed Accept-Encoding list
        available: Available encodings

    Returns:
        Best matching encoding or None
    """
    for encoding, qvalue in accepted:
        if qvalue == 0:
            continue
        if encoding == "*":
            return available[0] if available else None
        if encoding in available:
            return encoding
    return None


def build_vary_header(fields: list[str]) -> str:
    """
    Build Cache-Control Vary header value.

    Args:
        fields: Field names (e.g. ["Accept-Encoding", "Accept-Language"])

    Returns:
        Comma-separated Vary header value
    """
    return ", ".join(f.strip() for f in fields if f.strip())


def negotiate_content_type(
    accept_header: str | None,
    supported_types: list[str],
    default: str = "application/octet-stream",
) -> str:
    """
    Convenience function for content type negotiation.

    Args:
        accept_header: Accept header value
        supported_types: Server-supported content types
        default: Default if no match

    Returns:
        Best matching content type
    """
    if not accept_header:
        return supported_types[0] if supported_types else default
    accepted = parse_accept_header(accept_header)
    match = match_content_type(accepted, supported_types)
    return match if match else default
