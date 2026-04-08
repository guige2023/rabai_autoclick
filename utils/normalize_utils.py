"""Normalize utilities for RabAI AutoClick.

Provides:
- String normalization (Unicode, case, whitespace)
- Number normalization
- Path normalization
- Data structure normalization
"""

from __future__ import annotations

import re
import unicodedata
from typing import (
    Any,
    Dict,
    List,
    Optional,
)


def normalize_unicode(
    text: str,
    form: str = "NFKC",
) -> str:
    """Normalize Unicode text.

    Args:
        text: Input text.
        form: Unicode normalization form (NFC, NFD, NFKC, NFKD).

    Returns:
        Normalized text.
    """
    return unicodedata.normalize(form, text)


def normalize_whitespace(text: str) -> str:
    """Normalize all whitespace to single spaces.

    Args:
        text: Input text.

    Returns:
        Text with normalized whitespace.
    """
    return " ".join(text.split())


def normalize_case(
    text: str,
    mode: str = "lower",
) -> str:
    """Normalize text case.

    Args:
        text: Input text.
        mode: 'lower', 'upper', 'title', 'capitalize'.

    Returns:
        Case-normalized text.
    """
    if mode == "lower":
        return text.lower()
    elif mode == "upper":
        return text.upper()
    elif mode == "title":
        return text.title()
    elif mode == "capitalize":
        return text.capitalize()
    return text


def normalize_newlines(text: str) -> str:
    """Normalize all newline styles to \\n.

    Args:
        text: Input text.

    Returns:
        Text with normalized newlines.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


def normalize_number(
    value: float,
    decimals: int = 2,
) -> float:
    """Normalize a number to fixed decimal places.

    Args:
        value: Number to normalize.
        decimals: Number of decimal places.

    Returns:
        Rounded float.
    """
    return round(value, decimals)


def normalize_percentage(
    value: float,
    decimals: int = 1,
) -> float:
    """Normalize a value as percentage (0-100).

    Args:
        value: Value to normalize.
        decimals: Decimal places.

    Returns:
        Percentage value.
    """
    if value < 0:
        value = 0.0
    elif value > 100:
        value = 100.0
    return round(value, decimals)


def normalize_dict(
    data: Dict[str, Any],
    drop_none: bool = True,
    drop_empty: bool = False,
) -> Dict[str, Any]:
    """Normalize a dictionary.

    Args:
        data: Input dict.
        drop_none: Remove None values.
        drop_empty: Remove empty collections.

    Returns:
        Normalized dict.
    """
    result: Dict[str, Any] = {}
    for k, v in data.items():
        if drop_none and v is None:
            continue
        if drop_empty and not v:
            continue
        result[k] = v
    return result


def normalize_list(
    items: List[Any],
    dedupe: bool = True,
    sort: bool = False,
) -> List[Any]:
    """Normalize a list.

    Args:
        items: Input list.
        dedupe: Remove duplicates.
        sort: Sort the list.

    Returns:
        Normalized list.
    """
    result = items[:]
    if dedupe:
        seen: set = set()
        result = [x for x in result if not (x in seen or seen.add(x))]
    if sort:
        result = sorted(result)
    return result


def normalize_boolean(value: Any) -> bool:
    """Convert various truthy/falsy values to bool.

    Args:
        value: Value to convert.

    Returns:
        Boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "on")
    return bool(value)


def slugify(text: str) -> str:
    """Convert text to a URL-safe slug.

    Args:
        text: Input text.

    Returns:
        Slugified text.
    """
    text = normalize_unicode(text.lower())
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text.strip("-")


def normalize_email_local(email: str) -> str:
    """Normalize email local part (case, dots, plus).

    Args:
        email: Email address.

    Returns:
        Normalized local part.
    """
    local, domain = email.rsplit("@", 1)
    local = local.lower().split("+")[0].replace(".", "")
    return f"{local}@{domain}"


__all__ = [
    "normalize_unicode",
    "normalize_whitespace",
    "normalize_case",
    "normalize_newlines",
    "normalize_number",
    "normalize_percentage",
    "normalize_dict",
    "normalize_list",
    "normalize_boolean",
    "slugify",
    "normalize_email_local",
]
