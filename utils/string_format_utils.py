"""String formatting utilities for automation output and templating.

Provides text templating, string interpolation,
format helpers for generating automation output,
and Unicode/text normalization utilities.

Example:
    >>> from utils.string_format_utils import template, interpolate, pluralize
    >>> result = template('Clicking ${button} at (${x}, ${y})', button='OK', x=100, y=200)
    >>> pluralize(3, 'item', 'items')
"""

from __future__ import annotations

import re
import string
from typing import Any, Dict

__all__ = [
    "template",
    "interpolate",
    "pluralize",
    "title_case",
    "camel_to_snake",
    "snake_to_camel",
    "truncate",
    "word_wrap",
    "StringTemplate",
]


def template(text: str, **values) -> str:
    """Simple template substitution with ${var} syntax.

    Args:
        text: Template string with ${var} placeholders.
        **values: Keyword arguments for substitution.

    Returns:
        Interpolated string.

    Example:
        >>> template('Hello, ${name}!', name='World')
        'Hello, World!'
    """
    def replacer(match):
        key = match.group(1)
        return str(values.get(key, match.group(0)))
    return re.sub(r"\$\{(\w+)\}", replacer, text)


def interpolate(text: str, context: Dict[str, Any]) -> str:
    """Interpolate a template with a dictionary context.

    Args:
        text: Template string.
        context: Dictionary of variable values.

    Returns:
        Interpolated string.
    """
    return template(text, **context)


def pluralize(count: int, singular: str, plural: Optional[str] = None) -> str:
    """Return singular or plural form based on count.

    Args:
        count: Item count.
        singular: Singular form.
        plural: Plural form (auto-generated if None).

    Returns:
        Formatted string like '3 items'.
    """
    if plural is None:
        if singular.endswith("y"):
            plural = singular[:-1] + "ies"
        elif singular.endswith(("s", "x", "z", "ch", "sh")):
            plural = singular + "es"
        else:
            plural = singular + "s"
    word = singular if count == 1 else plural
    return f"{count} {word}"


def title_case(text: str, style: str = "title") -> str:
    """Convert text to title case with various styles.

    Args:
        text: Input text.
        style: 'title', 'sentence', or 'upper'.

    Returns:
        Title-cased string.
    """
    if style == "title":
        return text.title()
    elif style == "sentence":
        return text[0].upper() + text[1:] if text else text
    elif style == "upper":
        return text.upper()
    return text


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: camelCase string.

    Returns:
        snake_case string.

    Example:
        >>> camel_to_snake('myVariableName')
        'my_variable_name'
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_camel(text: str, upper: bool = False) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: snake_case string.
        upper: If True, use PascalCase (first letter uppercase).

    Returns:
        camelCase string.
    """
    components = text.split("_")
    first = components[0]
    if upper:
        first = first.title()
    return first + "".join(x.title() for x in components[1:])


def truncate(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to a maximum length with ellipsis.

    Args:
        text: Input text.
        max_length: Maximum length including suffix.
        suffix: Truncation indicator.

    Returns:
        Truncated string.
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def word_wrap(text: str, width: int = 80, break_long_words: bool = True) -> str:
    """Wrap text to a maximum line width.

    Args:
        text: Input text.
        width: Maximum line width.
        break_long_words: Whether to break long words.

    Returns:
        Wrapped string with newlines.
    """
    import textwrap

    return textwrap.fill(
        text,
        width=width,
        break_long_words=break_long_words,
        break_on_hyphens=False,
    )


class StringTemplate(string.Template):
    """Custom string template with $ interpolation."""

    delimiter = "$"
    idpattern = r"[_a-z][_a-z0-9]*"


def format_table(
    headers: list[str],
    rows: list[list[str]],
    padding: int = 2,
) -> str:
    """Format data as a text table.

    Args:
        headers: Column header names.
        rows: List of row data.
        padding: Spaces between columns.

    Returns:
        Formatted table string.
    """
    if not rows:
        return ""

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def format_row(cells):
        return " " * padding + (" " * padding).join(
            str(cells[i]).ljust(col_widths[i]) for i in range(len(cells))
        )

    separator = "+" + "+".join("-" * (w + padding) for w in col_widths) + "+"
    lines = [separator]
    lines.append(format_row(headers))
    lines.append(separator.replace("-", "="))
    for row in rows:
        lines.append(format_row(row))
    lines.append(separator)

    return "\n".join(lines)


def highlight_keywords(text: str, keywords: list[str], marker: str = "**") -> str:
    """Highlight keywords in text with a marker.

    Args:
        text: Input text.
        keywords: List of keywords to highlight.
        marker: Highlight marker (default: **).

    Returns:
        Text with highlighted keywords.
    """
    result = text
    for kw in keywords:
        pattern = re.compile(re.escape(kw), re.IGNORECASE)
        result = pattern.sub(f"{marker}{kw}{marker}", result)
    return result
