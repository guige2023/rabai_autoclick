"""HTML utilities for RabAI AutoClick.

Provides:
- HTML parsing and traversal
- Text extraction from HTML
- HTML sanitization helpers
- Tag and attribute manipulation
"""

from __future__ import annotations

import re
import html.entities
from typing import (
    Any,
    Dict,
    List,
    Optional,
)


def escape_html(text: str) -> str:
    """Escape HTML special characters.

    Args:
        text: Text to escape.

    Returns:
        Escaped text safe for HTML insertion.
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def unescape_html(text: str) -> str:
    """Unescape HTML entities.

    Args:
        text: HTML text with entities.

    Returns:
        Text with entities decoded.
    """
    def replace_entity(match: re.Match) -> str:
        entity = match.group(1)
        if entity.startswith("#x"):
            try:
                return chr(int(entity[2:], 16))
            except ValueError:
                return match.group(0)
        elif entity.startswith("#"):
            try:
                return chr(int(entity[1:]))
            except ValueError:
                return match.group(0)
        try:
            return chr(html.entities.name2codepoint[entity])
        except KeyError:
            return match.group(0)

    return re.sub(r"&([^;]+);", replace_entity, text)


def strip_tags(html_text: str) -> str:
    """Remove all HTML tags from text.

    Args:
        html_text: HTML string.

    Returns:
        Plain text with tags removed.
    """
    return re.sub(r"<[^>]+>", "", html_text)


def extract_text(html_text: str, preserve_newlines: bool = True) -> str:
    """Extract visible text from HTML.

    Args:
        html_text: HTML string.
        preserve_newlines: Whether to keep newlines.

    Returns:
        Extracted plain text.
    """
    text = strip_tags(html_text)
    text = unescape_html(text)
    text = text.strip()
    if preserve_newlines:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n+", "\n", text)
    else:
        text = re.sub(r"\s+", " ", text)
    return text


def get_tag_attributes(tag: str) -> Dict[str, str]:
    """Extract attributes from an HTML tag.

    Args:
        tag: HTML tag string like '<a href="url" class="foo">'.

    Returns:
        Dict of attribute names to values.
    """
    attrs: Dict[str, str] = {}
    match = re.match(r"<(\w+)\s+(.+?)>", tag, re.DOTALL)
    if not match:
        return attrs

    attr_string = match.group(2)
    for attr_match in re.finditer(
        r'(\w+)="([^"]*)"', attr_string
    ):
        attrs[attr_match.group(1)] = attr_match.group(2)

    return attrs


def make_links_clickable(text: str) -> str:
    """Convert URLs in text to HTML anchor tags.

    Args:
        text: Text containing URLs.

    Returns:
        Text with URLs wrapped in anchor tags.
    """
    url_pattern = r'(https?://[^\s<>"\'\)]+)'
    return re.sub(
        url_pattern,
        r'<a href="\1">\1</a>',
        text,
    )


def add_class(html: str, css_class: str) -> str:
    """Add a CSS class to an HTML tag.

    Args:
        html: HTML string.
        css_class: Class name to add.

    Returns:
        HTML with class added.
    """
    def replacer(match: re.Match) -> str:
        tag = match.group(0)
        attrs = get_tag_attributes(tag)
        current = attrs.get("class", "")
        if current:
            new_class = f"{current} {css_class}"
        else:
            new_class = css_class
        attrs["class"] = new_class
        attrs_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
        return f"<{attrs.get('', 'div')} {attrs_str}>"

    return re.sub(r"<(\w+)([^>]*)>", replacer, html, count=1)


def remove_class(html: str, css_class: str) -> str:
    """Remove a CSS class from an HTML tag.

    Args:
        html: HTML string.
        css_class: Class name to remove.

    Returns:
        HTML with class removed.
    """
    def replacer(match: re.Match) -> str:
        tag = match.group(0)
        attrs = get_tag_attributes(tag)
        current = attrs.get("class", "")
        classes = current.split()
        if css_class in classes:
            classes.remove(css_class)
        attrs["class"] = " ".join(classes)
        attrs_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
        return f"<{attrs.get('', 'div')} {attrs_str}>"

    return re.sub(r"<(\w+)([^>]*)>", replacer, html, count=1)


def build_tag(
    tag_name: str,
    content: str = "",
    **attrs: Any,
) -> str:
    """Build an HTML tag with attributes.

    Args:
        tag_name: Tag name (e.g., 'a', 'div').
        content: Inner content.
        **attrs: HTML attributes as keyword args.

    Returns:
        Complete HTML tag string.
    """
    attr_str = ""
    if attrs:
        attr_str = " " + " ".join(
            f'{k}="{v}"' for k, v in attrs.items()
        )
    if content:
        return f"<{tag_name}{attr_str}>{content}</{tag_name}>"
    return f"<{tag_name}{attr_str} />"


def parse_query_string(query: str) -> Dict[str, str]:
    """Parse a URL query string.

    Args:
        query: Query string (without leading ?).

    Returns:
        Dict of parameter names to values.
    """
    params: Dict[str, str] = {}
    for pair in query.split("&"):
        if "=" in pair:
            k, v = pair.split("=", 1)
            params[k] = v
    return params


__all__ = [
    "escape_html",
    "unescape_html",
    "strip_tags",
    "extract_text",
    "get_tag_attributes",
    "make_links_clickable",
    "add_class",
    "remove_class",
    "build_tag",
    "parse_query_string",
]
