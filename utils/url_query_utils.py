"""
URL query string parsing and building utilities.

Provides advanced URL manipulation, query parameter handling,
and URL template expansion.
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Any, Literal


def parse_qs(
    query: str,
    keep_blank_values: bool = True,
    max_depth: int = 10,
) -> dict[str, Any]:
    """
    Parse query string with nested bracket notation support.

    Supports:
      - foo=bar -> {"foo": "bar"}
      - foo=bar&foo=baz -> {"foo": ["bar", "baz"]}
      - foo[bar]=baz -> {"foo": {"bar": "baz"}}
      - foo[]=bar -> {"foo": ["bar"]}

    Args:
        query: Query string
        keep_blank_values: Keep empty values
        max_depth: Maximum nesting depth

    Returns:
        Parsed query dictionary
    """
    result: dict[str, Any] = {}

    for key, value in urllib.parse.parse_qsl(
        query,
        keep_blank_values=keep_blank_values,
    ):
        if "[" in key:
            parts = re.match(r"^([^\[]+)(.*)$", key)
            if parts:
                root = parts.group(1)
                path_str = parts.group(2)
                brackets = re.findall(r"\[([^\]]*)\]", path_str)
                target = result.setdefault(root, {})
                for i, bracket in enumerate(brackets[:-1]):
                    if bracket:
                        key_part = int(bracket) if bracket.isdigit() else bracket
                        target = target.setdefault(key_part, {})
                    else:
                        target = target.setdefault(len(target), {})
                last_bracket = brackets[-1]
                if last_bracket.isdigit():
                    target[int(last_bracket)] = value
                elif last_bracket:
                    target[last_bracket] = value
                else:
                    if isinstance(target, list):
                        target.append(value)
                    else:
                        target[len(target)] = value
        else:
            if key in result:
                existing = result[key]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    result[key] = [existing, value]
            else:
                result[key] = value

    return result


def build_qs(
    params: dict[str, Any],
    separator: str = "&",
    encode: bool = True,
) -> str:
    """
    Build query string from dict (with bracket notation).

    Args:
        params: Parameters dictionary
        separator: Pair separator
        encode: URL-encode values

    Returns:
        Query string
    """
    parts = []

    def _flatten(
        obj: Any,
        prefix: str = "",
    ) -> list[tuple[str, str]]:
        items: list[tuple[str, str]] = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_prefix = f"{prefix}[{k}]" if prefix else k
                items.extend(_flatten(v, new_prefix))
        elif isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                new_prefix = f"{prefix}[]" if prefix else f"{prefix}[{i}]"
                items.extend(_flatten(v, new_prefix))
        else:
            items.append((prefix, str(obj)))
        return items

    for key, value in params.items():
        for flat_key, flat_value in _flatten(value, key):
            if encode:
                flat_key = urllib.parse.quote(flat_key, safe="")
                flat_value = urllib.parse.quote(str(flat_value), safe="")
            parts.append(f"{flat_key}={flat_value}")

    return separator.join(parts)


def update_query_params(
    url: str,
    updates: dict[str, Any],
    remove: list[str] | None = None,
) -> str:
    """
    Update query parameters in a URL.

    Args:
        url: Original URL
        updates: Parameters to add/update
        remove: Parameters to remove

    Returns:
        Updated URL
    """
    parsed = urllib.parse.urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=False)

    if remove:
        for key in remove:
            query.pop(key, None)

    for key, value in updates.items():
        if value is None:
            query.pop(key, None)
        else:
            query[key] = value

    new_query = build_qs(query)
    return urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment,
    ))


def get_query_param(
    url: str,
    key: str,
    default: Any = None,
) -> Any:
    """Get a single query parameter from URL."""
    parsed = urllib.parse.urlparse(url)
    params = parse_qs(parsed.query)
    return params.get(key, default)


def merge_query_params(
    *urls: str,
    updates: dict[str, Any] | None = None,
) -> str:
    """
    Merge query parameters from multiple URLs.

    Args:
        *urls: URLs to merge (later URLs override earlier)
        updates: Additional updates

    Returns:
        Merged URL
    """
    if not urls:
        return ""
    parsed = urllib.parse.urlparse(urls[0])
    merged: dict[str, Any] = parse_qs(parsed.query)

    for url in urls[1:]:
        p = urllib.parse.urlparse(url)
        params = parse_qs(p.query)
        merged.update(params)

    if updates:
        merged.update(updates)

    new_query = build_qs(merged)
    return urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment,
    ))


def expand_template(
    template: str,
    variables: dict[str, Any],
    strict: bool = False,
) -> str:
    """
    Expand URL template with variables.

    Supports {var} and {var:regex} patterns.

    Args:
        template: URL template
        variables: Variable values
        strict: Raise error if variable not found

    Returns:
        Expanded URL
    """
    def replacer(match: re.Match) -> str:
        content = match.group(1)
        if ":" in content:
            var_name, pattern = content.split(":", 1)
            value = variables.get(var_name, "")
            if not re.match(f"^{pattern}$", str(value)):
                if strict:
                    raise ValueError(f"Variable {var_name}={value} doesn't match pattern {pattern}")
                return str(value)
            return str(value)
        if content not in variables and strict:
            raise KeyError(f"Missing required variable: {content}")
        return str(variables.get(content, match.group(0)))

    return re.sub(r"\{([^}]+)\}", replacer, template)
