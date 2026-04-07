"""String formatting utilities for RabAI AutoClick.

Provides:
- String templates
- Variable interpolation
- Text alignment
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


def interpolate(
    template: str,
    variables: Dict[str, Any],
    prefix: str = "{",
    suffix: str = "}",
) -> str:
    """Interpolate variables into template string.

    Args:
        template: Template string with {var} placeholders.
        variables: Dictionary of variable values.
        prefix: Placeholder prefix.
        suffix: Placeholder suffix.

    Returns:
        Interpolated string.
    """
    pattern = f"{re.escape(prefix)}([^\\{suffix}]+){re.escape(suffix)}"

    def replace(match):
        var_name = match.group(1)
        return str(variables.get(var_name, match.group(0)))

    return re.sub(pattern, replace, template)


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: camelCase string.

    Returns:
        snake_case string.
    """
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", text).lower()


def snake_to_camel(text: str, capitalize_first: bool = False) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: snake_case string.
        capitalize_first: Whether to capitalize first letter.

    Returns:
        camelCase string.
    """
    components = text.split("_")
    result = components[0]
    for component in components[1:]:
        result += component.capitalize()
    if capitalize_first and result:
        result = result.capitalize()
    return result


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case."""
    return text.replace("-", "_")


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case."""
    return text.replace("_", "-")


def title_case(text: str) -> str:
    """Convert text to Title Case."""
    return text.title()


def truncate(text: str, length: int, suffix: str = "...") -> str:
    """Truncate text to length.

    Args:
        text: Input text.
        length: Maximum length.
        suffix: Suffix for truncated text.

    Returns:
        Truncated text.
    """
    if len(text) <= length:
        return text
    return text[:length - len(suffix)] + suffix


def pad_left(text: str, width: int, char: str = " ") -> str:
    """Pad text to left."""
    return text.rjust(width, char)


def pad_right(text: str, width: int, char: str = " ") -> str:
    """Pad text to right."""
    return text.ljust(width, char)


def center_align(text: str, width: int, char: str = " ") -> str:
    """Center align text."""
    return text.center(width, char)
