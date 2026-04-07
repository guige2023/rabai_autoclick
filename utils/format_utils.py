"""
String and number formatting utilities.
"""

import re
import textwrap
from typing import Optional, List


def truncate(
    text: str,
    length: int,
    suffix: str = "...",
    word_boundary: bool = True
) -> str:
    """Truncate text to a maximum length."""
    if len(text) <= length:
        return text
    truncated_len = length - len(suffix)
    if truncated_len <= 0:
        return suffix[:length]
    if word_boundary:
        truncated = text[:truncated_len]
        last_space = truncated.rfind(" ")
        if last_space > 0:
            return truncated[:last_space] + suffix
        return truncated + suffix
    return text[:truncated_len] + suffix


def pad_center(text: str, width: int, fill_char: str = " ") -> str:
    """Center-align text within width."""
    return text.center(width, fill_char)


def pad_left(text: str, width: int, fill_char: str = " ") -> str:
    """Left-align text within width."""
    return text.ljust(width, fill_char)


def pad_right(text: str, width: int, fill_char: str = " ") -> str:
    """Right-align text within width."""
    return text.rjust(width, fill_char)


def word_wrap(text: str, width: int = 80) -> str:
    """Wrap text to specified width."""
    return textwrap.fill(text, width=width, break_long_words=False, break_on_hyphens=False)


def camel_to_snake(text: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_camel(text: str, upper_first: bool = False) -> str:
    """Convert snake_case to camelCase or PascalCase."""
    components = text.split("_")
    if upper_first:
        return "".join(x.title() for x in components)
    return components[0] + "".join(x.title() for x in components[1:])


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case."""
    return text.replace("-", "_")


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case."""
    return text.replace("_", "-")


def slugify(
    text: str,
    lower: bool = True,
    max_length: Optional[int] = None,
) -> str:
    """Convert text to a URL-safe slug."""
    import unicodedata
    # Remove diacriticals
    nfkd = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in nfkd if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    if lower:
        text = text.lower()
    if max_length:
        text = text[:max_length].rstrip("-")
    return text.strip("-")


def interpolate(template: str, values: dict, strict: bool = True) -> str:
    """Simple template interpolation using {placeholder} syntax."""
    def replacer(match):
        key = match.group(1)
        if key in values:
            return str(values[key])
        if strict:
            raise KeyError(f"Missing placeholder: {{{key}}}")
        return match.group(0)
    return re.sub(r"\{(\w+)\}", replacer, template)


def indent(text: str, spaces: int = 4, indent_first: bool = True) -> str:
    """Indent text by spaces."""
    prefix = " " * spaces
    if indent_first:
        return prefix + text.replace("\n", "\n" + prefix)
    return re.sub(r"^", " " * spaces, text, flags=re.MULTILINE)


def dedent(text: str) -> str:
    """Remove leading indentation from all lines."""
    lines = text.split("\n")
    non_empty = [l for l in lines if l.strip()]
    if not non_empty:
        return text
    min_indent = min(len(l) - len(l.lstrip()) for l in non_empty)
    return "\n".join(l[min_indent:] if min_indent > 0 else l for l in lines)
