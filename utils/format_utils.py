"""
String and number formatting utilities.

Provides:
- String padding, alignment, wrapping
- Number formatting (currency, scientific, engineering)
- Template interpolation
- Case conversion utilities
"""

from __future__ import annotations

import re
import string
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, Optional


def pad_left(text: str, width: int, char: str = " ") -> str:
    """Pad string to the left."""
    return text.rjust(width, char)


def pad_right(text: str, width: int, char: str = " ") -> str:
    """Pad string to the right."""
    return text.ljust(width, char)


def pad_center(text: str, width: int, char: str = " ") -> str:
    """Center-align string."""
    return text.center(width, char)


def wrap_text(text: str, width: int, indent: str = "", subsequent_indent: Optional[str] = None) -> str:
    """
    Wrap text to specified width.

    Args:
        text: Text to wrap
        width: Maximum line width
        indent: Indentation for first line
        subsequent_indent: Indentation for subsequent lines

    Returns:
        Wrapped text
    """
    if subsequent_indent is None:
        subsequent_indent = indent
    wrapper = textwrap.TextWrapper(width=width, subsequent_indent=subsequent_indent, break_long_words=False, break_on_hyphens=False)
    if indent:
        return textwrap.indent(wrapper.wrap(text) or [""], indent)
    return "\n".join(wrapper.wrap(text))


def format_currency(amount: float, currency_symbol: str = "$", decimal_places: int = 2, thousands_sep: str = ",") -> str:
    """
    Format a number as currency.

    Args:
        amount: The amount
        currency_symbol: Currency symbol
        decimal_places: Number of decimal places
        thousands_sep: Thousands separator

    Returns:
        Formatted currency string

    Example:
        >>> format_currency(1234567.89)
        '$1,234,567.89'
    """
    int_part = int(abs(amount))
    dec_part = abs(amount) - int_part

    formatted_int = f"{int_part:,}".replace(",", thousands_sep)
    if amount < 0:
        formatted_int = f"-{formatted_int}"

    if decimal_places == 0:
        return f"{currency_symbol}{formatted_int}"

    dec_str = f"{dec_part:.{decimal_places}f}".split(".")[1]
    return f"{currency_symbol}{formatted_int}.{dec_str}"


def format_scientific(value: float, precision: int = 4) -> str:
    """
    Format number in scientific notation.

    Args:
        value: Number to format
        precision: Decimal precision

    Returns:
        Scientific notation string

    Example:
        >>> format_scientific(0.000001234)
        '1.234e-06'
    """
    return f"{value:.{precision}e}"


def format_engineering(value: float, precision: int = 4) -> str:
    """
    Format number in engineering notation.

    Args:
        value: Number to format
        precision: Decimal precision

    Returns:
        Engineering notation string

    Example:
        >>> format_engineering(0.000001234)
        '1.234u'
    """
    if value == 0:
        return "0"

    import math

    exp = int(math.floor(math.log10(abs(value))))
    exp3 = exp - (exp % 3)
    mantissa = value / (10**exp3)

    suffixes = {-18: "a", -15: "f", -12: "p", -9: "n", -6: "u", -3: "m", 0: "", 3: "k", 6: "M", 9: "G", 12: "T", 15: "P", 18: "E"}

    suffix = suffixes.get(exp3, f"e{exp3}")
    return f"{mantissa:.{precision}f}{suffix}"


def format_phone(phone: str, format_str: str = "(XXX) XXX-XXXX") -> str:
    """
    Format a phone number.

    Args:
        phone: Digits-only phone number
        format_str: Format template with X placeholders

    Returns:
        Formatted phone number

    Example:
        >>> format_phone("5551234567")
        '(555) 123-4567'
    """
    digits = re.sub(r"\D", "", phone)
    if len(digits) > 10:
        format_str = "+" + format_str

    result = []
    digit_idx = 0
    for char in format_str:
        if char == "X":
            if digit_idx < len(digits):
                result.append(digits[digit_idx])
                digit_idx += 1
        else:
            result.append(char)
    return "".join(result)


def camel_to_snake(text: str) -> str:
    """
    Convert camelCase to snake_case.

    Example:
        >>> camel_to_snake("camelCase")
        'camel_case'
        >>> camel_to_snake("XMLParser")
        'xml_parser'
    """
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", text).lower()


def snake_to_camel(text: str, capitalize_first: bool = False) -> str:
    """
    Convert snake_case to camelCase.

    Example:
        >>> snake_to_camel("snake_case")
        'snakeCase'
        >>> snake_to_camel("snake_case", capitalize_first=True)
        'SnakeCase'
    """
    components = text.split("_")
    if capitalize_first:
        return "".join(comp.capitalize() for comp in components)
    return components[0] + "".join(comp.capitalize() for comp in components[1:])


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case."""
    return text.replace("-", "_")


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case."""
    return text.replace("_", "-")


def pascal_to_camel(text: str) -> str:
    """Convert PascalCase to camelCase."""
    if not text:
        return text
    return text[0].lower() + text[1:]


def title_to_snake(text: str) -> str:
    """Convert Title Case to snake_case."""
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", text)
    return text.replace(" ", "_").replace("__", "_").lower()


@dataclass
class TemplateFormatter:
    """String template formatter with custom delimiters."""

    template: str
    delimiter: str = "{"
    re_escape: str = r"\{([^}]+)\}"

    def __post_init__(self) -> None:
        if self.delimiter != "{":
            pattern = self.re_escape.replace("{", self.delimiter)
            self._compiled = re.compile(pattern)

    def format(self, **kwargs: Any) -> str:
        """Format template with keyword arguments."""
        if self.delimiter == "{":
            return self.template.format(**kwargs)

        def replacer(match: re.Match) -> str:
            key = match.group(1)
            keys = key.split(".")
            val: Any = kwargs
            for k in keys:
                if isinstance(val, dict):
                    val = val.get(k, match.group(0))
                else:
                    val = getattr(val, k, match.group(0))
            return str(val)

        return self._compiled.sub(replacer, self.template)

    def format_map(self, mapping: dict[str, Any]) -> str:
        """Format template with a mapping object."""
        return self.format(**dict(mapping))


def interpolate(text: str, values: dict[str, Any]) -> str:
    """
    Simple variable interpolation.

    Args:
        text: Template with {variable} placeholders
        values: Dictionary of values

    Returns:
        Interpolated string

    Example:
        >>> interpolate("Hello, {name}!", {"name": "World"})
        'Hello, World!'
    """
    formatter = TemplateFormatter(text)
    return formatter.format_map(values)


def word_count(text: str) -> dict[str, int]:
    """
    Count words in text.

    Returns:
        Dictionary of word -> count
    """
    words = re.findall(r"\b\w+\b", text.lower())
    return {word: words.count(word) for word in sorted(set(words))}


def highlight_keywords(text: str, keywords: list[str], prefix: str = "**", suffix: str = "**") -> str:
    """
    Highlight keywords in text.

    Args:
        text: Text to process
        keywords: Keywords to highlight
        prefix: Prefix for highlighted text
        suffix: Suffix for highlighted text

    Returns:
        Text with highlighted keywords
    """
    for kw in keywords:
        pattern = re.compile(re.escape(kw), re.IGNORECASE)
        text = pattern.sub(f"{prefix}{kw}{suffix}", text)
    return text


def remove_punctuation(text: str, keep: str = "") -> str:
    """
    Remove punctuation from text.

    Args:
        text: Input text
        keep: Punctuation to keep

    Returns:
        Text without punctuation
    """
    punct = set(string.punctuation) - set(keep)
    return "".join(c for c in text if c not in punct)


def dedupe_whitespace(text: str) -> str:
    """Replace multiple whitespace with single space."""
    return re.sub(r"\s+", " ", text).strip()


def split_camel_case(text: str) -> list[str]:
    """Split camelCase or PascalCase into words."""
    return re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+", text)
