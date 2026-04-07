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
