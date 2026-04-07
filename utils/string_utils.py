"""String utilities for RabAI AutoClick.

Provides:
- String manipulation helpers
- Text cleaning and formatting
- Pattern matching utilities
"""

import re
import unicodedata
from typing import List, Optional, Callable


def capitalize(text: str) -> str:
    """Capitalize first letter of string.

    Args:
        text: Input string.

    Returns:
        Capitalized string.
    """
    if not text:
        return text
    return text[0].upper() + text[1:]


def title_case(text: str) -> str:
    """Convert string to title case.

    Args:
        text: Input string.

    Returns:
        Title cased string.
    """
    return text.title()


def snake_to_camel(text: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: Snake case string.

    Returns:
        Camel case string.
    """
    components = text.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: Camel case string.

    Returns:
        Snake case string.
    """
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', text).lower()


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case.

    Args:
        text: Kebab case string.

    Returns:
        Snake case string.
    """
    return text.replace('-', '_')


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case.

    Args:
        text: Snake case string.

    Returns:
        Kebab case string.
    """
    return text.replace('_', '-')


def strip_whitespace(text: str) -> str:
    """Strip all whitespace from string.

    Args:
        text: Input string.

    Returns:
        String with whitespace removed.
    """
    return ''.join(text.split())


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace to single spaces.

    Args:
        text: Input string.

    Returns:
        String with normalized whitespace.
    """
    return ' '.join(text.split())


def truncate(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate string to max length.

    Args:
        text: Input string.
        max_length: Maximum length.
        suffix: Suffix to append if truncated.

    Returns:
        Truncated string.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def remove_prefix(text: str, prefix: str) -> str:
    """Remove prefix from string.

    Args:
        text: Input string.
        prefix: Prefix to remove.

    Returns:
        String without prefix.
    """
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text: str, suffix: str) -> str:
    """Remove suffix from string.

    Args:
        text: Input string.
        suffix: Suffix to remove.

    Returns:
        String without suffix.
    """
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def is_blank(text: str) -> bool:
    """Check if string is blank (empty or whitespace only).

    Args:
        text: Input string.

    Returns:
        True if blank.
    """
    return not text or text.isspace()


def is_numeric(text: str) -> bool:
    """Check if string contains only numeric characters.

    Args:
        text: Input string.

    Returns:
        True if numeric.
    """
    return text.isdigit()


def is_alpha(text: str) -> bool:
    """Check if string contains only alphabetic characters.

    Args:
        text: Input string.

    Returns:
        True if alphabetic.
    """
    return text.isalpha()


def is_alphanumeric(text: str) -> bool:
    """Check if string contains only alphanumeric characters.

    Args:
        text: Input string.

    Returns:
        True if alphanumeric.
    """
    return text.isalnum()


def count_words(text: str) -> int:
    """Count words in string.

    Args:
        text: Input string.

    Returns:
        Word count.
    """
    return len(text.split())


def reverse_words(text: str) -> str:
    """Reverse words in string.

    Args:
        text: Input string.

    Returns:
        String with reversed words.
    """
    return ' '.join(text.split()[::-1])


def reverse_string(text: str) -> str:
    """Reverse string characters.

    Args:
        text: Input string.

    Returns:
        Reversed string.
    """
    return text[::-1]


def is_palindrome(text: str) -> bool:
    """Check if string is a palindrome.

    Args:
        text: Input string.

    Returns:
        True if palindrome.
    """
    cleaned = ''.join(c.lower() for c in text if c.isalnum())
    return cleaned == cleaned[::-1]


def contains(text: str, substring: str, case_sensitive: bool = True) -> bool:
    """Check if string contains substring.

    Args:
        text: Input string.
        substring: Substring to find.
        case_sensitive: Whether to case sensitive.

    Returns:
        True if contains.
    """
    if not case_sensitive:
        text = text.lower()
        substring = substring.lower()
    return substring in text


def replace_all(text: str, old: str, new: str) -> str:
    """Replace all occurrences of substring.

    Args:
        text: Input string.
        old: Old substring.
        new: New substring.

    Returns:
        String with replacements.
    """
    return text.replace(old, new)


def split_lines(text: str) -> List[str]:
    """Split text into lines.

    Args:
        text: Input string.

    Returns:
        List of lines.
    """
    return text.splitlines()


def join_lines(lines: List[str]) -> str:
    """Join lines with newlines.

    Args:
        lines: List of lines.

    Returns:
        Joined string.
    """
    return '\n'.join(lines)


def indent(text: str, spaces: int) -> str:
    """Indent text by spaces.

    Args:
        text: Input string.
        spaces: Number of spaces to indent.

    Returns:
        Indented string.
    """
    indent_str = ' ' * spaces
    return '\n'.join(indent_str + line for line in text.splitlines())


def unindent(text: str) -> str:
    """Remove leading whitespace from each line.

    Args:
        text: Input string.

    Returns:
        Unindented string.
    """
    lines = text.splitlines()
    if not lines:
        return text
    min_indent = min(len(line) - len(line.lstrip()) for line in lines if line.strip())
    if min_indent == 0:
        return text
    return '\n'.join(line[min_indent:] for line in lines)


def remove_special_chars(text: str, keep: str = "") -> str:
    """Remove special characters from string.

    Args:
        text: Input string.
        keep: Characters to keep.

    Returns:
        String without special characters.
    """
    pattern = f"[^a-zA-Z0-9{re.escape(keep)}]"
    return re.sub(pattern, '', text)


def normalize_unicode(text: str) -> str:
    """Normalize unicode characters.

    Args:
        text: Input string.

    Returns:
        Normalized string.
    """
    return unicodedata.normalize('NFKD', text)


def to_ascii(text: str) -> str:
    """Convert string to ASCII, removing non-ASCII characters.

    Args:
        text: Input string.

    Returns:
        ASCII string.
    """
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')


def word_wrap(text: str, width: int) -> List[str]:
    """Wrap text to specified width.

    Args:
        text: Input string.
        width: Maximum line width.

    Returns:
        List of wrapped lines.
    """
    words = text.split()
    lines = []
    current_line = []
    current_length = 0

    for word in words:
        word_len = len(word)
        if current_length + word_len + len(current_line) <= width:
            current_line.append(word)
            current_length += word_len
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]
            current_length = word_len

    if current_line:
        lines.append(' '.join(current_line))

    return lines


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from string.

    Args:
        text: Input string.

    Returns:
        List of numbers.
    """
    pattern = r'-?\d+\.?\d*'
    matches = re.findall(pattern, text)
    return [float(m) for m in matches]


def extract_words(text: str) -> List[str]:
    """Extract all words from string.

    Args:
        text: Input string.

    Returns:
        List of words.
    """
    return re.findall(r'\b[a-zA-Z]+\b', text)


def filter_chars(text: str, condition: Callable[[str], bool]) -> str:
    """Filter characters based on condition.

    Args:
        text: Input string.
        condition: Function that returns True for chars to keep.

    Returns:
        Filtered string.
    """
    return ''.join(c for c in text if condition(c))


def map_chars(text: str, mapper: Callable[[str], str]) -> str:
    """Map characters using transformation function.

    Args:
        text: Input string.
        mapper: Transformation function.

    Returns:
        Transformed string.
    """
    return ''.join(mapper(c) for c in text)


def pad_left(text: str, width: int, char: str = " ") -> str:
    """Pad string on left to reach width.

    Args:
        text: Input string.
        width: Target width.
        char: Character to pad with.

    Returns:
        Padded string.
    """
    return text.rjust(width, char)


def pad_right(text: str, width: int, char: str = " ") -> str:
    """Pad string on right to reach width.

    Args:
        text: Input string.
        width: Target width.
        char: Character to pad with.

    Returns:
        Padded string.
    """
    return text.ljust(width, char)


def center_text(text: str, width: int, char: str = " ") -> str:
    """Center text within width.

    Args:
        text: Input string.
        width: Target width.
        char: Character to pad with.

    Returns:
        Centered string.
    """
    return text.center(width, char)
