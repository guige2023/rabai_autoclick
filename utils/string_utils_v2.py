"""String utilities v2 for RabAI AutoClick.

Provides:
- Advanced string manipulation
- Text wrapping and formatting
- Character encoding helpers
- Pattern matching utilities
"""

import re
import textwrap
import unicodedata
from typing import (
    Callable,
    List,
    Optional,
    Pattern,
)


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Args:
        text: camelCase string.

    Returns:
        snake_case string.
    """
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub("([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower()


def snake_to_camel(text: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        text: snake_case string.

    Returns:
        camelCase string.
    """
    components = text.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def snake_to_pascal(text: str) -> str:
    """Convert snake_case to PascalCase.

    Args:
        text: snake_case string.

    Returns:
        PascalCase string.
    """
    components = text.split("_")
    return "".join(x.title() for x in components)


def kebab_to_snake(text: str) -> str:
    """Convert kebab-case to snake_case.

    Args:
        text: kebab-case string.

    Returns:
        snake_case string.
    """
    return text.replace("-", "_")


def snake_to_kebab(text: str) -> str:
    """Convert snake_case to kebab-case.

    Args:
        text: snake_case string.

    Returns:
        kebab-case string.
    """
    return text.replace("_", "-")


def camel_to_kebab(text: str) -> str:
    """Convert camelCase to kebab-case.

    Args:
        text: camelCase string.

    Returns:
        kebab-case string.
    """
    return kebab_to_snake(camel_to_snake(text))


def kebab_to_camel(text: str) -> str:
    """Convert kebab-case to camelCase.

    Args:
        text: kebab-case string.

    Returns:
        camelCase string.
    """
    return snake_to_camel(kebab_to_snake(text))


def truncate(
    text: str,
    length: int,
    suffix: str = "...",
) -> str:
    """Truncate text to a maximum length.

    Args:
        text: Text to truncate.
        length: Maximum length.
        suffix: Suffix to add when truncated.

    Returns:
        Truncated text.
    """
    if len(text) <= length:
        return text
    return text[:length - len(suffix)] + suffix


def wrap_text(
    text: str,
    width: int = 80,
    subsequent_indent: Optional[str] = None,
    first_indent: Optional[str] = None,
) -> str:
    """Wrap text to specified width.

    Args:
        text: Text to wrap.
        width: Maximum line width.
        subsequent_indent: Indent for subsequent lines.
        first_indent: Indent for first line.

    Returns:
        Wrapped text.
    """
    return textwrap.fill(
        text,
        width=width,
        subsequent_indent=subsequent_indent or "",
        initial_indent=first_indent or "",
    )


def indent_text(
    text: str,
    indent: str = "    ",
    first_indent: Optional[str] = None,
) -> str:
    """Indent all lines in text.

    Args:
        text: Text to indent.
        indent: Indent string for each line.
        first_indent: Special indent for first line.

    Returns:
        Indented text.
    """
    lines = text.splitlines()
    if not lines:
        return text

    if first_indent is None:
        result = indent + lines[0] + "\n"
    else:
        result = first_indent + lines[0] + "\n"

    result += "\n".join(indent + line for line in lines[1:])
    return result


def remove_prefix(text: str, prefix: str) -> str:
    """Remove prefix from text if present.

    Args:
        text: Text to process.
        prefix: Prefix to remove.

    Returns:
        Text without prefix.
    """
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text: str, suffix: str) -> str:
    """Remove suffix from text if present.

    Args:
        text: Text to process.
        suffix: Suffix to remove.

    Returns:
        Text without suffix.
    """
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def strip_whitespace(text: str) -> str:
    """Remove leading/trailing whitespace and collapse inner whitespace.

    Args:
        text: Text to strip.

    Returns:
        Cleaned text.
    """
    return " ".join(text.split())


def collapse_whitespace(text: str) -> str:
    """Replace multiple whitespace with single space.

    Args:
        text: Text to process.

    Returns:
        Collapsed text.
    """
    return re.sub(r"\s+", " ", text).strip()


def remove_punctuation(text: str, keep: str = "") -> str:
    """Remove punctuation from text.

    Args:
        text: Text to process.
        keep: Punctuation characters to keep.

    Returns:
        Text without punctuation.
    """
    import string
    punct = set(string.punctuation) - set(keep)
    return "".join(c for c in text if c not in punct)


def count_words(text: str) -> int:
    """Count words in text.

    Args:
        text: Text to count.

    Returns:
        Number of words.
    """
    return len(re.findall(r"\w+", text))


def count_lines(text: str) -> int:
    """Count lines in text.

    Args:
        text: Text to count.

    Returns:
        Number of lines.
    """
    return len(text.splitlines())


def is_ascii(text: str) -> bool:
    """Check if text contains only ASCII characters.

    Args:
        text: Text to check.

    Returns:
        True if all ASCII.
    """
    try:
        text.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def normalize_unicode(
    text: str,
    form: str = "NFKC",
) -> str:
    """Normalize unicode text.

    Args:
        text: Text to normalize.
        form: Normalization form (NFC, NFD, NFKC, NFKD).

    Returns:
        Normalized text.
    """
    return unicodedata.normalize(form, text)


def remove_accents(text: str) -> str:
    """Remove accents/diacritics from text.

    Args:
        text: Text to process.

    Returns:
        Text without accents.
    """
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def to_title_case(text: str, style: str = "title") -> str:
    """Convert text to title case.

    Args:
        text: Text to convert.
        style: Style ("title", "capitalize", "sentence").

    Returns:
        Title-cased text.
    """
    if style == "title":
        return text.title()
    elif style == "capitalize":
        return text.capitalize()
    elif style == "sentence":
        # Only capitalize first letter of each sentence
        return re.sub(r"(^|[.!?]\s+)([a-z])", lambda m: m.group(1) + m.group(2).upper(), text)
    return text


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text.

    Args:
        text: Text to extract from.

    Returns:
        List of numbers.
    """
    return [float(x) for x in re.findall(r"-?\d+\.?\d*", text)]


def extract_ints(text: str) -> List[int]:
    """Extract all integers from text.

    Args:
        text: Text to extract from.

    Returns:
        List of integers.
    """
    return [int(x) for x in re.findall(r"-?\d+", text)]


def replace_multiple(
    text: str,
    replacements: dict[str, str],
) -> str:
    """Replace multiple substrings at once.

    Args:
        text: Text to process.
        replacements: Dict of old -> new replacements.

    Returns:
        Text with replacements applied.
    """
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def is_blank(text: Optional[str]) -> bool:
    """Check if text is blank (empty or whitespace only).

    Args:
        text: Text to check.

    Returns:
        True if blank.
    """
    if text is None:
        return True
    return not text or text.isspace()


def is_palindrome(text: str, ignore_case: bool = True, ignore_non_alnum: bool = True) -> bool:
    """Check if text is a palindrome.

    Args:
        text: Text to check.
        ignore_case: Ignore case differences.
        ignore_non_alnum: Ignore non-alphanumeric characters.

    Returns:
        True if palindrome.
    """
    if ignore_non_alnum:
        chars = [c.lower() for c in text if c.isalnum()]
    else:
        chars = list(text.lower() if ignore_case else text)

    return chars == chars[::-1]


def Levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Edit distance.
    """
    if len(s1) < len(s2):
        return Levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity(s1: str, s2: str) -> float:
    """Calculate string similarity (0-1).

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Similarity score (0-1).
    """
    if not s1 and not s2:
        return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    distance = Levenshtein_distance(s1, s2)
    return 1.0 - (distance / max_len)


def join_lines(
    lines: List[str],
    separator: str = "",
    prefix: Optional[str] = None,
) -> str:
    """Join lines with optional separator and prefix.

    Args:
        lines: Lines to join.
        separator: Separator between lines.
        prefix: Prefix for each line.

    Returns:
        Joined string.
    """
    if prefix:
        lines = [prefix + line for line in lines]
    return separator.join(lines)


def split_once(text: str, separator: str) -> tuple[str, str]:
    """Split text on first occurrence of separator.

    Args:
        text: Text to split.
        separator: Separator string.

    Returns:
        Tuple of (before, after).
    """
    parts = text.split(separator, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def extract_between(
    text: str,
    start: str,
    end: str,
    *,
    include_markers: bool = False,
) -> List[str]:
    """Extract all text between start and end markers.

    Args:
        text: Text to search.
        start: Start marker.
        end: End marker.
        include_markers: Include markers in result.

    Returns:
        List of extracted strings.
    """
    pattern = f"{re.escape(start)}(.*?){re.escape(end)}"
    if include_markers:
        pattern = f"({pattern})"
    return re.findall(pattern, text, re.DOTALL)


def mask_sensitive(
    text: str,
    pattern: str = r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
    replacement: str = "****-****-****-****",
) -> str:
    """Mask sensitive patterns in text (e.g., credit cards).

    Args:
        text: Text to process.
        pattern: Regex pattern to match.
        replacement: Replacement string.

    Returns:
        Text with masked patterns.
    """
    return re.sub(pattern, replacement, text)
