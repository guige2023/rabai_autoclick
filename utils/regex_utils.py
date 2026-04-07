"""Regex utilities for RabAI AutoClick.

Provides:
- Regex compilation and matching helpers
- Pattern building utilities
- Common regex patterns
"""

import re
from typing import Dict, List, Match, Optional, Pattern, Union


# Common regex patterns
PATTERNS: Dict[str, str] = {
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "url": r"^https?://[^\s<]+",
    "ipv4": r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    "ipv6": r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
    "mac_address": r"^(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})$",
    "phone_us": r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
    "zip_code_us": r"^\d{5}(?:-\d{4})?$",
    "hex_color": r"^#?(?:[0-9a-fA-F]{3}){1,2}$",
    "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    "slug": r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    "username": r"^[a-zA-Z0-9_-]{3,16}$",
    "password_strong": r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d@$!%*?&]{8,}$",
    "credit_card": r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13})$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "date_us": r"^\d{1,2}/\d{1,2}/\d{4}$",
    "time_24h": r"^(?:[01]?[0-9]|2[0-3]):[0-5][0-9]$",
    "time_12h": r"^(?:0?[1-9]|1[0-2]):[0-5][0-9]\s?(?:AM|PM|am|pm)$",
    "html_tag": r"<([a-zA-Z][a-zA-Z0-9]*)\b[^>]*>.*?</\1>",
    "xml_tag": r"<([a-zA-Z_][a-zA-Z0-9_]*)\b[^>]*>.*?</\1>",
}


def compile(pattern: str, flags: int = 0) -> Optional[Pattern[str]]:
    """Compile regex pattern.

    Args:
        pattern: Regex pattern string.
        flags: Optional regex flags.

    Returns:
        Compiled pattern or None if invalid.
    """
    try:
        return re.compile(pattern, flags)
    except re.error:
        return None


def match(pattern: str, text: str, flags: int = 0) -> Optional[Match[str]]:
    """Match pattern at start of text.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        flags: Optional regex flags.

    Returns:
        Match object or None.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return None
    return compiled.match(text)


def search(pattern: str, text: str, flags: int = 0) -> Optional[Match[str]]:
    """Search for pattern in text.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        flags: Optional regex flags.

    Returns:
        Match object or None.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return None
    return compiled.search(text)


def find_all(pattern: str, text: str, flags: int = 0) -> List[str]:
    """Find all matches of pattern in text.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        flags: Optional regex flags.

    Returns:
        List of matched strings.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return []
    return compiled.findall(text)


def find_iter(pattern: str, text: str, flags: int = 0) -> List[Match[str]]:
    """Find all matches with match objects.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        flags: Optional regex flags.

    Returns:
        List of match objects.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return []
    return list(compiled.finditer(text))


def replace(pattern: str, text: str, replacement: str, count: int = 0, flags: int = 0) -> str:
    """Replace pattern matches in text.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        replacement: Replacement string.
        count: Max replacements (0 for all).
        flags: Optional regex flags.

    Returns:
        Text with replacements.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return text
    return compiled.sub(replacement, text, count=count)


def split(pattern: str, text: str, maxsplit: int = 0, flags: int = 0) -> List[str]:
    """Split text by pattern.

    Args:
        pattern: Regex pattern.
        text: Text to split.
        maxsplit: Max splits (0 for unlimited).
        flags: Optional regex flags.

    Returns:
        List of text segments.
    """
    compiled = compile(pattern, flags)
    if compiled is None:
        return [text]
    return compiled.split(text, maxsplit=maxsplit)


def test(pattern: str, text: str, flags: int = 0) -> bool:
    """Test if pattern matches text.

    Args:
        pattern: Regex pattern.
        text: Text to test.
        flags: Optional regex flags.

    Returns:
        True if pattern matches.
    """
    return match(pattern, text, flags) is not None


def extract(pattern: str, text: str, group: int = 0, flags: int = 0) -> Optional[str]:
    """Extract first match group.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        group: Group number to extract (0 for full match).
        flags: Optional regex flags.

    Returns:
        Extracted string or None.
    """
    m = search(pattern, text, flags)
    if m is None:
        return None
    return m.group(group)


def extract_all(pattern: str, text: str, group: int = 0, flags: int = 0) -> List[str]:
    """Extract all match groups.

    Args:
        pattern: Regex pattern.
        text: Text to search.
        group: Group number to extract (0 for full match).
        flags: Optional regex flags.

    Returns:
        List of extracted strings.
    """
    matches = find_iter(pattern, text, flags)
    return [m.group(group) for m in matches if m.group(group) is not None]


def extract_dict(pattern: str, text: str, names: List[str], flags: int = 0) -> Optional[Dict[str, str]]:
    """Extract named groups into dict.

    Args:
        pattern: Regex pattern with named groups.
        text: Text to search.
        names: List of group names to extract.
        flags: Optional regex flags.

    Returns:
        Dict of group names to values or None.
    """
    m = search(pattern, text, flags)
    if m is None:
        return None
    result = {}
    for name in names:
        try:
            result[name] = m.group(name)
        except IndexError:
            pass
    return result if result else None


def is_match(pattern_name: str, text: str) -> bool:
    """Test if text matches a named pattern.

    Args:
        pattern_name: Name of pattern in PATTERNS dict.
        text: Text to test.

    Returns:
        True if matches.
    """
    if pattern_name not in PATTERNS:
        return False
    return test(PATTERNS[pattern_name], text)


def get_pattern(pattern_name: str) -> Optional[str]:
    """Get named pattern string.

    Args:
        pattern_name: Name of pattern in PATTERNS dict.

    Returns:
        Pattern string or None.
    """
    return PATTERNS.get(pattern_name)


def escape(text: str) -> str:
    """Escape special regex characters.

    Args:
        text: Text to escape.

    Returns:
        Escaped text.
    """
    return re.escape(text)


def unescape(text: str) -> str:
    """Unescape special regex characters.

    Args:
        text: Text to unescape.

    Returns:
        Unescaped text.
    """
    return text.replace(r"\.", ".").replace(r"\*", "*").replace(r"\?", "?")


def build_alternation(items: List[str]) -> str:
    """Build alternation pattern from items.

    Args:
        items: List of alternative strings.

    Returns:
        Regex alternation pattern.
    """
    return "|".join(re.escape(item) for item in items)


def build_sequence(pattern: str, separator: str = "") -> str:
    """Build pattern for sequence of items.

    Args:
        pattern: Single item pattern.
        separator: Optional separator pattern.

    Returns:
        Sequence pattern.
    """
    if separator:
        return f"{pattern}(?:{separator}{pattern})*"
    return f"{pattern}+"


def quantifier(pattern: str, min_count: int = 0, max_count: Optional[int] = None, greedy: bool = True) -> str:
    """Add quantifier to pattern.

    Args:
        pattern: Pattern to quantify.
        min_count: Minimum repetitions.
        max_count: Maximum repetitions (None for unlimited).
        greedy: Whether to match greedily.

    Returns:
        Quantified pattern.
    """
    if min_count == 0 and max_count == 1:
        quant = "?"
    elif min_count == 0 and max_count is None:
        quant = "*"
    elif min_count == 1 and max_count is None:
        quant = "+"
    elif max_count is None:
        quant = f"{{{min_count},}}"
    elif min_count == max_count:
        quant = f"{{{min_count}}}"
    else:
        quant = f"{{{min_count},{max_count}}}"
    if not greedy:
        quant += "?"
    return f"(?:{pattern}){quant}"


def lookahead(pattern: str, ahead: bool = True) -> str:
    """Add lookahead assertion.

    Args:
        pattern: Pattern for assertion.
        ahead: True for positive lookahead, False for negative.

    Returns:
        Pattern with lookahead.
    """
    prefix = "?=" if ahead else "?!"
    return f"(?={prefix}{pattern})"


def lookbehind(pattern: str, behind: bool = True) -> str:
    """Add lookbehind assertion.

    Args:
        pattern: Pattern for assertion.
        behind: True for positive lookbehind, False for negative.

    Returns:
        Pattern with lookbehind.
    """
    prefix = "?<=" if behind else "?<!"
    return f"(?<={prefix}{pattern})"


def capture(pattern: str, name: Optional[str] = None) -> str:
    """Add capture group.

    Args:
        pattern: Pattern to capture.
        name: Optional group name.

    Returns:
        Pattern with capture group.
    """
    if name:
        return f"(?P<{name}>{pattern})"
    return f"({pattern})"


def group(pattern: str) -> str:
    """Add non-capture group.

    Args:
        pattern: Pattern to group.

    Returns:
        Pattern with non-capture group.
    """
    return f"(?:{pattern})"


def word_boundary() -> str:
    """Get word boundary assertion.

    Returns:
        Word boundary pattern.
    """
    return r"\b"


def digit() -> str:
    """Get digit character class.

    Returns:
        Digit pattern.
    """
    return r"\d"


def non_digit() -> str:
    """Get non-digit character class.

    Returns:
        Non-digit pattern.
    """
    return r"\D"


def word() -> str:
    """Get word character class.

    Returns:
        Word pattern.
    """
    return r"\w"


def non_word() -> str:
    """Get non-word character class.

    Returns:
        Non-word pattern.
    """
    return r"\W"


def whitespace() -> str:
    """Get whitespace character class.

    Returns:
        Whitespace pattern.
    """
    return r"\s"


def non_whitespace() -> str:
    """Get non-whitespace character class.

    Returns:
        Non-whitespace pattern.
    """
    return r"\S"


def any_char() -> str:
    """Get any character pattern.

    Returns:
        Any character pattern.
    """
    return "."


def line_start() -> str:
    """Get line start anchor.

    Returns:
        Line start pattern.
    """
    return "^"


def line_end() -> str:
    """Get line end anchor.

    Returns:
        Line end pattern.
    """
    return "$"


def string_start() -> str:
    """Get string start anchor.

    Returns:
        String start pattern.
    """
    return r"\A"


def string_end() -> str:
    """Get string end anchor.

    Returns:
        String end pattern.
    """
    return r"\Z"