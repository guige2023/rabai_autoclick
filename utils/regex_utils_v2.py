"""
Regular expression utilities with pattern compilation, caching, and validation.

Provides enhanced regex functionality including pattern management,
match utilities, and common pattern templates.

Example:
    >>> from utils.regex_utils_v2 import RegexBuilder, find_all, is_valid
    >>> pattern = RegexBuilder().email().build()
    >>> find_all("test@example.com", pattern)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Match, Optional, Pattern, Union


@dataclass
class RegexMatch:
    """Container for regex match results."""
    text: str
    start: int
    end: int
    groups: tuple
    group_dict: Dict[str, str]
    pattern: str

    @property
    def span(self) -> tuple[int, int]:
        """Get (start, end) tuple."""
        return (self.start, self.end)

    def __repr__(self) -> str:
        return f"RegexMatch({self.text!r}, {self.start}, {self.end})"


class RegexCache:
    """
    Global regex pattern cache with TTL.

    Caches compiled patterns to avoid recompilation overhead.
    """

    def __init__(self, max_size: int = 100) -> None:
        """Initialize the cache."""
        self._cache: Dict[str, Pattern] = {}
        self._max_size = max_size

    def get(self, pattern: str) -> Optional[Pattern]:
        """Get a cached pattern."""
        return self._cache.get(pattern)

    def put(self, pattern: str, compiled: Pattern) -> None:
        """Cache a compiled pattern."""
        if len(self._cache) >= self._max_size:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[pattern] = compiled

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()


_global_cache = RegexCache()


def compile(
    pattern: str,
    flags: int = 0,
    use_cache: bool = True,
) -> Pattern:
    """
    Compile a regex pattern with optional caching.

    Args:
        pattern: Regular expression pattern.
        flags: Regex flags (re.IGNORECASE, etc.).
        use_cache: Use the global pattern cache.

    Returns:
        Compiled pattern object.
    """
    if use_cache:
        cached = _global_cache.get(pattern)
        if cached is not None:
            return cached

    compiled = re.compile(pattern, flags)

    if use_cache:
        _global_cache.put(pattern, compiled)

    return compiled


def find_all(
    text: str,
    pattern: str,
    flags: int = 0,
    group: int = 0,
    as_strings: bool = True,
) -> List[Any]:
    """
    Find all matches in text.

    Args:
        text: Text to search.
        pattern: Regular expression pattern.
        flags: Regex flags.
        group: Group index to extract (0 for full match).
        as_strings: Return strings instead of match objects.

    Returns:
        List of matches.
    """
    compiled = compile(pattern, flags)
    matches = compiled.finditer(text)

    if as_strings:
        return [m.group(group) for m in matches]
    return list(matches)


def find_one(
    text: str,
    pattern: str,
    flags: int = 0,
    default: Any = None,
) -> Optional[Any]:
    """
    Find the first match in text.

    Args:
        text: Text to search.
        pattern: Regular expression pattern.
        flags: Regex flags.
        default: Default value if no match.

    Returns:
        First match or default.
    """
    compiled = compile(pattern, flags)
    match = compiled.search(text)
    if match:
        return RegexMatch(
            text=match.group(0),
            start=match.start(),
            end=match.end(),
            groups=match.groups(),
            group_dict=match.groupdict(),
            pattern=pattern,
        )
    return default


def is_valid(pattern: str) -> bool:
    """
    Check if a pattern is a valid regex.

    Args:
        pattern: Regular expression pattern string.

    Returns:
        True if pattern is valid, False otherwise.
    """
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False


def replace(
    text: str,
    pattern: str,
    replacement: Union[str, Callable[[Match], str]],
    flags: int = 0,
    count: int = 0,
) -> str:
    """
    Replace matches in text.

    Args:
        text: Text to search.
        pattern: Regular expression pattern.
        replacement: Replacement string or function.
        flags: Regex flags.
        count: Maximum number of replacements (0 for all).

    Returns:
        Text with replacements.
    """
    compiled = compile(pattern, flags)
    return compiled.sub(replacement, text, count=count)


def split(
    text: str,
    pattern: str,
    flags: int = 0,
    maxsplit: int = 0,
) -> List[str]:
    """
    Split text by pattern.

    Args:
        text: Text to split.
        pattern: Regular expression pattern.
        flags: Regex flags.
        maxsplit: Maximum number of splits (0 for unlimited).

    Returns:
        List of split parts.
    """
    compiled = compile(pattern, flags)
    return compiled.split(text, maxsplit=maxsplit)


class RegexBuilder:
    """
    Fluent builder for constructing regex patterns.

    Provides methods for common pattern components like
    emails, URLs, phone numbers, etc.

    Example:
        >>> pattern = RegexBuilder().email().or_().url().build()
    """

    def __init__(self) -> None:
        """Initialize the builder."""
        self._parts: List[str] = []
        self._flags = 0

    def literal(self, text: str, escape: bool = True) -> "RegexBuilder":
        """Add a literal string."""
        if escape:
            text = re.escape(text)
        self._parts.append(text)
        return self

    def any_of(self, chars: str) -> "RegexBuilder":
        """Match any single character from a set."""
        self._parts.append(f"[{re.escape(chars)}]")
        return self

    def range(self, start: str, end: str) -> "RegexBuilder":
        """Match any character in a range."""
        self._parts.append(f"[{re.escape(start)}-{re.escape(end)}]")
        return self

    def digit(self) -> "RegexBuilder":
        """Match a digit (\\d)."""
        self._parts.append(r"\d")
        return self

    def non_digit(self) -> "RegexBuilder":
        """Match a non-digit (\\D)."""
        self._parts.append(r"\D")
        return self

    def word(self) -> "RegexBuilder":
        """Match a word character (\\w)."""
        self._parts.append(r"\w")
        return self

    def non_word(self) -> "RegexBuilder":
        """Match a non-word character (\\W)."""
        self._parts.append(r"\W")
        return self

    def whitespace(self) -> "RegexBuilder":
        """Match whitespace (\\s)."""
        self._parts.append(r"\s")
        return self

    def non_whitespace(self) -> "RegexBuilder":
        """Match non-whitespace (\\S)."""
        self._parts.append(r"\S")
        return self

    def any(self) -> "RegexBuilder":
        """Match any character except newline."""
        self._parts.append(".")
        return self

    def start(self) -> "RegexBuilder":
        """Match start of string."""
        self._parts.append("^")
        return self

    def end(self) -> "RegexBuilder":
        """Match end of string."""
        self._parts.append("$")
        return self

    def word_boundary(self) -> "RegexBuilder":
        """Match word boundary."""
        self._parts.append(r"\b")
        return self

    def optional(self) -> "RegexBuilder":
        """Make previous pattern optional."""
        self._parts.append("?")
        return self

    def one_or_more(self) -> "RegexBuilder":
        """Match one or more of previous."""
        self._parts.append("+")
        return self

    def zero_or_more(self) -> "RegexBuilder":
        """Match zero or more of previous."""
        self._parts.append("*")
        return self

    def exactly(self, n: int) -> "RegexBuilder":
        """Match exactly n times."""
        self._parts.append(f"{{{n}}}")
        return self

    def at_least(self, n: int) -> "RegexBuilder":
        """Match at least n times."""
        self._parts.append(f"{{{n},}}")
        return self

    def between(self, min_n: int, max_n: int) -> "RegexBuilder":
        """Match between min and max times."""
        self._parts.append(f"{{{min_n},{max_n}}}")
        return self

    def group(self, name: Optional[str] = None) -> "RegexBuilder":
        """Start a capturing group."""
        if name:
            self._parts.append(f"(?P<{name}>")
        else:
            self._parts.append("(")
        return self

    def non_capturing_group(self) -> "RegexBuilder":
        """Start a non-capturing group."""
        self._parts.append("(?:")
        return self

    def end_group(self) -> "RegexBuilder":
        """End a group."""
        self._parts.append(")")
        return self

    def or_(self) -> "RegexBuilder":
        """Add alternation."""
        self._parts.append("|")
        return self

    def email(self) -> "RegexBuilder":
        """Match an email address pattern."""
        self._parts.append(r"[\w.+-]+@[\w-]+\.[\w.-]+")
        return self

    def url(self) -> "RegexBuilder":
        """Match a URL pattern."""
        self._parts.append(r"https?://[\w\-._~:/?#\[\]@!$&'()*+,;=%]+")
        return self

    def ipv4(self) -> "RegexBuilder":
        """Match an IPv4 address pattern."""
        self._parts.append(
            r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
            r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
        )
        return self

    def phone(self) -> "RegexBuilder":
        """Match a phone number pattern."""
        self._parts.append(r"\+?[\d\s\-().]{10,}")
        return self

    def build(self, flags: int = 0) -> Pattern:
        """Build and compile the pattern."""
        pattern = "".join(self._parts)
        return compile(pattern, flags)

    def build_string(self) -> str:
        """Build the pattern as a string."""
        return "".join(self._parts)


class PatternTemplate:
    """
    Pre-compiled common regex patterns.

    Provides efficient access to commonly used patterns.
    """

    EMAIL = compile(r"[\w.+-]+@[\w-]+\.[\w.-]+", re.IGNORECASE)
    URL = compile(r"https?://[\w\-._~:/?#\[\]@!$&'()*+,;=%]+")
    PHONE = compile(r"\+?[\d\s\-().]{10,}")
    IPV4 = compile(
        r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
    )
    UUID = compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        re.IGNORECASE
    )
    ISO_DATE = compile(r"\d{4}-\d{2}-\d{2}")
    ISO_DATETIME = compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    HEX_COLOR = compile(r"#[0-9a-fA-F]{6}")
    CREDIT_CARD = compile(r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}")


def extract_emails(text: str) -> List[str]:
    """Extract all email addresses from text."""
    return find_all(text, PatternTemplate.EMAIL.pattern, as_strings=True)


def extract_urls(text: str) -> List[str]:
    """Extract all URLs from text."""
    return find_all(text, PatternTemplate.URL.pattern, as_strings=True)


def extract_phones(text: str) -> List[str]:
    """Extract all phone numbers from text."""
    return find_all(text, PatternTemplate.PHONE.pattern, as_strings=True)


def extract_ips(text: str) -> List[str]:
    """Extract all IPv4 addresses from text."""
    return find_all(text, PatternTemplate.IPV4.pattern, as_strings=True)
