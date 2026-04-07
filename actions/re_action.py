"""re action extensions for rabai_autoclick.

Provides regular expression utilities, pattern compilation,
matching, and replacement operations.
"""

from __future__ import annotations

import re
from typing import Any, Callable

__all__ = [
    "compile",
    "match",
    "search",
    "findall",
    "finditer",
    "sub",
    "subn",
    "split",
    "fullmatch",
    "escape",
    "unescape",
    "is_match",
    "is_valid_pattern",
    "get_groups",
    "get_group_dict",
    "replace_all",
    "replace_first",
    "replace_func",
    "split_all",
    "split_with_separators",
    "pattern_exists",
    "pattern_count",
    "extract",
    "extract_all",
    "validate_pattern",
    "RegexBuilder",
    "RegexMatcher",
    "PatternCache",
]


def compile(pattern: str, flags: int = 0) -> re.Pattern:
    """Compile a regular expression pattern.

    Args:
        pattern: Regex pattern string.
        flags: Regex flags.

    Returns:
        Compiled pattern.
    """
    return re.compile(pattern, flags)


def match(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
) -> re.Match | None:
    """Match pattern at start of text.

    Args:
        pattern: Pattern to match.
        text: Text to search.
        flags: Regex flags (if pattern is string).

    Returns:
        Match object or None.
    """
    if isinstance(pattern, str):
        return re.match(pattern, text, flags)
    return pattern.match(text)


def search(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
) -> re.Match | None:
    """Search for pattern in text.

    Args:
        pattern: Pattern to search.
        text: Text to search.
        flags: Regex flags.

    Returns:
        Match object or None.
    """
    if isinstance(pattern, str):
        return re.search(pattern, text, flags)
    return pattern.search(text)


def findall(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
) -> list:
    """Find all matches of pattern in text.

    Args:
        pattern: Pattern to find.
        text: Text to search.
        flags: Regex flags.

    Returns:
        List of matches.
    """
    if isinstance(pattern, str):
        return re.findall(pattern, text, flags)
    return pattern.findall(text)


def finditer(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
):
    """Find all matches as iterator.

    Args:
        pattern: Pattern to find.
        text: Text to search.
        flags: Regex flags.

    Returns:
        Iterator of match objects.
    """
    if isinstance(pattern, str):
        return re.finditer(pattern, text, flags)
    return pattern.finditer(text)


def sub(
    pattern: str | re.Pattern,
    repl: str | Callable,
    text: str,
    count: int = 0,
    flags: int = 0,
) -> str:
    """Replace pattern matches in text.

    Args:
        pattern: Pattern to replace.
        repl: Replacement string or function.
        text: Text to modify.
        count: Max replacements (0 = all).
        flags: Regex flags.

    Returns:
        Modified text.
    """
    if isinstance(pattern, str):
        return re.sub(pattern, repl, text, count=count, flags=flags)
    return pattern.sub(repl, text, count=count)


def subn(
    pattern: str | re.Pattern,
    repl: str | Callable,
    text: str,
    count: int = 0,
    flags: int = 0,
) -> tuple[str, int]:
    """Replace pattern and return count.

    Args:
        pattern: Pattern to replace.
        repl: Replacement string or function.
        text: Text to modify.
        count: Max replacements.
        flags: Regex flags.

    Returns:
        Tuple of (modified text, replacement count).
    """
    if isinstance(pattern, str):
        return re.subn(pattern, repl, text, count=count, flags=flags)
    return pattern.subn(repl, text, count=count)


def split(
    pattern: str | re.Pattern,
    text: str,
    maxsplit: int = 0,
    flags: int = 0,
) -> list[str]:
    """Split text by pattern.

    Args:
        pattern: Pattern to split on.
        text: Text to split.
        maxsplit: Maximum splits.
        flags: Regex flags.

    Returns:
        List of text parts.
    """
    if isinstance(pattern, str):
        return re.split(pattern, text, maxsplit=maxsplit, flags=flags)
    return pattern.split(text, maxsplit=maxsplit)


def fullmatch(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
) -> re.Match | None:
    """Match pattern entirely.

    Args:
        pattern: Pattern to match.
        text: Text to match.
        flags: Regex flags.

    Returns:
        Match object or None.
    """
    if isinstance(pattern, str):
        return re.fullmatch(pattern, text, flags)
    return pattern.fullmatch(text)


def escape(text: str, special: bool = True) -> str:
    """Escape special regex characters.

    Args:
        text: Text to escape.
        special: Escape special characters.

    Returns:
        Escaped text.
    """
    return re.escape(text) if special else text


def unescape(text: str) -> str:
    """Unescape escaped characters.

    Args:
        text: Text to unescape.

    Returns:
        Unescaped text.
    """
    return (
        text.replace(r"\.", ".")
        .replace(r"\*", "*")
        .replace(r"\+", "+")
        .replace(r"\?", "?")
        .replace(r"\^", "^")
        .replace(r"\$", "$")
        .replace(r"\[", "[")
        .replace(r"\]", "]")
        .replace(r"\{", "{")
        .replace(r"\}", "}")
        .replace(r"\|", "|")
        .replace(r"\(", "(")
        .replace(r"\)", ")")
        .replace(r"\\", "\\")
    )


def is_match(
    pattern: str | re.Pattern,
    text: str,
    flags: int = 0,
) -> bool:
    """Check if pattern matches text.

    Args:
        pattern: Pattern to check.
        text: Text to match.
        flags: Regex flags.

    Returns:
        True if pattern matches.
    """
    if isinstance(pattern, str):
        return bool(re.match(pattern, text, flags))
    return bool(pattern.match(text))


def is_valid_pattern(pattern: str) -> tuple[bool, str | None]:
    """Validate a regex pattern.

    Args:
        pattern: Pattern to validate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        re.compile(pattern)
        return True, None
    except re.error as e:
        return False, str(e)


def get_groups(match: re.Match) -> tuple:
    """Get all groups from match.

    Args:
        match: Match object.

    Returns:
        Tuple of group values.
    """
    return match.groups()


def get_group_dict(match: re.Match) -> dict[str | int, str | None]:
    """Get named groups from match.

    Args:
        match: Match object.

    Returns:
        Dict of group name to value.
    """
    return match.groupdict()


def replace_all(
    pattern: str,
    repl: str,
    text: str,
    flags: int = 0,
) -> str:
    """Replace all occurrences.

    Args:
        pattern: Pattern to replace.
        repl: Replacement string.
        text: Text to modify.
        flags: Regex flags.

    Returns:
        Modified text.
    """
    return re.sub(pattern, repl, text, flags=flags)


def replace_first(
    pattern: str,
    repl: str,
    text: str,
    flags: int = 0,
) -> str:
    """Replace first occurrence only.

    Args:
        pattern: Pattern to replace.
        repl: Replacement string.
        text: Text to modify.
        flags: Regex flags.

    Returns:
        Modified text.
    """
    return re.sub(pattern, repl, text, count=1, flags=flags)


def replace_func(
    pattern: str,
    func: Callable[[re.Match], str],
    text: str,
    flags: int = 0,
) -> str:
    """Replace using a function.

    Args:
        pattern: Pattern to replace.
        func: Function taking match, returning replacement.
        text: Text to modify.
        flags: Regex flags.

    Returns:
        Modified text.
    """
    return re.sub(pattern, func, text, flags=flags)


def split_all(
    pattern: str,
    text: str,
    maxsplit: int = 0,
    flags: int = 0,
) -> list[str]:
    """Split and keep separators.

    Args:
        pattern: Pattern to split on.
        text: Text to split.
        maxsplit: Maximum splits.
        flags: Regex flags.

    Returns:
        List including separators.
    """
    parts = re.split(f"({pattern})", text, maxsplit=maxsplit, flags=flags)
    return [p for p in parts if p]


def split_with_separators(
    pattern: str,
    text: str,
    flags: int = 0,
) -> list[str]:
    """Split and include separators.

    Args:
        pattern: Pattern to split on.
        text: Text to split.
        flags: Regex flags.

    Returns:
        List with separators included.
    """
    return re.findall(f".*?{pattern}|.+", text, flags=flags)


def pattern_exists(pattern: str, text: str, flags: int = 0) -> bool:
    """Check if pattern exists in text.

    Args:
        pattern: Pattern to search.
        text: Text to search.
        flags: Regex flags.

    Returns:
        True if pattern found.
    """
    return bool(re.search(pattern, text, flags))


def pattern_count(pattern: str, text: str, flags: int = 0) -> int:
    """Count pattern occurrences.

    Args:
        pattern: Pattern to count.
        text: Text to search.
        flags: Regex flags.

    Returns:
        Number of matches.
    """
    return len(re.findall(pattern, text, flags))


def extract(
    pattern: str,
    text: str,
    group: int | str = 0,
    flags: int = 0,
) -> str | None:
    """Extract first match.

    Args:
        pattern: Pattern to extract.
        text: Text to search.
        group: Group index or name.
        flags: Regex flags.

    Returns:
        Extracted string or None.
    """
    match = re.search(pattern, text, flags)
    if match:
        return match.group(group)
    return None


def extract_all(
    pattern: str,
    text: str,
    group: int | str = 0,
    flags: int = 0,
) -> list[str]:
    """Extract all matches.

    Args:
        pattern: Pattern to extract.
        text: Text to search.
        group: Group index or name.
        flags: Regex flags.

    Returns:
        List of extracted strings.
    """
    matches = re.findall(pattern, text, flags)
    if not matches:
        return []
    if isinstance(matches[0], tuple):
        return [m[group] if group < len(m) else m[-1] for m in matches]
    return matches


def validate_pattern(pattern: str) -> tuple[bool, str | None]:
    """Validate regex pattern.

    Args:
        pattern: Pattern to validate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        re.compile(pattern)
        return True, None
    except re.error as e:
        return False, str(e)


class RegexBuilder:
    """Builder for constructing regex patterns."""

    def __init__(self) -> None:
        self._parts: list[str] = []

    def literal(self, text: str) -> RegexBuilder:
        """Add literal text.

        Args:
            text: Text to add.

        Returns:
            Self for chaining.
        """
        self._parts.append(re.escape(text))
        return self

    def exact(self, text: str) -> RegexBuilder:
        """Add exact (unescaped) text.

        Args:
            text: Text to add.

        Returns:
            Self for chaining.
        """
        self._parts.append(text)
        return self

    def any_of(self, *chars: str) -> RegexBuilder:
        """Add character class [abc].

        Args:
            *chars: Characters to match.

        Returns:
            Self for chaining.
        """
        self._parts.append(f"[{''.join(chars)}]")
        return self

    def range(self, start: str, end: str) -> RegexBuilder:
        """Add character range [a-z].

        Args:
            start: Start character.
            end: End character.

        Returns:
            Self for chaining.
        """
        self._parts.append(f"[{start}-{end}]")
        return self

    def digit(self) -> RegexBuilder:
        """Add digit shorthand \\d."""
        self._parts.append(r"\d")
        return self

    def nondigit(self) -> RegexBuilder:
        """Add non-digit shorthand \\D."""
        self._parts.append(r"\D")
        return self

    def word(self) -> RegexBuilder:
        """Add word character shorthand \\w."""
        self._parts.append(r"\w")
        return self

    def nonword(self) -> RegexBuilder:
        """Add non-word shorthand \\W."""
        self._parts.append(r"\W")
        return self

    def space(self) -> RegexBuilder:
        """Add whitespace shorthand \\s."""
        self._parts.append(r"\s")
        return self

    def nonspace(self) -> RegexBuilder:
        """Add non-whitespace shorthand \\S."""
        self._parts.append(r"\S")
        return self

    def optional(self, part: str) -> RegexBuilder:
        """Make last part optional.

        Returns:
            Self for chaining.
        """
        if self._parts:
            self._parts[-1] = f"{self._parts[-1]}?"
        return self

    def one_or_more(self) -> RegexBuilder:
        """Make last part greedy one-or-more.

        Returns:
            Self for chaining.
        """
        if self._parts:
            self._parts[-1] = f"{self._parts[-1]}+"
        return self

    def zero_or_more(self) -> RegexBuilder:
        """Make last part greedy zero-or-more.

        Returns:
            Self for chaining.
        """
        if self._parts:
            self._parts[-1] = f"{self._parts[-1]}*"
        return self

    def group(self, name: str | None = None) -> RegexBuilder:
        """Add capturing group.

        Args:
            name: Optional group name.

        Returns:
            Self for chaining.
        """
        if name:
            self._parts.append(f"(?P<{name}>")
        else:
            self._parts.append("(")
        return self

    def end_group(self) -> RegexBuilder:
        """Close capturing group."""
        self._parts.append(")")
        return self

    def non_capturing(self) -> RegexBuilder:
        """Add non-capturing group."""
        self._parts.append("(?:")
        return self

    def lookahead(self, pattern: str, positive: bool = True) -> RegexBuilder:
        """Add lookahead.

        Args:
            pattern: Pattern to look ahead for.
            positive: True for positive, False for negative.

        Returns:
            Self for chaining.
        """
        prefix = "(?=" if positive else "(?!"
        self._parts.append(f"{prefix}{pattern})")
        return self

    def lookbehind(self, pattern: str, positive: bool = True) -> RegexBuilder:
        """Add lookbehind.

        Args:
            pattern: Pattern to look behind for.
            positive: True for positive, False for negative.

        Returns:
            Self for chaining.
        """
        prefix = "(?<=" if positive else "(?<!"
        self._parts.append(f"{prefix}{pattern})")
        return self

    def build(self) -> re.Pattern:
        """Build the compiled pattern.

        Returns:
            Compiled regex pattern.
        """
        return re.compile("".join(self._parts))

    def pattern(self) -> str:
        """Get the pattern string.

        Returns:
            Pattern string.
        """
        return "".join(self._parts)


class RegexMatcher:
    """Matcher for applying regex patterns."""

    def __init__(self, pattern: str | re.Pattern, flags: int = 0) -> None:
        if isinstance(pattern, str):
            self._pattern = re.compile(pattern, flags)
        else:
            self._pattern = pattern

    def match(self, text: str) -> re.Match | None:
        """Match at start."""
        return self._pattern.match(text)

    def search(self, text: str) -> re.Match | None:
        """Search in text."""
        return self._pattern.search(text)

    def findall(self, text: str) -> list:
        """Find all matches."""
        return self._pattern.findall(text)

    def finditer(self, text: str):
        """Find all matches as iterator."""
        return self._pattern.finditer(text)

    def sub(self, repl: str | Callable, text: str) -> str:
        """Replace matches."""
        return self._pattern.sub(repl, text)

    def split(self, text: str) -> list[str]:
        """Split by pattern."""
        return self._pattern.split(text)


class PatternCache:
    """Cache for compiled regex patterns."""

    def __init__(self, maxsize: int = 128) -> None:
        self._cache: dict[str, re.Pattern] = {}
        self._maxsize = maxsize

    def get(self, pattern: str, flags: int = 0) -> re.Pattern:
        """Get compiled pattern from cache.

        Args:
            pattern: Pattern string.
            flags: Regex flags.

        Returns:
            Compiled pattern.
        """
        key = (pattern, flags)
        if key not in self._cache:
            if len(self._cache) >= self._maxsize:
                oldest = next(iter(self._cache))
                del self._cache[oldest]
            self._cache[key] = re.compile(pattern, flags)
        return self._cache[key]

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
