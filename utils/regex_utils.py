"""Regex pattern compilation and matching utilities.

Provides pre-compiled patterns, common regex builders,
and pattern testing utilities for text processing.
"""

import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Match, Optional, Pattern, Union


@dataclass
class RegexPattern:
    """Compiled regex pattern with metadata.

    Example:
        pattern = RegexPattern(
            name="email",
            pattern=r"[\w.-]+@[\w.-]+\.\w+",
            flags=re.IGNORECASE
        )
        matches = pattern.findall(text)
    """
    name: str
    pattern: Union[str, Pattern[str]]
    flags: int = 0
    _compiled: Optional[Pattern[str]] = None

    def __post_init__(self) -> None:
        if isinstance(self.pattern, str):
            self._compiled = re.compile(self.pattern, self.flags)
        else:
            self._compiled = self.pattern

    @property
    def compiled(self) -> Pattern[str]:
        """Get compiled pattern."""
        if self._compiled is None:
            self._compiled = re.compile(self.pattern, self.flags)
        return self._compiled

    def match(self, text: str) -> Optional[Match[str]]:
        """Match from start of string."""
        return self.compiled.match(text)

    def search(self, text: str) -> Optional[Match[str]]:
        """Search for pattern in string."""
        return self.compiled.search(text)

    def findall(self, text: str) -> List[str]:
        """Find all matches."""
        return self.compiled.findall(text)

    def finditer(self, text: str):
        """Iterate over matches."""
        return self.compiled.finditer(text)

    def split(self, text: str) -> List[str]:
        """Split by pattern."""
        return self.compiled.split(text)

    def sub(self, text: str, replacement: str) -> str:
        """Replace matches."""
        return self.compiled.sub(replacement, text)

    def groups(self, text: str) -> Optional[tuple]:
        """Get groups from match."""
        m = self.match(text)
        return m.groups() if m else None

    def named_groups(self, text: str) -> Optional[Dict[str, str]]:
        """Get named groups from match."""
        m = self.match(text)
        return m.groupdict() if m else None


class RegexLibrary:
    """Library of pre-compiled common regex patterns.

    Example:
        lib = RegexLibrary()
        emails = lib.email.findall(text)
        phones = lib.phone.findall(text)
    """

    def __init__(self) -> None:
        self._patterns: Dict[str, RegexPattern] = {}
        self._init_common()

    def _init_common(self) -> None:
        """Initialize common patterns."""
        common = [
            ("email", r"[\w.-]+@[\w.-]+\.\w{2,}"),
            ("phone_us", r"\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}"),
            ("phone_intl", r"\+[0-9]{1,3}[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{2,4}"),
            ("url", r"https?://[\w.-]+(?:/[\w./-]*)?", re.IGNORECASE),
            ("ipv4", r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b"),
            ("ipv6", r"(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}"),
            ("mac_address", r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}"),
            ("uuid", r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE),
            ("hex_color", r"#(?:[0-9a-fA-F]{3}){1,2}"),
            ("date_iso", r"\d{4}-\d{2}-\d{2}"),
            ("date_us", r"\d{1,2}/\d{1,2}/\d{4}"),
            ("time_24h", r"(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?"),
            ("time_12h", r"(?:0?[1-9]|1[0-2]):[0-5]\d\s?(?:AM|PM|am|pm)"),
            ("integer", r"-?\d+"),
            ("decimal", r"-?\d+\.?\d*"),
            ("filename", r"[\w.-]+"),
            ("html_tag", r"<[^>]+>"),
            ("whitespace", r"\s+"),
            ("word", r"\w+"),
        ]

        for name, pattern, *rest in common:
            flags = rest[0] if rest else 0
            self._patterns[name] = RegexPattern(name=name, pattern=pattern, flags=flags)

    def __getattr__(self, name: str) -> Optional[RegexPattern]:
        """Get pattern by name."""
        return self._patterns.get(name)

    def register(
        self,
        name: str,
        pattern: str,
        flags: int = 0,
    ) -> RegexPattern:
        """Register a custom pattern.

        Args:
            name: Pattern identifier.
            pattern: Regex pattern string.
            flags: Regex flags.

        Returns:
            Created pattern.
        """
        rp = RegexPattern(name=name, pattern=pattern, flags=flags)
        self._patterns[name] = rp
        return rp

    def get(self, name: str) -> Optional[RegexPattern]:
        """Get pattern by name."""
        return self._patterns.get(name)

    def list_patterns(self) -> List[str]:
        """List all pattern names."""
        return list(self._patterns.keys())


class PatternBuilder:
    """Builder for constructing complex regex patterns.

    Example:
        pb = PatternBuilder()
        pattern = (
            pb.literal("start")
            .maybe("extra")
            .one_of(["a", "b", "c"])
            .named("num").one_or_more(pb.DIGIT)
            .literal("end")
            .build()
        )
    """

    DIGIT = r"\d"
    WORD = r"\w"
    WHITESPACE = r"\s"
    ANY = r"."

    def __init__(self) -> None:
        self._parts: List[str] = []

    def literal(self, text: str) -> "PatternBuilder":
        """Add literal text (escaped)."""
        self._parts.append(re.escape(text))
        return self

    def regex(self, pattern: str) -> "PatternBuilder":
        """Add raw regex pattern."""
        self._parts.append(pattern)
        return self

    def one_of(self, items: List[str]) -> "PatternBuilder":
        """Add alternation group."""
        escaped = [re.escape(item) for item in items]
        self._parts.append(f"(?:{'|'.join(escaped)})")
        return self

    def maybe(self, pattern: str) -> "PatternBuilder":
        """Make pattern optional."""
        self._parts.append(f"(?:{pattern})?")
        return self

    def zero_or_more(self, pattern: str) -> "PatternBuilder":
        """Zero or more repetitions."""
        self._parts.append(f"(?:{pattern})*")
        return self

    def one_or_more(self, pattern: str) -> "PatternBuilder":
        """One or more repetitions."""
        self._parts.append(f"(?:{pattern})+")
        return self

    def named(self, name: str, pattern: str) -> "PatternBuilder":
        """Add named capture group."""
        self._parts.append(f"(?P<{name}>{pattern})")
        return self

    def group(self, pattern: str) -> "PatternBuilder":
        """Add capturing group."""
        self._parts.append(f"({pattern})")
        return self

    def lookahead(self, pattern: str, positive: bool = True) -> "PatternBuilder":
        """Add lookahead assertion."""
        prefix = "=" if positive else "!"
        self._parts.append(f"(?{prefix}{pattern})")
        return self

    def build(self) -> str:
        """Build final pattern string."""
        return "".join(self._parts)

    def compile(self, flags: int = 0) -> Pattern[str]:
        """Compile to regex pattern."""
        return re.compile(self.build(), flags)


def test_pattern(
    pattern: str,
    test_strings: List[str],
    flags: int = 0,
) -> Dict[str, List[str]]:
    """Test a pattern against multiple strings.

    Args:
        pattern: Regex pattern.
        test_strings: Strings to test against.
        flags: Regex flags.

    Returns:
        Dict mapping strings to match groups.
    """
    compiled = re.compile(pattern, flags)
    results: Dict[str, List[str]] = {}

    for s in test_strings:
        matches = compiled.findall(s)
        results[s] = matches

    return results


from typing import Any
