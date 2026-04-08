"""Text Pattern and Validation Utilities.

Pattern matching and text validation for UI element content.
Supports regex patterns, fuzzy matching, and semantic text comparison.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Optional


class MatchType(Enum):
    """Types of text pattern matching."""

    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    REGEX = auto()
    FUZZY = auto()
    SEMANTIC = auto()


@dataclass
class MatchResult:
    """Result of a text pattern match.

    Attributes:
        matched: Whether the pattern matched.
        match_type: Type of match performed.
        score: Match score (0.0 to 1.0).
        matched_text: The text that matched.
        groups: Captured groups from regex match.
    """

    matched: bool
    match_type: MatchType
    score: float = 0.0
    matched_text: str = ""
    groups: tuple = ()


class TextPatternMatcher:
    """Matches text against various patterns.

    Example:
        matcher = TextPatternMatcher()
        result = matcher.match("Hello World", MatchType.CONTAINS, "World")
    """

    def __init__(self, case_sensitive: bool = False):
        """Initialize the matcher.

        Args:
            case_sensitive: Whether matching is case-sensitive.
        """
        self.case_sensitive = case_sensitive

    def match(
        self,
        text: str,
        match_type: MatchType,
        pattern: str,
        threshold: float = 0.8,
    ) -> MatchResult:
        """Match text against a pattern.

        Args:
            text: Text to match against.
            match_type: Type of matching to perform.
            pattern: Pattern to match.
            threshold: Score threshold for fuzzy/semantic matching.

        Returns:
            MatchResult with match details.
        """
        if not self.case_sensitive:
            text = text.lower()
            pattern = pattern.lower()

        if match_type == MatchType.EXACT:
            return self._exact_match(text, pattern)
        elif match_type == MatchType.CONTAINS:
            return self._contains_match(text, pattern)
        elif match_type == MatchType.STARTS_WITH:
            return self._starts_with_match(text, pattern)
        elif match_type == MatchType.ENDS_WITH:
            return self._ends_with_match(text, pattern)
        elif match_type == MatchType.REGEX:
            return self._regex_match(text if self.case_sensitive else text.lower(),
                                      pattern, self.case_sensitive)
        elif match_type == MatchType.FUZZY:
            return self._fuzzy_match(text, pattern, threshold)
        else:
            return MatchResult(matched=False, match_type=match_type)

    def _exact_match(self, text: str, pattern: str) -> MatchResult:
        """Perform exact matching."""
        matched = text == pattern
        return MatchResult(
            matched=matched,
            match_type=MatchType.EXACT,
            score=1.0 if matched else 0.0,
            matched_text=text if matched else "",
        )

    def _contains_match(self, text: str, pattern: str) -> MatchResult:
        """Perform contains matching."""
        matched = pattern in text
        return MatchResult(
            matched=matched,
            match_type=MatchType.CONTAINS,
            score=1.0 if matched else 0.0,
            matched_text=pattern if matched else "",
        )

    def _starts_with_match(self, text: str, pattern: str) -> MatchResult:
        """Perform starts-with matching."""
        matched = text.startswith(pattern)
        return MatchResult(
            matched=matched,
            match_type=MatchType.STARTS_WITH,
            score=1.0 if matched else 0.0,
            matched_text=pattern if matched else "",
        )

    def _ends_with_match(self, text: str, pattern: str) -> MatchResult:
        """Perform ends-with matching."""
        matched = text.endswith(pattern)
        return MatchResult(
            matched=matched,
            match_type=MatchType.ENDS_WITH,
            score=1.0 if matched else 0.0,
            matched_text=pattern if matched else "",
        )

    def _regex_match(self, text: str, pattern: str, case_sensitive: bool) -> MatchResult:
        """Perform regex matching."""
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            match = re.search(pattern, text, flags)
            if match:
                return MatchResult(
                    matched=True,
                    match_type=MatchType.REGEX,
                    score=1.0,
                    matched_text=match.group(),
                    groups=match.groups(),
                )
        except re.error:
            pass
        return MatchResult(matched=False, match_type=MatchType.REGEX)

    def _fuzzy_match(self, text: str, pattern: str, threshold: float) -> MatchResult:
        """Perform fuzzy string matching."""
        ratio = difflib.SequenceMatcher(None, pattern, text).ratio()
        matched = ratio >= threshold
        return MatchResult(
            matched=matched,
            match_type=MatchType.FUZZY,
            score=ratio,
            matched_text=text if matched else "",
        )


class PatternValidator:
    """Validates text content against multiple patterns.

    Example:
        validator = PatternValidator()
        validator.add_rule("email", r"^[a-z]+@[a-z]+\.[a-z]+$")
        validator.add_rule("phone", r"^\d{3}-\d{3}-\d{4}$")
        results = validator.validate("test@example.com")
    """

    def __init__(self):
        """Initialize the validator."""
        self._rules: dict[str, tuple[str, Callable[[str], bool]]] = {}

    def add_rule(
        self,
        name: str,
        pattern: str,
        match_type: MatchType = MatchType.REGEX,
    ) -> None:
        """Add a validation rule.

        Args:
            name: Name of the rule.
            pattern: Pattern to match.
            match_type: Type of matching to use.
        """
        matcher = TextPatternMatcher()

        def validator(text: str) -> bool:
            result = matcher.match(text, match_type, pattern)
            return result.matched

        self._rules[name] = (pattern, validator)

    def validate(self, text: str) -> dict[str, bool]:
        """Validate text against all rules.

        Args:
            text: Text to validate.

        Returns:
            Dictionary of rule name to pass/fail.
        """
        return {name: validator(text) for name, (_, validator) in self._rules.items()}

    def validate_strict(self, text: str, required_rules: list[str]) -> bool:
        """Validate text against specific required rules.

        Args:
            text: Text to validate.
            required_rules: List of required rule names.

        Returns:
            True if all required rules pass.
        """
        for rule_name in required_rules:
            if rule_name not in self._rules:
                return False
            _, validator = self._rules[rule_name]
            if not validator(text):
                return False
        return True


@dataclass
class TextDiff:
    """Represents differences between two text strings.

    Attributes:
        added: Text that was added.
        removed: Text that was removed.
        unchanged: Text that remained the same.
        similarity: Similarity score (0.0 to 1.0).
    """

    added: list[str] = tuple()
    removed: list[str] = tuple()
    unchanged: list[str] = tuple()
    similarity: float = 0.0


class TextComparator:
    """Compares text content and produces diffs.

    Example:
        comparator = TextComparator()
        diff = comparator.compare("Hello World", "Hello Universe")
    """

    def compare(self, text1: str, text2: str) -> TextDiff:
        """Compare two text strings.

        Args:
            text1: First text string.
            text2: Second text string.

        Returns:
            TextDiff with differences.
        """
        matcher = difflib.SequenceMatcher(None, text1, text2)
        similarity = matcher.ratio()

        added: list[str] = []
        removed: list[str] = []
        unchanged: list[str] = []

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                unchanged.append(text2[j1:j2])
            elif tag == "replace":
                removed.append(text1[i1:i2])
                added.append(text2[j1:j2])
            elif tag == "delete":
                removed.append(text1[i1:i2])
            elif tag == "insert":
                added.append(text2[j1:j2])

        return TextDiff(
            added=tuple(added),
            removed=tuple(removed),
            unchanged=tuple(unchanged),
            similarity=similarity,
        )

    def has_significant_changes(
        self,
        text1: str,
        text2: str,
        threshold: float = 0.9,
    ) -> bool:
        """Check if texts have significant differences.

        Args:
            text1: First text string.
            text2: Second text string.
            threshold: Similarity threshold.

        Returns:
            True if texts are different beyond threshold.
        """
        diff = self.compare(text1, text2)
        return diff.similarity < threshold


class DynamicTextMatcher:
    """Matches text with dynamic variable substitution.

    Handles patterns with placeholders that should be ignored during matching.

    Example:
        matcher = DynamicTextMatcher()
        result = matcher.match_with_variables(
            "User: John, ID: 12345",
            "User: {name}, ID: {id}",
        )
    """

    def __init__(self):
        """Initialize the matcher."""
        self._variable_pattern = re.compile(r"\{[^}]+\}")

    def match_with_variables(
        self,
        text: str,
        pattern: str,
        variable_pattern: Optional[str] = None,
    ) -> MatchResult:
        """Match text allowing for variable substitution in pattern.

        Args:
            text: Text to match.
            pattern: Pattern with {variable} placeholders.
            variable_pattern: Optional regex for variable values.

        Returns:
            MatchResult with extracted variable values.
        """
        # Extract variables from pattern
        pattern_vars = self._variable_pattern.findall(pattern)

        if not pattern_vars:
            # No variables, do exact match
            matcher = TextPatternMatcher()
            return matcher.match(text, MatchType.EXACT, pattern)

        # Build regex from pattern
        regex_pattern = pattern
        for var in pattern_vars:
            var_regex = variable_pattern or r".+?"
            regex_pattern = regex_pattern.replace(var, f"({var_regex})")

        try:
            matcher = TextPatternMatcher()
            return matcher.match(text, MatchType.REGEX, regex_pattern)
        except re.error:
            return MatchResult(matched=False, match_type=MatchType.REGEX)

    def extract_variables(
        self,
        text: str,
        pattern: str,
    ) -> dict[str, str]:
        """Extract variable values from text using pattern.

        Args:
            text: Text to extract from.
            pattern: Pattern with {variable} placeholders.

        Returns:
            Dictionary of variable names to extracted values.
        """
        result = self.match_with_variables(text, pattern)
        if not result.matched:
            return {}

        pattern_vars = self._variable_pattern.findall(pattern)
        return {
            var.strip("{}"): group
            for var, group in zip(pattern_vars, result.groups)
        }
