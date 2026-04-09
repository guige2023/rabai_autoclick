"""
Text finding and matching utilities for UI automation.

Provides text search across UI elements, fuzzy matching,
text pattern matching, and text extraction helpers.

Author: Auto-generated
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Protocol


class MatchType(Enum):
    """Type of text match."""
    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    FUZZY = auto()
    REGEX = auto()
    CASE_INSENSITIVE = auto()


@dataclass
class TextMatch:
    """Result of a text search match."""
    text: str
    match_type: MatchType
    confidence: float
    start_index: int = 0
    end_index: int = 0
    metadata: dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class TextSearchOptions:
    """Options for text searching."""
    match_type: MatchType = MatchType.CONTAINS
    case_sensitive: bool = True
    trim_whitespace: bool = True
    min_confidence: float = 0.8
    use_regex: bool = False


class TextMatcher:
    """
    Matches text against patterns with various strategies.
    
    Example:
        matcher = TextMatcher()
        result = matcher.match("Hello World", "hello", MatchType.CASE_INSENSITIVE)
    """
    
    def __init__(self, default_options: TextSearchOptions | None = None):
        self._default_options = default_options or TextSearchOptions()
        self._custom_matchers: dict[str, Callable[[str, str], float]] = {}
    
    def match(
        self,
        text: str,
        pattern: str,
        match_type: MatchType | None = None,
        options: TextSearchOptions | None = None,
    ) -> TextMatch:
        """
        Match text against a pattern.
        
        Args:
            text: The text to search in
            pattern: The pattern to search for
            match_type: Type of match to perform
            options: Search options
            
        Returns:
            TextMatch with results
        """
        opts = options or self._default_options
        match_type = match_type or opts.match_type
        
        if opts.trim_whitespace:
            text = text.strip()
            pattern = pattern.strip()
        
        text_lower = text.lower() if not opts.case_sensitive else text
        pattern_lower = pattern.lower() if not opts.case_sensitive else pattern
        
        if match_type == MatchType.EXACT:
            return self._match_exact(text, pattern, text_lower, pattern_lower, opts)
        elif match_type == MatchType.CONTAINS:
            return self._match_contains(text, pattern, text_lower, pattern_lower, opts)
        elif match_type == MatchType.STARTS_WITH:
            return self._match_starts_with(text, pattern, text_lower, pattern_lower, opts)
        elif match_type == MatchType.ENDS_WITH:
            return self._match_ends_with(text, pattern, text_lower, pattern_lower, opts)
        elif match_type == MatchType.FUZZY:
            return self._match_fuzzy(text, pattern, opts)
        elif match_type == MatchType.REGEX:
            return self._match_regex(text, pattern, opts)
        elif match_type == MatchType.CASE_INSENSITIVE:
            return self._match_case_insensitive(text, pattern, text_lower, pattern_lower, opts)
        
        return TextMatch(text, match_type, 0.0)
    
    def _match_exact(
        self, text: str, pattern: str,
        text_lower: str, pattern_lower: str,
        options: TextSearchOptions,
    ) -> TextMatch:
        match = text == pattern if options.case_sensitive else text_lower == pattern_lower
        return TextMatch(
            text=pattern,
            match_type=MatchType.EXACT,
            confidence=1.0 if match else 0.0,
            start_index=0 if match else -1,
            end_index=len(pattern) if match else 0,
        )
    
    def _match_contains(
        self, text: str, pattern: str,
        text_lower: str, pattern_lower: str,
        options: TextSearchOptions,
    ) -> TextMatch:
        idx = text_lower.find(pattern_lower)
        if idx >= 0:
            return TextMatch(
                text=pattern,
                match_type=MatchType.CONTAINS,
                confidence=1.0,
                start_index=idx,
                end_index=idx + len(pattern),
            )
        return TextMatch(text, MatchType.CONTAINS, 0.0)
    
    def _match_starts_with(
        self, text: str, pattern: str,
        text_lower: str, pattern_lower: str,
        options: TextSearchOptions,
    ) -> TextMatch:
        if text_lower.startswith(pattern_lower):
            return TextMatch(
                text=pattern,
                match_type=MatchType.STARTS_WITH,
                confidence=1.0,
                start_index=0,
                end_index=len(pattern),
            )
        return TextMatch(text, MatchType.STARTS_WITH, 0.0)
    
    def _match_ends_with(
        self, text: str, pattern: str,
        text_lower: str, pattern_lower: str,
        options: TextSearchOptions,
    ) -> TextMatch:
        if text_lower.endswith(pattern_lower):
            return TextMatch(
                text=pattern,
                match_type=MatchType.ENDS_WITH,
                confidence=1.0,
                start_index=len(text) - len(pattern),
                end_index=len(text),
            )
        return TextMatch(text, MatchType.ENDS_WITH, 0.0)
    
    def _match_fuzzy(
        self, text: str, pattern: str, options: TextSearchOptions
    ) -> TextMatch:
        confidence = difflib.SequenceMatcher(None, pattern.lower(), text.lower()).ratio()
        return TextMatch(
            text=pattern,
            match_type=MatchType.FUZZY,
            confidence=confidence,
        )
    
    def _match_regex(
        self, text: str, pattern: str, options: TextSearchOptions
    ) -> TextMatch:
        try:
            regex = re.compile(pattern, 0 if options.case_sensitive else re.IGNORECASE)
            match = regex.search(text)
            if match:
                return TextMatch(
                    text=pattern,
                    match_type=MatchType.REGEX,
                    confidence=1.0,
                    start_index=match.start(),
                    end_index=match.end(),
                    metadata={"groups": match.groups()},
                )
        except re.error:
            pass
        return TextMatch(text, MatchType.REGEX, 0.0)
    
    def _match_case_insensitive(
        self, text: str, pattern: str,
        text_lower: str, pattern_lower: str,
        options: TextSearchOptions,
    ) -> TextMatch:
        idx = text_lower.find(pattern_lower)
        if idx >= 0:
            return TextMatch(
                text=pattern,
                match_type=MatchType.CASE_INSENSITIVE,
                confidence=1.0,
                start_index=idx,
                end_index=idx + len(pattern),
            )
        return TextMatch(text, MatchType.CASE_INSENSITIVE, 0.0)
    
    def register_custom_matcher(
        self, name: str, matcher: Callable[[str, str], float]
    ) -> None:
        """
        Register a custom matching function.
        
        Args:
            name: Name of the matcher
            matcher: Function(text, pattern) -> confidence [0.0, 1.0]
        """
        self._custom_matchers[name] = matcher
    
    def match_custom(
        self, name: str, text: str, pattern: str
    ) -> TextMatch | None:
        """Execute a custom matcher by name."""
        if name not in self._custom_matchers:
            return None
        confidence = self._custom_matchers[name](text, pattern)
        return TextMatch(
            text=pattern,
            match_type=MatchType.EXACT,
            confidence=confidence,
        )


class TextFinder:
    """
    Finds text across multiple elements or strings.
    
    Example:
        finder = TextFinder()
        results = finder.find_all(["Hello", "World", "Hello World"], "hello")
    """
    
    def __init__(self, matcher: TextMatcher | None = None):
        self._matcher = matcher or TextMatcher()
    
    def find_first(
        self,
        texts: list[str],
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
    ) -> tuple[int, TextMatch] | None:
        """
        Find first matching text in a list.
        
        Returns:
            Tuple of (index, TextMatch) or None
        """
        for i, text in enumerate(texts):
            match = self._matcher.match(text, pattern, match_type)
            if match.confidence >= 0.8:
                return (i, match)
        return None
    
    def find_all(
        self,
        texts: list[str],
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
    ) -> list[tuple[int, TextMatch]]:
        """
        Find all matching texts.
        
        Returns:
            List of (index, TextMatch) tuples
        """
        results = []
        for i, text in enumerate(texts):
            match = self._matcher.match(text, pattern, match_type)
            if match.confidence >= 0.8:
                results.append((i, match))
        return results
    
    def filter_texts(
        self,
        texts: list[str],
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
        min_confidence: float = 0.8,
    ) -> list[str]:
        """Filter texts that match the pattern."""
        results = []
        for text in texts:
            match = self._matcher.match(text, pattern, match_type)
            if match.confidence >= min_confidence:
                results.append(text)
        return results


def normalize_text(text: str) -> str:
    """
    Normalize text for comparison.
    
    - Strips whitespace
    - Converts to lowercase
    - Removes extra spaces
    """
    text = text.strip()
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text


def extract_numbers(text: str) -> list[float]:
    """Extract all numbers from text."""
    pattern = r"-?\d+\.?\d*"
    matches = re.findall(pattern, text)
    return [float(m) for m in matches]


def extract_emails(text: str) -> list[str]:
    """Extract email addresses from text."""
    pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    return re.findall(pattern, text)


def extract_urls(text: str) -> list[str]:
    """Extract URLs from text."""
    pattern = r"https?://[^\s<>\"]+"
    return re.findall(pattern, text)
