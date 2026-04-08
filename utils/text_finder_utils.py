"""Text finder utilities.

This module provides utilities for finding text within UI elements,
OCR results, and screen regions.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class TextMatch:
    """A text match result."""
    text: str
    start: int
    end: int
    confidence: float = 1.0
    bounds: Optional[Tuple[int, int, int, int]] = None  # x, y, width, height


@dataclass
class TextSearchOptions:
    """Options for text search."""
    case_sensitive: bool = False
    whole_word: bool = False
    regex: bool = False
    fuzzy: bool = False
    fuzzy_threshold: float = 0.8


class TextFinder:
    """Finds text within text sources."""

    def __init__(self) -> None:
        self._sources: Dict[str, str] = {}

    def add_source(self, name: str, text: str) -> None:
        self._sources[name] = text

    def remove_source(self, name: str) -> bool:
        if name in self._sources:
            del self._sources[name]
            return True
        return False

    def find_all(
        self,
        pattern: str,
        options: Optional[TextSearchOptions] = None,
    ) -> Dict[str, List[TextMatch]]:
        results: Dict[str, List[TextMatch]] = {}
        opts = options or TextSearchOptions()

        for name, text in self._sources.items():
            matches = self._find_in_text(pattern, text, opts)
            if matches:
                results[name] = matches

        return results

    def find_first(
        self,
        pattern: str,
        options: Optional[TextSearchOptions] = None,
    ) -> Optional[Tuple[str, TextMatch]]:
        opts = options or TextSearchOptions()
        for name, text in self._sources.items():
            matches = self._find_in_text(pattern, text, opts)
            if matches:
                return (name, matches[0])
        return None

    def _find_in_text(
        self,
        pattern: str,
        text: str,
        options: TextSearchOptions,
    ) -> List[TextMatch]:
        matches: List[TextMatch] = []

        if options.regex:
            flags = 0 if options.case_sensitive else re.IGNORECASE
            regex = re.compile(pattern, flags)
            for m in regex.finditer(text):
                matches.append(TextMatch(
                    text=m.group(),
                    start=m.start(),
                    end=m.end(),
                    confidence=1.0,
                ))
        elif options.fuzzy:
            matches = self._fuzzy_find(pattern, text, options)
        else:
            search_text = text if options.case_sensitive else text.lower()
            search_pattern = pattern if options.case_sensitive else pattern.lower()
            if options.whole_word:
                search_pattern = r'\b' + re.escape(search_pattern) + r'\b'
                for m in re.finditer(search_pattern, search_text):
                    original = text[m.start():m.end()]
                    matches.append(TextMatch(
                        text=original,
                        start=m.start(),
                        end=m.end(),
                    ))
            else:
                start = 0
                while True:
                    idx = search_text.find(search_pattern, start)
                    if idx == -1:
                        break
                    original = text[idx:idx + len(pattern)]
                    matches.append(TextMatch(
                        text=original,
                        start=idx,
                        end=idx + len(pattern),
                    ))
                    start = idx + 1

        return matches

    def _fuzzy_find(
        self,
        pattern: str,
        text: str,
        options: TextSearchOptions,
    ) -> List[TextMatch]:
        import difflib
        matches: List[TextMatch] = []
        pattern_lower = pattern.lower()
        text_lower = text.lower()

        words = text.split()
        for i, word in enumerate(words):
            word_start_in_text = text_lower.find(word.lower(), 0 if i == 0 else sum(len(w) + 1 for w in words[:i]))
            ratio = difflib.SequenceMatcher(None, pattern_lower, word.lower()).ratio()
            if ratio >= options.fuzzy_threshold:
                matches.append(TextMatch(
                    text=word,
                    start=word_start_in_text,
                    end=word_start_in_text + len(word),
                    confidence=ratio,
                ))

        return matches


def highlight_matches(
    text: str,
    matches: List[TextMatch],
    prefix: str = "【",
    suffix: str = "】",
) -> str:
    """Insert highlight markers around matches in text.

    Args:
        text: Original text.
        matches: List of matches to highlight.
        prefix: Prefix marker.
        suffix: Suffix marker.

    Returns:
        Text with highlights.
    """
    if not matches:
        return text

    sorted_matches = sorted(matches, key=lambda m: m.start)
    result_parts = []
    last_end = 0

    for m in sorted_matches:
        result_parts.append(text[last_end:m.start])
        result_parts.append(prefix)
        result_parts.append(m.text)
        result_parts.append(suffix)
        last_end = m.end

    result_parts.append(text[last_end:])
    return "".join(result_parts)


__all__ = [
    "TextMatch",
    "TextSearchOptions",
    "TextFinder",
    "highlight_matches",
]
