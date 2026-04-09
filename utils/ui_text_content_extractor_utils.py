"""
UI Text Content Extractor Utilities

Extract and normalize text content from UI elements for
text comparison, search indexing, and content analysis.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Callable


@dataclass
class TextExtractionResult:
    """Result of text content extraction from a UI element."""
    raw_text: str
    normalized_text: str
    word_count: int
    char_count: int
    has_numbers: bool
    has_urls: bool
    language_hint: str  # 'latin', 'cjk', 'mixed', 'unknown'


def normalize_text(text: str, remove_extra_whitespace: bool = True) -> str:
    """Normalize text for comparison and search."""
    if remove_extra_whitespace:
        text = re.sub(r"\s+", " ", text).strip()
    return text


def count_words(text: str) -> int:
    """Count words in text (handles multiple languages)."""
    # Split on whitespace for Latin, count characters for CJK
    latin_words = len(text.split())
    # CJK characters (Chinese, Japanese, Korean, etc.)
    cjk_chars = len(re.findall(r"[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]", text))
    return latin_words + cjk_chars


def detect_language_hint(text: str) -> str:
    """Detect a language hint based on character ranges."""
    latin = bool(re.search(r"[a-zA-Z]", text))
    cjk = bool(re.search(r"[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]", text))
    korean = bool(re.search(r"[\uac00-\ud7af]", text))

    if cjk and not latin:
        return "cjk"
    if korean and not cjk and not latin:
        return "korean"
    if latin and not cjk:
        return "latin"
    if latin and cjk:
        return "mixed"
    return "unknown"


def extract_urls(text: str) -> List[str]:
    """Extract URLs from text."""
    url_pattern = r"https?://[^\s<>\"]+"
    return re.findall(url_pattern, text)


def extract_numbers(text: str) -> List[str]:
    """Extract all numeric values from text."""
    return re.findall(r"-?\d+\.?\d*", text)


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to a maximum length."""
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


class UITextContentExtractor:
    """Extract and analyze text content from UI elements."""

    def __init__(
        self,
        default_max_length: int = 1000,
    ):
        self.default_max_length = default_max_length

    def extract(
        self,
        raw_text: str,
        max_length: Optional[int] = None,
    ) -> TextExtractionResult:
        """
        Extract and analyze text content.

        Args:
            raw_text: Raw text from a UI element.
            max_length: Maximum length to extract (default from config).

        Returns:
            TextExtractionResult with analysis.
        """
        max_len = max_length or self.default_max_length
        normalized = normalize_text(raw_text)
        if len(normalized) > max_len:
            normalized = truncate_text(normalized, max_len)

        return TextExtractionResult(
            raw_text=raw_text,
            normalized_text=normalized,
            word_count=count_words(normalized),
            char_count=len(normalized),
            has_numbers=bool(extract_numbers(normalized)),
            has_urls=bool(extract_urls(normalized)),
            language_hint=detect_language_hint(normalized),
        )

    def extract_all(
        self,
        elements: List,
        get_text: Callable[[any], str],
    ) -> List[TextExtractionResult]:
        """Extract text content from a list of UI elements."""
        return [self.extract(get_text(elem)) for elem in elements]

    def search_by_text(
        self,
        elements: List,
        query: str,
        get_text: Callable[[any], str],
        case_sensitive: bool = False,
    ) -> List:
        """Search elements whose text matches a query."""
        query_norm = query if case_sensitive else query.lower()
        results = []
        for elem in elements:
            text = get_text(elem)
            text_cmp = text if case_sensitive else text.lower()
            if query_norm in text_cmp:
                results.append(elem)
        return results
