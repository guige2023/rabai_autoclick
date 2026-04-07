"""Text processor action for text manipulation.

This module provides text processing capabilities including
cleaning, formatting, searching, and replacing text.

Example:
    >>> action = TextProcessorAction()
    >>> result = action.execute(operation="clean", text="  Hello   World  ")
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class TextStats:
    """Text statistics."""
    length: int
    word_count: int
    line_count: int
    char_count: dict[str, int]


class TextProcessorAction:
    """Text processing and manipulation action.

    Provides text cleaning, formatting, search, and
    replace operations with regex support.

    Example:
        >>> action = TextProcessorAction()
        >>> result = action.execute(
        ...     operation="replace",
        ...     text="Hello World",
        ...     find="World",
        ...     replace="Python"
        ... )
    """

    def __init__(self) -> None:
        """Initialize text processor."""
        self._last_result: Optional[str] = None

    def execute(
        self,
        operation: str,
        text: str,
        find: Optional[str] = None,
        replace: Optional[str] = None,
        pattern: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute text operation.

        Args:
            operation: Operation (clean, replace, split, join, etc.).
            text: Input text.
            find: String to find.
            replace: Replacement string.
            pattern: Regex pattern.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "clean":
            result["text"] = self._clean_text(text)
        elif op == "replace":
            if find is None:
                raise ValueError("find required for 'replace'")
            result["text"] = text.replace(find, replace or "")
        elif op == "replace_regex":
            if not pattern:
                raise ValueError("pattern required for 'replace_regex'")
            result["text"] = re.sub(pattern, replace or "", text)
        elif op == "split":
            delimiter = kwargs.get("delimiter", "\n")
            result["parts"] = text.split(delimiter)
            result["count"] = len(result["parts"])
        elif op == "join":
            parts = kwargs.get("parts", [])
            separator = kwargs.get("separator", "")
            result["text"] = separator.join(parts)
        elif op == "trim":
            result["text"] = text.strip()
        elif op == "lower":
            result["text"] = text.lower()
        elif op == "upper":
            result["text"] = text.upper()
        elif op == "title":
            result["text"] = text.title()
        elif op == "capitalize":
            result["text"] = text.capitalize()
        elif op == "reverse":
            result["text"] = text[::-1]
        elif op == "word_count":
            result.update(self._count_words(text))
        elif op == "stats":
            result.update(self._get_stats(text))
        elif op == "contains":
            result["contains"] = find in text if find else False
        elif op == "starts_with":
            result["starts_with"] = text.startswith(find) if find else False
        elif op == "ends_with":
            result["ends_with"] = text.endswith(find) if find else False
        elif op == "extract_emails":
            result["emails"] = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
        elif op == "extract_urls":
            result["urls"] = re.findall(r"https?://[^\s<>\"]+", text)
        elif op == "extract_numbers":
            result["numbers"] = re.findall(r"-?\d+(?:\.\d+)?", text)
        elif op == "extract_pattern":
            if not pattern:
                raise ValueError("pattern required")
            result["matches"] = re.findall(pattern, text)
        elif op == "normalize_whitespace":
            result["text"] = re.sub(r"\s+", " ", text).strip()
        elif op == "remove_digits":
            result["text"] = re.sub(r"\d+", "", text)
        elif op == "remove_punctuation":
            result["text"] = re.sub(r"[^\w\s]", "", text)
        elif op == "slugify":
            result["text"] = self._slugify(text)
        elif op == "truncate":
            length = kwargs.get("length", 100)
            suffix = kwargs.get("suffix", "...")
            result["text"] = self._truncate(text, length, suffix)
        elif op == "wrap":
            width = kwargs.get("width", 80)
            result["text"] = self._wrap_text(text, width)
        elif op == "lines":
            result["lines"] = text.splitlines()
            result["count"] = len(result["lines"])
        elif op == "unique_lines":
            result["text"] = "\n".join(dict.fromkeys(text.splitlines()))
        elif op == "sort_lines":
            result["text"] = "\n".join(sorted(text.splitlines()))
        elif op == "reverse_lines":
            result["text"] = "\n".join(reversed(text.splitlines()))
        elif op == "highlight":
            if not find:
                raise ValueError("find required for 'highlight'")
            result["text"] = text.replace(find, f"**{find}**")
        else:
            raise ValueError(f"Unknown operation: {operation}")

        if "text" in result:
            self._last_result = result["text"]

        return result

    def _clean_text(self, text: str) -> str:
        """Clean text by removing extra whitespace and control chars.

        Args:
            text: Text to clean.

        Returns:
            Cleaned text.
        """
        # Remove control characters
        text = "".join(char for char in text if unicodedata.category(char)[0] != "C" or char in "\n\t")
        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _count_words(self, text: str) -> dict[str, Any]:
        """Count words in text.

        Args:
            text: Text to count.

        Returns:
            Count result.
        """
        words = text.split()
        return {
            "word_count": len(words),
            "words": words[:100],  # First 100 words
        }

    def _get_stats(self, text: str) -> dict[str, Any]:
        """Get text statistics.

        Args:
            text: Text to analyze.

        Returns:
            Statistics dictionary.
        """
        char_counts: dict[str, int] = {}
        for char in text:
            char_counts[char] = char_counts.get(char, 0) + 1

        return {
            "length": len(text),
            "word_count": len(text.split()),
            "line_count": len(text.splitlines()),
            "char_count": char_counts,
            "unique_chars": len(char_counts),
        }

    def _slugify(self, text: str) -> str:
        """Convert text to URL-safe slug.

        Args:
            text: Text to slugify.

        Returns:
            Slug string.
        """
        # Normalize unicode
        text = unicodedata.normalize("NFKD", text)
        # Remove non-alphanumeric
        text = re.sub(r"[^\w\s-]", "", text)
        # Replace spaces with hyphens
        text = re.sub(r"[-\s]+", "-", text)
        return text.lower().strip("-_")

    def _truncate(self, text: str, length: int, suffix: str) -> str:
        """Truncate text to length.

        Args:
            text: Text to truncate.
            length: Maximum length.
            suffix: Suffix for truncated text.

        Returns:
            Truncated text.
        """
        if len(text) <= length:
            return text
        return text[:length - len(suffix)].rstrip() + suffix

    def _wrap_text(self, text: str, width: int) -> str:
        """Wrap text to specified width.

        Args:
            text: Text to wrap.
            width: Line width.

        Returns:
            Wrapped text.
        """
        import textwrap
        return textwrap.fill(text, width=width)

    def apply_function(
        self,
        func: Callable[[str], str],
    ) -> dict[str, Any]:
        """Apply function to last result.

        Args:
            func: Function to apply.

        Returns:
            Result dictionary.
        """
        if self._last_result is None:
            return {"success": False, "error": "No previous result"}

        try:
            result = func(self._last_result)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}
