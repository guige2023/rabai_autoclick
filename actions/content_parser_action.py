"""Content parser action for extracting structured data from text.

This module provides content parsing with regex patterns,
named entity recognition, and structured extraction.

Example:
    >>> action = ContentParserAction()
    >>> result = action.execute(text="Contact: john@example.com", patterns=["email"])
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ParsedEntity:
    """Represents a parsed entity."""
    type: str
    value: str
    start: int
    end: int
    confidence: float = 1.0


@dataclass
class ParsePattern:
    """Pattern for content extraction."""
    name: str
    pattern: str
    flags: int = 0
    group: Optional[int] = None


class ContentParserAction:
    """Content parsing action with pattern matching.

    Extracts structured data from unstructured text using
    regex patterns and named entity patterns.

    Example:
        >>> action = ContentParserAction()
        >>> result = action.execute(
        ...     text="Call 555-123-4567 or email support@example.com",
        ...     patterns=["phone", "email"]
        ... )
    """

    # Built-in patterns for common entities
    BUILT_IN_PATTERNS: dict[str, str] = {
        "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "phone": r"(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}",
        "url": r"https?://[^\s<>\"]+",
        "ip_address": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "date": r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        "time": r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?\b",
        "price": r"\$\d+(?:,\d{3})*(?:\.\d{2})?",
        "hashtag": r"#[a-zA-Z0-9_]+",
        "mention": r"@[a-zA-Z0-9_]+",
        "zip_code": r"\b\d{5}(?:-\d{4})?\b",
        "credit_card": r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
        "ssn": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
    }

    def __init__(self) -> None:
        """Initialize content parser."""
        self._custom_patterns: dict[str, ParsePattern] = {}

    def execute(
        self,
        text: str,
        patterns: Optional[list[str]] = None,
        custom_patterns: Optional[list[dict[str, str]]] = None,
        extract_all: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute content parsing.

        Args:
            text: Text content to parse.
            patterns: List of built-in pattern names to use.
            custom_patterns: List of custom pattern definitions.
            extract_all: Whether to extract all matches or just first.
            **kwargs: Additional parameters.

        Returns:
            Parsing result dictionary.

        Raises:
            ValueError: If text is empty.
        """
        if not text:
            raise ValueError("Text content is required")

        result: dict[str, Any] = {"success": True, "text_length": len(text)}

        # Register custom patterns
        if custom_patterns:
            for cp in custom_patterns:
                name = cp.get("name", f"custom_{len(self._custom_patterns)}")
                pattern = cp.get("pattern", "")
                flags = int(cp.get("flags", 0))
                group = cp.get("group")
                self._custom_patterns[name] = ParsePattern(
                    name=name,
                    pattern=pattern,
                    flags=flags,
                    group=int(group) if group else None,
                )

        # Combine patterns
        all_patterns: dict[str, ParsePattern] = {}
        all_patterns.update(self.BUILT_IN_PATTERNS)
        all_patterns.update({k: ParsePattern(name=k, pattern=v) for k, v in self._custom_patterns.items()})

        # Parse with requested patterns
        entities: list[ParsedEntity] = []

        for pattern_name in patterns or []:
            if pattern_name not in all_patterns:
                continue

            p = all_patterns[pattern_name]
            compiled = re.compile(p.pattern, p.flags)

            if extract_all:
                for match in compiled.finditer(text):
                    value = match.group(p.group) if p.group is not None else match.group(0)
                    entities.append(ParsedEntity(
                        type=pattern_name,
                        value=value,
                        start=match.start(),
                        end=match.end(),
                    ))
            else:
                match = compiled.search(text)
                if match:
                    value = match.group(p.group) if p.group is not None else match.group(0)
                    entities.append(ParsedEntity(
                        type=pattern_name,
                        value=value,
                        start=match.start(),
                        end=match.end(),
                    ))

        # Organize results by type
        by_type: dict[str, list[str]] = {}
        for entity in entities:
            if entity.type not in by_type:
                by_type[entity.type] = []
            by_type[entity.type].append(entity.value)

        result["entities"] = entities
        result["by_type"] = by_type
        result["total_count"] = len(entities)

        return result

    def add_pattern(self, name: str, pattern: str, flags: int = 0) -> None:
        """Add custom pattern.

        Args:
            name: Pattern name.
            pattern: Regex pattern string.
            flags: Regex flags.
        """
        self._custom_patterns[name] = ParsePattern(name=name, pattern=pattern, flags=flags)

    def extract_between(
        self,
        text: str,
        start_pattern: str,
        end_pattern: str,
        include_markers: bool = False,
    ) -> list[str]:
        """Extract text between two patterns.

        Args:
            text: Text to search in.
            start_pattern: Start pattern.
            end_pattern: End pattern.
            include_markers: Whether to include markers in result.

        Returns:
            List of extracted strings.
        """
        results: list[str] = []
        pattern = f"{start_pattern}(.*?){end_pattern}"

        for match in re.finditer(pattern, text, re.DOTALL):
            if include_markers:
                results.append(match.group(0))
            else:
                results.append(match.group(1))

        return results

    def split_by_pattern(
        self,
        text: str,
        pattern: str,
        max_split: int = 0,
    ) -> list[str]:
        """Split text by pattern.

        Args:
            text: Text to split.
            pattern: Split pattern.
            max_split: Maximum number of splits (0 = unlimited).

        Returns:
            List of text segments.
        """
        if max_split > 0:
            parts = re.split(pattern, text, maxsplit=max_split)
        else:
            parts = re.split(pattern, text)
        return [p for p in parts if p.strip()]

    def replace_patterns(
        self,
        text: str,
        replacements: dict[str, str],
    ) -> str:
        """Replace patterns in text.

        Args:
            text: Text to process.
            replacements: Dict of pattern -> replacement.

        Returns:
            Processed text.
        """
        result = text
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result)
        return result

    def extract_sentences(self, text: str) -> list[str]:
        """Split text into sentences.

        Args:
            text: Text to split.

        Returns:
            List of sentences.
        """
        # Simple sentence splitting
        sentence_pattern = r"[.!?]+[\s\n]+"
        sentences = re.split(sentence_pattern, text)
        return [s.strip() for s in sentences if s.strip()]

    def extract_words(self, text: str, min_length: int = 1) -> list[str]:
        """Extract words from text.

        Args:
            text: Text to process.
            min_length: Minimum word length.

        Returns:
            List of words.
        """
        words = re.findall(r"\b[a-zA-Z]+\b", text)
        return [w for w in words if len(w) >= min_length]

    def extract_numbers(self, text: str) -> list[float]:
        """Extract numbers from text.

        Args:
            text: Text to process.

        Returns:
            List of numbers as floats.
        """
        number_pattern = r"-?\d+(?:\.\d+)?"
        matches = re.findall(number_pattern, text)
        return [float(m) for m in matches]
