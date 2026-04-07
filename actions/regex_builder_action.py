"""Regex builder action for constructing and testing regex patterns.

This module provides regex pattern building with common
pattern templates and testing capabilities.

Example:
    >>> action = RegexBuilderAction()
    >>> result = action.execute(pattern="email", text="test@example.com")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class RegexPattern:
    """Represents a regex pattern."""
    name: str
    pattern: str
    description: str
    flags: int = 0


@dataclass
class MatchResult:
    """Regex match result."""
    matched: bool
    groups: list[str]
    group_dict: dict[str, str]
    span: tuple[int, int]


class RegexBuilderAction:
    """Regex pattern builder and tester action.

    Builds and tests regex patterns with common templates
    and named group support.

    Example:
        >>> action = RegexBuilderAction()
        >>> result = action.execute(
        ...     operation="match",
        ...     pattern=r"\\d{4}-\\d{2}-\\d{2}",
        ...     text="2024-01-15"
        ... )
    """

    # Common regex patterns
    TEMPLATES: dict[str, str] = {
        "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "phone_us": r"(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}",
        "phone_intl": r"\+?[0-9]{1,4}[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,9}",
        "url": r"https?://[^\s<>\"]+",
        "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "ipv6": r"(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}",
        "date_iso": r"\d{4}-\d{2}-\d{2}",
        "date_us": r"\d{1,2}/\d{1,2}/\d{2,4}",
        "time_24h": r"\b\d{2}:\d{2}(?::\d{2})?\b",
        "time_12h": r"\b\d{1,2}:\d{2}\s*(?:AM|PM)\b",
        "zip_us": r"\b\d{5}(?:-\d{4})?\b",
        "credit_card": r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
        "ssn": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        "hex_color": r"#(?:[0-9a-fA-F]{3}){1,2}\b",
        "uuid": r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "html_tag": r"<([a-zA-Z][a-zA-Z0-9]*)\b[^>]*>.*?</\1>",
        "hashtag": r"#[a-zA-Z0-9_]+",
        "mention": r"@[a-zA-Z0-9_]+",
        "price": r"\$\d+(?:,\d{3})*(?:\.\d{2})?",
        "percentage": r"\d+(?:\.\d+)?%",
    }

    def __init__(self) -> None:
        """Initialize regex builder."""
        self._last_matches: list[MatchResult] = []

    def execute(
        self,
        operation: str,
        pattern: Optional[str] = None,
        text: str = "",
        flags: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute regex operation.

        Args:
            operation: Operation (match, search, replace, split, findall).
            pattern: Regex pattern.
            text: Text to operate on.
            flags: List of flag names (ignorecase, multiline, etc.).
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If pattern is missing for matching operations.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        # Build flags
        flag_val = self._parse_flags(flags or [])

        # Handle template patterns
        if pattern and pattern in self.TEMPLATES:
            pattern = self.TEMPLATES[pattern]

        if op == "match":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._match(pattern, text, flag_val))

        elif op == "search":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._search(pattern, text, flag_val))

        elif op == "replace":
            if not pattern:
                raise ValueError("pattern required")
            replacement = kwargs.get("replacement", "")
            result.update(self._replace(pattern, text, replacement, flag_val))

        elif op == "split":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._split(pattern, text, flag_val))

        elif op == "findall":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._findall(pattern, text, flag_val))

        elif op == "finditer":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._finditer(pattern, text, flag_val))

        elif op == "test":
            if not pattern:
                raise ValueError("pattern required")
            result["matches"] = bool(re.search(pattern, text, flag_val))

        elif op == "validate":
            if not pattern:
                raise ValueError("pattern required")
            result.update(self._validate_pattern(pattern))

        elif op == "list_templates":
            result["templates"] = list(self.TEMPLATES.keys())

        elif op == "get_template":
            if not pattern:
                raise ValueError("template name required")
            result["pattern"] = self.TEMPLATES.get(pattern, "")

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _parse_flags(self, flags: list[str]) -> int:
        """Parse flag names to regex flags.

        Args:
            flags: List of flag names.

        Returns:
            Regex flags integer.
        """
        flag_map = {
            "ignorecase": re.IGNORECASE,
            "multiline": re.MULTILINE,
            "dotall": re.DOTALL,
            "verbose": re.VERBOSE,
            "unicode": re.UNICODE,
        }
        return sum(flag_map.get(f.lower(), 0) for f in flags)

    def _match(self, pattern: str, text: str, flags: int) -> dict[str, Any]:
        """Match pattern at start of string.

        Args:
            pattern: Regex pattern.
            text: Text to match.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            match = compiled.match(text)

            if match:
                return {
                    "matched": True,
                    "group": match.group(),
                    "groups": match.groups(),
                    "span": match.span(),
                }
            return {"matched": False}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _search(self, pattern: str, text: str, flags: int) -> dict[str, Any]:
        """Search for pattern in text.

        Args:
            pattern: Regex pattern.
            text: Text to search.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            match = compiled.search(text)

            if match:
                return {
                    "matched": True,
                    "group": match.group(),
                    "groups": match.groups(),
                    "span": match.span(),
                    "start": match.start(),
                    "end": match.end(),
                }
            return {"matched": False}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _replace(
        self,
        pattern: str,
        text: str,
        replacement: str,
        flags: int,
    ) -> dict[str, Any]:
        """Replace pattern matches.

        Args:
            pattern: Regex pattern.
            text: Text to search.
            replacement: Replacement string.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            result = compiled.sub(replacement, text)
            count = len(compiled.findall(text))
            return {"text": result, "count": count}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _split(self, pattern: str, text: str, flags: int) -> dict[str, Any]:
        """Split text by pattern.

        Args:
            pattern: Regex pattern.
            text: Text to split.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            parts = compiled.split(text)
            return {"parts": parts, "count": len(parts)}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _findall(self, pattern: str, text: str, flags: int) -> dict[str, Any]:
        """Find all pattern matches.

        Args:
            pattern: Regex pattern.
            text: Text to search.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            matches = compiled.findall(text)
            return {"matches": matches, "count": len(matches)}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _finditer(self, pattern: str, text: str, flags: int) -> dict[str, Any]:
        """Find all pattern matches with positions.

        Args:
            pattern: Regex pattern.
            text: Text to search.
            flags: Regex flags.

        Returns:
            Result dictionary.
        """
        try:
            compiled = re.compile(pattern, flags)
            matches = []
            for m in compiled.finditer(text):
                matches.append({
                    "match": m.group(),
                    "groups": m.groups(),
                    "span": m.span(),
                    "start": m.start(),
                    "end": m.end(),
                })
            return {"matches": matches, "count": len(matches)}

        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

    def _validate_pattern(self, pattern: str) -> dict[str, Any]:
        """Validate regex pattern.

        Args:
            pattern: Pattern to validate.

        Returns:
            Validation result.
        """
        try:
            re.compile(pattern)
            return {"valid": True}
        except re.error as e:
            return {"valid": False, "error": str(e)}
