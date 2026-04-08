"""
Tag Parser Utilities

Provides utilities for parsing and matching tags
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any


class TagParser:
    """
    Parses and matches element tags.
    
    Handles tag extraction and comparison
    for element identification.
    """

    def __init__(self) -> None:
        self._tag_aliases: dict[str, str] = {}

    def register_alias(self, alias: str, canonical: str) -> None:
        """Register a tag alias."""
        self._alias[alias] = canonical

    def parse_tags(self, tag_string: str) -> list[str]:
        """
        Parse space-separated tags.
        
        Args:
            tag_string: Space-separated tag string.
            
        Returns:
            List of individual tags.
        """
        return [t.strip() for t in tag_string.split() if t.strip()]

    def matches_any(
        self,
        element_tags: list[str],
        query_tags: list[str],
    ) -> bool:
        """Check if element matches any query tag."""
        for qtag in query_tags:
            canonical = self._tag_aliases.get(qtag, qtag)
            for etag in element_tags:
                etag_canonical = self._tag_aliases.get(etag, etag)
                if etag_canonical == canonical:
                    return True
        return False

    def matches_all(
        self,
        element_tags: list[str],
        query_tags: list[str],
    ) -> bool:
        """Check if element matches all query tags."""
        for qtag in query_tags:
            canonical = self._tag_aliases.get(qtag, qtag)
            found = False
            for etag in element_tags:
                etag_canonical = self._tag_aliases.get(etag, etag)
                if etag_canonical == canonical:
                    found = True
                    break
            if not found:
                return False
        return True
