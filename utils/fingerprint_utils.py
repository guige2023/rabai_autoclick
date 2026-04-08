"""
Fingerprint Utilities

Provides utilities for generating element and UI
fingerprints in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import hashlib
import json


class ElementFingerprint:
    """
    Generates fingerprints for UI elements.
    
    Creates unique identifiers based on element
    properties for tracking and caching.
    """

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}

    def generate(self, element: dict[str, Any]) -> str:
        """
        Generate a fingerprint for an element.
        
        Args:
            element: Element dictionary.
            
        Returns:
            Unique fingerprint string.
        """
        props = self._extract_properties(element)
        normalized = json.dumps(props, sort_keys=True)
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def _extract_properties(self, element: dict[str, Any]) -> dict[str, Any]:
        """Extract relevant properties for fingerprinting."""
        return {
            "tag": element.get("tag"),
            "id": element.get("id"),
            "class": element.get("class"),
            "type": element.get("type"),
            "name": element.get("name"),
            "role": element.get("role"),
        }

    def get_cached(self, element: dict[str, Any]) -> str | None:
        """Get cached fingerprint for element."""
        fp = self.generate(element)
        return self._cache.get(fp)

    def cache_fingerprint(
        self,
        element: dict[str, Any],
        value: str,
    ) -> None:
        """Cache a fingerprint-value pair."""
        fp = self.generate(element)
        self._cache[fp] = value

    def clear_cache(self) -> None:
        """Clear the fingerprint cache."""
        self._cache.clear()


def generate_ui_fingerprint(
    elements: list[dict[str, Any]],
) -> str:
    """
    Generate a fingerprint for entire UI state.
    
    Args:
        elements: List of element dictionaries.
        
    Returns:
        UI state fingerprint.
    """
    fingerprints = []
    for elem in elements:
        fp_gen = ElementFingerprint()
        fingerprints.append(fp_gen.generate(elem))
    combined = "".join(sorted(fingerprints))
    return hashlib.sha256(combined.encode()).hexdigest()[:24]
