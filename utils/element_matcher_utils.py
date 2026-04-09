"""
Element matching and similarity utilities for UI automation.

Provides element matching based on various attributes,
similarity scoring, and element deduplication.

Author: Auto-generated
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass
class ElementSignature:
    """
    A unique signature for an UI element.
    
    Used for element deduplication and matching.
    """
    tag: str
    text: str
    attrs: tuple[tuple[str, str], ...]
    position_hash: str
    
    @classmethod
    def from_element(
        cls,
        tag: str,
        text: str,
        attributes: dict[str, Any],
        x: float = 0,
        y: float = 0,
    ) -> ElementSignature:
        """Create signature from element properties."""
        attr_items = tuple(
            (k, str(v)) for k, v in sorted(attributes.items())
            if v is not None
        )
        
        pos_hash = hashlib.md5(f"{x},{y}".encode()).hexdigest()[:8]
        
        return cls(
            tag=tag.lower(),
            text=text or "",
            attrs=attr_items,
            position_hash=pos_hash,
        )
    
    def matches(self, other: ElementSignature) -> bool:
        """Check if this signature matches another."""
        return (
            self.tag == other.tag
            and self.text == other.text
            and self.attrs == other.attrs
        )
    
    def similarity(self, other: ElementSignature) -> float:
        """Calculate similarity score [0.0, 1.0]."""
        score = 0.0
        total = 4.0
        
        if self.tag == other.tag:
            score += 1.0
        if self.text == other.text:
            score += 1.0
        
        # Attribute similarity
        common_attrs = set(self.attrs) & set(other.attrs)
        total_attrs = set(self.attrs) | set(other.attrs)
        if total_attrs:
            score += len(common_attrs) / len(total_attrs)
        
        # Position hash match
        if self.position_hash == other.position_hash:
            score += 1.0
        
        return score / total


@dataclass
class MatchCriteria:
    """Criteria for element matching."""
    tag: str | None = None
    text: str | None = None
    text_contains: str | None = None
    text_starts_with: str | None = None
    attributes: dict[str, Any] | None = None
    attribute_contains: dict[str, str] | None = None
    visible: bool | None = None
    enabled: bool | None = None
    min_width: float | None = None
    min_height: float | None = None
    max_width: float | None = None
    max_height: float | None = None
    position_x: float | None = None
    position_y: float | None = None
    index: int | None = None
    

class ElementMatcher:
    """
    Matches elements against criteria.
    
    Example:
        matcher = ElementMatcher()
        if matcher.matches(element, MatchCriteria(tag="button", text="Submit")):
            print("Found submit button")
    """
    
    def __init__(self, strict: bool = False):
        self._strict = strict
    
    def matches(
        self,
        element: dict[str, Any],
        criteria: MatchCriteria,
    ) -> bool:
        """
        Check if element matches criteria.
        
        Args:
            element: Element dictionary with keys like 'tag', 'text', 'attributes', etc.
            criteria: Match criteria
            
        Returns:
            True if all criteria match
        """
        # Tag match
        if criteria.tag is not None:
            if element.get("tag", "").lower() != criteria.tag.lower():
                return False
        
        # Text exact match
        if criteria.text is not None:
            if element.get("text", "") != criteria.text:
                return False
        
        # Text contains
        if criteria.text_contains is not None:
            text = element.get("text", "")
            if criteria.text_contains not in text:
                return False
        
        # Text starts with
        if criteria.text_starts_with is not None:
            text = element.get("text", "")
            if not text.startswith(criteria.text_starts_with):
                return False
        
        # Attribute exact match
        if criteria.attributes:
            elem_attrs = element.get("attributes", {})
            for key, value in criteria.attributes.items():
                if elem_attrs.get(key) != value:
                    return False
        
        # Attribute contains
        if criteria.attribute_contains:
            elem_attrs = element.get("attributes", {})
            for key, substr in criteria.attribute_contains.items():
                value = str(elem_attrs.get(key, ""))
                if substr not in value:
                    return False
        
        # Visible check
        if criteria.visible is not None:
            if element.get("visible", True) != criteria.visible:
                return False
        
        # Enabled check
        if criteria.enabled is not None:
            if element.get("enabled", True) != criteria.enabled:
                return False
        
        # Size checks
        bounds = element.get("bounds", {})
        width = bounds.get("width", 0)
        height = bounds.get("height", 0)
        
        if criteria.min_width is not None and width < criteria.min_width:
            return False
        if criteria.min_height is not None and height < criteria.min_height:
            return False
        if criteria.max_width is not None and width > criteria.max_width:
            return False
        if criteria.max_height is not None and height > criteria.max_height:
            return False
        
        # Position checks
        if criteria.position_x is not None:
            if abs(bounds.get("x", 0) - criteria.position_x) > 1:
                return False
        if criteria.position_y is not None:
            if abs(bounds.get("y", 0) - criteria.position_y) > 1:
                return False
        
        return True
    
    def find_matches(
        self,
        elements: Sequence[dict[str, Any]],
        criteria: MatchCriteria,
    ) -> list[dict[str, Any]]:
        """
        Find all elements matching criteria.
        
        Args:
            elements: List of element dictionaries
            criteria: Match criteria
            
        Returns:
            List of matching elements
        """
        results = []
        for elem in elements:
            if self.matches(elem, criteria):
                results.append(elem)
        return results
    
    def find_first(
        self,
        elements: Sequence[dict[str, Any]],
        criteria: MatchCriteria,
    ) -> dict[str, Any] | None:
        """Find first element matching criteria."""
        for elem in elements:
            if self.matches(elem, criteria):
                return elem
        return None
    
    def index_of(
        self,
        elements: Sequence[dict[str, Any]],
        criteria: MatchCriteria,
    ) -> int:
        """Find index of first element matching criteria, or -1."""
        for i, elem in enumerate(elements):
            if self.matches(elem, criteria):
                return i
        return -1


class ElementDeduplicator:
    """
    Deduplicates elements based on their signatures.
    
    Example:
        dedup = ElementDeduplicator()
        unique_elements = dedup.deduplicate(elements)
    """
    
    def __init__(self, similarity_threshold: float = 0.9):
        self._threshold = similarity_threshold
    
    def deduplicate(
        self,
        elements: Sequence[dict[str, Any]],
        key_func: str = "auto",
    ) -> list[dict[str, Any]]:
        """
        Remove duplicate elements.
        
        Args:
            elements: List of element dictionaries
            key_func: Key function type ('auto', 'signature', 'position')
            
        Returns:
            List of unique elements
        """
        if key_func == "signature":
            return self._dedupe_by_signature(elements)
        elif key_func == "position":
            return self._dedupe_by_position(elements)
        else:
            return self._dedupe_auto(elements)
    
    def _dedupe_by_signature(
        self, elements: Sequence[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        seen_signatures: set[str] = set()
        unique = []
        
        for elem in elements:
            sig = self._create_signature(elem)
            if sig not in seen_signatures:
                seen_signatures.add(sig)
                unique.append(elem)
        
        return unique
    
    def _dedupe_by_position(
        self, elements: Sequence[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        seen_positions: set[tuple[float, float]] = set()
        unique = []
        
        for elem in elements:
            bounds = elem.get("bounds", {})
            pos = (bounds.get("x", 0), bounds.get("y", 0))
            
            if pos not in seen_positions:
                seen_positions.add(pos)
                unique.append(elem)
        
        return unique
    
    def _dedupe_auto(
        self, elements: Sequence[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        unique = []
        
        for elem in elements:
            is_duplicate = False
            
            for existing in unique:
                sig1 = self._create_signature(elem)
                sig2 = self._create_signature(existing)
                
                if sig1 == sig2:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique.append(elem)
        
        return unique
    
    def _create_signature(self, elem: dict[str, Any]) -> str:
        tag = elem.get("tag", "").lower()
        text = elem.get("text", "") or ""
        attrs = elem.get("attributes", {})
        
        sig_str = f"{tag}|{text}|{sorted(attrs.items())}"
        return hashlib.md5(sig_str.encode()).hexdigest()


def find_elements_by_role(
    elements: Sequence[dict[str, Any]],
    role: str,
) -> list[dict[str, Any]]:
    """Find elements by accessibility role."""
    return [
        e for e in elements
        if e.get("role", "").lower() == role.lower()
    ]


def find_elements_by_label(
    elements: Sequence[dict[str, Any]],
    label: str,
    contains: bool = True,
) -> list[dict[str, Any]]:
    """Find elements by label text."""
    results = []
    for e in elements:
        elem_label = e.get("label", "") or e.get("text", "")
        if contains:
            if label in elem_label:
                results.append(e)
        else:
            if label == elem_label:
                results.append(e)
    return results


def find_clickable_elements(
    elements: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find all clickable elements."""
    clickable_roles = {"button", "link", "menuitem", "checkbox", "radio"}
    
    return [
        e for e in elements
        if e.get("clickable", False)
        or e.get("role", "").lower() in clickable_roles
        or "button" in e.get("tag", "").lower()
    ]
