"""Element matching utilities for finding UI elements by properties.

This module provides utilities for matching UI elements by various
properties including text content, attributes, bounds, and visual
characteristics, useful for element identification in automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple, Callable, Dict, Any


class MatchCriteria(Enum):
    """Criteria for element matching."""
    TEXT = auto()
    PARTIAL_TEXT = auto()
    REGEX = auto()
    ATTRIBUTE = auto()
    BOUNDS = auto()
    VISIBILITY = auto()
    ELEMENT_TYPE = auto()
    ALL = auto()


@dataclass
class ElementMatcher:
    """Matcher configuration for finding elements."""
    criteria: MatchCriteria
    value: Any
    case_sensitive: bool = True
    tolerance: int = 5  # For bounds matching


@dataclass
class MatchResult:
    """Result of element matching."""
    found: bool
    elements: List[Dict[str, Any]]
    match_count: int
    
    @property
    def first_match(self) -> Optional[Dict[str, Any]]:
        return self.elements[0] if self.elements else None


def match_by_text(
    text: str,
    elements: List[Dict[str, Any]],
    case_sensitive: bool = True,
) -> MatchResult:
    """Match elements by exact text.
    
    Args:
        text: Text to match.
        elements: List of element dictionaries.
        case_sensitive: Whether match is case-sensitive.
    
    Returns:
        MatchResult with matched elements.
    """
    search_text = text if case_sensitive else text.lower()
    
    matched = []
    for elem in elements:
        elem_text = elem.get("text", "") or elem.get("name", "")
        compare_text = elem_text if case_sensitive else elem_text.lower()
        
        if compare_text == search_text:
            matched.append(elem)
    
    return MatchResult(
        found=len(matched) > 0,
        elements=matched,
        match_count=len(matched),
    )


def match_by_partial_text(
    partial_text: str,
    elements: List[Dict[str, Any]],
    case_sensitive: bool = True,
) -> MatchResult:
    """Match elements containing partial text.
    
    Args:
        partial_text: Text substring to match.
        elements: List of element dictionaries.
        case_sensitive: Whether match is case-sensitive.
    
    Returns:
        MatchResult with matched elements.
    """
    search_text = partial_text if case_sensitive else partial_text.lower()
    
    matched = []
    for elem in elements:
        elem_text = elem.get("text", "") or elem.get("name", "")
        compare_text = elem_text if case_sensitive else elem_text.lower()
        
        if search_text in compare_text:
            matched.append(elem)
    
    return MatchResult(
        found=len(matched) > 0,
        elements=matched,
        match_count=len(matched),
    )


def match_by_bounds(
    x: int,
    y: int,
    width: int,
    height: int,
    elements: List[Dict[str, Any]],
    tolerance: int = 5,
) -> MatchResult:
    """Match elements by bounding box.
    
    Args:
        x: Expected X coordinate.
        y: Expected Y coordinate.
        width: Expected width.
        height: Expected height.
        elements: List of element dictionaries.
        tolerance: Pixel tolerance for matching.
    
    Returns:
        MatchResult with matched elements.
    """
    matched = []
    for elem in elements:
        bounds = elem.get("bounds", {})
        if not bounds:
            bounds = elem.get("rect", {})
        
        if not bounds:
            continue
        
        bx = bounds.get("x", 0) or bounds.get("left", 0)
        by = bounds.get("y", 0) or bounds.get("top", 0)
        bw = bounds.get("width", bounds.get("w", 0))
        bh = bounds.get("height", bounds.get("h", 0))
        
        if (abs(bx - x) <= tolerance and
            abs(by - y) <= tolerance and
            abs(bw - width) <= tolerance and
            abs(bh - height) <= tolerance):
            matched.append(elem)
    
    return MatchResult(
        found=len(matched) > 0,
        elements=matched,
        match_count=len(matched),
    )


def match_by_attribute(
    attribute: str,
    value: Any,
    elements: List[Dict[str, Any]],
) -> MatchResult:
    """Match elements by attribute value.
    
    Args:
        attribute: Attribute name to match.
        value: Expected attribute value.
        elements: List of element dictionaries.
    
    Returns:
        MatchResult with matched elements.
    """
    matched = []
    for elem in elements:
        elem_value = elem.get(attribute)
        if elem_value == value:
            matched.append(elem)
    
    return MatchResult(
        found=len(matched) > 0,
        elements=matched,
        match_count=len(matched),
    )


def match_by_type(
    element_type: str,
    elements: List[Dict[str, Any]],
    case_sensitive: bool = False,
) -> MatchResult:
    """Match elements by type/class.
    
    Args:
        element_type: Element type to match.
        elements: List of element dictionaries.
        case_sensitive: Whether match is case-sensitive.
    
    Returns:
        MatchResult with matched elements.
    """
    search_type = element_type if case_sensitive else element_type.lower()
    
    matched = []
    for elem in elements:
        elem_type = elem.get("type", "") or elem.get("role", "") or elem.get("className", "")
        compare_type = elem_type if case_sensitive else elem_type.lower()
        
        if search_type == compare_type:
            matched.append(elem)
    
    return MatchResult(
        found=len(matched) > 0,
        elements=matched,
        match_count=len(matched),
    )


def match_visible_elements(
    elements: List[Dict[str, Any]],
) -> MatchResult:
    """Filter to only visible elements.
    
    Args:
        elements: List of element dictionaries.
    
    Returns:
        MatchResult with visible elements.
    """
    visible = []
    for elem in elements:
        if elem.get("visible", True) and elem.get("enabled", True):
            visible.append(elem)
    
    return MatchResult(
        found=len(visible) > 0,
        elements=visible,
        match_count=len(visible),
    )


def match_interactive_elements(
    elements: List[Dict[str, Any]],
) -> MatchResult:
    """Filter to only interactive elements.
    
    Args:
        elements: List of element dictionaries.
    
    Returns:
        MatchResult with interactive elements.
    """
    interactive = []
    for elem in elements:
        is_visible = elem.get("visible", True)
        is_enabled = elem.get("enabled", True)
        is_clickable = elem.get("clickable", True)
        
        if is_visible and (is_enabled or is_clickable):
            interactive.append(elem)
    
    return MatchResult(
        found=len(interactive) > 0,
        elements=interactive,
        match_count=len(interactive),
    )


def find_element_at_point(
    x: int,
    y: int,
    elements: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Find the topmost element at a specific point.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        elements: List of element dictionaries.
    
    Returns:
        Element dictionary or None.
    """
    found_elements = []
    
    for elem in elements:
        bounds = elem.get("bounds", {}) or elem.get("rect", {})
        
        if not bounds:
            continue
        
        bx = bounds.get("x", 0) or bounds.get("left", 0)
        by = bounds.get("y", 0) or bounds.get("top", 0)
        bw = bounds.get("width", bounds.get("w", 0))
        bh = bounds.get("height", bounds.get("h", 0))
        
        if bx <= x < bx + bw and by <= y < by + bh:
            found_elements.append((bx, by, elem))
    
    if not found_elements:
        return None
    
    found_elements.sort(key=lambda e: (e[0], e[1]))
    
    return found_elements[-1][2]
