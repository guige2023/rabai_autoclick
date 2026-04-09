"""
Element anchor finder utilities.

Find anchor elements for reliable element location.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class Anchor:
    """An anchor element for finding other elements."""
    element_id: str
    anchor_type: str
    relationship: str
    offset_x: int = 0
    offset_y: int = 0


@dataclass
class AnchorResult:
    """Result of anchor-based element finding."""
    found: bool
    x: Optional[int] = None
    y: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    confidence: float = 0.0


class AnchorFinder:
    """Find elements using anchor relationships."""
    
    def __init__(self):
        self._anchor_registry: dict[str, Anchor] = {}
    
    def register_anchor(self, anchor: Anchor) -> None:
        """Register an anchor element."""
        self._anchor_registry[anchor.element_id] = anchor
    
    def unregister_anchor(self, element_id: str) -> None:
        """Unregister an anchor element."""
        self._anchor_registry.pop(element_id, None)
    
    def get_anchor(self, element_id: str) -> Optional[Anchor]:
        """Get an anchor by element ID."""
        return self._anchor_registry.get(element_id)
    
    def find_by_anchor(
        self,
        anchor_id: str,
        elements: dict[str, Any]
    ) -> AnchorResult:
        """Find element using anchor relationship."""
        anchor = self._anchor_registry.get(anchor_id)
        
        if not anchor:
            return AnchorResult(found=False)
        
        if anchor.relationship == "above":
            return self._find_above(anchor, elements)
        elif anchor.relationship == "below":
            return self._find_below(anchor, elements)
        elif anchor.relationship == "left_of":
            return self._find_left_of(anchor, elements)
        elif anchor.relationship == "right_of":
            return self._find_right_of(anchor, elements)
        elif anchor.relationship == "near":
            return self._find_near(anchor, elements)
        
        return AnchorResult(found=False)
    
    def _find_above(self, anchor: Anchor, elements: dict[str, Any]) -> AnchorResult:
        """Find element above anchor."""
        anchor_elem = elements.get(anchor.element_id)
        if not anchor_elem:
            return AnchorResult(found=False)
        
        best_match = None
        best_y = float('inf')
        
        for elem_id, elem in elements.items():
            if elem_id == anchor.element_id:
                continue
            
            elem_y = elem.get("y", float('inf'))
            if elem_y < best_y and elem_y < anchor_elem.get("y", 0):
                best_y = elem_y
                best_match = elem
        
        if best_match:
            return AnchorResult(
                found=True,
                x=best_match.get("x"),
                y=best_match.get("y"),
                width=best_match.get("width"),
                height=best_match.get("height"),
                confidence=0.8
            )
        
        return AnchorResult(found=False)
    
    def _find_below(self, anchor: Anchor, elements: dict[str, Any]) -> AnchorResult:
        """Find element below anchor."""
        anchor_elem = elements.get(anchor.element_id)
        if not anchor_elem:
            return AnchorResult(found=False)
        
        best_match = None
        best_y = float('-inf')
        
        for elem_id, elem in elements.items():
            if elem_id == anchor.element_id:
                continue
            
            elem_y = elem.get("y", 0)
            if elem_y > best_y and elem_y > anchor_elem.get("y", 0):
                best_y = elem_y
                best_match = elem
        
        if best_match:
            return AnchorResult(
                found=True,
                x=best_match.get("x"),
                y=best_match.get("y"),
                width=best_match.get("width"),
                height=best_match.get("height"),
                confidence=0.8
            )
        
        return AnchorResult(found=False)
    
    def _find_left_of(self, anchor: Anchor, elements: dict[str, Any]) -> AnchorResult:
        """Find element to the left of anchor."""
        anchor_elem = elements.get(anchor.element_id)
        if not anchor_elem:
            return AnchorResult(found=False)
        
        best_match = None
        best_x = float('inf')
        
        for elem_id, elem in elements.items():
            if elem_id == anchor.element_id:
                continue
            
            elem_x = elem.get("x", float('inf'))
            if elem_x < best_x and elem_x < anchor_elem.get("x", 0):
                best_x = elem_x
                best_match = elem
        
        if best_match:
            return AnchorResult(
                found=True,
                x=best_match.get("x"),
                y=best_match.get("y"),
                width=best_match.get("width"),
                height=best_match.get("height"),
                confidence=0.8
            )
        
        return AnchorResult(found=False)
    
    def _find_right_of(self, anchor: Anchor, elements: dict[str, Any]) -> AnchorResult:
        """Find element to the right of anchor."""
        anchor_elem = elements.get(anchor.element_id)
        if not anchor_elem:
            return AnchorResult(found=False)
        
        best_match = None
        best_x = float('-inf')
        
        for elem_id, elem in elements.items():
            if elem_id == anchor.element_id:
                continue
            
            elem_x = elem.get("x", 0)
            if elem_x > best_x and elem_x > anchor_elem.get("x", 0):
                best_x = elem_x
                best_match = elem
        
        if best_match:
            return AnchorResult(
                found=True,
                x=best_match.get("x"),
                y=best_match.get("y"),
                width=best_match.get("width"),
                height=best_match.get("height"),
                confidence=0.8
            )
        
        return AnchorResult(found=False)
    
    def _find_near(self, anchor: Anchor, elements: dict[str, Any]) -> AnchorResult:
        """Find element near anchor."""
        anchor_elem = elements.get(anchor.element_id)
        if not anchor_elem:
            return AnchorResult(found=False)
        
        best_match = None
        best_distance = float('inf')
        
        anchor_x = anchor_elem.get("x", 0)
        anchor_y = anchor_elem.get("y", 0)
        
        for elem_id, elem in elements.items():
            if elem_id == anchor.element_id:
                continue
            
            elem_x = elem.get("x", 0)
            elem_y = elem.get("y", 0)
            
            distance = ((elem_x - anchor_x) ** 2 + (elem_y - anchor_y) ** 2) ** 0.5
            
            if distance < best_distance:
                best_distance = distance
                best_match = elem
        
        if best_match and best_distance < 100:
            return AnchorResult(
                found=True,
                x=best_match.get("x"),
                y=best_match.get("y"),
                width=best_match.get("width"),
                height=best_match.get("height"),
                confidence=max(0.5, 1.0 - best_distance / 100)
            )
        
        return AnchorResult(found=False)
