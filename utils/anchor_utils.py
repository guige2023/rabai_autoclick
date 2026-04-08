"""
Anchor Utilities

Provides anchor-based element finding and navigation
for UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Anchor:
    """Represents an anchor point for element finding."""
    element_id: str | None = None
    text: str | None = None
    index: int = 0
    offset_x: int = 0
    offset_y: int = 0


class AnchorMatcher:
    """
    Matches elements based on anchor criteria.
    
    Anchors provide relative positioning hints for
    finding elements that may change position.
    """

    def __init__(self) -> None:
        self._anchors: dict[str, Anchor] = {}

    def register_anchor(self, name: str, anchor: Anchor) -> None:
        """Register a named anchor."""
        self._anchors[name] = anchor

    def get_anchor(self, name: str) -> Anchor | None:
        """Get a registered anchor by name."""
        return self._anchors.get(name)

    def find_with_anchor(
        self,
        elements: list[dict[str, Any]],
        anchor: Anchor,
    ) -> dict[str, Any] | None:
        """
        Find an element using anchor criteria.
        
        Args:
            elements: List of element dictionaries.
            anchor: Anchor criteria for matching.
            
        Returns:
            Matching element or None.
        """
        for elem in elements:
            if anchor.element_id and elem.get("id") == anchor.element_id:
                return elem
            if anchor.text and anchor.text in elem.get("text", ""):
                return elem
        return None

    def resolve_anchor_position(
        self,
        anchor: Anchor,
        elements: list[dict[str, Any]],
    ) -> tuple[int, int] | None:
        """
        Resolve anchor to screen coordinates.
        
        Returns:
            (x, y) tuple or None if anchor cannot be resolved.
        """
        element = self.find_with_anchor(elements, anchor)
        if not element:
            return None
        bounds = element.get("bounds", {})
        x = bounds.get("x", 0) + anchor.offset_x
        y = bounds.get("y", 0) + anchor.offset_y
        return (x, y)


def create_anchor(
    element_id: str | None = None,
    text: str | None = None,
    index: int = 0,
    offset_x: int = 0,
    offset_y: int = 0,
) -> Anchor:
    """
    Create a new anchor with the given parameters.
    
    Args:
        element_id: Element ID to match.
        text: Text content to match.
        index: Index for multiple matches.
        offset_x: X offset from matched position.
        offset_y: Y offset from matched position.
        
    Returns:
        New Anchor instance.
    """
    return Anchor(
        element_id=element_id,
        text=text,
        index=index,
        offset_x=offset_x,
        offset_y=offset_y,
    )
