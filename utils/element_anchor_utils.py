"""
Element anchor and reference utilities for UI automation.

This module provides utilities for creating stable references
to UI elements using anchors and relative positioning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any, Tuple
from enum import Enum, auto


class AnchorPosition(Enum):
    """Positions relative to an anchor."""
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    CENTER_LEFT = auto()
    CENTER = auto()
    CENTER_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()


@dataclass
class Anchor:
    """
    Anchor point for element positioning.

    Attributes:
        element_id: ID of the anchor element.
        position: Which part of the anchor to use.
        offset_x: X offset from anchor position.
        offset_y: Y offset from anchor position.
    """
    element_id: str
    position: AnchorPosition = AnchorPosition.CENTER
    offset_x: float = 0.0
    offset_y: float = 0.0


@dataclass
class ElementTarget:
    """
    Target position for an action.

    Created by resolving an anchor to screen coordinates.
    """
    x: float
    y: float
    anchor: Optional[Anchor] = None
    element_bounds: Optional[Tuple[float, float, float, float]] = None


@dataclass
class AnchorReference:
    """
    Reference to an element using an anchor.

    Provides stable element targeting even when the
    element moves or changes.
    """
    anchor: Anchor
    anchor_element_bounds: Tuple[float, float, float, float]
    target: ElementTarget

    @property
    def screen_position(self) -> Tuple[float, float]:
        """Get the screen position of the target."""
        return (self.target.x, self.target.y)


class AnchorResolver:
    """
    Resolves anchors to screen coordinates.

    Takes element bounds and calculates target positions
    based on anchor configurations.
    """

    def __init__(self) -> None:
        self._element_cache: Dict[str, Tuple[float, float, float, float]] = {}

    def cache_bounds(
        self,
        element_id: str,
        bounds: Tuple[float, float, float, float],
    ) -> None:
        """Cache element bounds for anchor resolution."""
        self._element_cache[element_id] = bounds

    def get_cached_bounds(self, element_id: str) -> Optional[Tuple[float, float, float, float]]:
        """Get cached bounds for an element."""
        return self._element_cache.get(element_id)

    def clear_cache(self, element_id: Optional[str] = None) -> None:
        """Clear cached bounds."""
        if element_id:
            self._element_cache.pop(element_id, None)
        else:
            self._element_cache.clear()

    def resolve(self, anchor: Anchor) -> Optional[ElementTarget]:
        """
        Resolve an anchor to screen coordinates.

        Returns ElementTarget with calculated position.
        """
        bounds = self._element_cache.get(anchor.element_id)
        if not bounds:
            return None

        x, y, width, height = bounds

        # Calculate anchor point
        anchor_x, anchor_y = self._get_anchor_point(
            x, y, width, height, anchor.position
        )

        # Apply offset
        target_x = anchor_x + anchor.offset_x
        target_y = anchor_y + anchor.offset_y

        return ElementTarget(
            x=target_x,
            y=target_y,
            anchor=anchor,
            element_bounds=bounds,
        )

    def _get_anchor_point(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        position: AnchorPosition,
    ) -> Tuple[float, float]:
        """Calculate point on rectangle based on anchor position."""
        if position == AnchorPosition.TOP_LEFT:
            return (x, y)
        elif position == AnchorPosition.TOP_CENTER:
            return (x + width / 2, y)
        elif position == AnchorPosition.TOP_RIGHT:
            return (x + width, y)
        elif position == AnchorPosition.CENTER_LEFT:
            return (x, y + height / 2)
        elif position == AnchorPosition.CENTER:
            return (x + width / 2, y + height / 2)
        elif position == AnchorPosition.CENTER_RIGHT:
            return (x + width, y + height / 2)
        elif position == AnchorPosition.BOTTOM_LEFT:
            return (x, y + height)
        elif position == AnchorPosition.BOTTOM_CENTER:
            return (x + width / 2, y + height)
        elif position == AnchorPosition.BOTTOM_RIGHT:
            return (x + width, y + height)

        return (x + width / 2, y + height / 2)


class AnchorBuilder:
    """
    Fluent builder for creating anchors.

    Provides a chainable API for anchor configuration.
    """

    def __init__(self, element_id: str) -> None:
        self._element_id = element_id
        self._position = AnchorPosition.CENTER
        self._offset_x: float = 0.0
        self._offset_y: float = 0.0

    @property
    def top_left(self) -> Anchor:
        """Anchor to top-left of element."""
        return Anchor(self._element_id, AnchorPosition.TOP_LEFT, self._offset_x, self._offset_y)

    @property
    def top_center(self) -> Anchor:
        """Anchor to top-center of element."""
        return Anchor(self._element_id, AnchorPosition.TOP_CENTER, self._offset_x, self._offset_y)

    @property
    def top_right(self) -> Anchor:
        """Anchor to top-right of element."""
        return Anchor(self._element_id, AnchorPosition.TOP_RIGHT, self._offset_x, self._offset_y)

    @property
    def center_left(self) -> Anchor:
        """Anchor to center-left of element."""
        return Anchor(self._element_id, AnchorPosition.CENTER_LEFT, self._offset_x, self._offset_y)

    @property
    def center(self) -> Anchor:
        """Anchor to center of element."""
        return Anchor(self._element_id, AnchorPosition.CENTER, self._offset_x, self._offset_y)

    @property
    def center_right(self) -> Anchor:
        """Anchor to center-right of element."""
        return Anchor(self._element_id, AnchorPosition.CENTER_RIGHT, self._offset_x, self._offset_y)

    @property
    def bottom_left(self) -> Anchor:
        """Anchor to bottom-left of element."""
        return Anchor(self._element_id, AnchorPosition.BOTTOM_LEFT, self._offset_x, self._offset_y)

    @property
    def bottom_center(self) -> Anchor:
        """Anchor to bottom-center of element."""
        return Anchor(self._element_id, AnchorPosition.BOTTOM_CENTER, self._offset_x, self._offset_y)

    @property
    def bottom_right(self) -> Anchor:
        """Anchor to bottom-right of element."""
        return Anchor(self._element_id, AnchorPosition.BOTTOM_RIGHT, self._offset_x, self._offset_y)

    def offset(self, dx: float, dy: float) -> AnchorBuilder:
        """Set offset from anchor point."""
        self._offset_x = dx
        self._offset_y = dy
        return self

    def above(self, amount: float = 0) -> AnchorBuilder:
        """Position above the anchor point."""
        self._offset_y = -abs(amount) if amount else -10
        return self

    def below(self, amount: float = 0) -> AnchorBuilder:
        """Position below the anchor point."""
        self._offset_y = abs(amount) if amount else 10
        return self

    def left_of(self, amount: float = 0) -> AnchorBuilder:
        """Position to the left of the anchor point."""
        self._offset_x = -abs(amount) if amount else -10
        return self

    def right_of(self, amount: float = 0) -> AnchorBuilder:
        """Position to the right of the anchor point."""
        self._offset_x = abs(amount) if amount else 10
        return self


class AnchorRegistry:
    """
    Registry of named anchors for reuse.

    Useful for maintaining a set of frequently used
    anchor points across automation scripts.
    """

    def __init__(self) -> None:
        self._anchors: Dict[str, Anchor] = {}
        self._resolver = AnchorResolver()

    def register(self, name: str, anchor: Anchor) -> AnchorRegistry:
        """Register a named anchor."""
        self._anchors[name] = anchor
        return self

    def unregister(self, name: str) -> bool:
        """Unregister a named anchor."""
        if name in self._anchors:
            del self._anchors[name]
            return True
        return False

    def get(self, name: str) -> Optional[Anchor]:
        """Get a registered anchor."""
        return self._anchors.get(name)

    def resolve(self, name: str) -> Optional[ElementTarget]:
        """Resolve a registered anchor to screen coordinates."""
        anchor = self._anchors.get(name)
        if anchor:
            return self._resolver.resolve(anchor)
        return None

    def list_names(self) -> List[str]:
        """List all registered anchor names."""
        return list(self._anchors.keys())


def create_anchor(element_id: str, position: AnchorPosition) -> Anchor:
    """Create a simple anchor for an element."""
    return Anchor(element_id=element_id, position=position)


def offset_anchor(anchor: Anchor, dx: float, dy: float) -> Anchor:
    """Create a new anchor with offset applied."""
    return Anchor(
        element_id=anchor.element_id,
        position=anchor.position,
        offset_x=anchor.offset_x + dx,
        offset_y=anchor.offset_y + dy,
    )
