"""Region of Interest (ROI) utilities for UI automation.

This module provides utilities for defining, manipulating, and managing
regions of interest (ROIs) used in UI automation workflows.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ROIType(Enum):
    """Types of regions of interest."""
    STATIC = auto()       # Fixed position and size
    RELATIVE = auto()     # Relative to another element/window
    DYNAMIC = auto()      # Computed at runtime
    MULTI = auto()        # Multiple sub-regions


class ROIAnchor(Enum):
    """Anchor points for relative positioning."""
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    MIDDLE_LEFT = auto()
    CENTER = auto()
    MIDDLE_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()


@dataclass
class Point:
    """A 2D point."""
    x: float
    y: float

    def offset(self, dx: float, dy: float) -> Point:
        """Return a new point offset by dx, dy."""
        return Point(self.x + dx, self.y + dy)

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5

    def as_tuple(self) -> tuple[float, float]:
        """Return as (x, y) tuple."""
        return (self.x, self.y)


@dataclass
class Size:
    """A 2D size."""
    width: float
    height: float

    @property
    def area(self) -> float:
        """Return the area."""
        return self.width * self.height

    @property
    def aspect_ratio(self) -> float:
        """Return width/height aspect ratio."""
        return self.width / self.height if self.height != 0 else 0.0

    def scale(self, factor: float) -> Size:
        """Return a new size scaled by factor."""
        return Size(self.width * factor, self.height * factor)


@dataclass
class Rect:
    """A rectangle defined by origin point and size."""
    origin: Point
    size: Size

    @classmethod
    def from_coords(cls, x: float, y: float, width: float, height: float) -> Rect:
        """Create from coordinate values."""
        return cls(Point(x, y), Size(width, height))

    @classmethod
    def from_points(cls, top_left: Point, bottom_right: Point) -> Rect:
        """Create from two opposite corners."""
        width = bottom_right.x - top_left.x
        height = bottom_right.y - top_left.y
        return cls(top_left, Size(width, height))

    @property
    def x1(self) -> float:
        """Left edge x coordinate."""
        return self.origin.x

    @property
    def y1(self) -> float:
        """Top edge y coordinate."""
        return self.origin.y

    @property
    def x2(self) -> float:
        """Right edge x coordinate."""
        return self.origin.x + self.size.width

    @property
    def y2(self) -> float:
        """Bottom edge y coordinate."""
        return self.origin.y + self.size.height

    @property
    def center(self) -> Point:
        """Return the center point."""
        return Point(
            self.origin.x + self.size.width / 2,
            self.origin.y + self.size.height / 2,
        )

    @property
    def top_left(self) -> Point:
        """Return the top-left corner."""
        return self.origin

    @property
    def top_right(self) -> Point:
        """Return the top-right corner."""
        return Point(self.x2, self.origin.y)

    @property
    def bottom_left(self) -> Point:
        """Return the bottom-left corner."""
        return Point(self.origin.x, self.y2)

    @property
    def bottom_right(self) -> Point:
        """Return the bottom-right corner."""
        return Point(self.x2, self.y2)

    def contains(self, point: Point) -> bool:
        """Check if a point is inside this rectangle."""
        return (
            self.x1 <= point.x < self.x2
            and self.y1 <= point.y < self.y2
        )

    def intersects(self, other: Rect) -> bool:
        """Check if this rectangle intersects another."""
        return not (
            self.x2 <= other.x1 or other.x2 <= self.x1
            or self.y2 <= other.y1 or other.y2 <= self.y1
        )

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Return the intersection rectangle, or None if no overlap."""
        if not self.intersects(other):
            return None
        x1 = max(self.x1, other.x1)
        y1 = max(self.y1, other.y1)
        x2 = min(self.x2, other.x2)
        y2 = min(self.y2, other.y2)
        return Rect.from_coords(x1, y1, x2 - x1, y2 - y1)

    def union(self, other: Rect) -> Rect:
        """Return the bounding box containing both rectangles."""
        x1 = min(self.x1, other.x1)
        y1 = min(self.y1, other.y1)
        x2 = max(self.x2, other.x2)
        y2 = max(self.y2, other.y2)
        return Rect.from_coords(x1, y1, x2 - x1, y2 - y1)

    def expand(self, margin: float) -> Rect:
        """Return a rectangle expanded by margin in all directions."""
        return Rect.from_coords(
            self.x1 - margin,
            self.y1 - margin,
            self.size.width + 2 * margin,
            self.size.height + 2 * margin,
        )

    def shrink(self, margin: float) -> Rect:
        """Return a rectangle shrunk by margin in all directions."""
        return self.expand(-margin)

    def anchor_point(self, anchor: ROIAnchor) -> Point:
        """Return the point at the given anchor position."""
        anchors = {
            ROIAnchor.TOP_LEFT: self.top_left,
            ROIAnchor.TOP_CENTER: Point(self.center.x, self.y1),
            ROIAnchor.TOP_RIGHT: self.top_right,
            ROIAnchor.MIDDLE_LEFT: Point(self.x1, self.center.y),
            ROIAnchor.CENTER: self.center,
            ROIAnchor.MIDDLE_RIGHT: Point(self.x2, self.center.y),
            ROIAnchor.BOTTOM_LEFT: self.bottom_left,
            ROIAnchor.BOTTOM_CENTER: Point(self.center.x, self.y2),
            ROIAnchor.BOTTOM_RIGHT: self.bottom_right,
        }
        return anchors[anchor]

    def relative_to_absolute(
        self, anchor_point: Point, anchor: ROIAnchor
    ) -> Rect:
        """Compute absolute position from a relative anchor."""
        local = self.anchor_point(anchor)
        dx = anchor_point.x - local.x
        dy = anchor_point.y - local.y
        return Rect.from_coords(
            self.x1 + dx, self.y1 + dy,
            self.size.width, self.size.height,
        )


@dataclass
class RegionOfInterest:
    """A named region of interest for UI automation.

    Attributes:
        id: Unique identifier for this ROI.
        name: Human-readable name.
        rect: The rectangle bounds.
        roi_type: Type of ROI (static, relative, dynamic, multi).
        anchor: Anchor point for relative positioning.
        metadata: Additional key-value metadata.
        tags: Set of string tags for categorization.
        enabled: Whether this ROI is active.
        priority: Priority for multi-ROI selection (higher = first).
        confidence_threshold: Minimum confidence for detection.
        parent_id: Optional ID of a parent ROI for nesting.
    """
    name: str
    rect: Rect
    roi_type: ROIType = ROIType.STATIC
    anchor: ROIAnchor = ROIAnchor.TOP_LEFT
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = field(default_factory=dict)
    tags: set[str] = field(default_factory=set)
    enabled: bool = True
    priority: int = 0
    confidence_threshold: float = 0.8
    parent_id: Optional[str] = None

    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is inside this ROI."""
        return self.rect.contains(Point(x, y))

    def matches_tag(self, tag: str) -> bool:
        """Check if ROI has a given tag."""
        return tag in self.tags

    def add_tag(self, tag: str) -> None:
        """Add a tag to this ROI."""
        self.tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        """Remove a tag from this ROI."""
        self.tags.discard(tag)

    def set_metadata(self, key: str, value: Any) -> None:
        """Set a metadata key-value pair."""
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get a metadata value."""
        return self.metadata.get(key, default)


class ROIManager:
    """Manages collections of regions of interest.

    Provides CRUD operations, spatial queries, and filtering
    for collections of ROI objects.
    """

    def __init__(self) -> None:
        """Initialize an empty ROI manager."""
        self._rois: dict[str, RegionOfInterest] = {}

    def add(self, roi: RegionOfInterest) -> str:
        """Register an ROI and return its ID."""
        self._rois[roi.id] = roi
        return roi.id

    def get(self, roi_id: str) -> Optional[RegionOfInterest]:
        """Retrieve an ROI by ID."""
        return self._rois.get(roi_id)

    def remove(self, roi_id: str) -> bool:
        """Remove an ROI by ID. Returns True if it existed."""
        if roi_id in self._rois:
            del self._rois[roi_id]
            return True
        return False

    def update(self, roi_id: str, **kwargs: Any) -> bool:
        """Update ROI attributes. Returns True if found."""
        roi = self._rois.get(roi_id)
        if not roi:
            return False
        for key, value in kwargs.items():
            if hasattr(roi, key):
                setattr(roi, key, value)
        return True

    def find_by_name(self, name: str) -> list[RegionOfInterest]:
        """Find all ROIs with matching name (case-insensitive)."""
        name_lower = name.lower()
        return [r for r in self._rois.values() if r.name.lower() == name_lower]

    def find_by_tag(self, tag: str) -> list[RegionOfInterest]:
        """Find all ROIs with a given tag."""
        return [r for r in self._rois.values() if r.matches_tag(tag)]

    def find_at_point(self, x: float, y: float) -> list[RegionOfInterest]:
        """Find all ROIs containing the given point, sorted by priority."""
        found = [r for r in self._rois.values() if r.enabled and r.contains_point(x, y)]
        found.sort(key=lambda r: r.priority, reverse=True)
        return found

    def find_intersecting(self, rect: Rect) -> list[RegionOfInterest]:
        """Find all ROIs that intersect a given rectangle."""
        return [r for r in self._rois.values() if r.enabled and r.rect.intersects(rect)]

    def list_all(self, enabled_only: bool = False) -> list[RegionOfInterest]:
        """List all ROIs, optionally filtering to enabled only."""
        rois = self._rois.values()
        if enabled_only:
            rois = [r for r in rois if r.enabled]
        return sorted(rois, key=lambda r: r.priority, reverse=True)

    def clear(self) -> None:
        """Remove all ROIs."""
        self._rois.clear()

    @property
    def count(self) -> int:
        """Return the number of registered ROIs."""
        return len(self._rois)
