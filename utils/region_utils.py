"""
Region of Interest (ROI) Utilities for UI Automation

Provides ROI detection, tracking, and management for
identifying and monitoring UI regions in automation flows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ROI:
    """Region of Interest definition."""
    x: float
    y: float
    width: float
    height: float
    name: str = ""
    confidence: float = 1.0
    created_at: float = field(default_factory=time.time)


@dataclass
class ROITrackingState:
    """State information for a tracked ROI."""
    roi: ROI
    last_seen: float
    confidence_history: list[float] = field(default_factory=list)
    is_stable: bool = True
    move_count: int = 0


@dataclass
class ROIEvent:
    """Event from ROI monitoring."""
    event_type: str
    roi: ROI
    timestamp: float
    metadata: dict = field(default_factory=dict)


class ROIDetector:
    """
    Detects and tracks Regions of Interest in UI automation.

    Supports static ROI definitions, dynamic detection,
    and movement tracking.
    """

    def __init__(self) -> None:
        self._static_rois: dict[str, ROI] = {}
        self._tracked_rois: dict[str, ROITrackingState] = {}
        self._event_callbacks: list[Callable[[ROIEvent], None]] = []

    def add_static_roi(self, roi: ROI) -> None:
        """
        Add a static (fixed) region of interest.

        Args:
            roi: ROI to add
        """
        self._static_rois[roi.name] = roi

    def remove_static_roi(self, name: str) -> bool:
        """Remove a static ROI by name."""
        if name in self._static_rois:
            del self._static_rois[name]
            return True
        return False

    def get_roi(self, name: str) -> Optional[ROI]:
        """Get a static ROI by name."""
        return self._static_rois.get(name)

    def detect_dynamic_roi(
        self,
        image_data: bytes,
        min_size: tuple[float, float] = (50, 50),
        max_size: tuple[float, float] = (500, 500),
    ) -> list[ROI]:
        """
        Detect dynamic ROIs in image data.

        This is a placeholder implementation that would use
        computer vision techniques in a real scenario.

        Args:
            image_data: Raw image bytes
            min_size: Minimum ROI dimensions (width, height)
            max_size: Maximum ROI dimensions (width, height)

        Returns:
            List of detected ROIs
        """
        # In a real implementation, this would use CV techniques
        # For now, return empty list
        return []

    def track_roi(
        self,
        roi: ROI,
        update_position: Callable[[ROI], tuple[float, float] | None],
    ) -> ROITrackingState:
        """
        Start tracking a dynamic ROI.

        Args:
            roi: Initial ROI definition
            update_position: Callback that returns new position or None if lost

        Returns:
            ROITrackingState for the tracked ROI
        """
        state = ROITrackingState(
            roi=roi,
            last_seen=time.time(),
        )
        self._tracked_rois[roi.name] = state
        return state

    def update_tracked_roi(self, name: str) -> Optional[ROIEvent]:
        """
        Update a tracked ROI's position.

        Args:
            name: Name of the tracked ROI

        Returns:
            ROIEvent if position changed significantly, None otherwise
        """
        if name not in self._tracked_rois:
            return None

        state = self._tracked_rois[name]
        old_pos = (state.roi.x, state.roi.y)

        # In real implementation, call the update callback
        # For now, simulate no movement
        new_pos = (state.roi.x, state.roi.y)

        state.last_seen = time.time()

        # Calculate movement
        dx = new_pos[0] - old_pos[0]
        dy = new_pos[1] - old_pos[1]
        distance = (dx**2 + dy**2) ** 0.5

        if distance > 5.0:  # Threshold for movement detection
            state.move_count += 1
            state.roi.x, state.roi.y = new_pos
            return ROIEvent(
                event_type="moved",
                roi=state.roi,
                timestamp=time.time(),
                metadata={"distance": distance, "dx": dx, "dy": dy},
            )

        return None

    def get_stable_rois(self, stability_threshold: int = 3) -> list[ROI]:
        """
        Get ROIs that have been stable (not moving much).

        Args:
            stability_threshold: Number of updates before considered stable

        Returns:
            List of stable ROIs
        """
        return [
            state.roi
            for state in self._tracked_rois.values()
            if state.move_count < stability_threshold
        ]

    def register_event_callback(
        self,
        callback: Callable[[ROIEvent], None],
    ) -> None:
        """Register a callback for ROI events."""
        self._event_callbacks.append(callback)

    def _emit_event(self, event: ROIEvent) -> None:
        """Emit an ROI event to all callbacks."""
        for callback in self._event_callbacks:
            callback(event)


def calculate_roi_center(roi: ROI) -> tuple[float, float]:
    """Calculate the center point of a ROI."""
    return (
        roi.x + roi.width / 2,
        roi.y + roi.height / 2,
    )


def calculate_roi_intersection(roi1: ROI, roi2: ROI) -> Optional[ROI]:
    """
    Calculate the intersection of two ROIs.

    Args:
        roi1: First ROI
        roi2: Second ROI

    Returns:
        Intersection ROI if exists, None otherwise
    """
    x1 = max(roi1.x, roi2.x)
    y1 = max(roi1.y, roi2.y)
    x2 = min(roi1.x + roi1.width, roi2.x + roi2.width)
    y2 = min(roi1.y + roi1.height, roi2.y + roi2.height)

    if x1 < x2 and y1 < y2:
        return ROI(
            x=x1, y=y1,
            width=x2 - x1, height=y2 - y1,
            name=f"intersection_{roi1.name}_{roi2.name}",
        )
    return None


def calculate_roi_union(roi1: ROI, roi2: ROI) -> ROI:
    """
    Calculate the union (bounding box) of two ROIs.

    Args:
        roi1: First ROI
        roi2: Second ROI

    Returns:
        Union ROI that encompasses both
    """
    x = min(roi1.x, roi2.x)
    y = min(roi1.y, roi2.y)
    right1 = roi1.x + roi1.width
    right2 = roi2.x + roi2.width
    bottom1 = roi1.y + roi1.height
    bottom2 = roi2.y + roi2.height

    return ROI(
        x=x, y=y,
        width=max(right1, right2) - x,
        height=max(bottom1, bottom2) - y,
        name=f"union_{roi1.name}_{roi2.name}",
    )


def is_point_in_roi(x: float, y: float, roi: ROI) -> bool:
    """Check if a point is inside a ROI."""
    return (
        roi.x <= x <= roi.x + roi.width and
        roi.y <= y <= roi.y + roi.height
    )


def calculate_roi_overlap_ratio(roi1: ROI, roi2: ROI) -> float:
    """
    Calculate the overlap ratio between two ROIs.

    Args:
        roi1: First ROI
        roi2: Second ROI

    Returns:
        Overlap ratio (0.0 to 1.0)
    """
    intersection = calculate_roi_intersection(roi1, roi2)
    if intersection is None:
        return 0.0

    intersection_area = intersection.width * intersection.height
    area1 = roi1.width * roi1.height
    area2 = roi2.width * roi2.height
    min_area = min(area1, area2)

    return intersection_area / min_area if min_area > 0 else 0.0
