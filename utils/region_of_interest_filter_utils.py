"""
Region of Interest Filter Utilities

Filter accessibility tree nodes and UI elements based on
spatial regions of interest (ROIs), supporting multi-monitor setups.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable


@dataclass
class RegionOfInterest:
    """A rectangular region of interest."""
    name: str
    x: float
    y: float
    width: float
    height: float
    display_index: int = 0

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is within this ROI."""
        return self.x <= px <= self.x + self.width and self.y <= py <= self.y + self.height

    def overlaps_bounds(
        self,
        bx: float, by: float, bw: float, bh: float,
    ) -> bool:
        """Check if ROI overlaps with a bounding box."""
        return not (
            bx + bw < self.x or bx > self.x + self.width
            or by + bh < self.y or by > self.y + self.height
        )


@dataclass
class ROIFilterOptions:
    """Options for ROI filtering."""
    include_overlapping: bool = True  # Include elements that overlap ROI
    min_overlap_percent: float = 0.1  # Minimum overlap to include (0.0 to 1.0)


class RegionOfInterestFilter:
    """
    Filter UI elements by their relationship to one or more
    regions of interest (ROIs).
    """

    def __init__(self, options: Optional[ROIFilterOptions] = None):
        self.options = options or ROIFilterOptions()
        self._rois: List[RegionOfInterest] = []

    def add_roi(self, roi: RegionOfInterest) -> None:
        """Add a region of interest."""
        self._rois.append(roi)

    def add_roi_from_bounds(
        self,
        name: str,
        x: float, y: float, width: float, height: float,
        display_index: int = 0,
    ) -> RegionOfInterest:
        """Add an ROI defined by its bounds."""
        roi = RegionOfInterest(
            name=name,
            x=x, y=y, width=width, height=height,
            display_index=display_index,
        )
        self._rois.append(roi)
        return roi

    def clear_rois(self) -> None:
        """Remove all ROIs."""
        self._rois.clear()

    def filter_by_point(
        self,
        elements: List,
        get_bounds: Callable[[any], Tuple[float, float, float, float]],
        px: float, py: float,
    ) -> List:
        """Filter elements that contain a specific point."""
        results = []
        for elem in elements:
            bx, by, bw, bh = get_bounds(elem)
            cx, cy = bx + bw / 2, by + bh / 2  # center point
            if self._point_in_any_roi(cx, cy):
                results.append(elem)
        return results

    def filter_by_bounds(
        self,
        elements: List,
        get_bounds: Callable[[any], Tuple[float, float, float, float]],
    ) -> List:
        """Filter elements that overlap any ROI."""
        results = []
        for elem in elements:
            bx, by, bw, bh = get_bounds(elem)
            if self._overlaps_any_roi(bx, by, bw, bh):
                results.append(elem)
        return results

    def _point_in_any_roi(self, px: float, py: float) -> bool:
        """Check if a point falls within any ROI."""
        for roi in self._rois:
            if roi.contains_point(px, py):
                return True
        return False

    def _overlaps_any_roi(
        self,
        bx: float, by: float, bw: float, bh: float,
    ) -> bool:
        """Check if a bounding box overlaps any ROI."""
        if not self.options.include_overlapping:
            # Only include if center is in ROI
            cx, cy = bx + bw / 2, by + bh / 2
            return self._point_in_any_roi(cx, cy)

        for roi in self._rois:
            if roi.overlaps_bounds(bx, by, bw, bh):
                return True
        return False

    def get_roi_count(self) -> int:
        """Get the number of registered ROIs."""
        return len(self._rois)
