"""Screen region ROI (Region of Interest) selector utilities."""
from typing import List, Tuple, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
import enum


class ROIMode(enum.Enum):
    """Mode for ROI selection."""
    RECTANGLE = enum.auto()
    POLYGON = enum.auto()
    ELLIPSE = enum.auto()


@dataclass
class ROI:
    """Region of interest definition."""
    mode: ROIMode
    points: List[Tuple[float, float]]
    label: str = ""
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def contains(self, x: float, y: float) -> bool:
        """Check if a point is inside this ROI.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            True if point is inside the ROI.
        """
        if self.mode == ROIMode.RECTANGLE:
            return self._contains_rect(x, y)
        elif self.mode == ROIMode.POLYGON:
            return self._contains_polygon(x, y)
        elif self.mode == ROIMode.ELLIPSE:
            return self._contains_ellipse(x, y)
        return False

    def _contains_rect(self, x: float, y: float) -> bool:
        """Point-in-rectangle test."""
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        return min(xs) <= x <= max(xs) and min(ys) <= y <= max(ys)

    def _contains_polygon(self, x: float, y: float) -> bool:
        """Ray casting point-in-polygon test."""
        n = len(self.points)
        inside = False
        j = n - 1
        for i in range(n):
            xi, yi = self.points[i]
            xj, yj = self.points[j]
            if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-9) + xi):
                inside = not inside
            j = i
        return inside

    def _contains_ellipse(self, x: float, y: float) -> bool:
        """Point-in-ellipse test using bounding box as ellipse approximation."""
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        cx = (min(xs) + max(xs)) / 2
        cy = (min(ys) + max(ys)) / 2
        rx = (max(xs) - min(xs)) / 2
        ry = (max(ys) - min(ys)) / 2
        if rx == 0 or ry == 0:
            return False
        return ((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2 <= 1

    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Get bounding box of the ROI.
        
        Returns:
            (min_x, min_y, max_x, max_y).
        """
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        return (min(xs), min(ys), max(xs), max(ys))

    def get_area(self) -> float:
        """Get approximate area of the ROI.
        
        Returns:
            Area in square pixels.
        """
        if self.mode == ROIMode.RECTANGLE:
            xs = [p[0] for p in self.points]
            ys = [p[1] for p in self.points]
            return (max(xs) - min(xs)) * (max(ys) - min(ys))
        elif self.mode == ROIMode.ELLIPSE:
            xs = [p[0] for p in self.points]
            ys = [p[1] for p in self.points]
            rx = (max(xs) - min(xs)) / 2
            ry = (max(ys) - min(ys)) / 2
            return 3.14159 * rx * ry
        elif self.mode == ROIMode.POLYGON:
            # Shoelace formula
            n = len(self.points)
            area = 0.0
            for i in range(n):
                j = (i + 1) % n
                area += self.points[i][0] * self.points[j][1]
                area -= self.points[j][0] * self.points[i][1]
            return abs(area) / 2
        return 0.0


class ROIManager:
    """Manages multiple regions of interest for screen analysis.
    
    Provides utilities for creating, storing, querying, and filtering
    ROIs for targeted screen region operations.
    
    Example:
        manager = ROIManager()
        manager.add_rectangle("button_area", [(100, 100), (200, 200)], label="Primary Button")
        manager.add_polygon("complex_shape", [(0, 0), (100, 0), (50, 100)])
        
        # Check if click is in any ROI
        hit = manager.hit_test(150, 150)
        if hit:
            print(f"Clicked in: {hit.label}")
    """

    def __init__(self) -> None:
        """Initialize the ROI manager."""
        self._rois: Dict[str, ROI] = {}
        self._overlap_groups: Dict[str, List[str]] = {}

    def add_rectangle(
        self,
        roi_id: str,
        corners: List[Tuple[float, float]],
        label: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ROI:
        """Add a rectangular ROI.
        
        Args:
            roi_id: Unique identifier for this ROI.
            corners: Two opposite corners [(x1, y1), (x2, y2)].
            label: Human-readable label.
            metadata: Optional metadata dictionary.
            
        Returns:
            Created ROI object.
        """
        points = [corners[0], (corners[1][0], corners[0][1]), corners[1], (corners[0][0], corners[1][1])]
        roi = ROI(mode=ROIMode.RECTANGLE, points=points, label=label or roi_id, metadata=metadata or {})
        self._rois[roi_id] = roi
        return roi

    def add_polygon(
        self,
        roi_id: str,
        vertices: List[Tuple[float, float]],
        label: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ROI:
        """Add a polygonal ROI.
        
        Args:
            roi_id: Unique identifier for this ROI.
            vertices: List of polygon vertices in order.
            label: Human-readable label.
            metadata: Optional metadata dictionary.
            
        Returns:
            Created ROI object.
        """
        roi = ROI(mode=ROIMode.POLYGON, points=vertices, label=label or roi_id, metadata=metadata or {})
        self._rois[roi_id] = roi
        return roi

    def add_ellipse(
        self,
        roi_id: str,
        bounding_box: List[Tuple[float, float]],
        label: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ROI:
        """Add an elliptical ROI defined by its bounding box.
        
        Args:
            roi_id: Unique identifier for this ROI.
            bounding_box: Two opposite corners of the ellipse bounding box.
            label: Human-readable label.
            metadata: Optional metadata dictionary.
            
        Returns:
            Created ROI object.
        """
        roi = ROI(mode=ROIMode.ELLIPSE, points=bounding_box, label=label or roi_id, metadata=metadata or {})
        self._rois[roi_id] = roi
        return roi

    def remove(self, roi_id: str) -> bool:
        """Remove an ROI by ID.
        
        Args:
            roi_id: ID of ROI to remove.
            
        Returns:
            True if ROI was removed.
        """
        return bool(self._rois.pop(roi_id, None))

    def get(self, roi_id: str) -> Optional[ROI]:
        """Get an ROI by ID.
        
        Args:
            roi_id: ROI identifier.
            
        Returns:
            ROI object or None.
        """
        return self._rois.get(roi_id)

    def hit_test(self, x: float, y: float) -> Optional[ROI]:
        """Find the first ROI containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            First matching ROI or None.
        """
        for roi in self._rois.values():
            if roi.contains(x, y):
                return roi
        return None

    def find_all(self, x: float, y: float) -> List[ROI]:
        """Find all ROIs containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            List of all matching ROIs.
        """
        return [roi for roi in self._rois.values() if roi.contains(x, y)]

    def list_all(self) -> List[ROI]:
        """List all registered ROIs.
        
        Returns:
            List of ROI objects.
        """
        return list(self._rois.values())

    def clear(self) -> None:
        """Remove all ROIs."""
        self._rois.clear()
        self._overlap_groups.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about registered ROIs.
        
        Returns:
            Dictionary with ROI statistics.
        """
        by_mode = {}
        for roi in self._rois.values():
            mode_name = roi.mode.name
            by_mode[mode_name] = by_mode.get(mode_name, 0) + 1
        
        total_area = sum(roi.get_area() for roi in self._rois.values())
        
        return {
            "total_rois": len(self._rois),
            "by_mode": by_mode,
            "total_area": round(total_area, 2),
        }
