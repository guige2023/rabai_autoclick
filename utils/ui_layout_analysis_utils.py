"""
UI Layout Analysis Utilities - Layout analysis and structure detection.

This module provides utilities for analyzing UI layouts, detecting
layout patterns (lists, grids, forms), and understanding spatial
relationships between UI elements.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class LayoutRegion:
    """Represents a detected region in a layout.
    
    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Region width.
        height: Region height.
        region_type: Type of region detected.
        confidence: Detection confidence (0.0 to 1.0).
        elements: IDs of elements in this region.
    """
    x: int
    y: int
    width: int
    height: int
    region_type: str = "unknown"
    confidence: float = 1.0
    elements: list[str] = field(default_factory=list)
    
    @property
    def right(self) -> int:
        """Get right edge X coordinate."""
        return self.x + self.width
    
    @property
    def bottom(self) -> int:
        """Get bottom edge Y coordinate."""
        return self.y + self.height
    
    @property
    def center_x(self) -> int:
        """Get center X coordinate."""
        return self.x + self.width // 2
    
    @property
    def center_y(self) -> int:
        """Get center Y coordinate."""
        return self.y + self.height // 2
    
    @property
    def bounds(self) -> tuple[int, int, int, int]:
        """Get bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)
    
    def area(self) -> int:
        """Get region area."""
        return self.width * self.height
    
    def contains_point(self, px: int, py: int) -> bool:
        """Check if point is inside region."""
        return self.x <= px < self.right and self.y <= py < self.bottom
    
    def intersects(self, other: LayoutRegion) -> bool:
        """Check if regions intersect."""
        return not (
            self.right <= other.x
            or other.right <= self.x
            or self.bottom <= other.y
            or other.bottom <= self.y
        )


@dataclass
class LayoutPattern:
    """Represents a detected layout pattern.
    
    Attributes:
        pattern_type: Type of pattern (list, grid, form, etc.).
        confidence: Detection confidence.
        regions: Regions that form this pattern.
        properties: Pattern-specific properties.
    """
    pattern_type: str
    confidence: float = 1.0
    regions: list[LayoutRegion] = field(default_factory=list)
    properties: dict = field(default_factory=dict)


class LayoutAnalyzer:
    """Analyzes UI layouts to detect patterns and structures.
    
    Provides methods for detecting common layout patterns,
    analyzing spatial relationships, and extracting layout structure.
    
    Example:
        >>> analyzer = LayoutAnalyzer()
        >>> patterns = analyzer.detect_patterns(elements)
        >>> for pattern in patterns:
        ...     print(f"{pattern.pattern_type}: {pattern.confidence}")
    """
    
    def __init__(self) -> None:
        """Initialize the layout analyzer."""
        self._elements: list[dict] = []
    
    def set_elements(self, elements: Sequence[dict]) -> None:
        """Set elements to analyze.
        
        Args:
            elements: List of element dictionaries with bounds.
        """
        self._elements = list(elements)
    
    def detect_gaps(self, axis: str = "horizontal") -> list[int]:
        """Detect gaps between elements.
        
        Args:
            axis: "horizontal" or "vertical".
            
        Returns:
            List of gap positions.
        """
        if not self._elements:
            return []
        
        if axis == "horizontal":
            sorted_elements = sorted(self._elements, key=lambda e: e.get("x", 0))
            gaps: list[int] = []
            
            for i in range(len(sorted_elements) - 1):
                current = sorted_elements[i]
                next_elem = sorted_elements[i + 1]
                
                current_right = current.get("x", 0) + current.get("width", 0)
                next_left = next_elem.get("x", 0)
                
                if next_left > current_right:
                    gaps.append(current_right)
            
            return gaps
        else:
            sorted_elements = sorted(self._elements, key=lambda e: e.get("y", 0))
            gaps: list[int] = []
            
            for i in range(len(sorted_elements) - 1):
                current = sorted_elements[i]
                next_elem = sorted_elements[i + 1]
                
                current_bottom = current.get("y", 0) + current.get("height", 0)
                next_top = next_elem.get("y", 0)
                
                if next_top > current_bottom:
                    gaps.append(current_bottom)
            
            return gaps
    
    def detect_alignment(
        self,
        axis: str = "horizontal"
    ) -> dict[str, list[dict]]:
        """Detect aligned elements.
        
        Args:
            axis: "horizontal" or "vertical".
            
        Returns:
            Dictionary of alignment lines to element lists.
        """
        if not self._elements:
            return {}
        
        alignment_dict: dict[str, list[dict]] = {}
        
        for elem in self._elements:
            if axis == "horizontal":
                key = f"y_{elem.get('y', 0)}"
            else:
                key = f"x_{elem.get('x', 0)}"
            
            if key not in alignment_dict:
                alignment_dict[key] = []
            alignment_dict[key].append(elem)
        
        return {
            k: v for k, v in alignment_dict.items()
            if len(v) > 1
        }
    
    def detect_grid(self, tolerance: int = 5) -> Optional[LayoutPattern]:
        """Detect if elements form a grid pattern.
        
        Args:
            tolerance: Position tolerance in pixels.
            
        Returns:
            Detected grid pattern, or None.
        """
        if len(self._elements) < 4:
            return None
        
        x_positions = sorted(set(e.get("x", 0) for e in self._elements))
        y_positions = sorted(set(e.get("y", 0) for e in self._elements))
        
        x_clusters = self._cluster_positions(x_positions, tolerance)
        y_clusters = self._cluster_positions(y_positions, tolerance)
        
        if len(x_clusters) >= 2 and len(y_clusters) >= 2:
            return LayoutPattern(
                pattern_type="grid",
                confidence=min(len(x_clusters) / 3, len(y_clusters) / 3, 1.0),
                properties={
                    "columns": len(x_clusters),
                    "rows": len(y_clusters),
                    "x_step": x_clusters[1] - x_clusters[0] if len(x_clusters) > 1 else 0,
                    "y_step": y_clusters[1] - y_clusters[0] if len(y_clusters) > 1 else 0
                }
            )
        
        return None
    
    def detect_list(self, axis: str = "vertical") -> Optional[LayoutPattern]:
        """Detect if elements form a list pattern.
        
        Args:
            axis: "vertical" or "horizontal".
            
        Returns:
            Detected list pattern, or None.
        """
        if len(self._elements) < 3:
            return None
        
        if axis == "vertical":
            positions = [(e.get("y", 0), e) for e in self._elements]
            key_func = lambda e: e.get("x", 0)
        else:
            positions = [(e.get("x", 0), e) for e in self._elements]
            key_func = lambda e: e.get("y", 0)
        
        positions.sort()
        
        gaps = []
        for i in range(len(positions) - 1):
            current_pos = positions[i][0]
            next_pos = positions[i + 1][0]
            
            current_elem = positions[i][1]
            next_elem = positions[i + 1][1]
            
            if axis == "vertical":
                gap = next_pos - (current_elem.get("y", 0) + current_elem.get("height", 0))
            else:
                gap = next_pos - (current_elem.get("x", 0) + current_elem.get("width", 0))
            
            gaps.append(gap)
        
        if gaps:
            avg_gap = sum(gaps) / len(gaps)
            consistent = all(abs(g - avg_gap) < avg_gap * 0.3 for g in gaps)
            
            if consistent:
                return LayoutPattern(
                    pattern_type="list",
                    confidence=0.8 if consistent else 0.5,
                    properties={
                        "direction": axis,
                        "item_height": avg_gap if axis == "vertical" else None,
                        "item_width": avg_gap if axis == "horizontal" else None,
                        "item_count": len(self._elements)
                    }
                )
        
        return None
    
    def detect_columns(self, tolerance: int = 5) -> list[LayoutRegion]:
        """Detect column regions.
        
        Args:
            tolerance: Position tolerance.
            
        Returns:
            List of detected column regions.
        """
        if not self._elements:
            return []
        
        x_positions = sorted(set(e.get("x", 0) for e in self._elements))
        clusters = self._cluster_positions(x_positions, tolerance)
        
        columns: list[LayoutRegion] = []
        y_min = min(e.get("y", 0) for e in self._elements)
        y_max = max(e.get("y", 0) + e.get("height", 0) for e in self._elements)
        
        for i, x in enumerate(clusters):
            width = (
                clusters[i + 1] - x if i + 1 < len(clusters)
                else max(e.get("x", 0) + e.get("width", 0) for e in self._elements) - x
            )
            columns.append(LayoutRegion(
                x=x,
                y=y_min,
                width=width,
                height=y_max - y_min,
                region_type="column"
            ))
        
        return columns
    
    def detect_rows(self, tolerance: int = 5) -> list[LayoutRegion]:
        """Detect row regions.
        
        Args:
            tolerance: Position tolerance.
            
        Returns:
            List of detected row regions.
        """
        if not self._elements:
            return []
        
        y_positions = sorted(set(e.get("y", 0) for e in self._elements))
        clusters = self._cluster_positions(y_positions, tolerance)
        
        rows: list[LayoutRegion] = []
        x_min = min(e.get("x", 0) for e in self._elements)
        x_max = max(e.get("x", 0) + e.get("width", 0) for e in self._elements)
        
        for i, y in enumerate(clusters):
            height = (
                clusters[i + 1] - y if i + 1 < len(clusters)
                else max(e.get("y", 0) + e.get("height", 0) for e in self._elements) - y
            )
            rows.append(LayoutRegion(
                x=x_min,
                y=y,
                width=x_max - x_min,
                height=height,
                region_type="row"
            ))
        
        return rows
    
    def detect_patterns(self) -> list[LayoutPattern]:
        """Detect all layout patterns.
        
        Returns:
            List of detected patterns.
        """
        patterns: list[LayoutPattern] = []
        
        grid = self.detect_grid()
        if grid:
            patterns.append(grid)
        
        vlist = self.detect_list("vertical")
        if vlist:
            patterns.append(vlist)
        
        hlist = self.detect_list("horizontal")
        if hlist:
            patterns.append(hlist)
        
        return patterns
    
    @staticmethod
    def _cluster_positions(positions: list[int], tolerance: int) -> list[int]:
        """Cluster nearby positions.
        
        Args:
            positions: Sorted positions.
            tolerance: Clustering tolerance.
            
        Returns:
            List of cluster centers.
        """
        if not positions:
            return []
        
        clusters: list[list[int]] = []
        current_cluster = [positions[0]]
        
        for pos in positions[1:]:
            if pos - current_cluster[-1] <= tolerance:
                current_cluster.append(pos)
            else:
                clusters.append(current_cluster)
                current_cluster = [pos]
        
        clusters.append(current_cluster)
        
        return [sum(c) // len(c) for c in clusters]


@dataclass
class SpatialRelation:
    """Represents a spatial relationship between two elements.
    
    Attributes:
        from_id: Source element ID.
        to_id: Target element ID.
        relation: Type of relation (left-of, right-of, above, below, etc.).
        distance: Distance between elements.
    """
    from_id: str
    to_id: str
    relation: str
    distance: float = 0.0


class SpatialAnalyzer:
    """Analyzes spatial relationships between UI elements.
    
    Provides methods for finding elements by their spatial
    relationships to other elements.
    
    Example:
        >>> analyzer = SpatialAnalyzer()
        >>> elements_to_left = analyzer.get_elements_left_of(element_id, all_elements)
    """
    
    def get_elements_left_of(
        self,
        element_id: str,
        elements: list[dict],
        tolerance: int = 10
    ) -> list[dict]:
        """Get elements to the left of target.
        
        Args:
            element_id: Target element ID.
            elements: All elements.
            tolerance: Vertical overlap tolerance.
            
        Returns:
            Elements positioned to the left.
        """
        target = next((e for e in elements if e.get("id") == element_id), None)
        if not target:
            return []
        
        target_x = target.get("x", 0)
        target_y = target.get("y", 0)
        target_height = target.get("height", 0)
        
        results = []
        for elem in elements:
            if elem.get("id") == element_id:
                continue
            
            elem_right = elem.get("x", 0) + elem.get("width", 0)
            elem_y = elem.get("y", 0)
            elem_height = elem.get("height", 0)
            
            if elem_right <= target_x:
                y_overlap = min(
                    target_y + target_height,
                    elem_y + elem_height
                ) - max(target_y, elem_y)
                
                if y_overlap >= 0 or abs(y_overlap) <= tolerance:
                    results.append(elem)
        
        return sorted(results, key=lambda e: e.get("x", 0), reverse=True)
    
    def get_elements_right_of(
        self,
        element_id: str,
        elements: list[dict],
        tolerance: int = 10
    ) -> list[dict]:
        """Get elements to the right of target."""
        target = next((e for e in elements if e.get("id") == element_id), None)
        if not target:
            return []
        
        target_right = target.get("x", 0) + target.get("width", 0)
        target_y = target.get("y", 0)
        target_height = target.get("height", 0)
        
        results = []
        for elem in elements:
            if elem.get("id") == element_id:
                continue
            
            elem_x = elem.get("x", 0)
            elem_y = elem.get("y", 0)
            elem_height = elem.get("height", 0)
            
            if elem_x >= target_right:
                y_overlap = min(
                    target_y + target_height,
                    elem_y + elem_height
                ) - max(target_y, elem_y)
                
                if y_overlap >= 0 or abs(y_overlap) <= tolerance:
                    results.append(elem)
        
        return sorted(results, key=lambda e: e.get("x", 0))
    
    def get_elements_above(
        self,
        element_id: str,
        elements: list[dict],
        tolerance: int = 10
    ) -> list[dict]:
        """Get elements above target."""
        target = next((e for e in elements if e.get("id") == element_id), None)
        if not target:
            return []
        
        target_y = target.get("y", 0)
        target_x = target.get("x", 0)
        target_width = target.get("width", 0)
        
        results = []
        for elem in elements:
            if elem.get("id") == element_id:
                continue
            
            elem_bottom = elem.get("y", 0) + elem.get("height", 0)
            elem_x = elem.get("x", 0)
            elem_width = elem.get("width", 0)
            
            if elem_bottom <= target_y:
                x_overlap = min(
                    target_x + target_width,
                    elem_x + elem_width
                ) - max(target_x, elem_x)
                
                if x_overlap >= 0 or abs(x_overlap) <= tolerance:
                    results.append(elem)
        
        return sorted(results, key=lambda e: e.get("y", 0), reverse=True)
    
    def get_elements_below(
        self,
        element_id: str,
        elements: list[dict],
        tolerance: int = 10
    ) -> list[dict]:
        """Get elements below target."""
        target = next((e for e in elements if e.get("id") == element_id), None)
        if not target:
            return []
        
        target_bottom = target.get("y", 0) + target.get("height", 0)
        target_x = target.get("x", 0)
        target_width = target.get("width", 0)
        
        results = []
        for elem in elements:
            if elem.get("id") == element_id:
                continue
            
            elem_y = elem.get("y", 0)
            elem_x = elem.get("x", 0)
            elem_width = elem.get("width", 0)
            
            if elem_y >= target_bottom:
                x_overlap = min(
                    target_x + target_width,
                    elem_x + elem_width
                ) - max(target_x, elem_x)
                
                if x_overlap >= 0 or abs(x_overlap) <= tolerance:
                    results.append(elem)
        
        return sorted(results, key=lambda e: e.get("y", 0))
    
    def find_nearest(
        self,
        element_id: str,
        elements: list[dict]
    ) -> Optional[dict]:
        """Find the nearest element to target."""
        target = next((e for e in elements if e.get("id") == element_id), None)
        if not target:
            return None
        
        target_center_x = target.get("x", 0) + target.get("width", 0) // 2
        target_center_y = target.get("y", 0) + target.get("height", 0) // 2
        
        def distance(elem: dict) -> float:
            cx = elem.get("x", 0) + elem.get("width", 0) // 2
            cy = elem.get("y", 0) + elem.get("height", 0) // 2
            return ((target_center_x - cx) ** 2 + (target_center_y - cy) ** 2) ** 0.5
        
        candidates = [e for e in elements if e.get("id") != element_id]
        if not candidates:
            return None
        
        return min(candidates, key=distance)
