"""UI distance utilities for automation.

Provides utilities for calculating distances between UI elements,
pathfinding, proximity detection, and spatial relationships.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple


@dataclass
class Point:
    """2D point."""
    x: float
    y: float
    
    def distance_to(self, other: "Point") -> float:
        """Calculate distance to another point.
        
        Args:
            other: Other point.
            
        Returns:
            Distance.
        """
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)
    
    def manhattan_distance_to(self, other: "Point") -> float:
        """Calculate Manhattan distance to another point.
        
        Args:
            other: Other point.
            
        Returns:
            Manhattan distance.
        """
        return abs(self.x - other.x) + abs(self.y - other.y)
    
    def angle_to(self, other: "Point") -> float:
        """Calculate angle to another point in radians.
        
        Args:
            other: Other point.
            
        Returns:
            Angle in radians.
        """
        return math.atan2(other.y - self.y, other.x - self.x)


@dataclass
class Rect:
    """2D rectangle."""
    x: float
    y: float
    width: float
    height: float
    
    @property
    def center(self) -> Point:
        """Get center point.
        
        Returns:
            Center point.
        """
        return Point(self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def left(self) -> float:
        return self.x
    
    @property
    def right(self) -> float:
        return self.x + self.width
    
    @property
    def top(self) -> float:
        return self.y
    
    @property
    def bottom(self) -> float:
        return self.y + self.height
    
    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is inside rect.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            True if inside.
        """
        return self.left <= x <= self.right and self.top <= y <= self.bottom
    
    def distance_to_point(self, x: float, y: float) -> float:
        """Calculate distance to a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            Distance.
        """
        dx = max(self.left - x, 0, x - self.right)
        dy = max(self.top - y, 0, y - self.bottom)
        return math.sqrt(dx * dx + dy * dy)
    
    def intersects(self, other: "Rect") -> bool:
        """Check if intersects another rect.
        
        Args:
            other: Other rect.
            
        Returns:
            True if intersects.
        """
        return not (
            self.right < other.left or
            self.left > other.right or
            self.bottom < other.top or
            self.top > other.bottom
        )


class ProximityDetector:
    """Detects elements in proximity to each other.
    
    Provides utilities for finding nearby elements
    and managing proximity relationships.
    """
    
    def __init__(self, threshold: float = 50.0) -> None:
        """Initialize the proximity detector.
        
        Args:
            threshold: Proximity threshold distance.
        """
        self.threshold = threshold
        self._elements: Dict[str, Rect] = {}
    
    def register_element(
        self,
        element_id: str,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> None:
        """Register an element.
        
        Args:
            element_id: Element identifier.
            x: X position.
            y: Y position.
            width: Element width.
            height: Element height.
        """
        self._elements[element_id] = Rect(x, y, width, height)
    
    def unregister_element(self, element_id: str) -> None:
        """Unregister an element.
        
        Args:
            element_id: Element identifier.
        """
        if element_id in self._elements:
            del self._elements[element_id]
    
    def get_nearby_elements(
        self,
        element_id: str
    ) -> List[Tuple[str, float]]:
        """Get elements near a specific element.
        
        Args:
            element_id: Element identifier.
            
        Returns:
            List of (element_id, distance) tuples.
        """
        if element_id not in self._elements:
            return []
        
        element = self._elements[element_id]
        center = element.center
        
        nearby = []
        
        for other_id, other_rect in self._elements.items():
            if other_id == element_id:
                continue
            
            distance = center.distance_to(other_rect.center)
            
            if distance <= self.threshold:
                nearby.append((other_id, distance))
        
        nearby.sort(key=lambda x: x[1])
        return nearby
    
    def get_elements_in_radius(
        self,
        x: float,
        y: float,
        radius: float
    ) -> List[Tuple[str, float]]:
        """Get elements within a radius of a point.
        
        Args:
            x: Center X.
            y: Center Y.
            radius: Search radius.
            
        Returns:
            List of (element_id, distance) tuples.
        """
        center = Point(x, y)
        
        results = []
        
        for element_id, rect in self._elements.items():
            distance = rect.distance_to_point(x, y)
            if distance <= radius:
                results.append((element_id, distance))
        
        results.sort(key=lambda x: x[1])
        return results


class SpatialGraph:
    """Represents spatial relationships between elements.
    
    Builds a graph of element connections based on
    proximity and spatial relationships.
    """
    
    def __init__(self) -> None:
        """Initialize the spatial graph."""
        self._nodes: Set[str] = set()
        self._edges: Dict[str, List[Tuple[str, float]]] = {}
    
    def add_node(self, node_id: str) -> None:
        """Add a node to the graph.
        
        Args:
            node_id: Node identifier.
        """
        self._nodes.add(node_id)
        if node_id not in self._edges:
            self._edges[node_id] = []
    
    def add_edge(
        self,
        from_id: str,
        to_id: str,
        distance: float
    ) -> None:
        """Add an edge between nodes.
        
        Args:
            from_id: Source node.
            to_id: Target node.
            distance: Edge distance.
        """
        if from_id not in self._nodes:
            self.add_node(from_id)
        if to_id not in self._nodes:
            self.add_node(to_id)
        
        self._edges[from_id].append((to_id, distance))
    
    def get_neighbors(self, node_id: str) -> List[Tuple[str, float]]:
        """Get neighbors of a node.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            List of (neighbor_id, distance) tuples.
        """
        return self._edges.get(node_id, [])
    
    def find_nearest(
        self,
        node_id: str
    ) -> Optional[Tuple[str, float]]:
        """Find nearest node to a given node.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            (nearest_id, distance) or None.
        """
        neighbors = self.get_neighbors(node_id)
        if not neighbors:
            return None
        
        return min(neighbors, key=lambda x: x[1])
    
    def find_path(
        self,
        from_id: str,
        to_id: str
    ) -> Optional[List[str]]:
        """Find path between two nodes.
        
        Args:
            from_id: Start node.
            to_id: End node.
            
        Returns:
            List of node IDs forming path, or None.
        """
        if from_id not in self._nodes or to_id not in self._nodes:
            return None
        
        if from_id == to_id:
            return [from_id]
        
        visited = set()
        queue = [(from_id, [from_id])]
        
        while queue:
            current, path = queue.pop(0)
            
            if current == to_id:
                return path
            
            if current in visited:
                continue
            
            visited.add(current)
            
            for neighbor, _ in self.get_neighbors(current):
                if neighbor not in visited:
                    queue.append((neighbor, path + [neighbor]))
        
        return None


class PathFinder:
    """Finds paths between UI elements.
    
    Provides pathfinding capabilities for
    navigation through UI layouts.
    """
    
    def __init__(self, proximity_threshold: float = 100.0) -> None:
        """Initialize the pathfinder.
        
        Args:
            proximity_threshold: Distance to consider elements connected.
        """
        self.proximity_threshold = proximity_threshold
        self._elements: Dict[str, Rect] = {}
        self._graph = SpatialGraph()
    
    def register_element(
        self,
        element_id: str,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> None:
        """Register an element.
        
        Args:
            element_id: Element identifier.
            x: X position.
            y: Y position.
            width: Element width.
            height: Element height.
        """
        self._elements[element_id] = Rect(x, y, width, height)
        self._graph.add_node(element_id)
        
        for other_id, other_rect in self._elements.items():
            if other_id == element_id:
                continue
            
            center1 = self._elements[element_id].center
            center2 = other_rect.center
            distance = center1.distance_to(center2)
            
            if distance <= self.proximity_threshold:
                self._graph.add_edge(element_id, other_id, distance)
                self._graph.add_edge(other_id, element_id, distance)
    
    def find_path(
        self,
        from_id: str,
        to_id: str
    ) -> Optional[List[str]]:
        """Find path between elements.
        
        Args:
            from_id: Start element.
            to_id: End element.
            
        Returns:
            List of element IDs forming path.
        """
        return self._graph.find_path(from_id, to_id)
    
    def get_nearest_element(self, x: float, y: float) -> Optional[str]:
        """Get nearest element to a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            Element ID or None.
        """
        if not self._elements:
            return None
        
        nearest = None
        min_distance = float('inf')
        
        for element_id, rect in self._elements.items():
            distance = rect.distance_to_point(x, y)
            if distance < min_distance:
                min_distance = distance
                nearest = element_id
        
        return nearest


def calculate_distance(
    x1: float,
    y1: float,
    x2: float,
    y2: float
) -> float:
    """Calculate Euclidean distance between two points.
    
    Args:
        x1: First X coordinate.
        y1: First Y coordinate.
        x2: Second X coordinate.
        y2: Second Y coordinate.
        
    Returns:
        Distance.
    """
    dx = x2 - x1
    dy = y2 - y1
    return math.sqrt(dx * dx + dy * dy)


def calculate_manhattan_distance(
    x1: float,
    y1: float,
    x2: float,
    y2: float
) -> float:
    """Calculate Manhattan distance between two points.
    
    Args:
        x1: First X coordinate.
        y1: First Y coordinate.
        x2: Second X coordinate.
        y2: Second Y coordinate.
        
    Returns:
        Manhattan distance.
    """
    return abs(x2 - x1) + abs(y2 - y1)


def calculate_path_length(path: List[Tuple[float, float]]) -> float:
    """Calculate total length of a path.
    
    Args:
        path: List of (x, y) coordinates.
        
    Returns:
        Total path length.
    """
    if len(path) < 2:
        return 0.0
    
    total = 0.0
    for i in range(1, len(path)):
        total += calculate_distance(
            path[i-1][0], path[i-1][1],
            path[i][0], path[i][1]
        )
    
    return total
