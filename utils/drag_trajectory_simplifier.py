"""Drag trajectory simplifier using Ramer-Douglas-Peucker algorithm."""
from typing import List, Tuple, Optional
import math


class DragTrajectorySimplifier:
    """Simplifies drag trajectories using the Ramer-Douglas-Peucker algorithm.
    
    Reduces the number of points in a drag trajectory while preserving
    its essential shape, useful for template matching and gesture recognition.
    
    Example:
        simplifier = DragTrajectorySimplifier()
        simplified = simplifier.simplify(
            points=[(0,0), (10,5), (20,10), (100,50), (200,100)],
            epsilon=5.0
        )
    """

    def __init__(self, default_epsilon: float = 2.0) -> None:
        self._default_epsilon = default_epsilon

    def simplify(
        self,
        points: List[Tuple[float, float]],
        epsilon: Optional[float] = None,
    ) -> List[Tuple[float, float]]:
        """Simplify a trajectory using RDP algorithm."""
        eps = epsilon if epsilon is not None else self._default_epsilon
        
        if len(points) < 3:
            return points.copy()
        
        return self._ramer_douglas_peucker(points, eps)

    def _ramer_douglas_peucker(
        self,
        points: List[Tuple[float, float]],
        epsilon: float,
    ) -> List[Tuple[float, float]]:
        """Ramer-Douglas-Peucker algorithm implementation."""
        if len(points) < 3:
            return points
        
        max_dist = 0.0
        max_idx = 0
        end_idx = len(points) - 1
        
        start = points[0]
        end = points[end_idx]
        
        for i in range(1, end_idx):
            dist = self._perpendicular_distance(points[i], start, end)
            if dist > max_dist:
                max_dist = dist
                max_idx = i
        
        if max_dist > epsilon:
            left = self._ramer_douglas_peucker(points[:max_idx + 1], epsilon)
            right = self._ramer_douglas_peucker(points[max_idx:], epsilon)
            return left[:-1] + right
        else:
            return [start, end]

    def _perpendicular_distance(
        self,
        point: Tuple[float, float],
        line_start: Tuple[float, float],
        line_end: Tuple[float, float],
    ) -> float:
        """Calculate perpendicular distance from point to line."""
        dx = line_end[0] - line_start[0]
        dy = line_end[1] - line_start[1]
        
        line_length_sq = dx * dx + dy * dy
        
        if line_length_sq == 0:
            return math.sqrt((point[0] - line_start[0]) ** 2 + (point[1] - line_start[1]) ** 2)
        
        t = max(0, min(1, ((point[0] - line_start[0]) * dx + (point[1] - line_start[1]) * dy) / line_length_sq))
        
        proj_x = line_start[0] + t * dx
        proj_y = line_start[1] + t * dy
        
        return math.sqrt((point[0] - proj_x) ** 2 + (point[1] - proj_y) ** 2)
