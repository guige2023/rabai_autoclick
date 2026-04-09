"""Drag vector normalizer for standardizing drag trajectories."""
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
import math


@dataclass
class DragVector:
    """Represents a normalized drag gesture."""
    start_point: Tuple[float, float]
    end_point: Tuple[float, float]
    distance: float
    angle: float
    duration: float
    velocity: float
    direction: str


class DragVectorNormalizer:
    """Normalizes and analyzes drag vectors for consistent gesture recognition.
    
    Converts raw drag trajectories into standardized representations
    with normalized coordinates, angles, and motion characteristics.
    
    Example:
        normalizer = DragVectorNormalizer()
        vector = normalizer.normalize(
            points=[(100, 100), (150, 120), (200, 150)],
            timestamps=[0.0, 0.05, 0.1]
        )
        print(f"Drag direction: {vector.direction}, angle: {vector.angle:.1f}°")
    """

    def __init__(self, screen_width: int = 1920, screen_height: int = 1080) -> None:
        """Initialize the normalizer.
        
        Args:
            screen_width: Reference screen width for normalization.
            screen_height: Reference screen height for normalization.
        """
        self._screen_width = screen_width
        self._screen_height = screen_height

    def normalize(
        self,
        points: List[Tuple[float, float]],
        timestamps: Optional[List[float]] = None,
    ) -> Optional[DragVector]:
        """Normalize a drag trajectory into a standard vector representation.
        
        Args:
            points: List of (x, y) coordinates defining the drag path.
            timestamps: Optional list of timestamps for each point.
            
        Returns:
            Normalized DragVector or None if insufficient points.
        """
        if len(points) < 2:
            return None
        
        start = points[0]
        end = points[-1]
        
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        
        distance = math.sqrt(dx * dx + dy * dy)
        angle = math.degrees(math.atan2(dy, dx))
        
        duration = 0.0
        velocity = 0.0
        if timestamps and len(timestamps) >= 2:
            duration = timestamps[-1] - timestamps[0]
            if duration > 0:
                velocity = distance / duration
        
        direction = self._classify_direction(angle)
        
        return DragVector(
            start_point=start,
            end_point=end,
            distance=distance,
            angle=angle,
            duration=duration,
            velocity=velocity,
            direction=direction,
        )

    def normalize_points(
        self,
        points: List[Tuple[float, float]],
        target_count: int = 10,
    ) -> List[Tuple[float, float]]:
        """Resample drag points to a uniform distribution.
        
        Args:
            points: Original drag trajectory points.
            target_count: Number of points in output.
            
        Returns:
            Resampled list of (x, y) coordinates.
        """
        if len(points) <= 2:
            return points.copy()
        
        # Calculate total path length
        total_length = 0.0
        lengths = [0.0]
        for i in range(1, len(points)):
            dx = points[i][0] - points[i-1][0]
            dy = points[i][1] - points[i-1][1]
            total_length += math.sqrt(dx * dx + dy * dy)
            lengths.append(total_length)
        
        if total_length == 0:
            return points.copy()
        
        # Resample at uniform intervals
        step = total_length / (target_count - 1)
        result = [points[0]]
        
        current_length = step
        points_idx = 1
        
        for _ in range(target_count - 2):
            while points_idx < len(points) and lengths[points_idx] < current_length:
                points_idx += 1
            
            if points_idx >= len(points):
                result.append(points[-1])
                continue
            
            # Interpolate between points[points_idx-1] and points[points_idx]
            prev_length = lengths[points_idx - 1]
            segment_length = lengths[points_idx] - prev_length
            
            if segment_length > 0:
                ratio = (current_length - prev_length) / segment_length
                x = points[points_idx - 1][0] + ratio * (points[points_idx][0] - points[points_idx - 1][0])
                y = points[points_idx - 1][1] + ratio * (points[points_idx][1] - points[points_idx][1])
                result.append((x, y))
            else:
                result.append(points[points_idx])
            
            current_length += step
        
        result.append(points[-1])
        return result

    def normalize_coordinates(
        self,
        points: List[Tuple[float, float]],
    ) -> List[Tuple[float, float]]:
        """Normalize point coordinates relative to screen dimensions.
        
        Args:
            points: Raw screen coordinates.
            
        Returns:
            Normalized coordinates in [0, 1] range.
        """
        return [
            (x / self._screen_width, y / self._screen_height)
            for x, y in points
        ]

    def denormalize_coordinates(
        self,
        points: List[Tuple[float, float]],
    ) -> List[Tuple[int, int]]:
        """Convert normalized coordinates back to screen pixels.
        
        Args:
            points: Normalized coordinates in [0, 1] range.
            
        Returns:
            Screen pixel coordinates.
        """
        return [
            (int(x * self._screen_width), int(y * self._screen_height))
            for x, y in points
        ]

    def _classify_direction(self, angle: float) -> str:
        """Classify drag angle into a named direction.
        
        Args:
            angle: Angle in degrees (0 = right, 90 = down).
            
        Returns:
            Direction name like "right", "up-right", "down-left", etc.
        """
        directions = [
            (-22.5, 22.5, "right"),
            (22.5, 67.5, "down-right"),
            (67.5, 112.5, "down"),
            (112.5, 157.5, "down-left"),
            (157.5, 180, "left"),
            (-180, -157.5, "left"),
            (-157.5, -112.5, "up-left"),
            (-112.5, -67.5, "up"),
            (-67.5, -22.5, "up-right"),
        ]
        
        for start, end, name in directions:
            if start <= angle < end:
                return name
        return "right"

    def calculate_smoothness(
        self,
        points: List[Tuple[float, float]],
    ) -> float:
        """Calculate trajectory smoothness (0 = jerky, 1 = perfectly smooth).
        
        Args:
            points: Drag trajectory points.
            
        Returns:
            Smoothness score between 0 and 1.
        """
        if len(points) < 3:
            return 1.0
        
        # Calculate direction changes
        direction_changes = 0
        for i in range(1, len(points) - 1):
            dx1 = points[i][0] - points[i-1][0]
            dy1 = points[i][1] - points[i-1][1]
            dx2 = points[i+1][0] - points[i][0]
            dy2 = points[i+1][1] - points[i][1]
            
            # Cross product magnitude indicates direction change
            cross = abs(dx1 * dy2 - dy1 * dx2)
            len1 = math.sqrt(dx1 * dx1 + dy1 * dy1)
            len2 = math.sqrt(dx2 * dx2 + dy2 * dy2)
            
            if len1 > 0 and len2 > 0:
                direction_changes += cross / (len1 * len2)
        
        avg_change = direction_changes / max(len(points) - 2, 1)
        return max(0.0, 1.0 - avg_change)

    def get_characteristics(
        self,
        points: List[Tuple[float, float]],
        timestamps: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        """Get comprehensive characteristics of a drag gesture.
        
        Args:
            points: Drag trajectory points.
            timestamps: Optional timestamps for velocity calculations.
            
        Returns:
            Dictionary with gesture characteristics.
        """
        vector = self.normalize(points, timestamps)
        if not vector:
            return {}
        
        smoothness = self.calculate_smoothness(points)
        normalized = self.normalize_points(points, 10)
        
        # Calculate bounding box
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        
        return {
            "direction": vector.direction,
            "angle": round(vector.angle, 2),
            "distance": round(vector.distance, 2),
            "duration": round(vector.duration, 3) if vector.duration else 0,
            "velocity": round(vector.velocity, 2) if vector.velocity else 0,
            "smoothness": round(smoothness, 3),
            "bounding_box": {
                "min_x": min(xs),
                "max_x": max(xs),
                "min_y": min(ys),
                "max_y": max(ys),
            },
            "point_count": len(points),
        }
