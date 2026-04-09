"""Touch trajectory validator for validating touch gesture trajectories."""
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum, auto


class ValidationResult(Enum):
    """Result of trajectory validation."""
    VALID = auto()
    TOO_SHORT = auto()
    TOO_LONG = auto()
    INVALID_PATH = auto()
    EXCESSIVE_JITTER = auto()


@dataclass
class TrajectoryValidationReport:
    """Report of trajectory validation."""
    is_valid: bool
    result: ValidationResult
    length: float
    duration: float
    avg_velocity: float
    max_deviation: float
    issues: List[str]


class TouchTrajectoryValidator:
    """Validates touch trajectories for gesture recognition.
    
    Checks trajectories for minimum length, maximum duration,
    path validity, and excessive jitter.
    
    Example:
        validator = TouchTrajectoryValidator()
        report = validator.validate(
            points=[(0,0), (50,50), (100,100)],
            timestamps=[0.0, 0.1, 0.2]
        )
        if not report.is_valid:
            print(f"Invalid: {report.issues}")
    """

    def __init__(
        self,
        min_length: float = 50.0,
        max_duration: float = 2.0,
        max_jitter: float = 10.0,
        min_points: int = 3,
    ) -> None:
        self._min_length = min_length
        self._max_duration = max_duration
        self._max_jitter = max_jitter
        self._min_points = min_points

    def validate(
        self,
        points: List[Tuple[float, float]],
        timestamps: List[float],
    ) -> Optional[TrajectoryValidationReport]:
        """Validate a touch trajectory."""
        if len(points) < self._min_points:
            return TrajectoryValidationReport(
                is_valid=False,
                result=ValidationResult.TOO_SHORT,
                length=0,
                duration=0,
                avg_velocity=0,
                max_deviation=0,
                issues=["Insufficient points"],
            )
        
        if len(timestamps) < 2:
            return None
        
        length = self._calculate_length(points)
        duration = timestamps[-1] - timestamps[0]
        velocities = self._calculate_velocities(points, timestamps)
        avg_vel = sum(velocities) / len(velocities) if velocities else 0
        max_dev = self._calculate_max_deviation(points)
        
        issues: List[str] = []
        
        if length < self._min_length:
            issues.append(f"Trajectory too short: {length:.1f} < {self._min_length}")
        
        if duration > self._max_duration:
            issues.append(f"Trajectory too long: {duration:.2f}s > {self._max_duration}s")
        
        if max_dev > self._max_jitter:
            issues.append(f"Excessive jitter: {max_dev:.1f} > {self._max_jitter}")
        
        result = ValidationResult.VALID
        if issues:
            if "Insufficient points" in issues:
                result = ValidationResult.TOO_SHORT
            elif "too short" in issues[0]:
                result = ValidationResult.TOO_SHORT
            elif "too long" in issues[0]:
                result = ValidationResult.TOO_LONG
            elif "jitter" in issues[0]:
                result = ValidationResult.EXCESSIVE_JITTER
        
        return TrajectoryValidationReport(
            is_valid=len(issues) == 0,
            result=result,
            length=length,
            duration=duration,
            avg_velocity=avg_vel,
            max_deviation=max_dev,
            issues=issues,
        )

    def _calculate_length(self, points: List[Tuple[float, float]]) -> float:
        """Calculate total path length."""
        total = 0.0
        for i in range(1, len(points)):
            dx = points[i][0] - points[i-1][0]
            dy = points[i][1] - points[i-1][1]
            total += (dx * dx + dy * dy) ** 0.5
        return total

    def _calculate_velocities(
        self,
        points: List[Tuple[float, float]],
        timestamps: List[float],
    ) -> List[float]:
        """Calculate instantaneous velocities."""
        velocities = []
        for i in range(1, len(points)):
            dx = points[i][0] - points[i-1][0]
            dy = points[i][1] - points[i-1][1]
            dt = timestamps[i] - timestamps[i-1]
            if dt > 0:
                velocities.append(((dx * dx + dy * dy) ** 0.5) / dt)
        return velocities

    def _calculate_max_deviation(self, points: List[Tuple[float, float]]) -> float:
        """Calculate maximum deviation from straight line."""
        if len(points) < 3:
            return 0.0
        
        start = points[0]
        end = points[-1]
        
        max_dev = 0.0
        for point in points[1:-1]:
            dev = self._perpendicular_distance(point, start, end)
            max_dev = max(max_dev, dev)
        
        return max_dev

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
            return ((point[0] - line_start[0]) ** 2 + (point[1] - line_start[1]) ** 2) ** 0.5
        
        t = max(0, min(1, ((point[0] - line_start[0]) * dx + (point[1] - line_start[1]) * dy) / line_length_sq))
        proj_x = line_start[0] + t * dx
        proj_y = line_start[1] + t * dy
        
        return ((point[0] - proj_x) ** 2 + (point[1] - proj_y) ** 2) ** 0.5
