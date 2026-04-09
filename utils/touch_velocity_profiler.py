"""Touch velocity profiler for analyzing and characterizing touch gestures."""
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import math


@dataclass
class VelocityProfile:
    """Profile of a touch gesture's velocity characteristics."""
    avg_velocity: float
    max_velocity: float
    min_velocity: float
    peak_velocity: float
    peak_position: int
    velocity_std_dev: float
    direction_changes: int
    acceleration_phases: List[str]


class TouchVelocityProfiler:
    """Profiles touch velocity patterns for gesture classification and tuning.
    
    Analyzes touch trajectories to extract velocity characteristics,
    acceleration patterns, and motion profiles.
    
    Example:
        profiler = TouchVelocityProfiler()
        profile = profiler.profile(
            points=[(0, 0), (100, 100), (200, 200)],
            timestamps=[0.0, 0.1, 0.2]
        )
        print(f"Peak velocity: {profile.peak_velocity}")
    """

    def __init__(self, velocity_threshold_slow: float = 100.0, velocity_threshold_fast: float = 800.0) -> None:
        self._slow = velocity_threshold_slow
        self._fast = velocity_threshold_fast

    def profile(
        self,
        points: List[Tuple[float, float]],
        timestamps: List[float],
    ) -> Optional[VelocityProfile]:
        """Generate a velocity profile for a touch gesture."""
        if len(points) < 2 or len(timestamps) < 2:
            return None
        
        velocities = []
        for i in range(1, len(points)):
            dx = points[i][0] - points[i-1][0]
            dy = points[i][1] - points[i-1][1]
            dt = timestamps[i] - timestamps[i-1]
            if dt > 0:
                v = math.sqrt(dx * dx + dy * dy) / dt
                velocities.append(v)
        
        if not velocities:
            return None
        
        avg_v = sum(velocities) / len(velocities)
        max_v = max(velocities)
        min_v = min(velocities)
        peak_idx = velocities.index(max_v)
        
        variance = sum((v - avg_v) ** 2 for v in velocities) / len(velocities)
        std_dev = math.sqrt(variance)
        
        dir_changes = self._count_direction_changes(points)
        phases = self._identify_acceleration_phases(velocities)
        
        return VelocityProfile(
            avg_velocity=avg_v,
            max_velocity=max_v,
            min_velocity=min_v,
            peak_velocity=max_v,
            peak_position=peak_idx,
            velocity_std_dev=std_dev,
            direction_changes=dir_changes,
            acceleration_phases=phases,
        )

    def _count_direction_changes(self, points):
        if len(points) < 3:
            return 0
        changes = 0
        for i in range(1, len(points) - 1):
            dx1 = points[i][0] - points[i-1][0]
            dy1 = points[i][1] - points[i-1][1]
            dx2 = points[i+1][0] - points[i][0]
            dy2 = points[i+1][1] - points[i][1]
            cross = abs(dx1 * dy2 - dy1 * dx2)
            if abs(cross) > 100:
                changes += 1
        return changes

    def _identify_acceleration_phases(self, velocities):
        if len(velocities) < 3:
            return []
        phases = []
        for i in range(1, len(velocities)):
            diff = velocities[i] - velocities[i-1]
            if diff > 50:
                phases.append("acceleration")
            elif diff < -50:
                phases.append("deceleration")
            else:
                phases.append("constant")
        return phases

    def classify_velocity(self, velocity: float) -> str:
        if velocity < self._slow:
            return "slow"
        elif velocity > self._fast:
            return "fast"
        return "medium"

    def is_flick(self, profile: VelocityProfile) -> bool:
        return profile.avg_velocity > 500 and profile.direction_changes < 2

    def is_press_and_hold(self, profile: VelocityProfile) -> bool:
        return profile.avg_velocity < 20 and profile.direction_changes == 0

    def get_motion_quality(self, profile: VelocityProfile) -> str:
        if profile.velocity_std_dev / (profile.avg_velocity + 1) < 0.2:
            return "smooth"
        elif profile.direction_changes > 5:
            return "jerky"
        return "normal"
