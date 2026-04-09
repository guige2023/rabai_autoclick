"""Mouse trajectory utilities for natural movement simulation.

This module provides utilities for generating natural mouse trajectories
with bezier curves, noise, and human-like movement patterns, useful
for making automation feel more natural.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Tuple, Optional, Callable
import math
import random


class TrajectoryType(Enum):
    """Type of trajectory generation."""
    LINEAR = auto()
    BEZIER = auto()
    CURVED = auto()
    NATURAL = auto()
    BOUNCE = auto()


@dataclass
class TrajectoryConfig:
    """Configuration for trajectory generation."""
    trajectory_type: TrajectoryType = TrajectoryType.NATURAL
    duration_ms: float = 500.0
    steps: int = 30
    noise_factor: float = 0.1
    overshoot: float = 0.0
    bounce_factor: float = 0.3


def generate_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    config: Optional[TrajectoryConfig] = None,
) -> List[Tuple[int, int]]:
    """Generate a mouse trajectory between two points.
    
    Args:
        start_x: Start X coordinate.
        start_y: Start Y coordinate.
        end_x: End X coordinate.
        end_y: End Y coordinate.
        config: Trajectory configuration.
    
    Returns:
        List of (x, y) coordinates for the trajectory.
    """
    config = config or TrajectoryConfig()
    
    if config.trajectory_type == TrajectoryType.LINEAR:
        return _linear_trajectory(start_x, start_y, end_x, end_y, config.steps)
    elif config.trajectory_type == TrajectoryType.BEZIER:
        return _bezier_trajectory(start_x, start_y, end_x, end_y, config.steps)
    elif config.trajectory_type == TrajectoryType.CURVED:
        return _curved_trajectory(start_x, start_y, end_x, end_y, config.steps)
    elif config.trajectory_type == TrajectoryType.BOUNCE:
        return _bounce_trajectory(start_x, start_y, end_x, end_y, config.steps, config.bounce_factor)
    else:
        return _natural_trajectory(start_x, start_y, end_x, end_y, config.steps, config.noise_factor)


def _linear_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int,
) -> List[Tuple[int, int]]:
    """Generate linear trajectory."""
    trajectory = []
    for i in range(steps + 1):
        t = i / steps
        x = int(start_x + (end_x - start_x) * t)
        y = int(start_y + (end_y - start_y) * t)
        trajectory.append((x, y))
    return trajectory


def _bezier_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int,
) -> List[Tuple[int, int]]:
    """Generate bezier curve trajectory."""
    control_x = start_x + (end_x - start_x) * 0.5 + random.randint(-50, 50)
    control_y = start_y - 50 + random.randint(-20, 20)
    
    trajectory = []
    for i in range(steps + 1):
        t = i / steps
        one_minus_t = 1 - t
        
        x = int(
            one_minus_t ** 2 * start_x +
            2 * one_minus_t * t * control_x +
            t ** 2 * end_x
        )
        y = int(
            one_minus_t ** 2 * start_y +
            2 * one_minus_t * t * control_y +
            t ** 2 * end_y
        )
        trajectory.append((x, y))
    
    return trajectory


def _curved_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int,
) -> List[Tuple[int, int]]:
    """Generate curved trajectory using sine wave."""
    trajectory = []
    mid_x = (start_x + end_x) // 2
    amplitude = (end_y - start_y) // 4
    
    for i in range(steps + 1):
        t = i / steps
        x = int(start_x + (end_x - start_x) * t)
        curve = math.sin(t * math.pi) * amplitude
        y = int(start_y + (end_y - start_y) * t + curve)
        trajectory.append((x, y))
    
    return trajectory


def _natural_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int,
    noise_factor: float,
) -> List[Tuple[int, int]]:
    """Generate natural-looking trajectory with noise."""
    dx = end_x - start_x
    dy = end_y - start_y
    
    max_noise_x = int(abs(dx) * noise_factor)
    max_noise_y = int(abs(dy) * noise_factor)
    
    trajectory = []
    for i in range(steps + 1):
        t = i / steps
        
        ease_t = _ease_out_quart(t)
        
        x = start_x + int(dx * ease_t)
        y = start_y + int(dy * ease_t)
        
        if i > 0 and i < steps:
            noise_x = random.randint(-max_noise_x, max_noise_x) if max_noise_x > 0 else 0
            noise_y = random.randint(-max_noise_y, max_noise_y) if max_noise_y > 0 else 0
            x += noise_x
            y += noise_y
        
        trajectory.append((x, y))
    
    trajectory[-1] = (end_x, end_y)
    
    return trajectory


def _bounce_trajectory(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    steps: int,
    bounce_factor: float,
) -> List[Tuple[int, int]]:
    """Generate trajectory with bounce effect."""
    trajectory = []
    
    bounce_height = abs(end_y - start_y) * bounce_factor
    
    for i in range(steps + 1):
        t = i / steps
        
        x = int(start_x + (end_x - start_x) * t)
        
        linear_y = start_y + (end_y - start_y) * t
        bounce = bounce_height * math.sin(t * math.pi * 2) * (1 - t)
        y = int(linear_y - bounce)
        
        trajectory.append((x, y))
    
    trajectory[-1] = (end_x, end_y)
    
    return trajectory


def _ease_out_quart(t: float) -> float:
    """Ease out quart function."""
    return 1 - (1 - t) ** 4


def get_trajectory_velocity_profile(
    trajectory: List[Tuple[int, int]],
    duration_ms: float,
) -> List[float]:
    """Calculate velocity at each point of trajectory.
    
    Args:
        trajectory: List of (x, y) coordinates.
        duration_ms: Total duration in milliseconds.
    
    Returns:
        List of velocities (pixels per ms) at each point.
    """
    velocities = []
    step_ms = duration_ms / len(trajectory)
    
    for i, (x, y) in enumerate(trajectory):
        if i == 0:
            velocities.append(0.0)
        else:
            prev_x, prev_y = trajectory[i - 1]
            dx = x - prev_x
            dy = y - prev_y
            distance = math.sqrt(dx * dx + dy * dy)
            velocity = distance / step_ms
            velocities.append(velocity)
    
    return velocities


def smooth_trajectory(
    trajectory: List[Tuple[int, int]],
    window_size: int = 3,
) -> List[Tuple[int, int]]:
    """Smooth trajectory using moving average.
    
    Args:
        trajectory: Input trajectory.
        window_size: Smoothing window size.
    
    Returns:
        Smoothed trajectory.
    """
    if len(trajectory) <= window_size:
        return trajectory
    
    smoothed = []
    half_window = window_size // 2
    
    for i in range(len(trajectory)):
        start_idx = max(0, i - half_window)
        end_idx = min(len(trajectory), i + half_window + 1)
        
        window_points = trajectory[start_idx:end_idx]
        avg_x = int(sum(p[0] for p in window_points) / len(window_points))
        avg_y = int(sum(p[1] for p in window_points) / len(window_points))
        smoothed.append((avg_x, avg_y))
    
    smoothed[-1] = trajectory[-1]
    
    return smoothed
