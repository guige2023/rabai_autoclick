"""
Scroll Momentum Utilities for UI Automation.

This module provides utilities for simulating and analyzing
scroll momentum physics in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class ScrollDirection(Enum):
    """Scroll direction."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    UNKNOWN = "unknown"


@dataclass
class ScrollFrame:
    """A single frame of scroll data."""
    offset_x: float
    offset_y: float
    velocity_x: float
    velocity_y: float
    timestamp: float
    phase: str = "active"


@dataclass
class MomentumState:
    """State of scroll momentum simulation."""
    position: float
    velocity: float
    friction: float
    timestamp: float
    is_active: bool = True


class ScrollMomentumPhysics:
    """Simulates scroll momentum with friction and decay."""

    def __init__(
        self,
        friction: float = 0.95,
        min_velocity: float = 0.5,
        max_velocity: float = 5000.0,
    ) -> None:
        self._friction: float = friction
        self._min_velocity: float = min_velocity
        self._max_velocity: float = max_velocity
        self._state_x: Optional[MomentumState] = None
        self._state_y: Optional[MomentumState] = None
        self._velocity_history: List[ScrollFrame] = []

    def set_friction(self, friction: float) -> None:
        """Set the friction coefficient (0-1)."""
        self._friction = max(0.0, min(1.0, friction))

    def apply_impulse(self, velocity_x: float, velocity_y: float) -> None:
        """Apply an initial velocity impulse to start momentum."""
        self._state_x = MomentumState(
            position=0.0,
            velocity=self._clamp_velocity(velocity_x),
            friction=self._friction,
            timestamp=time.time(),
        )
        self._state_y = MomentumState(
            position=0.0,
            velocity=self._clamp_velocity(velocity_y),
            friction=self._friction,
            timestamp=time.time(),
        )

    def update(self, dt: float) -> Tuple[float, float]:
        """Update momentum state and return position deltas."""
        dx, dy = 0.0, 0.0

        if self._state_x is not None and self._state_x.is_active:
            self._state_x.velocity *= self._friction
            dx = self._state_x.velocity * dt
            self._state_x.position += dx

            if abs(self._state_x.velocity) < self._min_velocity:
                self._state_x.is_active = False

        if self._state_y is not None and self._state_y.is_active:
            self._state_y.velocity *= self._friction
            dy = self._state_y.velocity * dt
            self._state_y.position += dy

            if abs(self._state_y.velocity) < self._min_velocity:
                self._state_y.is_active = False

        return (dx, dy)

    def is_active(self) -> bool:
        """Check if any momentum is still active."""
        x_active = self._state_x is not None and self._state_x.is_active
        y_active = self._state_y is not None and self._state_y.is_active
        return x_active or y_active

    def stop(self) -> None:
        """Immediately stop all momentum."""
        self._state_x = None
        self._state_y = None

    def _clamp_velocity(self, velocity: float) -> float:
        """Clamp velocity to allowed range."""
        return max(-self._max_velocity, min(self._max_velocity, velocity))

    def get_velocity(self) -> Tuple[float, float]:
        """Get current velocity."""
        vx = self._state_x.velocity if self._state_x else 0.0
        vy = self._state_y.velocity if self._state_y else 0.0
        return (vx, vy)

    def record_frame(
        self,
        offset_x: float,
        offset_y: float,
        velocity_x: float,
        velocity_y: float,
    ) -> None:
        """Record a scroll frame for velocity analysis."""
        frame = ScrollFrame(
            offset_x=offset_x,
            offset_y=offset_y,
            velocity_x=velocity_x,
            velocity_y=velocity_y,
            timestamp=time.time(),
        )
        self._velocity_history.append(frame)

    def calculate_flick_velocity(self) -> Tuple[float, float]:
        """Calculate velocity from the last few frames."""
        if len(self._velocity_history) < 2:
            return (0.0, 0.0)

        recent = self._velocity_history[-3:]
        if len(recent) < 2:
            return (recent[0].velocity_x, recent[0].velocity_y)

        dt = recent[-1].timestamp - recent[0].timestamp
        if dt <= 0:
            return (0.0, 0.0)

        dx = recent[-1].offset_x - recent[0].offset_x
        dy = recent[-1].offset_y - recent[0].offset_y

        return (dx / dt, dy / dt)

    def clear_history(self) -> None:
        """Clear velocity history."""
        self._velocity_history.clear()


def create_momentum_physics(
    friction: float = 0.95,
    **kwargs: Any,
) -> ScrollMomentumPhysics:
    """Create a scroll momentum physics instance."""
    return ScrollMomentumPhysics(
        friction=friction,
        min_velocity=kwargs.get("min_velocity", 0.5),
        max_velocity=kwargs.get("max_velocity", 5000.0),
    )
