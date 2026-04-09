"""
Physics-based animation utilities for UI automation.

Provides spring, bounce, and physics-based animations for smooth
UI transitions and gesture simulations.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Optional, Protocol


@dataclass
class SpringConfig:
    """Configuration for spring-based animations."""
    stiffness: float = 180.0
    damping: float = 12.0
    mass: float = 1.0
    threshold: float = 0.001


@dataclass
class PhysicsState:
    """State of a physics simulation at a point in time."""
    position: float
    velocity: float
    acceleration: float
    time: float


class SpringInterpolator:
    """Spring-based interpolation for natural animations."""
    
    def __init__(self, config: Optional[SpringConfig] = None):
        self.config = config or SpringConfig()
        self._state: Optional[PhysicsState] = None
    
    def start(self, from_value: float, to_value: float) -> float:
        """Start spring animation from one value to another."""
        self._state = PhysicsState(
            position=from_value,
            velocity=0.0,
            acceleration=0.0,
            time=0.0
        )
        return self._tick(self._state, to_value)
    
    def _tick(self, state: PhysicsState, target: float) -> float:
        """Calculate next state based on spring physics."""
        displacement = state.position - target
        spring_force = -self.config.stiffness * displacement
        damping_force = -self.config.damping * state.velocity
        total_force = spring_force + damping_force
        
        new_acceleration = total_force / self.config.mass
        new_velocity = state.velocity + new_acceleration * 0.016
        new_position = state.position + new_velocity * 0.016
        
        self._state = PhysicsState(
            position=new_position,
            velocity=new_velocity,
            acceleration=new_acceleration,
            time=state.time + 0.016
        )
        return new_position
    
    def step(self, target: float) -> float:
        """Advance one frame toward target."""
        if self._state is None:
            return target
        return self._tick(self._state, target)
    
    def is_settled(self) -> bool:
        """Check if animation has settled."""
        if self._state is None:
            return True
        return (
            abs(self._state.velocity) < self.config.threshold and
            abs(self._state.position - self._find_equilibrium()) < self.config.threshold
        )
    
    def _find_equilibrium(self) -> float:
        """Find equilibrium position of spring."""
        if self._state is None:
            return 0.0
        return self._state.position


class BounceCurve:
    """Predefined bounce easing curves."""
    
    @staticmethod
    def ease_in_bounce(t: float) -> float:
        """Ease-in bounce: starts slow, ends with bounce."""
        if t < 0.5:
            return BounceCurve._ease_in(t * 2)
        return BounceCurve._ease_out_bounce(t * 2 - 1)
    
    @staticmethod
    def ease_out_bounce(t: float) -> float:
        """Ease-out bounce: starts fast, ends with bounce."""
        if t < 0.5:
            return BounceCurve._ease_out(t * 2)
        return BounceCurve._ease_in_bounce(t * 2 - 1)
    
    @staticmethod
    def _ease_in(t: float) -> float:
        return t * t * t
    
    @staticmethod
    def _ease_out(t: float) -> float:
        return 1 - (1 - t) ** 3
    
    @staticmethod
    def _ease_in_bounce(t: float) -> float:
        if t < 0.5:
            return (1 - BounceCurve._ease_out_bounce(1 - t * 2)) * 0.5 + 0.5
        return BounceCurve._ease_out_bounce(t * 2 - 1) * 0.5 + 0.5
    
    @staticmethod
    def _ease_out_bounce(t: float) -> float:
        if t < 1 / 2.75:
            return 7.5625 * t * t
        elif t < 2 / 2.75:
            t -= 1.5 / 2.75
            return 7.5625 * t * t + 0.75
        elif t < 2.5 / 2.75:
            t -= 2.25 / 2.75
            return 7.5625 * t * t + 0.9375
        t -= 2.625 / 2.75
        return 7.5625 * t * t + 0.984375


class PhysicsEngine:
    """2D physics engine for gesture simulation."""
    
    def __init__(self):
        self._gravity: float = 980.0
        self._friction: float = 0.85
    
    def simulate_fall(
        self,
        start_y: float,
        velocity_y: float,
        dt: float
    ) -> tuple[float, float]:
        """Simulate object falling under gravity."""
        new_velocity = velocity_y + self._gravity * dt
        new_y = start_y + new_velocity * dt
        return new_y, new_velocity
    
    def apply_friction(self, velocity_x: float, velocity_y: float) -> tuple[float, float]:
        """Apply friction to velocities."""
        return velocity_x * self._friction, velocity_y * self._friction
    
    def simulate_bounce(
        self,
        position: float,
        velocity: float,
        ground_y: float,
        restitution: float = 0.6
    ) -> tuple[float, float, bool]:
        """Simulate bounce off ground."""
        if position >= ground_y:
            new_velocity = -velocity * restitution
            return ground_y, new_velocity, True
        return position, velocity, False


@dataclass
class TrajectoryPoint:
    """A point in a physics-based trajectory."""
    x: float
    y: float
    vx: float
    vy: float
    time: float


class TrajectorySimulator:
    """Simulate projectile-like trajectories for gestures."""
    
    def __init__(self, gravity: float = 980.0):
        self._gravity = gravity
    
    def parabolic(
        self,
        start: tuple[float, float],
        velocity: tuple[float, float],
        duration: float,
        steps: int = 60
    ) -> list[TrajectoryPoint]:
        """Generate parabolic trajectory points."""
        points = []
        dt = duration / steps
        x, y = start
        vx, vy = velocity
        
        for i in range(steps + 1):
            points.append(TrajectoryPoint(x, y, vx, vy, i * dt))
            vy += self._gravity * dt
            x += vx * dt
            y += vy * dt
        
        return points
    
    def arc(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        peak_height: float,
        steps: int = 60
    ) -> list[TrajectoryPoint]:
        """Generate arc trajectory from start to end with peak height."""
        points = []
        for i in range(steps + 1):
            t = i / steps
            x = start[0] + (end[0] - start[0]) * t
            y = start[1] + (end[1] - start[1]) * t
            arc = 4 * peak_height * t * (1 - t)
            y -= arc
            points.append(TrajectoryPoint(x, y, 0, 0, t * duration))
        
        duration = 1.0
        return points
