"""
Scroll Acceleration Utilities

Provides scroll acceleration curves and momentum calculation for natural
scrolling behavior in automation workflows.
"""

from typing import Tuple, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import math


class AccelerationProfile(Enum):
    """Predefined acceleration profiles for scroll behavior."""
    LINEAR = "linear"
    QUADRATIC = "quadratic"
    CUBIC = "cubic"
    QUARTIC = "quartic"
    ELASTIC = "elastic"
    BOUNCE = "bounce"
    SMOOTH = "smooth"
    AGGRESSIVE = "aggressive"


@dataclass(frozen=True)
class ScrollAccelerationConfig:
    """Configuration for scroll acceleration behavior."""
    initial_velocity: float = 100.0
    max_velocity: float = 2000.0
    friction: float = 0.95
    acceleration_rate: float = 1.5
    profile: AccelerationProfile = AccelerationProfile.QUADRATIC


class ScrollAccelerator:
    """
    Calculates scroll velocity and distance based on acceleration curves.
    
    Supports multiple acceleration profiles for different scroll behaviors
    including smooth, aggressive, elastic, and bounce effects.
    
    Example:
        >>> accelerator = ScrollAccelerator(ScrollAccelerationConfig(
        ...     initial_velocity=150.0,
        ...     profile=AccelerationProfile.QUADRATIC
        ... ))
        >>> velocity, distance = accelerator.calculate(0.5)
        >>> print(f"Velocity: {velocity:.2f}, Distance: {distance:.2f}")
    """
    
    PROFILE_CURVES: dict[AccelerationProfile, Callable[[float], float]] = {
        AccelerationProfile.LINEAR: lambda t: t,
        AccelerationProfile.QUADRATIC: lambda t: t ** 2,
        AccelerationProfile.CUBIC: lambda t: t ** 3,
        AccelerationProfile.QUARTIC: lambda t: t ** 4,
        AccelerationProfile.ELASTIC: lambda t: math.sin(t * math.pi * 2) * math.exp(-t * 3),
        AccelerationProfile.BOUNCE: lambda t: abs(math.sin(t * math.pi * 3) * (1 - t)),
        AccelerationProfile.SMOOTH: lambda t: 1 - math.pow(1 - t, 3),
        AccelerationProfile.AGGRESSIVE: lambda t: math.pow(t, 0.5),
    }
    
    def __init__(self, config: Optional[ScrollAccelerationConfig] = None) -> None:
        """
        Initialize scroll accelerator with configuration.
        
        Args:
            config: Acceleration configuration. Uses defaults if not provided.
        """
        self._config = config or ScrollAccelerationConfig()
        self._curve = self.PROFILE_CURVES[self._config.profile]
        self._velocity = self._config.initial_velocity
        self._distance = 0.0
    
    @property
    def current_velocity(self) -> float:
        """Get current scroll velocity in pixels per second."""
        return self._velocity
    
    @property
    def current_distance(self) -> float:
        """Get total accumulated scroll distance in pixels."""
        return self._distance
    
    def calculate(self, delta_time: float) -> Tuple[float, float]:
        """
        Calculate velocity and distance for a time delta.
        
        Args:
            delta_time: Time elapsed since last calculation in seconds.
            
        Returns:
            Tuple of (velocity, distance) for the time delta.
        """
        progress = min(delta_time * self._config.acceleration_rate, 1.0)
        curve_value = self._curve(progress)
        
        target_velocity = self._config.initial_velocity + (
            self._config.max_velocity - self._config.initial_velocity
        ) * curve_value
        
        self._velocity = self._velocity + (target_velocity - self._velocity) * self._config.friction
        self._velocity = min(max(self._velocity, 0), self._config.max_velocity)
        
        delta_distance = self._velocity * delta_time
        self._distance += delta_distance
        
        return self._velocity, delta_distance
    
    def reset(self) -> None:
        """Reset accelerator state to initial values."""
        self._velocity = self._config.initial_velocity
        self._distance = 0.0
    
    def apply_friction(self, factor: float) -> None:
        """
        Apply additional friction to slow scrolling.
        
        Args:
            factor: Friction multiplier (0.0 to 1.0, lower = more friction).
        """
        self._velocity *= factor


class ScrollMomentumCalculator:
    """
    Calculates scroll momentum and deceleration for natural momentum scrolling.
    
    Implements physics-based momentum calculation with configurable
    friction coefficients for different surface types.
    """
    
    SURFACE_FRICTION: dict[str, float] = {
        "smooth": 0.98,
        "rough": 0.92,
        "rubber": 0.85,
        "wet": 0.88,
        "custom": 0.95,
    }
    
    def __init__(self, surface: str = "smooth") -> None:
        """
        Initialize momentum calculator with surface type.
        
        Args:
            surface: Surface type key from SURFACE_FRICTION dictionary.
        """
        self._friction = self.SURFACE_FRICTION.get(surface, 0.95)
        self._initial_velocity: float = 0.0
        self._deceleration_distance: float = 0.0
    
    def calculate_deceleration(
        self,
        initial_velocity: float,
        friction: Optional[float] = None
    ) -> Tuple[float, float]:
        """
        Calculate deceleration curve and total stopping distance.
        
        Args:
            initial_velocity: Starting velocity in pixels per second.
            friction: Override friction coefficient (0.0 to 1.0).
            
        Returns:
            Tuple of (deceleration_rate, total_distance_to_stop).
        """
        friction = friction if friction is not None else self._friction
        self._initial_velocity = abs(initial_velocity)
        
        deceleration_rate = 1.0 - friction
        total_distance = self._initial_velocity * friction / deceleration_rate if deceleration_rate > 0 else float('inf')
        
        self._deceleration_distance = total_distance
        return deceleration_rate, total_distance
    
    def get_velocity_at_distance(self, distance: float) -> float:
        """
        Get velocity at a specific distance during deceleration.
        
        Args:
            distance: Distance traveled since initial velocity.
            
        Returns:
            Velocity at the given distance.
        """
        if self._deceleration_distance == 0:
            return 0.0
        progress = min(distance / self._deceleration_distance, 1.0)
        return self._initial_velocity * (1 - progress) * self._friction
    
    def simulate_momentum(
        self,
        initial_velocity: float,
        steps: int = 60,
        dt: float = 1/60
    ) -> list[Tuple[float, float]]:
        """
        Simulate momentum scrolling over time.
        
        Args:
            initial_velocity: Starting velocity in pixels per second.
            steps: Number of simulation steps.
            dt: Time per step in seconds.
            
        Returns:
            List of (time, velocity) tuples for the simulation.
        """
        friction = self._friction
        velocity = abs(initial_velocity)
        results: list[Tuple[float, float]] = []
        time = 0.0
        
        for _ in range(steps):
            if velocity < 0.1:
                velocity = 0.0
            results.append((time, velocity))
            velocity *= friction
            time += dt
        
        return results


def create_acceleration_profile(
    profile: AccelerationProfile,
    initial_velocity: float = 100.0,
    max_velocity: float = 2000.0
) -> ScrollAccelerator:
    """
    Factory function to create a scroll accelerator with a specific profile.
    
    Args:
        profile: The acceleration profile to use.
        initial_velocity: Starting velocity in pixels per second.
        max_velocity: Maximum achievable velocity.
        
    Returns:
        Configured ScrollAccelerator instance.
    """
    config = ScrollAccelerationConfig(
        initial_velocity=initial_velocity,
        max_velocity=max_velocity,
        profile=profile
    )
    return ScrollAccelerator(config)


def calculate_fling_velocity(
    velocity_x: float,
    velocity_y: float,
    friction: float = 0.95
) -> Tuple[float, float, float]:
    """
    Calculate velocity components and magnitude for a fling gesture.
    
    Args:
        velocity_x: Initial X velocity in pixels per second.
        velocity_y: Initial Y velocity in pixels per second.
        friction: Friction coefficient affecting deceleration.
        
    Returns:
        Tuple of (velocity_magnitude, direction_angle, friction_coefficient).
    """
    magnitude = math.sqrt(velocity_x ** 2 + velocity_y ** 2)
    direction = math.atan2(velocity_y, velocity_x) if magnitude > 0 else 0.0
    return magnitude, direction, friction
