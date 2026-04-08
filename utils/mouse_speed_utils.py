"""Mouse speed and movement configuration utilities."""

from typing import Tuple, Callable, Optional
from dataclasses import dataclass
import time


@dataclass
class MouseSpeedProfile:
    """Mouse speed configuration profile."""
    base_speed: float = 500.0
    acceleration: float = 2.0
    max_speed: float = 2000.0
    smooth_factor: float = 0.5


class MouseSpeedController:
    """Control mouse speed with acceleration profiles."""

    def __init__(self, profile: Optional[MouseSpeedProfile] = None):
        """Initialize mouse speed controller.
        
        Args:
            profile: Speed profile to use.
        """
        self.profile = profile or MouseSpeedProfile()
        self._current_speed: float = self.profile.base_speed
        self._last_update: float = time.time()
        self._velocity: Tuple[float, float] = (0.0, 0.0)

    def get_speed(self, distance: float, duration: float) -> float:
        """Calculate appropriate speed for movement.
        
        Args:
            distance: Distance to move in pixels.
            duration: Duration to complete in seconds.
        
        Returns:
            Speed in pixels per second.
        """
        if duration <= 0:
            return self.profile.max_speed
        base_calc_speed = distance / duration
        speed = min(base_calc_speed * self.profile.acceleration, self.profile.max_speed)
        return max(speed, self.profile.base_speed)

    def update_velocity(self, dx: float, dy: float, dt: float) -> None:
        """Update velocity tracking.
        
        Args:
            dx, dy: Delta movement.
            dt: Time delta.
        """
        if dt <= 0:
            return
        self._velocity = (dx / dt, dy / dt)

    def get_smooth_speed(self, target_speed: float) -> float:
        """Get smoothed speed towards target.
        
        Args:
            target_speed: Target speed.
        
        Returns:
            Smoothed speed value.
        """
        self._current_speed += (target_speed - self._current_speed) * self.profile.smooth_factor
        return self._current_speed

    def calculate_duration(
        self,
        distance: float,
        speed_multiplier: float = 1.0
    ) -> float:
        """Calculate movement duration based on distance.
        
        Args:
            distance: Distance in pixels.
            speed_multiplier: Speed multiplier.
        
        Returns:
            Duration in seconds.
        """
        speed = self.profile.base_speed * speed_multiplier
        return distance / speed if speed > 0 else 0

    def ease_in_out(self, t: float) -> float:
        """Ease in-out function for smooth movement.
        
        Args:
            t: Progress 0-1.
        
        Returns:
            Eased progress value.
        """
        if t < 0.5:
            return 2 * t * t
        return 1 - ((-2 * t + 2) ** 2) / 2

    def interpolate_position(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        t: float
    ) -> Tuple[float, float]:
        """Interpolate position with easing.
        
        Args:
            start: Start (x, y).
            end: End (x, y).
            t: Progress 0-1.
        
        Returns:
            Interpolated (x, y).
        """
        eased_t = self.ease_in_out(t)
        x = start[0] + (end[0] - start[0]) * eased_t
        y = start[1] + (end[1] - start[1]) * eased_t
        return (x, y)


class SpeedCurve:
    """Predefined speed curves for different scenarios."""

    @staticmethod
    def slow() -> MouseSpeedProfile:
        """Slow speed profile for precise movements."""
        return MouseSpeedProfile(base_speed=100, acceleration=1.0, max_speed=300)

    @staticmethod
    def normal() -> MouseSpeedProfile:
        """Normal speed profile for everyday use."""
        return MouseSpeedProfile(base_speed=500, acceleration=1.5, max_speed=1500)

    @staticmethod
    def fast() -> MouseSpeedProfile:
        """Fast speed profile for quick movements."""
        return MouseSpeedProfile(base_speed=1000, acceleration=2.0, max_speed=3000)

    @staticmethod
    def gaming() -> MouseSpeedProfile:
        """Gaming profile for responsive gameplay."""
        return MouseSpeedProfile(base_speed=800, acceleration=2.5, max_speed=4000)
