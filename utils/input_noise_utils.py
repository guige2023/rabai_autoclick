"""Input Noise Generator.

Adds realistic noise and variation to input events for natural automation behavior.
Simulates human-like input patterns with configurable jitter and micro-movements.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Optional, Protocol, TypeVar


T = TypeVar("T")


@dataclass
class NoiseConfig:
    """Configuration for input noise generation.

    Attributes:
        position_jitter: Maximum pixel displacement for position jitter.
        timing_jitter_ms: Range for timing variation in milliseconds.
        pressure_variation: Variation factor for pressure-sensitive inputs.
        angle_jitter_deg: Maximum angle variation in degrees.
        velocity_variance: Multiplier for velocity variation.
    """

    position_jitter: float = 3.0
    timing_jitter_ms: tuple[int, int] = (0, 15)
    pressure_variation: float = 0.05
    angle_jitter_deg: float = 2.0
    velocity_variance: float = 0.15


class NoiseGenerator:
    """Generates realistic noise for input simulation.

    Produces micro-variations that make automated inputs appear more human-like.
    Useful for reducing detection of automation scripts.

    Example:
        generator = NoiseGenerator(NoiseConfig(position_jitter=5.0))
        noisy_x, noisy_y = generator.add_position_noise(1000, 500)
        delay_ms = generator.add_timing_noise()
    """

    def __init__(self, config: Optional[NoiseConfig] = None, seed: Optional[int] = None):
        """Initialize the noise generator.

        Args:
            config: Noise configuration. Uses defaults if not provided.
            seed: Random seed for reproducibility.
        """
        self.config = config or NoiseConfig()
        self._random = random.Random(seed)

    def add_position_noise(self, x: float, y: float) -> tuple[float, float]:
        """Add Gaussian-distributed noise to position coordinates.

        Args:
            x: Original x coordinate.
            y: Original y coordinate.

        Returns:
            Tuple of (noisy_x, noisy_y).
        """
        jitter = self.config.position_jitter
        noise_x = self._random.gauss(0, jitter / 3)
        noise_y = self._random.gauss(0, jitter / 3)
        return x + noise_x, y + noise_y

    def add_timing_noise(self, base_ms: int = 0) -> int:
        """Add random timing variation.

        Args:
            base_ms: Base delay in milliseconds.

        Returns:
            Total delay in milliseconds.
        """
        min_jitter, max_jitter = self.config.timing_jitter_ms
        jitter = self._random.randint(min_jitter, max_jitter)
        return base_ms + jitter

    def add_pressure_noise(self, pressure: float) -> float:
        """Add noise to pressure-sensitive input values.

        Args:
            pressure: Original pressure value (0.0 to 1.0).

        Returns:
            Noisy pressure value clamped to valid range.
        """
        variation = self.config.pressure_variation
        noise = self._random.gauss(0, variation / 3)
        return max(0.0, min(1.0, pressure + noise))

    def add_angle_noise(self, angle_deg: float) -> float:
        """Add noise to angle measurements.

        Args:
            angle_deg: Original angle in degrees.

        Returns:
            Noisy angle in degrees.
        """
        jitter = self.config.angle_jitter_deg
        noise = self._random.gauss(0, jitter / 3)
        return angle_deg + noise

    def add_velocity_noise(self, velocity: float) -> float:
        """Add noise to velocity values.

        Args:
            velocity: Original velocity value.

        Returns:
            Noisy velocity value.
        """
        variance = self.config.velocity_variance
        noise_factor = 1.0 + self._random.gauss(0, variance / 3)
        return velocity * max(0.1, noise_factor)

    def curve_points(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        num_points: int = 10,
    ) -> list[tuple[float, float]]:
        """Generate interpolated points with noise between two positions.

        Args:
            start: Starting (x, y) coordinates.
            end: Ending (x, y) coordinates.
            num_points: Number of intermediate points.

        Returns:
            List of (x, y) tuples forming a noisy path.
        """
        points = []
        for i in range(num_points + 1):
            t = i / num_points
            x = start[0] + (end[0] - start[0]) * t
            y = start[1] + (end[1] - start[1]) * t
            noisy_x, noisy_y = self.add_position_noise(x, y)
            points.append((noisy_x, noisy_y))
        return points


class InputNoiseApplicator:
    """Applies noise to input event streams.

    Wraps input generation functions to inject realistic variations.
    Can be used as a decorator or context manager.

    Example:
        applicator = InputNoiseApplicator()
        noisy_click = applicator.wrap(click_function)
        noisy_click(x=100, y=200)
    """

    def __init__(self, config: Optional[NoiseConfig] = None):
        """Initialize the applicator.

        Args:
            config: Noise configuration for the applicator.
        """
        self.generator = NoiseGenerator(config)

    def wrap_position(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap position-aware functions with noise.

        Args:
            func: Function that accepts x, y keyword arguments.

        Returns:
            Wrapped function with position noise applied.
        """

        def wrapped(*args, **kwargs) -> T:
            if "x" in kwargs:
                kwargs["x"], kwargs["y"] = self.generator.add_position_noise(
                    kwargs.pop("x"), kwargs.pop("y")
                )
            return func(*args, **kwargs)

        return wrapped

    def wrap_timing(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap timing-sensitive functions with delays.

        Args:
            func: Function to wrap.

        Returns:
            Wrapped function with random delays.
        """

        def wrapped(*args, **kwargs) -> T:
            delay_ms = self.generator.add_timing_noise()
            time.sleep(delay_ms / 1000.0)
            return func(*args, **kwargs)

        return wrapped

    def wrap(self, func: Callable[..., T]) -> Callable[..., T]:
        """Apply all noise transformations to a function.

        Args:
            func: Function to wrap with noise.

        Returns:
            Wrapped function with all noise applied.
        """
        wrapped = self.wrap_position(func)
        return self.wrap_timing(wrapped)
