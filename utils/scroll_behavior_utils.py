"""
Scroll Behavior Utilities for UI Automation

Provides smooth scrolling, momentum scrolling, and
scroll gesture simulation for touch and mouse input.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from enum import Enum, auto


class ScrollDirection(Enum):
    """Scroll direction."""
    UP = auto()
    DOWN = auto()
    LEFT = auto()
    RIGHT = auto()
    VERTICAL = auto()
    HORIZONTAL = auto()


@dataclass
class ScrollPhase:
    """Represents a phase in a scroll gesture."""
    direction: ScrollDirection
    distance: float
    velocity: float
    duration: float


@dataclass
class ScrollConfig:
    """Configuration for scroll behavior."""
    scroll_amount: int = 100
    smooth_scroll_duration: float = 0.3
    momentum_decay: float = 0.95
    flick_threshold: float = 500.0
    bounce_enabled: bool = True
    scroll_step: int = 10


@dataclass
class ScrollEvent:
    """Represents a single scroll event."""
    direction: ScrollDirection
    delta: float
    timestamp: float
    momentum: float = 0.0


class ScrollBehavior:
    """
    Manages scroll behavior with smooth and momentum scrolling.

    Provides methods for smooth scrolling, momentum-based scrolling,
    and intelligent scroll direction detection.
    """

    def __init__(self, config: ScrollConfig | None = None) -> None:
        self.config = config or ScrollConfig()
        self._velocity_history: list[float] = []
        self._last_scroll_time: float = 0.0
        self._scroll_position: int = 0
        self._momentum_active: bool = False

    def scroll_up(self, amount: int | None = None) -> ScrollEvent:
        """Scroll up by specified amount."""
        delta = amount or self.config.scroll_amount
        self._scroll_position += delta
        self._record_velocity(delta)
        return ScrollEvent(
            direction=ScrollDirection.UP,
            delta=delta,
            timestamp=time.time(),
            momentum=self._calculate_momentum(),
        )

    def scroll_down(self, amount: int | None = None) -> ScrollEvent:
        """Scroll down by specified amount."""
        delta = amount or self.config.scroll_amount
        self._scroll_position -= delta
        self._record_velocity(-delta)
        return ScrollEvent(
            direction=ScrollDirection.DOWN,
            delta=-delta,
            timestamp=time.time(),
            momentum=self._calculate_momentum(),
        )

    def scroll_left(self, amount: int | None = None) -> ScrollEvent:
        """Scroll left by specified amount."""
        delta = amount or self.config.scroll_amount
        self._record_velocity(delta)
        return ScrollEvent(
            direction=ScrollDirection.LEFT,
            delta=delta,
            timestamp=time.time(),
            momentum=self._calculate_momentum(),
        )

    def scroll_right(self, amount: int | None = None) -> ScrollEvent:
        """Scroll right by specified amount."""
        delta = amount or self.config.scroll_amount
        self._record_velocity(-delta)
        return ScrollEvent(
            direction=ScrollDirection.RIGHT,
            delta=-delta,
            timestamp=time.time(),
            momentum=self._calculate_momentum(),
        )

    def _record_velocity(self, delta: float) -> None:
        """Record scroll velocity for momentum calculation."""
        current_time = time.time()
        time_delta = current_time - self._last_scroll_time
        if time_delta > 0:
            velocity = delta / time_delta
            self._velocity_history.append(velocity)
            if len(self._velocity_history) > 10:
                self._velocity_history.pop(0)
        self._last_scroll_time = current_time

    def _calculate_momentum(self) -> float:
        """Calculate current scroll momentum."""
        if not self._velocity_history:
            return 0.0
        return sum(self._velocity_history) / len(self._velocity_history)

    def is_flick(self, velocity: float) -> bool:
        """Check if the scroll velocity qualifies as a flick gesture."""
        return abs(velocity) > self.config.flick_threshold

    def get_momentum_scroll_phases(
        self,
        initial_velocity: float,
        decay: float | None = None,
    ) -> list[ScrollPhase]:
        """
        Calculate scroll phases with momentum decay.

        Args:
            initial_velocity: Initial scroll velocity
            decay: Decay factor per phase (0.0-1.0)

        Returns:
            List of ScrollPhase objects
        """
        decay = decay or self.config.momentum_decay
        phases: list[ScrollPhase] = []
        velocity = initial_velocity
        direction = ScrollDirection.DOWN if initial_velocity < 0 else ScrollDirection.UP
        distance = 0.0
        duration = 0.0

        while abs(velocity) > 1.0 and duration < 2.0:
            phase_distance = velocity * 0.016  # ~60fps timestep
            distance += abs(phase_distance)
            duration += 0.016
            phases.append(ScrollPhase(
                direction=direction,
                distance=phase_distance,
                velocity=velocity,
                duration=0.016,
            ))
            velocity *= decay

        return phases

    def smooth_scroll_steps(
        self,
        direction: ScrollDirection,
        total_distance: float,
        num_steps: int | None = None,
    ) -> list[tuple[float, float]]:
        """
        Generate smooth scroll steps with easing.

        Args:
            direction: Scroll direction
            total_distance: Total distance to scroll
            num_steps: Number of steps (default: based on scroll_amount)

        Returns:
            List of (scroll_amount, timestamp) tuples
        """
        if num_steps is None:
            num_steps = max(1, int(abs(total_distance) / self.config.scroll_step))

        steps: list[tuple[float, float]] = []
        base_time = time.time()

        for i in range(num_steps):
            progress = (i + 1) / num_steps
            # Ease-out curve
            eased_progress = 1.0 - (1.0 - progress) ** 2
            delta = total_distance * (eased_progress - (i / num_steps))
            timestamp = base_time + (i + 1) * (self.config.smooth_scroll_duration / num_steps)
            steps.append((delta, timestamp))

        return steps

    def page_scroll(self, direction: ScrollDirection) -> int:
        """
        Perform a page scroll (full viewport scroll).

        Args:
            direction: Scroll direction

        Returns:
            Scroll delta in pixels
        """
        # Assume ~800px viewport height
        delta = 800
        if direction == ScrollDirection.DOWN:
            return -delta
        elif direction == ScrollDirection.UP:
            return delta
        return 0

    def detect_scroll_direction(
        self,
        delta_x: float,
        delta_y: float,
    ) -> ScrollDirection:
        """
        Detect primary scroll direction from delta values.

        Args:
            delta_x: Horizontal delta
            delta_y: Vertical delta

        Returns:
            Dominant ScrollDirection
        """
        if abs(delta_y) > abs(delta_x):
            return ScrollDirection.UP if delta_y > 0 else ScrollDirection.DOWN
        else:
            return ScrollDirection.LEFT if delta_x > 0 else ScrollDirection.RIGHT

    def reset(self) -> None:
        """Reset scroll state."""
        self._velocity_history = []
        self._scroll_position = 0
        self._momentum_active = False

    def get_position(self) -> int:
        """Get current scroll position."""
        return self._scroll_position


def calculate_overscroll(
    distance: float,
    boundary: float,
    elasticity: float = 0.3,
) -> float:
    """
    Calculate overscroll effect (rubber-band) at boundaries.

    Args:
        distance: Distance past boundary
        boundary: Boundary limit
        elasticity: Elasticity factor

    Returns:
        Adjusted distance with overscroll
    """
    if distance <= boundary:
        return distance
    overscroll = distance - boundary
    # Apply diminishing returns
    return boundary + overscroll * (1.0 / (1.0 + elasticity * overscroll / boundary))
