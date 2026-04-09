"""
Animation Waiter Action Module.

Waits for UI animations and transitions to complete before
proceeding with automation, preventing premature interactions.
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class AnimationState:
    """State of an animated element."""
    element_id: str
    is_animating: bool
    progress: float
    duration: float
    elapsed: float


class AnimationWaiter:
    """Waits for animations and transitions to complete."""

    DEFAULT_TIMEOUT = 10.0
    DEFAULT_POLL_INTERVAL = 0.05

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ):
        """
        Initialize animation waiter.

        Args:
            timeout: Maximum wait time in seconds.
            poll_interval: Time between animation checks.
        """
        self.timeout = timeout
        self.poll_interval = poll_interval

    def wait_for_animation(
        self,
        get_state: Callable[[], AnimationState],
    ) -> bool:
        """
        Wait for an animation to complete.

        Args:
            get_state: Function that returns current AnimationState.

        Returns:
            True if animation completed, False if timeout.
        """
        start = time.time()

        while time.time() - start < self.timeout:
            state = get_state()

            if not state.is_animating:
                return True

            if state.progress >= 1.0:
                return True

            time.sleep(self.poll_interval)

        return False

    def wait_for_transition(
        self,
        get_element: Callable[[], dict],
        property_name: str,
        expected_value: any,
    ) -> bool:
        """
        Wait for a CSS transition to reach expected value.

        Args:
            get_element: Function returning element state dict.
            property_name: CSS property to watch.
            expected_value: Value to wait for.

        Returns:
            True if reached, False if timeout.
        """
        start = time.time()

        while time.time() - start < self.timeout:
            elem = get_element()
            current = elem.get(property_name)

            if current == expected_value:
                return True

            time.sleep(self.poll_interval)

        return False

    def wait_for_element_to_stop(
        self,
        get_bounds: Callable[[], tuple[int, int, int, int]],
    ) -> bool:
        """
        Wait for element bounds to stop changing.

        Args:
            get_bounds: Function returning (x1, y1, x2, y2).

        Returns:
            True if stable, False if timeout.
        """
        last_bounds = None
        stable_start = None
        required_stable = 0.1

        while time.time() - self.timeout < self.timeout:
            current_bounds = get_bounds()

            if last_bounds is None:
                last_bounds = current_bounds
                stable_start = time.time()
            elif current_bounds == last_bounds:
                if time.time() - stable_start >= required_stable:
                    return True
            else:
                last_bounds = current_bounds
                stable_start = time.time()

            time.sleep(self.poll_interval)

        return False

    def estimate_animation_duration(
        self,
        css_text: str,
    ) -> float:
        """
        Estimate animation duration from CSS text.

        Args:
            css_text: CSS property string.

        Returns:
            Estimated duration in seconds.
        """
        import re

        patterns = [
            r"transition-duration:\s*([\d.]+)(ms|s)",
            r"animation-duration:\s*([\d.]+)(ms|s)",
        ]

        max_duration = 0.0

        for pattern in patterns:
            matches = re.findall(pattern, css_text, re.IGNORECASE)
            for value, unit in matches:
                duration = float(value)
                if unit.lower() == "ms":
                    duration /= 1000.0
                max_duration = max(max_duration, duration)

        return max_duration
