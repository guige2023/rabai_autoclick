"""Input Delay Profile Utilities.

Manages configurable delay profiles for input simulation.
Supports adaptive delays based on element type and context.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class DelayProfile(Enum):
    """Predefined delay profiles.

    FAST: Minimal delays for speed testing.
    NORMAL: Balanced delays for typical automation.
    HUMAN: Realistic delays mimicking human input.
    CAUTIOUS: Extra delays for unreliable UIs.
    """

    FAST = auto()
    NORMAL = auto()
    HUMAN = auto()
    CAUTIOUS = auto()


@dataclass
class DelayConfig:
    """Configuration for input delays.

    Attributes:
        min_key_delay_ms: Minimum delay between keystrokes.
        max_key_delay_ms: Maximum delay between keystrokes.
        min_mouse_delay_ms: Minimum delay after mouse action.
        max_mouse_delay_ms: Maximum delay after mouse action.
        initial_delay_ms: Delay before first action.
        post_action_delay_ms: Fixed delay after each action.
        think_time_ms: Random think time between actions.
    """

    min_key_delay_ms: int = 30
    max_key_delay_ms: int = 80
    min_mouse_delay_ms: int = 50
    max_mouse_delay_ms: int = 150
    initial_delay_ms: int = 500
    post_action_delay_ms: int = 100
    think_time_ms: int = 200


@dataclass
class ActionDelay:
    """Delay specification for an action type.

    Attributes:
        action_type: Type of action being performed.
        base_delay_ms: Base delay in milliseconds.
        variance_ms: Random variance to add.
        skip_chance: Probability of skipping delay (0.0 to 1.0).
    """

    action_type: str
    base_delay_ms: int
    variance_ms: int = 0
    skip_chance: float = 0.0


class DelayProfileRegistry:
    """Registry of predefined delay profiles.

    Example:
        registry = DelayProfileRegistry()
        config = registry.get_config(DelayProfile.HUMAN)
    """

    PROFILES = {
        DelayProfile.FAST: DelayConfig(
            min_key_delay_ms=10,
            max_key_delay_ms=30,
            min_mouse_delay_ms=20,
            max_mouse_delay_ms=50,
            initial_delay_ms=100,
            post_action_delay_ms=0,
            think_time_ms=0,
        ),
        DelayProfile.NORMAL: DelayConfig(
            min_key_delay_ms=30,
            max_key_delay_ms=80,
            min_mouse_delay_ms=50,
            max_mouse_delay_ms=150,
            initial_delay_ms=500,
            post_action_delay_ms=100,
            think_time_ms=200,
        ),
        DelayProfile.HUMAN: DelayConfig(
            min_key_delay_ms=50,
            max_key_delay_ms=150,
            min_mouse_delay_ms=100,
            max_mouse_delay_ms=300,
            initial_delay_ms=1000,
            post_action_delay_ms=200,
            think_time_ms=500,
        ),
        DelayProfile.CAUTIOUS: DelayConfig(
            min_key_delay_ms=100,
            max_key_delay_ms=300,
            min_mouse_delay_ms=200,
            max_mouse_delay_ms=500,
            initial_delay_ms=2000,
            post_action_delay_ms=500,
            think_time_ms=1000,
        ),
    }

    @classmethod
    def get_config(cls, profile: DelayProfile) -> DelayConfig:
        """Get delay configuration for a profile.

        Args:
            profile: DelayProfile to get config for.

        Returns:
            DelayConfig for the profile.
        """
        return cls.PROFILES.get(profile, cls.PROFILES[DelayProfile.NORMAL])


class InputDelayManager:
    """Manages input delays for automation.

    Applies delays based on configured profile and action context.

    Example:
        manager = InputDelayManager(DelayProfile.HUMAN)
        manager.before_action("click")
        # ... perform click ...
        manager.after_action()
    """

    def __init__(
        self,
        profile: DelayProfile = DelayProfile.NORMAL,
        seed: Optional[int] = None,
    ):
        """Initialize the delay manager.

        Args:
            profile: DelayProfile to use.
            seed: Random seed for reproducible delays.
        """
        self.config = DelayProfileRegistry.get_config(profile)
        self._random = random.Random(seed)
        self._action_delays: dict[str, ActionDelay] = {}
        self._enabled = True

    def enable(self) -> None:
        """Enable delay application."""
        self._enabled = True

    def disable(self) -> None:
        """Disable delay application."""
        self._enabled = False

    def before_action(
        self,
        action_type: str,
        target_info: Optional[dict] = None,
    ) -> None:
        """Apply delay before an action.

        Args:
            action_type: Type of action (click, type, scroll, etc.).
            target_info: Optional target element information.
        """
        if not self._enabled:
            return

        delay_ms = self._calculate_pre_delay(action_type, target_info)
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    def after_action(
        self,
        action_type: str,
        result: Optional[dict] = None,
    ) -> None:
        """Apply delay after an action.

        Args:
            action_type: Type of action performed.
            result: Optional action result information.
        """
        if not self._enabled:
            return

        delay_ms = self._calculate_post_delay(action_type, result)
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    def _calculate_pre_delay(
        self,
        action_type: str,
        target_info: Optional[dict],
    ) -> int:
        """Calculate pre-action delay.

        Args:
            action_type: Action type.
            target_info: Target information.

        Returns:
            Delay in milliseconds.
        """
        # Check for specific action delay
        if action_type in self._action_delays:
            ad = self._action_delays[action_type]
            if self._random.random() < ad.skip_chance:
                return 0
            return ad.base_delay_ms + self._random.randint(0, ad.variance_ms)

        # Check for think time
        if self.config.think_time_ms > 0 and target_info:
            think = self._random.randint(0, self.config.think_time_ms)
            return think

        return 0

    def _calculate_post_delay(
        self,
        action_type: str,
        result: Optional[dict],
    ) -> int:
        """Calculate post-action delay.

        Args:
            action_type: Action type.
            result: Action result.

        Returns:
            Delay in milliseconds.
        """
        # Check for specific action delay
        if action_type in self._action_delays:
            ad = self._action_delays[action_type]
            return ad.base_delay_ms + self._random.randint(0, ad.variance_ms)

        # Use config-based delays
        if action_type in ("key", "type", "keyboard"):
            return self._random.randint(
                self.config.min_key_delay_ms,
                self.config.max_key_delay_ms,
            )
        elif action_type in ("click", "double_click", "right_click", "mouse"):
            return self._random.randint(
                self.config.min_mouse_delay_ms,
                self.config.max_mouse_delay_ms,
            )
        elif action_type == "scroll":
            return self._random.randint(50, 150)

        return self.config.post_action_delay_ms

    def register_action_delay(self, delay: ActionDelay) -> None:
        """Register a custom delay for an action type.

        Args:
            delay: ActionDelay specification.
        """
        self._action_delays[delay.action_type] = delay

    def get_initial_delay(self) -> int:
        """Get initial delay before first action.

        Returns:
            Delay in milliseconds.
        """
        return self.config.initial_delay_ms

    def apply_context_adaptive(
        self,
        context: dict,
    ) -> None:
        """Adjust delays based on context.

        Args:
            context: Context information with keys like:
                - is_slow_element: Element loads slowly.
                - is_critical: Critical action, minimize delay.
                - requires_wait: UI requires extra wait time.
        """
        if context.get("is_critical"):
            self.config.think_time_ms = 0
            self.config.post_action_delay_ms = 50
        elif context.get("requires_wait"):
            self.config.think_time_ms = int(self.config.think_time_ms * 2)
            self.config.post_action_delay_ms = int(self.config.post_action_delay_ms * 2)


class DelayRecorder:
    """Records actual delays for analysis.

    Example:
        recorder = DelayRecorder()
        recorder.record_pre_delay("click", 150)
        recorder.record_post_delay("click", 200)
        stats = recorder.get_stats()
    """

    def __init__(self):
        """Initialize the recorder."""
        self._pre_delays: dict[str, list[int]] = {}
        self._post_delays: dict[str, list[int]] = {}

    def record_pre_delay(self, action_type: str, delay_ms: int) -> None:
        """Record a pre-action delay.

        Args:
            action_type: Type of action.
            delay_ms: Actual delay in milliseconds.
        """
        if action_type not in self._pre_delays:
            self._pre_delays[action_type] = []
        self._pre_delays[action_type].append(delay_ms)

    def record_post_delay(self, action_type: str, delay_ms: int) -> None:
        """Record a post-action delay.

        Args:
            action_type: Type of action.
            delay_ms: Actual delay in milliseconds.
        """
        if action_type not in self._post_delays:
            self._post_delays[action_type] = []
        self._post_delays[action_type].append(delay_ms)

    def get_stats(self) -> dict[str, dict]:
        """Get statistics for recorded delays.

        Returns:
            Dictionary with statistics per action type.
        """
        stats = {}
        all_types = set(self._pre_delays.keys()) | set(self._post_delays.keys())

        for action_type in all_types:
            pre = self._pre_delays.get(action_type, [])
            post = self._post_delays.get(action_type, [])
            stats[action_type] = {
                "pre_avg": sum(pre) / len(pre) if pre else 0,
                "pre_count": len(pre),
                "post_avg": sum(post) / len(post) if post else 0,
                "post_count": len(post),
            }

        return stats
