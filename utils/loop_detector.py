"""
Loop Detector Utility

Detects repeated patterns in automation sequences.
Useful for identifying infinite loops and repeated actions.

Example:
    >>> detector = LoopDetector(max_history=50, threshold=3)
    >>> detector.record("click_button")
    >>> detector.record("click_button")
    >>> detector.record("click_button")
    >>> print(detector.detect_loop())  # Returns loop info if found
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class LoopInfo:
    """Information about a detected loop."""
    action: str
    repeat_count: int
    first_seen: float
    last_seen: float
    confidence: float  # 0.0 to 1.0


class LoopDetector:
    """
    Detects repeated actions that may indicate loops.

    Args:
        max_history: Maximum number of actions to keep in history.
        threshold: Minimum repeats to flag as potential loop.
        time_window: Time window in seconds to consider.
    """

    def __init__(
        self,
        max_history: int = 50,
        threshold: int = 3,
        time_window: float = 10.0,
    ) -> None:
        self.max_history = max_history
        self.threshold = threshold
        self.time_window = time_window
        self._history: list[tuple[str, float]] = []  # (action, timestamp)
        self._lock = threading.Lock()
        self._callbacks: list[Callable[[LoopInfo], None]] = []

    def record(self, action: str, metadata: Optional[Any] = None) -> None:
        """
        Record an action for loop detection.

        Args:
            action: Action identifier (e.g., "click_button").
            metadata: Optional metadata about the action.
        """
        with self._lock:
            now = time.time()

            # Prune old entries
            cutoff = now - self.time_window
            self._history = [
                (a, t) for a, t in self._history
                if t >= cutoff
            ]

            self._history.append((action, now))

            # Limit history size
            if len(self._history) > self.max_history:
                self._history = self._history[-self.max_history:]

            # Check for loop
            loop = self._check_loop(action)
            if loop:
                self._notify_loop(loop)

    def detect_loop(self) -> Optional[LoopInfo]:
        """
        Check if the current sequence contains a loop.

        Returns:
            LoopInfo if loop detected, None otherwise.
        """
        with self._lock:
            if len(self._history) < self.threshold:
                return None

            actions = [a for a, _ in self._history]
            counter = Counter(actions)

            for action, count in counter.items():
                if count >= self.threshold:
                    # Get timestamps
                    timestamps = [t for a, t in self._history if a == action]
                    return LoopInfo(
                        action=action,
                        repeat_count=count,
                        first_seen=min(timestamps),
                        last_seen=max(timestamps),
                        confidence=min(count / 10.0, 1.0),
                    )

        return None

    def _check_loop(self, action: str) -> Optional[LoopInfo]:
        """Internal check for loop on a specific action."""
        recent = [
            (a, t) for a, t in self._history[-self.max_history:]
            if a == action
        ]

        if len(recent) >= self.threshold:
            timestamps = [t for _, t in recent]
            return LoopInfo(
                action=action,
                repeat_count=len(recent),
                first_seen=min(timestamps),
                last_seen=max(timestamps),
                confidence=min(len(recent) / 10.0, 1.0),
            )

        return None

    def add_callback(self, callback: Callable[[LoopInfo], None]) -> None:
        """Register callback for loop detection events."""
        self._callbacks.append(callback)

    def _notify_loop(self, loop: LoopInfo) -> None:
        """Dispatch loop event to callbacks."""
        for cb in self._callbacks:
            try:
                cb(loop)
            except Exception:
                pass

    def get_history(self, limit: Optional[int] = None) -> list[tuple[str, float]]:
        """Get action history."""
        with self._lock:
            if limit:
                return list(self._history[-limit:])
            return list(self._history)

    def clear(self) -> None:
        """Clear action history."""
        with self._lock:
            self._history.clear()

    def get_action_counts(self) -> dict[str, int]:
        """Get counts of each action in history."""
        with self._lock:
            actions = [a for a, _ in self._history]
            return dict(Counter(actions))


class AdaptiveThrottle:
    """
    Adaptive throttling based on loop detection.

    Slows down repeated actions to prevent resource exhaustion.
    """

    def __init__(
        self,
        base_delay: float = 0.1,
        max_delay: float = 2.0,
        multiplier: float = 1.5,
    ) -> None:
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self._detector = LoopDetector(threshold=2)
        self._current_delay = base_delay
        self._last_action: Optional[str] = None

    def record_action(self, action: str) -> None:
        """Record an action and adjust throttle if needed."""
        self._detector.record(action)

        loop = self._detector.detect_loop()
        if loop and loop.action == self._last_action:
            # Same action repeating, increase delay
            self._current_delay = min(self._current_delay * self.multiplier, self.max_delay)
        else:
            # Different action, reset delay
            self._current_delay = self.base_delay

        self._last_action = action

    def get_delay(self) -> float:
        """Get current recommended delay before next action."""
        return self._current_delay

    def reset(self) -> None:
        """Reset throttle to base delay."""
        self._current_delay = self.base_delay
        self._detector.clear()
        self._last_action = None
