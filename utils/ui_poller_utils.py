"""
UI polling utilities for waiting on element states.

This module provides utilities for polling UI elements until
a condition is met or a timeout occurs.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from typing import Callable, Optional, Any, List
from enum import Enum, auto


class PollResult(Enum):
    """Result of a poll operation."""
    SUCCESS = auto()
    TIMEOUT = auto()
    ERROR = auto()


@dataclass
class PollOutcome:
    """
    Outcome of a polling operation.

    Attributes:
        result: The poll result status.
        value: The polled value if successful.
        iterations: Number of poll iterations.
        elapsed: Total time elapsed.
        error: Error message if failed.
    """
    result: PollResult
    value: Any = None
    iterations: int = 0
    elapsed: float = 0.0
    error: Optional[str] = None


class Poller:
    """
    Polls a condition until success, timeout, or error.

    Provides flexible waiting with various strategies
    and post-poll callbacks.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        interval: float = 0.5,
    ) -> None:
        self._timeout = timeout
        self._interval = interval
        self._on_success: Optional[Callable[[Any], None]] = None
        self._on_timeout: Optional[Callable[[], None]] = None
        self._on_error: Optional[Callable[[Exception], None]] = None

    def timeout(self, seconds: float) -> Poller:
        """Set poll timeout."""
        self._timeout = max(0.1, seconds)
        return self

    def interval(self, seconds: float) -> Poller:
        """Set poll interval."""
        self._interval = max(0.01, seconds)
        return self

    def on_success(self, callback: Callable[[Any], None]) -> Poller:
        """Set callback for successful poll."""
        self._on_success = callback
        return self

    def on_timeout(self, callback: Callable[[], None]) -> Poller:
        """Set callback for timeout."""
        self._on_timeout = callback
        return self

    def on_error(self, callback: Callable[[Exception], None]) -> Poller:
        """Set callback for error."""
        self._on_error = callback
        return self

    def poll(
        self,
        condition: Callable[[], Any],
    ) -> PollOutcome:
        """
        Poll condition until truthy, timeout, or exception.

        Args:
            condition: Callable that returns the value to check.

        Returns:
            PollOutcome with result and statistics.
        """
        start_time = time.time()
        iterations = 0

        while True:
            iterations += 1
            elapsed = time.time() - start_time

            if elapsed >= self._timeout:
                if self._on_timeout:
                    self._on_timeout()
                return PollOutcome(
                    result=PollResult.TIMEOUT,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=f"Timeout after {elapsed:.2f}s",
                )

            try:
                value = condition()

                if value:
                    if self._on_success:
                        self._on_success(value)
                    return PollOutcome(
                        result=PollResult.SUCCESS,
                        value=value,
                        iterations=iterations,
                        elapsed=elapsed,
                    )

            except Exception as e:
                if self._on_error:
                    self._on_error(e)
                return PollOutcome(
                    result=PollResult.ERROR,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=str(e),
                )

            time.sleep(self._interval)


class ExponentialBackoffPoller(Poller):
    """
    Poller with exponentially increasing intervals.

    Starts at a base interval and increases until max.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        base_interval: float = 0.1,
        max_interval: float = 5.0,
        multiplier: float = 1.5,
    ) -> None:
        super().__init__(timeout, base_interval)
        self._base_interval = base_interval
        self._max_interval = max_interval
        self._multiplier = multiplier

    def poll(self, condition: Callable[[], Any]) -> PollOutcome:
        """Poll with exponential backoff."""
        start_time = time.time()
        iterations = 0
        current_interval = self._base_interval

        while True:
            iterations += 1
            elapsed = time.time() - start_time

            if elapsed >= self._timeout:
                if self._on_timeout:
                    self._on_timeout()
                return PollOutcome(
                    result=PollResult.TIMEOUT,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=f"Timeout after {elapsed:.2f}s",
                )

            try:
                value = condition()

                if value:
                    if self._on_success:
                        self._on_success(value)
                    return PollOutcome(
                        result=PollResult.SUCCESS,
                        value=value,
                        iterations=iterations,
                        elapsed=elapsed,
                    )

            except Exception as e:
                if self._on_error:
                    self._on_error(e)
                return PollOutcome(
                    result=PollResult.ERROR,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=str(e),
                )

            time.sleep(current_interval)
            current_interval = min(current_interval * self._multiplier, self._max_interval)


class UntilPoller(Poller):
    """
    Poller that waits until a specific value is returned.

    Useful for waiting for an element to reach a specific state.
    """

    def __init__(
        self,
        expected: Any,
        timeout: float = 30.0,
        interval: float = 0.5,
    ) -> None:
        super().__init__(timeout, interval)
        self._expected = expected

    def until(
        self,
        condition: Callable[[], Any],
    ) -> PollOutcome:
        """Poll until condition returns expected value."""
        start_time = time.time()
        iterations = 0

        while True:
            iterations += 1
            elapsed = time.time() - start_time

            if elapsed >= self._timeout:
                if self._on_timeout:
                    self._on_timeout()
                return PollOutcome(
                    result=PollResult.TIMEOUT,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=f"Timeout after {elapsed:.2f}s",
                )

            try:
                value = condition()

                if value == self._expected:
                    if self._on_success:
                        self._on_success(value)
                    return PollOutcome(
                        result=PollResult.SUCCESS,
                        value=value,
                        iterations=iterations,
                        elapsed=elapsed,
                    )

            except Exception as e:
                if self._on_error:
                    self._on_error(e)
                return PollOutcome(
                    result=PollResult.ERROR,
                    iterations=iterations,
                    elapsed=elapsed,
                    error=str(e),
                )

            time.sleep(self._interval)


def wait_for(
    condition: Callable[[], Any],
    timeout: float = 30.0,
    interval: float = 0.5,
) -> Any:
    """
    Simple wait_for helper.

    Blocks until condition returns truthy or timeout.

    Returns the polled value or raises TimeoutError.
    """
    outcome = Poller(timeout=timeout, interval=interval).poll(condition)

    if outcome.result == PollResult.SUCCESS:
        return outcome.value

    if outcome.result == PollResult.TIMEOUT:
        raise TimeoutError(outcome.error)

    raise RuntimeError(outcome.error)


def wait_until(
    condition: Callable[[], Any],
    expected: Any,
    timeout: float = 30.0,
    interval: float = 0.5,
) -> Any:
    """
    Wait until condition returns a specific value.

    Blocks until condition returns expected value or timeout.
    """
    outcome = UntilPoller(expected, timeout, interval).until(condition)

    if outcome.result == PollResult.SUCCESS:
        return outcome.value

    if outcome.result == PollResult.TIMEOUT:
        raise TimeoutError(outcome.error)

    raise RuntimeError(outcome.error)


class ElementStateWatcher:
    """
    Watches element state changes over time.

    Useful for detecting when elements appear, disappear,
    or change state.
    """

    def __init__(
        self,
        poll_interval: float = 0.5,
    ) -> None:
        self._poll_interval = poll_interval
        self._callbacks: List[Callable[[str, Any], None]] = []
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._last_state: dict = {}

    def add_callback(self, callback: Callable[[str, Any], None]) -> ElementStateWatcher:
        """Add callback for state changes."""
        self._callbacks.append(callback)
        return self

    def watch(
        self,
        name: str,
        getter: Callable[[], Any],
        timeout: float = 30.0,
    ) -> ElementStateWatcher:
        """Start watching a value."""
        self._last_state[name] = getter()

        def watch_loop() -> None:
            start_time = time.time()
            while self._running:
                if time.time() - start_time > timeout:
                    break

                try:
                    current = getter()
                    if current != self._last_state.get(name):
                        self._last_state[name] = current
                        for callback in self._callbacks:
                            callback(name, current)
                except Exception:
                    pass

                time.sleep(self._poll_interval)

        self._running = True
        self._thread = threading.Thread(target=watch_loop, daemon=True)
        self._thread.start()

        return self

    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
