"""
Watchdog Timer Utility

Monitors for stalled operations and timeout conditions.
Useful for detecting hung automation steps.

Example:
    >>> watchdog = WatchdogTimer(timeout=10.0)
    >>> watchdog.start()
    >>> # ... perform operation ...
    >>> watchdog.stop()
    >>> if watchdog.timed_out:
    ...     print("Operation timed out!")
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional


class WatchdogTimer:
    """
    A timer that monitors for timeout conditions.

    Args:
        timeout: Timeout duration in seconds.
        on_timeout: Optional callback when timeout occurs.
    """

    def __init__(
        self,
        timeout: float,
        on_timeout: Optional[Callable[[], None]] = None,
    ) -> None:
        self.timeout = timeout
        self.on_timeout = on_timeout
        self._start_time: Optional[float] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._timed_out = False
        self._lock = threading.Lock()
        self._cancelled = threading.Event()

    def start(self) -> None:
        """Start the watchdog timer."""
        if self._running:
            return

        self._start_time = time.time()
        self._running = True
        self._timed_out = False
        self._cancelled.clear()
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> bool:
        """
        Stop the watchdog timer.

        Returns:
            True if timer was running and stopped, False otherwise.
        """
        if not self._running:
            return False

        self._running = False
        self._cancelled.set()

        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None

        return True

    def reset(self) -> None:
        """Reset the timer to zero."""
        with self._lock:
            if self._running:
                self._start_time = time.time()

    def extend(self, additional_time: float) -> None:
        """Extend the timeout by additional_time seconds."""
        with self._lock:
            if self._start_time is not None:
                self._start_time += additional_time

    @property
    def timed_out(self) -> bool:
        """Return whether timeout was triggered."""
        return self._timed_out

    @property
    def elapsed(self) -> float:
        """Return elapsed time since start."""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    @property
    def remaining(self) -> float:
        """Return remaining time before timeout."""
        remaining = self.timeout - self.elapsed
        return max(0.0, remaining)

    def _watch_loop(self) -> None:
        """Background monitoring loop."""
        deadline = self._start_time + self.timeout if self._start_time else 0.0

        while self._running:
            if self._cancelled.wait(0.1):
                break

            if time.time() >= deadline:
                with self._lock:
                    if self._running and not self._timed_out:
                        self._timed_out = True
                        if self.on_timeout:
                            try:
                                self.on_timeout()
                            except Exception:
                                pass
                break


class MultiWatchdog:
    """
    Manages multiple watchdog timers for parallel operation monitoring.

    Args:
        default_timeout: Default timeout for new timers.
    """

    def __init__(self, default_timeout: float = 30.0) -> None:
        self.default_timeout = default_timeout
        self._timers: dict[str, WatchdogTimer] = {}
        self._lock = threading.Lock()

    def start(
        self,
        name: str,
        timeout: Optional[float] = None,
        on_timeout: Optional[Callable[[], None]] = None,
    ) -> WatchdogTimer:
        """
        Start a named watchdog timer.

        Args:
            name: Timer identifier.
            timeout: Timeout in seconds (uses default if not provided).
            on_timeout: Optional callback.

        Returns:
            The WatchdogTimer instance.
        """
        with self._lock:
            if name in self._timers:
                self._timers[name].stop()

            timer = WatchdogTimer(
                timeout=timeout or self.default_timeout,
                on_timeout=on_timeout,
            )
            timer.start()
            self._timers[name] = timer
            return timer

    def stop(self, name: str) -> bool:
        """
        Stop a named watchdog timer.

        Returns:
            True if timer was found and stopped.
        """
        with self._lock:
            if name in self._timers:
                self._timers[name].stop()
                del self._timers[name]
                return True
        return False

    def reset(self, name: str) -> bool:
        """Reset a named watchdog timer."""
        with self._lock:
            if name in self._timers:
                self._timers[name].reset()
                return True
        return False

    def extend(self, name: str, additional_time: float) -> bool:
        """Extend a named watchdog timer's deadline."""
        with self._lock:
            if name in self._timers:
                self._timers[name].extend(additional_time)
                return True
        return False

    def is_running(self, name: str) -> bool:
        """Check if a timer is running."""
        with self._lock:
            return name in self._timers and self._timers[name]._running

    def get_timed_out(self) -> list[str]:
        """Get list of timers that have timed out."""
        with self._lock:
            return [name for name, timer in self._timers.items() if timer.timed_out]

    def stop_all(self) -> None:
        """Stop all running timers."""
        with self._lock:
            for timer in self._timers.values():
                timer.stop()
            self._timers.clear()
