"""Timeout and deadline utilities.

Provides timeout handling for operations and
deadline-based execution control.
"""

import signal
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when operation times out."""
    pass


class Deadline:
    """Deadline tracker for time-bound operations.

    Example:
        deadline = Deadline(timeout=5.0)
        while not deadline.expired:
            if deadline.remaining <= 1.0:
                print("Last second!")
            time.sleep(0.1)
    """

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout
        self._start = time.monotonic()

    @property
    def elapsed(self) -> float:
        """Get elapsed time since creation."""
        return time.monotonic() - self._start

    @property
    def remaining(self) -> float:
        """Get remaining time until deadline."""
        return max(0.0, self._timeout - self.elapsed)

    @property
    def expired(self) -> bool:
        """Check if deadline has passed."""
        return self.elapsed >= self._timeout

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for timeout or deadline.

        Args:
            timeout: Additional timeout.

        Returns:
            True if deadline not yet reached.
        """
        wait_time = min(self.remaining, timeout) if timeout else self.remaining
        if wait_time > 0:
            time.sleep(wait_time)
        return not self.expired


@contextmanager
def timeout_context(seconds: float, error_message: str = "Operation timed out"):
    """Context manager for timeout.

    Example:
        with timeout_context(5.0):
            do_long_operation()
    """
    def timeout_handler(signum, frame):
        raise TimeoutError(error_message)

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(int(seconds))
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def with_timeout(func: Callable[..., T], timeout: float) -> Callable[..., Optional[T]]:
    """Decorator to add timeout to function.

    Example:
        @with_timeout(5.0)
        def long_operation():
            pass
    """
    def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
        try:
            with timeout_context(timeout):
                return func(*args, **kwargs)
        except TimeoutError:
            return None
    return wrapper


class Timer:
    """One-shot or periodic timer.

    Example:
        timer = Timer(interval=1.0, callback=do_work)
        timer.start()
        timer.stop()
    """

    def __init__(
        self,
        interval: float,
        callback: Callable[[], None],
        once: bool = False,
    ) -> None:
        self._interval = interval
        self._callback = callback
        self._once = once
        self._running = False
        self._timer: Optional[threading.Timer] = None

    def start(self) -> None:
        """Start the timer."""
        if self._running:
            return
        self._running = True
        self._schedule()

    def stop(self) -> None:
        """Stop the timer."""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _run(self) -> None:
        if not self._running:
            return
        try:
            self._callback()
        except Exception:
            pass
        if self._running and not self._once:
            self._schedule()

    def _schedule(self) -> None:
        if self._running:
            self._timer = threading.Timer(self._interval, self._run)
            self._timer.daemon = True
            self._timer.start()

    @property
    def running(self) -> bool:
        return self._running


class Stopwatch:
    """High-precision stopwatch for timing.

    Example:
        sw = Stopwatch()
        sw.start()
        do_work()
        print(f"Elapsed: {sw.elapsed:.3f}s")
        sw.stop()
    """

    def __init__(self) -> None:
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._running = False

    def start(self) -> None:
        """Start the stopwatch."""
        self._start_time = time.perf_counter()
        self._running = True
        self._end_time = None

    def stop(self) -> None:
        """Stop the stopwatch."""
        if self._running:
            self._end_time = time.perf_counter()
            self._running = False

    def reset(self) -> None:
        """Reset the stopwatch."""
        self._start_time = None
        self._end_time = None
        self._running = False

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self._start_time is None:
            return 0.0
        if self._running:
            return time.perf_counter() - self._start_time
        if self._end_time is not None:
            return self._end_time - self._start_time
        return 0.0

    def __enter__(self) -> "Stopwatch":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


class RateTracker:
    """Track rate of operations over time.

    Example:
        tracker = RateTracker(window=60.0)
        for item in items:
            tracker.record()
        print(f"Rate: {tracker.rate:.2f} items/sec")
    """

    def __init__(self, window: float = 60.0) -> None:
        self._window = window
        self._timestamps: list = []
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record an operation."""
        now = time.monotonic()
        with self._lock:
            self._timestamps.append(now)
            self._cleanup(now)

    def _cleanup(self, now: float) -> None:
        cutoff = now - self._window
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    @property
    def count(self) -> int:
        """Get count within window."""
        with self._lock:
            self._cleanup(time.monotonic())
            return len(self._timestamps)

    @property
    def rate(self) -> float:
        """Get operations per second."""
        with self._lock:
            self._cleanup(time.monotonic())
            if not self._timestamps:
                return 0.0
            span = self._timestamps[-1] - self._timestamps[0]
            if span > 0:
                return len(self._timestamps) / span
            return 0.0

    def reset(self) -> None:
        """Reset tracker."""
        with self._lock:
            self._timestamps.clear()
