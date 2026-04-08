"""Rate limiting utilities for controlling operation frequency."""

from typing import Optional, Callable, TypeVar
import time
import threading


T = TypeVar("T")


class RateLimiter:
    """Token bucket rate limiter for controlling operation frequency."""

    def __init__(
        self,
        max_calls: int,
        period: float,
        burst: bool = False
    ):
        """Initialize rate limiter.
        
        Args:
            max_calls: Maximum calls allowed per period.
            period: Time period in seconds.
            burst: If True, allow burst of max_calls at start.
        """
        self.max_calls = max_calls
        self.period = period
        self.burst = burst
        self._tokens = max_calls if burst else 0.0
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, waiting if necessary.
        
        Args:
            tokens: Number of tokens to acquire.
        
        Returns:
            True if tokens acquired.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
            time.sleep(0.01)

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking.
        
        Args:
            tokens: Number of tokens to acquire.
        
        Returns:
            True if tokens acquired, False otherwise.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        refill = elapsed * (self.max_calls / self.period)
        self._tokens = min(self._tokens + refill, self.max_calls)
        self._last_refill = now

    def reset(self) -> None:
        """Reset the rate limiter."""
        with self._lock:
            self._tokens = self.max_calls if self.burst else 0.0
            self._last_refill = time.time()


def rate_limit(max_calls: int, period: float) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to rate-limit a function.
    
    Args:
        max_calls: Maximum calls per period.
        period: Time period in seconds.
    
    Returns:
        Decorated function with rate limiting.
    """
    limiter = RateLimiter(max_calls, period)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs):
            limiter.acquire()
            return func(*args, **kwargs)
        return wrapper
    return decorator


class SlidingWindowRateLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_calls: int, window: float):
        """Initialize sliding window rate limiter.
        
        Args:
            max_calls: Maximum calls in the window.
            window: Time window in seconds.
        """
        self.max_calls = max_calls
        self.window = window
        self._calls: list = []
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        """Acquire a slot in the window.
        
        Returns:
            True if acquired, False if limit reached.
        """
        with self._lock:
            now = time.time()
            self._calls = [t for t in self._calls if now - t < self.window]
            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return True
            return False

    def wait_and_acquire(self, timeout: Optional[float] = None) -> bool:
        """Wait for a slot to become available.
        
        Args:
            timeout: Maximum time to wait in seconds.
        
        Returns:
            True if acquired, False if timeout.
        """
        start = time.time()
        while True:
            if self.acquire():
                return True
            if timeout and time.time() - start >= timeout:
                return False
            time.sleep(0.01)
