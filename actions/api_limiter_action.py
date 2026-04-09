"""API Rate Limiter Action Module.

Token bucket and sliding window rate limiting for API clients.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import time
import threading
import logging

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


@dataclass
class RateLimitStatus:
    """Current rate limit status."""
    allowed: bool
    remaining: int
    limit: int
    reset_at: float
    retry_after: Optional[float] = None


class APITokenBucketLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_update = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_and_acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> float:
        """Wait until tokens are available and acquire.
        
        Returns:
            Seconds waited.
        
        Raises:
            RateLimitExceeded: If timeout is reached.
        """
        start = time.time()
        while True:
            if self.acquire(tokens):
                return time.time() - start
            if timeout and (time.time() - start) >= timeout:
                raise RateLimitExceeded(f"Could not acquire {tokens} tokens in {timeout}s")
            time.sleep(0.01)


class APISlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_sec: float) -> None:
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._requests: List[float] = []
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        now = time.time()
        with self._lock:
            cutoff = now - self.window_sec
            self._requests = [t for t in self._requests if t > cutoff]
            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def wait_and_allow(self, timeout: Optional[float] = None) -> float:
        """Wait until a request is allowed.
        
        Returns:
            Seconds waited.
        
        Raises:
            RateLimitExceeded: If timeout is reached.
        """
        start = time.time()
        while True:
            if self.is_allowed():
                return time.time() - start
            if timeout and (time.time() - start) >= timeout:
                raise RateLimitExceeded(f"Rate limit not released within {timeout}s")
            time.sleep(0.05)


class APIRateLimiterAction:
    """Unified rate limiter combining token bucket and sliding window.
    
    Supports per-endpoint, per-client, and global rate limits.
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_capacity: int = 20,
        window_sec: float = 60.0,
        max_per_window: int = 100,
    ) -> None:
        self._bucket = APITokenBucketLimiter(rate=requests_per_second, capacity=burst_capacity)
        self._window = APISlidingWindowLimiter(max_requests=max_per_window, window_sec=window_sec)
        self._per_endpoint: Dict[str, APITokenBucketLimiter] = {}
        self._enabled = True

    def check(self, endpoint: Optional[str] = None) -> RateLimitStatus:
        """Check if a request is allowed.
        
        Args:
            endpoint: Optional endpoint name for per-endpoint limiting.
        
        Returns:
            RateLimitStatus with current limit state.
        """
        now = time.time()
        if endpoint and endpoint not in self._per_endpoint:
            self._per_endpoint[endpoint] = APITokenBucketLimiter(rate=5.0, capacity=10)

        if endpoint:
            allowed = self._per_endpoint[endpoint].acquire(1)
        else:
            allowed = self._bucket.acquire(1) and self._window.is_allowed()

        remaining = int(self._bucket._tokens) if not endpoint else int(self._per_endpoint[endpoint]._tokens)
        return RateLimitStatus(
            allowed=allowed and self._enabled,
            remaining=max(0, remaining),
            limit=self._bucket.capacity,
            reset_at=now + 1.0,
        )

    def acquire(
        self,
        endpoint: Optional[str] = None,
        timeout: Optional[float] = 10.0,
    ) -> float:
        """Acquire permission to make a request.
        
        Args:
            endpoint: Optional endpoint name.
            timeout: Max seconds to wait.
        
        Returns:
            Seconds waited for acquisition.
        
        Raises:
            RateLimitExceeded: If cannot acquire within timeout.
        """
        if endpoint:
            return self._per_endpoint.setdefault(
                endpoint, APITokenBucketLimiter(rate=5.0, capacity=10)
            ).wait_and_acquire(1, timeout)
        return self._bucket.wait_and_acquire(1, timeout)

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable rate limiting."""
        self._enabled = enabled

    def get_status(self) -> Dict[str, Any]:
        """Get current limiter status."""
        return {
            "enabled": self._enabled,
            "bucket_tokens": round(self._bucket._tokens, 2),
            "bucket_capacity": self._bucket.capacity,
            "window_count": len(self._window._requests),
            "window_limit": self._window.max_requests,
            "endpoint_count": len(self._per_endpoint),
        }
