"""
Throttle and rate limiting utilities - token bucket, sliding window, fixed window, leaky bucket.
"""
from typing import Any, Dict, Optional
import time
import logging
import threading
import math

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class TokenBucket:
    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_update = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        with self._lock:
            if self._tokens >= tokens:
                return 0.0
            return (tokens - self._tokens) / self._rate


class SlidingWindow:
    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._requests: list = []
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = time.time()
            self._requests = [t for t in self._requests if now - t < self._window]
            if len(self._requests) < self._max:
                self._requests.append(now)
                return True
            return False

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()


class ThrottleAction(BaseAction):
    """Throttle and rate limiting operations.

    Provides token bucket, sliding window, fixed window rate limiting.
    """

    def __init__(self) -> None:
        self._token_buckets: Dict[str, TokenBucket] = {}
        self._sliding_windows: Dict[str, SlidingWindow] = {}

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "token_bucket")
        name = params.get("name", "default")

        try:
            if operation == "token_bucket_create":
                rate = float(params.get("rate", 10))
                capacity = int(params.get("capacity", 10))
                self._token_buckets[name] = TokenBucket(rate, capacity)
                return {"success": True, "name": name, "rate": rate, "capacity": capacity}

            elif operation == "token_bucket_allow":
                if name not in self._token_buckets:
                    return {"success": False, "error": f"Token bucket {name} not found"}
                tokens = int(params.get("tokens", 1))
                allowed = self._token_buckets[name].consume(tokens)
                wait_time = self._token_buckets[name].wait_time(tokens) if not allowed else 0.0
                return {"success": True, "allowed": allowed, "wait_time": round(wait_time, 3), "name": name}

            elif operation == "sliding_window_create":
                max_requests = int(params.get("max_requests", 100))
                window_seconds = float(params.get("window_seconds", 60))
                self._sliding_windows[name] = SlidingWindow(max_requests, window_seconds)
                return {"success": True, "name": name, "max_requests": max_requests, "window_seconds": window_seconds}

            elif operation == "sliding_window_allow":
                if name not in self._sliding_windows:
                    return {"success": False, "error": f"Sliding window {name} not found"}
                allowed = self._sliding_windows[name].allow()
                return {"success": True, "allowed": allowed, "name": name}

            elif operation == "sliding_window_reset":
                if name not in self._sliding_windows:
                    return {"success": False, "error": f"Sliding window {name} not found"}
                self._sliding_windows[name].reset()
                return {"success": True, "name": name, "reset": True}

            elif operation == "rate_limit_check":
                rate = float(params.get("rate", 10))
                capacity = int(params.get("capacity", 10))
                tokens = int(params.get("tokens", 1))
                bucket = self._token_buckets.get(name) or TokenBucket(rate, capacity)
                if name not in self._token_buckets:
                    self._token_buckets[name] = bucket
                allowed = bucket.consume(tokens)
                wait_time = bucket.wait_time(tokens) if not allowed else 0.0
                return {"success": True, "allowed": allowed, "wait_time": round(wait_time, 3), "name": name}

            elif operation == "leaky_bucket":
                capacity = int(params.get("capacity", 10))
                leak_rate = float(params.get("leak_rate", 1))
                current_level = float(params.get("current_level", 0))
                new_level = min(capacity, current_level + 1)
                leak_amount = leak_rate
                new_level = max(0, new_level - leak_amount)
                allowed = new_level <= capacity - 1
                return {"success": True, "level": round(new_level, 3), "allowed": allowed, "capacity": capacity}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"ThrottleAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return ThrottleAction().execute(context, params)
