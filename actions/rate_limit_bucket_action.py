"""Rate limiting action module for RabAI AutoClick.

Provides rate limiting algorithms: token bucket, sliding window,
leaky bucket, and fixed window with multi-tier limits.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenBucketConfig:
    """Token bucket configuration."""
    capacity: float  # Max tokens in bucket
    refill_rate: float  # Tokens per second
    initial_tokens: Optional[float] = None


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: float
    retry_after: Optional[float]  # Seconds until next allowed request


class TokenBucketAction(BaseAction):
    """Token bucket rate limiter.
    
    Each request consumes one token. Tokens refill at a constant
    rate. When bucket is empty, requests are rejected.
    
    Args:
        capacity: Maximum tokens (requests allowed in burst)
        refill_rate: Tokens per second
    """

    def __init__(self, capacity: float = 100.0, refill_rate: float = 10.0):
        super().__init__()
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.time()

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def execute(
        self,
        action: str,
        tokens_requested: float = 1.0,
        client_key: Optional[str] = None
    ) -> ActionResult:
        try:
            if action == "allow":
                self._refill()
                if self._tokens >= tokens_requested:
                    self._tokens -= tokens_requested
                    return ActionResult(success=True, data={
                        "allowed": True,
                        "remaining": round(self._tokens, 4),
                        "retry_after": None
                    })
                else:
                    retry_after = (tokens_requested - self._tokens) / self.refill_rate
                    return ActionResult(success=True, data={
                        "allowed": False,
                        "remaining": round(self._tokens, 4),
                        "retry_after": round(retry_after, 4)
                    })

            elif action == "status":
                self._refill()
                return ActionResult(success=True, data={
                    "capacity": self.capacity,
                    "tokens_available": round(self._tokens, 4),
                    "refill_rate": self.refill_rate,
                    "utilization": round(self._tokens / self.capacity, 4)
                })

            elif action == "reset":
                self._tokens = self.capacity
                self._last_refill = time.time()
                return ActionResult(success=True, data={"reset": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class SlidingWindowAction(BaseAction):
    """Sliding window rate limiter.
    
    Counts requests in a rolling time window. More accurate than
    fixed window but requires more memory.
    
    Args:
        max_requests: Maximum requests per window
        window_seconds: Window size in seconds
    """

    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0):
        super().__init__()
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: deque = deque()

    def _clean_window(self, now: float):
        cutoff = now - self.window_seconds
        while self._requests and self._requests[0] < cutoff:
            self._requests.popleft()

    def execute(
        self,
        action: str,
        client_key: Optional[str] = None,
        tokens_requested: int = 1
    ) -> ActionResult:
        try:
            if action == "allow":
                now = time.time()
                self._clean_window(now)
                if len(self._requests) + tokens_requested <= self.max_requests:
                    for _ in range(tokens_requested):
                        self._requests.append(now)
                    return ActionResult(success=True, data={
                        "allowed": True,
                        "remaining": self.max_requests - len(self._requests),
                        "window_size": self.window_seconds
                    })
                else:
                    oldest = self._requests[0] if self._requests else now
                    retry_after = oldest + self.window_seconds - now
                    return ActionResult(success=True, data={
                        "allowed": False,
                        "remaining": max(0, self.max_requests - len(self._requests)),
                        "retry_after": round(max(0, retry_after), 4)
                    })

            elif action == "status":
                now = time.time()
                self._clean_window(now)
                return ActionResult(success=True, data={
                    "current_requests": len(self._requests),
                    "max_requests": self.max_requests,
                    "window_seconds": self.window_seconds,
                    "utilization": round(len(self._requests) / self.max_requests, 4)
                })

            elif action == "reset":
                self._requests.clear()
                return ActionResult(success=True, data={"reset": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class LeakyBucketAction(BaseAction):
    """Leaky bucket rate limiter.
    
    Requests are processed at a constant rate. Excess requests
    are queued or dropped. Useful for smoothing output rate.
    
    Args:
        capacity: Maximum queue size
        leak_rate: Requests per second (processing rate)
    """

    def __init__(self, capacity: int = 100, leak_rate: float = 10.0):
        super().__init__()
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._queue_size = 0
        self._last_leak = time.time()

    def _leak(self):
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._queue_size = max(0, self._queue_size - leaked)
        self._last_leak = now

    def execute(self, action: str) -> ActionResult:
        try:
            if action == "enqueue":
                self._leak()
                if self._queue_size < self.capacity:
                    self._queue_size += 1
                    return ActionResult(success=True, data={
                        "enqueued": True,
                        "queue_size": self._queue_size,
                        "capacity": self.capacity
                    })
                else:
                    return ActionResult(success=True, data={
                        "enqueued": False,
                        "queue_size": self._queue_size,
                        "capacity": self.capacity,
                        "dropped": True
                    })

            elif action == "drain":
                self._leak()
                return ActionResult(success=True, data={
                    "queue_size": round(self._queue_size, 4),
                    "leak_rate": self.leak_rate
                })

            elif action == "status":
                self._leak()
                return ActionResult(success=True, data={
                    "queue_size": round(self._queue_size, 4),
                    "capacity": self.capacity,
                    "leak_rate": self.leak_rate,
                    "utilization": round(self._queue_size / self.capacity, 4)
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class MultiTierRateLimitAction(BaseAction):
    """Multi-tier rate limiter supporting per-client limits.
    
    Manages separate rate limiters per client key, with
    global limits and per-client limits.
    
    Args:
        global_limit: Global rate limit
        per_client_limit: Per-client rate limit
    """

    def __init__(self, global_max: int = 1000, per_client_max: int = 100,
                 window_seconds: float = 60.0):
        super().__init__()
        self.global_max = global_max
        self.per_client_max = per_client_max
        self.window_seconds = window_seconds
        self._client_windows: Dict[str, SlidingWindowAction] = {}
        self._global_window = SlidingWindowAction(global_max, window_seconds)

    def execute(
        self,
        action: str,
        client_key: Optional[str] = None,
        tokens_requested: int = 1
    ) -> ActionResult:
        try:
            if action == "allow":
                if not client_key:
                    return ActionResult(success=False, error="client_key required")

                # Check global
                global_res = self._global_window.execute("allow", tokens_requested=tokens_requested)
                if not global_res.data["allowed"]:
                    return ActionResult(success=True, data={
                        "allowed": False, "reason": "global_limit_exceeded",
                        "retry_after": global_res.data.get("retry_after")
                    })

                # Check per-client
                if client_key not in self._client_windows:
                    self._client_windows[client_key] = SlidingWindowAction(
                        self.per_client_max, self.window_seconds
                    )
                client_res = self._client_windows[client_key].execute("allow", tokens_requested=tokens_requested)

                if not client_res.data["allowed"]:
                    return ActionResult(success=True, data={
                        "allowed": False, "reason": "client_limit_exceeded",
                        "client": client_key, "retry_after": client_res.data.get("retry_after")
                    })

                return ActionResult(success=True, data={
                    "allowed": True, "client": client_key
                })

            elif action == "status":
                global_res = self._global_window.execute("status")
                client_statuses = {}
                for k, v in self._client_windows.items():
                    sr = v.execute("status")
                    client_statuses[k] = sr.data
                return ActionResult(success=True, data={
                    "global": global_res.data,
                    "clients": client_statuses
                })

            elif action == "reset_client":
                if client_key and client_key in self._client_windows:
                    self._client_windows[client_key].execute("reset")
                return ActionResult(success=True, data={"client_key": client_key, "reset": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
