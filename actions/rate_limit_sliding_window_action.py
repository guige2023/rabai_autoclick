"""Rate Limit Sliding Window Action Module.

Provides sliding window algorithm for
rate limiting.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SlidingWindowLimiter:
    """Sliding window rate limiter."""
    limiter_id: str
    max_requests: int
    window_seconds: float
    requests: List[float] = field(default_factory=list)


class SlidingWindowRateLimitManager:
    """Manages sliding window rate limiting."""

    def __init__(self):
        self._limiters: Dict[str, SlidingWindowLimiter] = {}
        self._lock = threading.RLock()

    def create_limiter(
        self,
        limiter_id: str,
        max_requests: int,
        window_seconds: float
    ) -> bool:
        """Create a limiter."""
        with self._lock:
            if limiter_id in self._limiters:
                return False

            self._limiters[limiter_id] = SlidingWindowLimiter(
                limiter_id=limiter_id,
                max_requests=max_requests,
                window_seconds=window_seconds
            )
            return True

    def is_allowed(self, limiter_id: str) -> bool:
        """Check if request is allowed."""
        with self._lock:
            limiter = self._limiters.get(limiter_id)
            if not limiter:
                return False

            now = time.time()
            cutoff = now - limiter.window_seconds

            limiter.requests = [t for t in limiter.requests if t > cutoff]

            if len(limiter.requests) < limiter.max_requests:
                limiter.requests.append(now)
                return True

            return False

    def get_limiter_info(self, limiter_id: str) -> Optional[Dict]:
        """Get limiter info."""
        with self._lock:
            limiter = self._limiters.get(limiter_id)
            if not limiter:
                return None

            now = time.time()
            cutoff = now - limiter.window_seconds

            active_requests = [t for t in limiter.requests if t > cutoff]

            return {
                "limiter_id": limiter.limiter_id,
                "max_requests": limiter.max_requests,
                "window_seconds": limiter.window_seconds,
                "current_requests": len(active_requests),
                "remaining": limiter.max_requests - len(active_requests)
            }


class SlidingWindowRateLimitAction(BaseAction):
    """Action for sliding window rate limiting."""

    def __init__(self):
        super().__init__("rate_limit_sliding_window")
        self._manager = SlidingWindowRateLimitManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute rate limit action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "is_allowed":
                return self._is_allowed(params)
            elif operation == "info":
                return self._info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create limiter."""
        success = self._manager.create_limiter(
            limiter_id=params.get("limiter_id", ""),
            max_requests=params.get("max_requests", 100),
            window_seconds=params.get("window_seconds", 60)
        )
        return ActionResult(success=success)

    def _is_allowed(self, params: Dict) -> ActionResult:
        """Check if allowed."""
        allowed = self._manager.is_allowed(params.get("limiter_id", ""))
        return ActionResult(success=True, data={"allowed": allowed})

    def _info(self, params: Dict) -> ActionResult:
        """Get limiter info."""
        info = self._manager.get_limiter_info(params.get("limiter_id", ""))
        if info is None:
            return ActionResult(success=False, message="Limiter not found")
        return ActionResult(success=True, data=info)
