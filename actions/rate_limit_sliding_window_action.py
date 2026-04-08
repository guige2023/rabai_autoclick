"""Rate Limit Sliding Window Action Module.

Provides sliding window rate limiting with
hit counting and window management.
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
    max_hits: int
    window_seconds: float
    hits: List[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


class SlidingWindowManager:
    """Manages sliding window rate limiters."""

    def __init__(self):
        self._limiters: Dict[str, SlidingWindowLimiter] = {}

    def create_limiter(
        self,
        name: str,
        max_hits: int,
        window_seconds: float
    ) -> None:
        """Create a sliding window limiter."""
        self._limiters[name] = SlidingWindowLimiter(
            max_hits=max_hits,
            window_seconds=window_seconds
        )

    def check(self, name: str) -> tuple[bool, int, float]:
        """Check if request is allowed."""
        limiter = self._limiters.get(name)
        if not limiter:
            return True, 0, 0

        with limiter.lock:
            now = time.time()
            cutoff = now - limiter.window_seconds

            limiter.hits = [t for t in limiter.hits if t > cutoff]

            remaining = limiter.max_hits - len(limiter.hits)

            if len(limiter.hits) < limiter.max_hits:
                limiter.hits.append(now)
                return True, remaining - 1, limiter.window_seconds

            return False, 0, limiter.hits[0] + limiter.window_seconds - now

    def reset(self, name: str) -> bool:
        """Reset limiter."""
        limiter = self._limiters.get(name)
        if not limiter:
            return False

        with limiter.lock:
            limiter.hits.clear()
        return True


class RateLimitSlidingWindowAction(BaseAction):
    """Action for sliding window rate limiting."""

    def __init__(self):
        super().__init__("rate_limit_sliding_window")
        self._manager = SlidingWindowManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute sliding window action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "check":
                return self._check(params)
            elif operation == "reset":
                return self._reset(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create limiter."""
        self._manager.create_limiter(
            name=params.get("name", ""),
            max_hits=params.get("max_hits", 100),
            window_seconds=params.get("window_seconds", 60)
        )
        return ActionResult(success=True)

    def _check(self, params: Dict) -> ActionResult:
        """Check rate limit."""
        allowed, remaining, retry_after = self._manager.check(
            params.get("name", "")
        )
        return ActionResult(success=True, data={
            "allowed": allowed,
            "remaining": remaining,
            "retry_after_seconds": retry_after
        })

    def _reset(self, params: Dict) -> ActionResult:
        """Reset limiter."""
        success = self._manager.reset(params.get("name", ""))
        return ActionResult(success=success)
