"""API Rate Limiter Action Module.

Provides token bucket and sliding window rate limiting for API requests.
Supports per-endpoint, per-client, and global rate limit configurations.
"""

from __future__ import annotations

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit rule."""
    name: str
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    rate: float = 10.0
    burst: float = 20.0
    capacity: float = 20.0
    refill_rate: float = 10.0
    window_seconds: float = 60.0
    block_duration: float = 60.0
    enabled: bool = True
    scope: str = "global"


class TokenBucket:
    """Token bucket rate limiter implementation."""

    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """Attempt to consume tokens. Returns True if allowed."""
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def get_available(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self.tokens


class SlidingWindow:
    """Sliding window rate limiter implementation."""

    def __init__(self, window_seconds: float, max_requests: float):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self.requests: deque = deque()
        self._lock = threading.Lock()

    def consume(self) -> bool:
        """Attempt to record a request. Returns True if allowed."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()

            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            return max(0, self.max_requests - len(self.requests))


@dataclass
class LimitResult:
    """Result of a rate limit check."""
    allowed: bool
    limit_name: str
    remaining: float
    reset_at: float
    retry_after: float = 0.0


class ApiRateLimiterAction(BaseAction):
    """Apply rate limiting to API requests.

    Supports token bucket, sliding window, and fixed window strategies
    with configurable limits per endpoint or globally.
    """
    action_type = "api_rate_limiter"
    display_name = "API速率限制"
    description = "为API请求提供速率限制，支持令牌桶和滑动窗口策略"

    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, SlidingWindow] = {}
        self._configs: Dict[str, RateLimitConfig] = {}
        self._blocklist: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total": 0, "allowed": 0, "rejected": 0, "blocked": 0
        })

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit check.

        Args:
            context: Execution context.
            params: Dict with keys: action (check/reserve/config),
                   limit_name, tokens, client_id, config.

        Returns:
            ActionResult with rate limit status.
        """
        action = params.get("action", "check")
        limit_name = params.get("limit_name", "default")

        if action == "config":
            return self._configure_limit(params)

        if action == "stats":
            return self._get_stats(params)

        if action == "unblock":
            return self._unblock_client(params)

        return self._check_limit(context, params)

    def _configure_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Configure a new rate limit rule."""
        name = params.get("name", "default")
        strategy_str = params.get("strategy", "token_bucket")
        rate = float(params.get("rate", 10.0))
        burst = float(params.get("burst", 20.0))
        capacity = float(params.get("capacity", burst))
        refill_rate = float(params.get("refill_rate", rate))
        window = float(params.get("window_seconds", 60.0))
        scope = params.get("scope", "global")

        try:
            strategy = RateLimitStrategy(strategy_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown strategy: {strategy_str}"
            )

        config = RateLimitConfig(
            name=name,
            strategy=strategy,
            rate=rate,
            burst=burst,
            capacity=capacity,
            refill_rate=refill_rate,
            window_seconds=window,
            scope=scope
        )

        with self._lock:
            self._configs[name] = config

            if strategy == RateLimitStrategy.TOKEN_BUCKET:
                self._buckets[name] = TokenBucket(capacity, refill_rate)
            elif strategy in (RateLimitStrategy.SLIDING_WINDOW, RateLimitStrategy.FIXED_WINDOW):
                self._windows[name] = SlidingWindow(window, rate)

        return ActionResult(
            success=True,
            message=f"Rate limit '{name}' configured: {strategy.value} "
                    f"({rate}/s, burst {burst})",
            data={"name": name, "strategy": strategy.value, "rate": rate}
        )

    def _check_limit(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check if a request is allowed under rate limits."""
        limit_name = params.get("limit_name", "default")
        tokens = float(params.get("tokens", 1.0))
        client_id = params.get("client_id", "default")
        save_to_var = params.get("save_to_var", None)

        with self._lock:
            if client_id in self._blocklist:
                if time.time() < self._blocklist[client_id]:
                    result = LimitResult(
                        allowed=False,
                        limit_name=limit_name,
                        remaining=0,
                        reset_at=self._blocklist[client_id],
                        retry_after=self._blocklist[client_id] - time.time()
                    )
                    self._stats[limit_name]["total"] += 1
                    self._stats[limit_name]["blocked"] += 1
                    if save_to_var:
                        context.variables[save_to_var] = {
                            "allowed": False, "reason": "blocked",
                            "retry_after": result.retry_after
                        }
                    return ActionResult(
                        success=False,
                        message=f"Client {client_id} is blocked for "
                                f"{result.retry_after:.1f}s",
                        data={"limit_result": result.__dict__}
                    )
                else:
                    del self._blocklist[client_id]

        config = self._configs.get(limit_name)
        if not config:
            config = RateLimitConfig(name=limit_name)

        scope_key = f"{limit_name}:{client_id}" if config.scope == "per_client" else limit_name

        allowed = False
        remaining = 0.0
        reset_at = time.time() + config.window_seconds

        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            with self._lock:
                if scope_key not in self._buckets:
                    self._buckets[scope_key] = TokenBucket(
                        config.capacity, config.refill_rate
                    )
                bucket = self._buckets[scope_key]
                allowed = bucket.consume(tokens)
                remaining = bucket.get_available()

        elif config.strategy in (RateLimitStrategy.SLIDING_WINDOW, RateLimitStrategy.FIXED_WINDOW):
            with self._lock:
                if scope_key not in self._windows:
                    self._windows[scope_key] = SlidingWindow(
                        config.window_seconds, config.rate
                    )
                window = self._windows[scope_key]
                allowed = window.consume()
                remaining = window.get_remaining()

        result = LimitResult(
            allowed=allowed,
            limit_name=limit_name,
            remaining=remaining,
            reset_at=reset_at,
            retry_after=0.0 if allowed else config.window_seconds
        )

        with self._lock:
            self._stats[limit_name]["total"] += 1
            if allowed:
                self._stats[limit_name]["allowed"] += 1
            else:
                self._stats[limit_name]["rejected"] += 1

        if save_to_var:
            context.variables[save_to_var] = {
                "allowed": allowed, "remaining": remaining,
                "reset_at": reset_at, "retry_after": result.retry_after
            }

        if allowed:
            return ActionResult(
                success=True,
                message=f"Rate limit check passed: {remaining:.1f} tokens remaining",
                data={"limit_result": result.__dict__}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Rate limit exceeded for '{limit_name}': "
                        f"retry after {result.retry_after:.1f}s",
                data={"limit_result": result.__dict__}
            )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get rate limit statistics."""
        limit_name = params.get("limit_name", None)
        save_to_var = params.get("save_to_var", None)

        if limit_name:
            stats = {limit_name: self._stats.get(limit_name, {})}
        else:
            stats = dict(self._stats)

        if save_to_var:
            context.variables[save_to_var] = stats

        return ActionResult(
            success=True,
            message=f"Rate limit stats retrieved for "
                    f"{'all' if not limit_name else limit_name}",
            data={"stats": stats}
        )

    def _unblock_client(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a client from the blocklist."""
        client_id = params.get("client_id", "default")
        with self._lock:
            if client_id in self._blocklist:
                del self._blocklist[client_id]
                return ActionResult(
                    success=True,
                    message=f"Client {client_id} unblocked"
                )
            return ActionResult(
                success=False,
                message=f"Client {client_id} not in blocklist"
            )

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "limit_name": "default",
            "tokens": 1.0,
            "client_id": "default",
            "save_to_var": None,
            "strategy": "token_bucket",
            "rate": 10.0,
            "burst": 20.0,
            "window_seconds": 60.0,
            "scope": "global"
        }
