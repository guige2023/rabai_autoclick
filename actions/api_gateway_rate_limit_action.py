"""API Gateway Rate Limit Action Module.

Implements rate limiting policies for API gateways including
token bucket, sliding window, and fixed window algorithms.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10.0
    requests_per_minute: float = 100.0
    requests_per_hour: float = 1000.0
    burst_size: int = 20


class APIGatewayRateLimitAction(BaseAction):
    """
    API Gateway rate limiting.

    Implements rate limiting using various algorithms
    to protect backend services from overload.

    Example:
        rl = APIGatewayRateLimitAction()
        result = rl.execute(ctx, {"action": "check", "identity": "user-123", "cost": 1})
    """
    action_type = "api_gateway_rate_limit"
    display_name = "API网关限流"
    description = "API网关限流：令牌桶、滑动窗口、固定窗口算法"

    def __init__(self) -> None:
        super().__init__()
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._config = RateLimitConfig()
        self._algorithm = RateLimitAlgorithm.TOKEN_BUCKET

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "check":
                return self._check_rate_limit(params)
            elif action == "set_config":
                return self._set_config(params)
            elif action == "get_remaining":
                return self._get_remaining(params)
            elif action == "reset":
                return self._reset_limit(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit error: {str(e)}")

    def _check_rate_limit(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity", "default")
        cost = params.get("cost", 1)
        algorithm = params.get("algorithm", "token_bucket")

        if not identity:
            return ActionResult(success=False, message="identity is required")

        allowed, remaining, reset_in = self._check_limit(identity, cost, algorithm)

        if allowed:
            return ActionResult(success=True, message="Request allowed", data={"allowed": True, "remaining": remaining, "reset_in": reset_in})
        else:
            return ActionResult(success=False, message="Rate limit exceeded", data={"allowed": False, "remaining": 0, "retry_after": reset_in})

    def _check_limit(self, identity: str, cost: float, algorithm: str) -> tuple[bool, int, int]:
        now = time.time()
        bucket = self._buckets.get(identity)

        if algorithm == "token_bucket" or not bucket:
            if not bucket:
                bucket = {"tokens": self._config.requests_per_second, "last_update": now}
                self._buckets[identity] = bucket

            elapsed = now - bucket["last_update"]
            bucket["tokens"] = min(self._config.requests_per_second, bucket["tokens"] + elapsed)

            if bucket["tokens"] >= cost:
                bucket["tokens"] -= cost
                bucket["last_update"] = now
                remaining = int(bucket["tokens"])
                return True, remaining, 1
            else:
                return False, 0, int(1 / (bucket["tokens"] + 0.001))

        elif algorithm == "fixed_window":
            window_start = int(now / 60) * 60
            count_key = f"{identity}:{window_start}"

            if count_key not in bucket:
                bucket[count_key] = 0

            bucket[count_key] += cost

            if bucket[count_key] <= self._config.requests_per_minute:
                remaining = int(self._config.requests_per_minute - bucket[count_key])
                return True, remaining, int(60 - (now - window_start))
            else:
                return False, 0, int(60 - (now - window_start))

        elif algorithm == "sliding_window":
            window_start = now - 60
            if "requests" not in bucket:
                bucket["requests"] = []

            bucket["requests"] = [t for t in bucket.get("requests", []) if t > window_start]

            if len(bucket["requests"]) < self._config.requests_per_minute:
                bucket["requests"].append(now)
                remaining = int(self._config.requests_per_minute - len(bucket["requests"]))
                return True, remaining, 60
            else:
                return False, 0, int(bucket["requests"][0] + 60 - now)

        return True, int(self._config.requests_per_second), 1

    def _set_config(self, params: Dict[str, Any]) -> ActionResult:
        rps = params.get("requests_per_second", 10.0)
        rpm = params.get("requests_per_minute", 100.0)
        rh = params.get("requests_per_hour", 1000.0)
        burst = params.get("burst_size", 20)
        algorithm = params.get("algorithm", "token_bucket")

        self._config = RateLimitConfig(requests_per_second=rps, requests_per_minute=rpm, requests_per_hour=rh, burst_size=burst)

        try:
            self._algorithm = RateLimitAlgorithm(algorithm)
        except ValueError:
            self._algorithm = RateLimitAlgorithm.TOKEN_BUCKET

        return ActionResult(success=True, message="Rate limit config updated")

    def _get_remaining(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity", "default")

        if identity in self._buckets:
            bucket = self._buckets[identity]
            tokens = bucket.get("tokens", self._config.requests_per_second)
            return ActionResult(success=True, data={"identity": identity, "remaining": int(tokens)})

        return ActionResult(success=True, data={"identity": identity, "remaining": int(self._config.requests_per_second)})

    def _reset_limit(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity")

        if identity:
            if identity in self._buckets:
                del self._buckets[identity]
            return ActionResult(success=True, message=f"Rate limit reset for {identity}")
        else:
            self._buckets.clear()
            return ActionResult(success=True, message="All rate limits reset")
