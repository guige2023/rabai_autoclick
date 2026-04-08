# Copyright (c) 2024. coded by claude
"""API Quota Management Action Module.

Implements API quota tracking and enforcement using token bucket algorithm
with support for multiple rate limit tiers.
"""
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import threading
import logging

logger = logging.getLogger(__name__)


@dataclass
class QuotaLimit:
    requests_per_second: float
    requests_per_minute: float
    requests_per_hour: float
    burst_size: int


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = datetime.now()
        self.lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        now = datetime.now()
        elapsed = (now - self.last_update).total_seconds()
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now

    def available_tokens(self) -> float:
        with self.lock:
            self._refill()
            return self.tokens


class QuotaManager:
    def __init__(self, default_limit: Optional[QuotaLimit] = None):
        self.default_limit = default_limit or QuotaLimit(
            requests_per_second=10.0,
            requests_per_minute=100.0,
            requests_per_hour=1000.0,
            burst_size=20,
        )
        self._buckets: Dict[str, Dict[str, TokenBucket]] = {}
        self._lock = threading.Lock()

    def _get_buckets(self, client_id: str) -> Dict[str, TokenBucket]:
        if client_id not in self._buckets:
            limit = self.default_limit
            self._buckets[client_id] = {
                "second": TokenBucket(limit.requests_per_second, limit.burst_size),
                "minute": TokenBucket(limit.requests_per_minute / 60, limit.requests_per_minute),
                "hour": TokenBucket(limit.requests_per_hour / 3600, limit.requests_per_hour),
            }
        return self._buckets[client_id]

    def check_quota(self, client_id: str) -> Tuple[bool, str]:
        buckets = self._get_buckets(client_id)
        if not buckets["second"].consume():
            return False, "Rate limit exceeded (second)"
        if not buckets["minute"].consume():
            return False, "Rate limit exceeded (minute)"
        if not buckets["hour"].consume():
            return False, "Rate limit exceeded (hour)"
        return True, "OK"

    def get_remaining(self, client_id: str) -> Dict[str, float]:
        buckets = self._get_buckets(client_id)
        return {
            "second": buckets["second"].available_tokens(),
            "minute": buckets["minute"].available_tokens(),
            "hour": buckets["hour"].available_tokens(),
        }

    def reset_quota(self, client_id: str) -> None:
        with self._lock:
            if client_id in self._buckets:
                del self._buckets[client_id]
