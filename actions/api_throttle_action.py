"""API Throttle Action Module.

Implements token bucket and leaky bucket rate limiting algorithms
for API request throttling with configurable capacities and refill rates.
"""

import time
import threading
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class TokenBucketConfig:
    capacity: int = 100
    refill_rate: float = 10.0
    refill_interval_sec: float = 1.0


@dataclass
class LeakyBucketConfig:
    capacity: int = 100
    leak_rate: float = 10.0


class APIThrottleAction:
    """Token bucket and leaky bucket rate limiter for API calls."""

    def __init__(self) -> None:
        self._buckets: Dict[str, Dict] = {}
        self._lock = threading.RLock()

    def create_token_bucket(
        self,
        key: str,
        capacity: int = 100,
        refill_rate: float = 10.0,
        refill_interval_sec: float = 1.0,
    ) -> None:
        with self._lock:
            self._buckets[key] = {
                "type": "token",
                "tokens": float(capacity),
                "capacity": capacity,
                "refill_rate": refill_rate,
                "refill_interval": refill_interval_sec,
                "last_refill": time.time(),
            }

    def create_leaky_bucket(
        self,
        key: str,
        capacity: int = 100,
        leak_rate: float = 10.0,
    ) -> None:
        with self._lock:
            self._buckets[key] = {
                "type": "leaky",
                "water": 0.0,
                "capacity": capacity,
                "leak_rate": leak_rate,
                "last_leak": time.time(),
            }

    def acquire(self, key: str, tokens: int = 1) -> bool:
        with self._lock:
            bucket = self._buckets.get(key)
            if not bucket:
                return True
            if bucket["type"] == "token":
                return self._acquire_token_bucket(bucket, tokens)
            else:
                return self._acquire_leaky_bucket(bucket, tokens)

    def _acquire_token_bucket(self, bucket: Dict, tokens: int) -> bool:
        now = time.time()
        elapsed = now - bucket["last_refill"]
        refill_count = elapsed / bucket["refill_interval"] * bucket["refill_rate"]
        bucket["tokens"] = min(bucket["capacity"], bucket["tokens"] + refill_count)
        bucket["last_refill"] = now
        if bucket["tokens"] >= tokens:
            bucket["tokens"] -= tokens
            return True
        return False

    def _acquire_leaky_bucket(self, bucket: Dict, tokens: int) -> bool:
        now = time.time()
        elapsed = now - bucket["last_leak"]
        leaked = elapsed * bucket["leak_rate"]
        bucket["water"] = max(0, bucket["water"] - leaked)
        bucket["last_leak"] = now
        if bucket["water"] + tokens <= bucket["capacity"]:
            bucket["water"] += tokens
            return True
        return False

    def reset(self, key: str) -> bool:
        with self._lock:
            bucket = self._buckets.get(key)
            if not bucket:
                return False
            if bucket["type"] == "token":
                bucket["tokens"] = float(bucket["capacity"])
                bucket["last_refill"] = time.time()
            else:
                bucket["water"] = 0.0
                bucket["last_leak"] = time.time()
            return True

    def get_status(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            bucket = self._buckets.get(key)
            if not bucket:
                return None
            if bucket["type"] == "token":
                now = time.time()
                elapsed = now - bucket["last_refill"]
                refill_count = elapsed / bucket["refill_interval"] * bucket["refill_rate"]
                current_tokens = min(bucket["capacity"], bucket["tokens"] + refill_count)
                return {
                    "key": key,
                    "type": "token",
                    "current_tokens": current_tokens,
                    "capacity": bucket["capacity"],
                    "refill_rate": bucket["refill_rate"],
                    "utilization": current_tokens / bucket["capacity"],
                }
            else:
                now = time.time()
                elapsed = now - bucket["last_leak"]
                leaked = elapsed * bucket["leak_rate"]
                current_water = max(0, bucket["water"] - leaked)
                return {
                    "key": key,
                    "type": "leaky",
                    "current_water": current_water,
                    "capacity": bucket["capacity"],
                    "leak_rate": bucket["leak_rate"],
                    "utilization": current_water / bucket["capacity"],
                }

    def list_buckets(self) -> list:
        with self._lock:
            return [self.get_status(k) for k in self._buckets if self.get_status(k)]
