"""API scheduler and rate limiting for automation workflows.

Provides scheduled API calls, rate limiting, and quota management
for controlled API consumption.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    algorithm: RateLimitAlgorithm
    requests_per_second: float
    burst_size: int
    max_queue_size: int = 100


@dataclass
class QuotaLimit:
    """Quota limit definition."""
    quota_id: str
    name: str
    limit: int
    window_seconds: int
    used: int = 0
    reset_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledCall:
    """A scheduled API call."""
    schedule_id: str
    endpoint: str
    method: str
    payload: Optional[Dict[str, Any]]
    scheduled_at: float
    created_at: float = field(default_factory=time.time)
    status: str = "pending"
    executed_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    callback: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get time to wait before tokens available."""
        with self._lock:
            if self._tokens >= tokens:
                return 0.0
            needed = tokens - self._tokens
            return needed / self._rate


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        self._rate = rate
        self._capacity = capacity
        self._queue: deque = deque()
        self._lock = threading.Lock()

    def add(self) -> bool:
        """Try to add to bucket. Returns True if accepted."""
        with self._lock:
            self._leak()

            if len(self._queue) < self._capacity:
                self._queue.append(time.time())
                return True
            return False

    def _leak(self) -> None:
        """Remove expired items from bucket."""
        now = time.time()
        while self._queue and self._queue[0] + (1.0 / self._rate) <= now:
            self._queue.popleft()

    def get_wait_time(self) -> float:
        """Get time until oldest item can be processed."""
        with self._lock:
            self._leak()
            if not self._queue:
                return 0.0
            oldest = self._queue[0]
            return max(0.0, (oldest + (1.0 / self._rate)) - time.time())


class SlidingWindowCounter:
    """Sliding window rate counter."""

    def __init__(self, max_requests: int, window_seconds: int):
        self._max = max_requests
        self._window = window_seconds
        self._requests: deque = deque()
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        """Check if request is allowed under limit."""
        with self._lock:
            self._cleanup()
            return len(self._requests) < self._max

    def record(self) -> bool:
        """Record a request. Returns True if allowed."""
        with self._lock:
            self._cleanup()
            if len(self._requests) < self._max:
                self._requests.append(time.time())
                return True
            return False

    def _cleanup(self) -> None:
        """Remove requests outside the window."""
        cutoff = time.time() - self._window
        while self._requests and self._requests[0] < cutoff:
            self._requests.popleft()

    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self._lock:
            self._cleanup()
            return max(0, self._max - len(self._requests))


class QuotaManager:
    """Manages API quotas."""

    def __init__(self):
        self._quotas: Dict[str, QuotaLimit] = {}
        self._lock = threading.Lock()

    def create_quota(
        self,
        name: str,
        limit: int,
        window_seconds: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a new quota."""
        quota_id = str(uuid.uuid4())[:12]

        quota = QuotaLimit(
            quota_id=quota_id,
            name=name,
            limit=limit,
            window_seconds=window_seconds,
            metadata=metadata or {},
        )

        with self._lock:
            self._quotas[quota_id] = quota

        return quota_id

    def check_and_consume(self, quota_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Check if quota available and consume if so."""
        with self._lock:
            quota = self._quotas.get(quota_id)
            if not quota:
                return False, {"error": "Quota not found"}

            self._reset_if_needed(quota)

            if quota.used < quota.limit:
                quota.used += 1
                return True, {
                    "remaining": quota.limit - quota.used,
                    "reset_at": datetime.fromtimestamp(quota.reset_at).isoformat(),
                }

            return False, {
                "remaining": 0,
                "reset_at": datetime.fromtimestamp(quota.reset_at).isoformat(),
                "retry_after": quota.reset_at - time.time(),
            }

    def _reset_if_needed(self, quota: QuotaLimit) -> None:
        """Reset quota if window has passed."""
        if time.time() >= quota.reset_at:
            quota.used = 0
            quota.reset_at = time.time() + quota.window_seconds

    def get_quota_status(self, quota_id: str) -> Optional[Dict[str, Any]]:
        """Get current quota status."""
        with self._lock:
            quota = self._quotas.get(quota_id)
            if not quota:
                return None

            self._reset_if_needed(quota)

            return {
                "quota_id": quota.quota_id,
                "name": quota.name,
                "limit": quota.limit,
                "used": quota.used,
                "remaining": quota.limit - quota.used,
                "window_seconds": quota.window_seconds,
                "reset_at": datetime.fromtimestamp(quota.reset_at).isoformat(),
            }


class ScheduleManager:
    """Manages scheduled API calls."""

    def __init__(self):
        self._schedules: Dict[str, ScheduledCall] = {}
        self._lock = threading.Lock()
        self._callbacks: Dict[str, Callable] = {}

    def schedule(
        self,
        endpoint: str,
        method: str,
        scheduled_at: float,
        payload: Optional[Dict[str, Any]] = None,
        callback_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Schedule an API call for future execution."""
        schedule_id = str(uuid.uuid4())[:12]

        schedule = ScheduledCall(
            schedule_id=schedule_id,
            endpoint=endpoint,
            method=method,
            payload=payload,
            scheduled_at=scheduled_at,
            callback=callback_id,
            metadata=metadata or {},
        )

        with self._lock:
            self._schedules[schedule_id] = schedule

        return schedule_id

    def schedule_recurring(
        self,
        endpoint: str,
        method: str,
        interval_seconds: float,
        payload: Optional[Dict[str, Any]] = None,
        callback_id: Optional[str] = None,
        start_at: Optional[float] = None,
    ) -> str:
        """Schedule a recurring API call."""
        schedule_id = str(uuid.uuid4())[:12]
        start = start_at or time.time()

        schedule = ScheduledCall(
            schedule_id=schedule_id,
            endpoint=endpoint,
            method=method,
            payload=payload,
            scheduled_at=start,
            callback=callback_id,
            metadata={
                "recurring": True,
                "interval_seconds": interval_seconds,
                "next_scheduled_at": start + interval_seconds,
            },
        )

        with self._lock:
            self._schedules[schedule_id] = schedule

        return schedule_id

    def get_due_schedules(self) -> List[ScheduledCall]:
        """Get all schedules that are due for execution."""
        now = time.time()
        with self._lock:
            return [
                copy.deepcopy(s) for s in self._schedules.values()
                if s.status == "pending" and s.scheduled_at <= now
            ]

    def mark_executed(
        self,
        schedule_id: str,
        result: Any,
        error: Optional[str] = None,
    ) -> bool:
        """Mark a schedule as executed."""
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if not schedule:
                return False

            schedule.status = "completed" if not error else "failed"
            schedule.executed_at = time.time()
            schedule.result = result
            schedule.error = error

            if schedule.metadata.get("recurring"):
                interval = schedule.metadata.get("interval_seconds", 60)
                schedule.scheduled_at = time.time() + interval
                schedule.status = "pending"

            return True

    def cancel(self, schedule_id: str) -> bool:
        """Cancel a scheduled call."""
        with self._lock:
            if schedule_id in self._schedules:
                self._schedules[schedule_id].status = "cancelled"
                return True
            return False

    def get_schedule(self, schedule_id: str) -> Optional[ScheduledCall]:
        """Get schedule details."""
        with self._lock:
            return copy.deepcopy(self._schedules.get(schedule_id))


class AutomationSchedulerAction:
    """Action providing API scheduling and rate limiting."""

    def __init__(
        self,
        quota_manager: Optional[QuotaManager] = None,
        schedule_manager: Optional[ScheduleManager] = None,
    ):
        self._quota_manager = quota_manager or QuotaManager()
        self._schedule_manager = schedule_manager or ScheduleManager()
        self._rate_limiters: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def create_rate_limiter(
        self,
        name: str,
        algorithm: str = "token_bucket",
        rate: float = 10.0,
        burst: int = 20,
    ) -> Dict[str, Any]:
        """Create a rate limiter."""
        try:
            algo = RateLimitAlgorithm(algorithm.lower())
        except ValueError:
            algo = RateLimitAlgorithm.TOKEN_BUCKET

        if algo == RateLimitAlgorithm.TOKEN_BUCKET:
            limiter = TokenBucket(rate=rate, capacity=burst)
        elif algo == RateLimitAlgorithm.LEAKY_BUCKET:
            limiter = LeakyBucket(rate=rate, capacity=burst)
        elif algo == RateLimitAlgorithm.FIXED_WINDOW:
            limiter = SlidingWindowCounter(max_requests=int(rate * burst), window_seconds=1)
        elif algo == RateLimitAlgorithm.SLIDING_WINDOW:
            limiter = SlidingWindowCounter(max_requests=int(rate * burst), window_seconds=int(burst))
        else:
            limiter = TokenBucket(rate=rate, capacity=burst)

        with self._lock:
            self._rate_limiters[name] = limiter

        return {
            "name": name,
            "algorithm": algo.value,
            "rate": rate,
            "burst": burst,
        }

    def check_rate_limit(self, name: str) -> Dict[str, Any]:
        """Check if request is allowed under rate limit."""
        with self._lock:
            limiter = self._rate_limiters.get(name)

        if not limiter:
            return {"allowed": True, "name": name}

        if isinstance(limiter, TokenBucket):
            allowed = limiter.consume()
            wait_time = limiter.get_wait_time() if not allowed else 0.0
        elif isinstance(limiter, LeakyBucket):
            allowed = limiter.add()
            wait_time = limiter.get_wait_time() if not allowed else 0.0
        elif isinstance(limiter, SlidingWindowCounter):
            allowed = limiter.record()
            wait_time = 0.0 if allowed else 1.0
        else:
            allowed = True
            wait_time = 0.0

        return {
            "allowed": allowed,
            "name": name,
            "wait_seconds": wait_time,
        }

    def create_quota(
        self,
        name: str,
        limit: int,
        window_seconds: int,
    ) -> Dict[str, Any]:
        """Create a quota limit."""
        quota_id = self._quota_manager.create_quota(name, limit, window_seconds)
        return {"quota_id": quota_id, "name": name, "limit": limit, "window_seconds": window_seconds}

    def check_quota(self, quota_id: str) -> Dict[str, Any]:
        """Check and consume quota."""
        allowed, status = self._quota_manager.check_and_consume(quota_id)
        return {"allowed": allowed, **status}

    def schedule(
        self,
        endpoint: str,
        method: str,
        scheduled_at: Optional[float] = None,
        payload: Optional[Dict[str, Any]] = None,
        interval_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Schedule an API call."""
        scheduled_time = scheduled_at or (time.time() + (interval_seconds or 60))

        if interval_seconds:
            schedule_id = self._schedule_manager.schedule_recurring(
                endpoint=endpoint,
                method=method,
                interval_seconds=interval_seconds,
                payload=payload,
                start_at=scheduled_time,
            )
        else:
            schedule_id = self._schedule_manager.schedule(
                endpoint=endpoint,
                method=method,
                scheduled_at=scheduled_time,
                payload=payload,
            )

        return {
            "schedule_id": schedule_id,
            "endpoint": endpoint,
            "scheduled_at": datetime.fromtimestamp(scheduled_time).isoformat(),
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a scheduling operation.

        Required params:
            operation: str - 'schedule', 'create_limiter', 'check_limit', 'create_quota', 'check_quota'
        """
        operation = params.get("operation")

        if operation == "schedule":
            endpoint = params.get("endpoint")
            method = params.get("method", "GET")
            scheduled_at = params.get("scheduled_at")
            payload = params.get("payload")
            interval = params.get("interval_seconds")

            if not endpoint:
                raise ValueError("endpoint is required")

            return self.schedule(endpoint, method, scheduled_at, payload, interval)

        elif operation == "create_limiter":
            name = params.get("name")
            algorithm = params.get("algorithm", "token_bucket")
            rate = params.get("rate", 10.0)
            burst = params.get("burst", 20)

            if not name:
                raise ValueError("name is required")

            return self.create_rate_limiter(name, algorithm, rate, burst)

        elif operation == "check_limit":
            name = params.get("name")
            if not name:
                raise ValueError("name is required")
            return self.check_rate_limit(name)

        elif operation == "create_quota":
            name = params.get("name")
            limit = params.get("limit", 100)
            window = params.get("window_seconds", 3600)

            if not name:
                raise ValueError("name is required")

            return self.create_quota(name, limit, window)

        elif operation == "check_quota":
            quota_id = params.get("quota_id")
            if not quota_id:
                raise ValueError("quota_id is required")
            return self.check_quota(quota_id)

        elif operation == "get_due":
            schedules = self._schedule_manager.get_due_schedules()
            return {
                "due_count": len(schedules),
                "schedules": [
                    {
                        "schedule_id": s.schedule_id,
                        "endpoint": s.endpoint,
                        "method": s.method,
                        "scheduled_at": datetime.fromtimestamp(s.scheduled_at).isoformat(),
                    }
                    for s in schedules
                ],
            }

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_due_schedules(self) -> List[Dict[str, Any]]:
        """Get all due schedules."""
        schedules = self._schedule_manager.get_due_schedules()
        return [
            {
                "schedule_id": s.schedule_id,
                "endpoint": s.endpoint,
                "method": s.method,
                "payload": s.payload,
                "scheduled_at": datetime.fromtimestamp(s.scheduled_at).isoformat(),
            }
            for s in schedules
        ]

    def mark_executed(
        self,
        schedule_id: str,
        result: Any,
        error: Optional[str] = None,
    ) -> bool:
        """Mark a schedule as executed."""
        return self._schedule_manager.mark_executed(schedule_id, result, error)
