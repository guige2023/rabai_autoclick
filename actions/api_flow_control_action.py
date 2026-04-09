"""API Flow Control Action.

Implements flow control mechanisms including backpressure, adaptive
throttling, and request shaping for API clients.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Deque, Dict, Optional


class FlowControlMode(Enum):
    """Flow control strategy modes."""
    BLOCKING = "blocking"
    ADAPTIVE = "adaptive"
    SHAPING = "shaping"
    DROP = "drop"


@dataclass
class FlowControlConfig:
    """Configuration for flow control behavior."""
    max_concurrent: int = 10
    max_queue_size: int = 100
    adaptive_threshold: float = 0.8
    backoff_multiplier: float = 1.5
    recovery_rate: float = 0.1
    mode: FlowControlMode = FlowControlMode.ADAPTIVE


@dataclass
class FlowMetrics:
    """Flow control metrics snapshot."""
    active_requests: int = 0
    queued_requests: int = 0
    dropped_requests: int = 0
    rejected_requests: int = 0
    total_processed: int = 0
    avg_wait_time: float = 0.0
    last_update: float = field(default_factory=time.time)


class TokenBucket:
    """Token bucket for rate shaping."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if successful."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now


class APIFlowControlAction:
    """Manages flow control for API requests with backpressure support."""

    def __init__(self, config: Optional[FlowControlConfig] = None) -> None:
        self.config = config or FlowControlConfig()
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(
            self.config.max_concurrent
        )
        self._queue: Deque[asyncio.Task] = deque()
        self._token_bucket = TokenBucket(
            rate=self.config.recovery_rate,
            capacity=self.config.max_queue_size,
        )
        self._metrics = FlowMetrics()
        self._lock = asyncio.Lock()
        self._wait_times: Deque[float] = deque(maxlen=1000)

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire a flow control token. Returns True if acquired."""
        start = time.monotonic()

        if self.config.mode == FlowControlMode.DROP:
            if not self._token_bucket.consume(1):
                self._metrics.dropped_requests += 1
                return False

        if self.config.mode == FlowControlMode.SHAPING:
            while not self._token_bucket.consume(1):
                await asyncio.sleep(0.01)

        async with self._lock:
            if self._metrics.active_requests >= self.config.max_concurrent:
                if len(self._queue) >= self.config.max_queue_size:
                    self._metrics.rejected_requests += 1
                    return False
                self._metrics.queued_requests = len(self._queue)

        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout)
            wait_time = time.monotonic() - start
            self._wait_times.append(wait_time)

            async with self._lock:
                self._metrics.active_requests += 1
                self._metrics.total_processed += 1
                self._update_avg_wait(wait_time)

            return True
        except asyncio.TimeoutError:
            self._metrics.rejected_requests += 1
            return False

    def release(self) -> None:
        """Release a flow control token."""
        self._semaphore.release()
        async with self._lock:
            self._metrics.active_requests = max(0, self._metrics.active_requests - 1)

    def _update_avg_wait(self, wait_time: float) -> None:
        """Update average wait time."""
        if self._wait_times:
            total = sum(self._wait_times)
            self._metrics.avg_wait_time = total / len(self._wait_times)

    def get_metrics(self) -> FlowMetrics:
        """Get current flow control metrics."""
        with self._lock:
            self._metrics.last_update = time.time()
            return FlowMetrics(
                active_requests=self._metrics.active_requests,
                queued_requests=len(self._queue),
                dropped_requests=self._metrics.dropped_requests,
                rejected_requests=self._metrics.rejected_requests,
                total_processed=self._metrics.total_processed,
                avg_wait_time=self._metrics.avg_wait_time,
                last_update=self._metrics.last_update,
            )

    def reset(self) -> None:
        """Reset flow control state."""
        self._metrics = FlowMetrics()
        self._wait_times.clear()
        self._queue.clear()
        self._token_bucket = TokenBucket(
            rate=self.config.recovery_rate,
            capacity=self.config.max_queue_size,
        )
