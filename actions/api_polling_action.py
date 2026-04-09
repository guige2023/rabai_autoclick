"""
API Polling Action Module

Provides intelligent polling mechanisms for API calls.
Supports exponential backoff, adaptive polling, and result caching.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
from datetime import datetime, timedelta

T = TypeVar('T')


class PollingStrategy(Enum):
    """Polling strategy types."""
    FIXED = "fixed"
    LINEAR_BACKOFF = "linear_backoff"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    ADAPTIVE = "adaptive"


class PollingStatus(Enum):
    """Polling operation status."""
    PENDING = "pending"
    POLLING = "polling"
    COMPLETED = "completed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    MAX_ATTEMPTS = "max_attempts"


@dataclass
class PollingConfig:
    """Configuration for polling behavior."""
    strategy: PollingStrategy = PollingStrategy.EXPONENTIAL_BACKOFF
    initial_interval_ms: int = 1000
    max_interval_ms: int = 30000
    timeout_ms: int = 300000
    max_attempts: int = 50
    backoff_multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1


@dataclass
class PollingResult:
    """Result of a polling operation."""
    status: PollingStatus
    value: Optional[Any] = None
    attempts: int = 0
    elapsed_ms: float = 0.0
    error: Optional[Exception] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def success(self) -> bool:
        return self.status == PollingStatus.COMPLETED and self.value is not None


@dataclass
class PollingAttempt:
    """Record of a single polling attempt."""
    attempt_number: int
    timestamp: datetime
    response_time_ms: float
    result: Optional[Any] = None
    error: Optional[str] = None


class PollingCache:
    """Cache for polling results."""
    
    def __init__(self, ttl_seconds: int = 60):
        self._cache: dict[str, tuple[Any, datetime]] = {}
        self._ttl_seconds = ttl_seconds
    
    def _make_key(self, predicate_fn: Any, args: tuple, kwargs: dict) -> str:
        """Generate cache key."""
        key_data = f"{str(predicate_fn)}:{str(args)}:{str(sorted(kwargs.items()))}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, predicate_fn: Any, args: tuple, kwargs: dict) -> Optional[Any]:
        """Get cached result if fresh."""
        key = self._make_key(predicate_fn, args, kwargs)
        if key in self._cache:
            value, cached_at = self._cache[key]
            if datetime.now() - cached_at < timedelta(seconds=self._ttl_seconds):
                return value
            del self._cache[key]
        return None
    
    def set(self, predicate_fn: Any, args: tuple, kwargs: dict, value: Any) -> None:
        """Cache a result."""
        key = self._make_key(predicate_fn, args, kwargs)
        self._cache[key] = (value, datetime.now())
    
    def invalidate(self, predicate_fn: Any, args: tuple, kwargs: dict) -> None:
        """Invalidate a cached result."""
        key = self._make_key(predicate_fn, args, kwargs)
        self._cache.pop(key, None)
    
    def clear(self) -> None:
        """Clear all cached results."""
        self._cache.clear()


class ApiPollingAction:
    """
    Intelligent polling manager for API calls.
    
    Polls an API until a condition is met or timeout/max attempts reached.
    Supports multiple backoff strategies and result caching.
    
    Example:
        poller = ApiPollingAction()
        
        result = await poller.poll(
            fetch_fn=check_job_status,
            predicate=lambda status: status == "completed",
            timeout_ms=60000
        )
    """
    
    def __init__(self, config: Optional[PollingConfig] = None):
        self.config = config or PollingConfig()
        self._cache = PollingCache()
        self._history: deque[PollingAttempt] = deque(maxlen=1000)
        self._active_polls: dict[str, asyncio.Task] = {}
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._stats = {
            "total_polls": 0,
            "successful_polls": 0,
            "timeout_polls": 0,
            "cancelled_polls": 0,
            "total_attempts": 0
        }
    
    async def poll(
        self,
        fetch_fn: Callable[[], Any],
        predicate: Callable[[Any], bool],
        *,
        timeout_ms: Optional[int] = None,
        max_attempts: Optional[int] = None,
        on_attempt: Optional[Callable[[int, Any], None]] = None
    ) -> PollingResult:
        """
        Poll until predicate is satisfied or timeout/max attempts reached.
        
        Args:
            fetch_fn: Function to call to fetch current state
            predicate: Function that returns True when polling should stop
            timeout_ms: Maximum time to poll (uses config default if None)
            max_attempts: Maximum number of attempts (uses config default if None)
            on_attempt: Optional callback called after each attempt
            
        Returns:
            PollingResult with final status and value
        """
        poll_id = f"{id(fetch_fn)}_{id(predicate)}_{time.time()}"
        timeout = timeout_ms or self.config.timeout_ms
        max_attempts = max_attempts or self.config.max_attempts
        start_time = time.time()
        attempts = 0
        
        self._stats["total_polls"] += 1
        self._cancellation_events[poll_id] = asyncio.Event()
        
        try:
            while self._stats[f"attempts_{poll_id}"] < max_attempts:
                elapsed_ms = (time.time() - start_time) * 1000
                
                if elapsed_ms >= timeout:
                    self._stats["timeout_polls"] += 1
                    return PollingResult(
                        status=PollingStatus.TIMEOUT,
                        attempts=attempts,
                        elapsed_ms=elapsed_ms
                    )
                
                attempt_start = time.time()
                try:
                    value = await asyncio.get_event_loop().run_in_executor(
                        None, fetch_fn
                    )
                    attempts += 1
                    self._stats["total_attempts"] += 1
                    
                    attempt_time_ms = (time.time() - attempt_start) * 1000
                    
                    self._history.append(PollingAttempt(
                        attempt_number=attempts,
                        timestamp=datetime.now(),
                        response_time_ms=attempt_time_ms,
                        result=value
                    ))
                    
                    if on_attempt:
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: on_attempt(attempts, value)
                        )
                    
                    if predicate(value):
                        self._stats["successful_polls"] += 1
                        return PollingResult(
                            status=PollingStatus.COMPLETED,
                            value=value,
                            attempts=attempts,
                            elapsed_ms=elapsed_ms
                        )
                
                except Exception as e:
                    self._history.append(PollingAttempt(
                        attempt_number=attempts,
                        timestamp=datetime.now(),
                        response_time_ms=0,
                        error=str(e)
                    ))
                
                interval_ms = self._calculate_interval(attempts)
                
                try:
                    await asyncio.wait_for(
                        asyncio.sleep(interval_ms / 1000.0),
                        timeout=timeout / 1000.0
                    )
                except asyncio.TimeoutError:
                    pass
            
            return PollingResult(
                status=PollingStatus.MAX_ATTEMPTS,
                attempts=attempts,
                elapsed_ms=(time.time() - start_time) * 1000
            )
        
        finally:
            self._cancellation_events.pop(poll_id, None)
    
    def _calculate_interval(self, attempt: int) -> int:
        """Calculate next polling interval based on strategy."""
        base = self.config.initial_interval_ms
        
        if self.config.strategy == PollingStrategy.FIXED:
            return base
        
        elif self.config.strategy == PollingStrategy.LINEAR_BACKOFF:
            interval = base + (attempt * base * (self.config.backoff_multiplier - 1))
            return min(interval, self.config.max_interval_ms)
        
        elif self.config.strategy == PollingStrategy.EXPONENTIAL_BACKOFF:
            interval = base * (self.config.backoff_multiplier ** (attempt - 1))
            interval = min(interval, self.config.max_interval_ms)
            
            if self.config.jitter:
                jitter_range = interval * self.config.jitter_factor
                interval += (hash(time.time()) % 100) / 100 * jitter_range
            
            return int(interval)
        
        elif self.config.strategy == PollingStrategy.ADAPTIVE:
            if len(self._history) >= 2:
                recent_times = [
                    a.response_time_ms for a in list(self._history)[-5:]
                    if a.response_time_ms > 0
                ]
                if recent_times:
                    avg_response = sum(recent_times) / len(recent_times)
                    interval = min(avg_response * 2, self.config.max_interval_ms)
                    return int(interval)
            return base
        
        return base
    
    async def poll_with_cache(
        self,
        fetch_fn: Callable[[], Any],
        predicate: Callable[[Any], bool],
        cache_ttl_seconds: int = 60,
        **kwargs
    ) -> PollingResult:
        """Poll with result caching."""
        cached = self._cache.get(fetch_fn, (), {})
        if cached and predicate(cached):
            return PollingResult(
                status=PollingStatus.COMPLETED,
                value=cached,
                attempts=0,
                elapsed_ms=0
            )
        
        result = await self.poll(fetch_fn, predicate, **kwargs)
        
        if result.success:
            self._cache.set(fetch_fn, (), {}, result.value)
        
        return result
    
    def cancel(self, poll_id: Optional[str] = None) -> None:
        """Cancel active poll(s)."""
        if poll_id and poll_id in self._cancellation_events:
            self._cancellation_events[poll_id].set()
            self._stats["cancelled_polls"] += 1
        else:
            for event in self._cancellation_events.values():
                event.set()
            self._stats["cancelled_polls"] += len(self._cancellation_events)
    
    def get_history(self, limit: int = 100) -> list[PollingAttempt]:
        """Get polling history."""
        return list(self._history)[-limit:]
    
    def get_stats(self) -> dict[str, Any]:
        """Get polling statistics."""
        return {
            **self._stats,
            "cache_size": len(self._cache._cache),
            "active_polls": len(self._active_polls),
            "success_rate": (
                self._stats["successful_polls"] / self._stats["total_polls"]
                if self._stats["total_polls"] > 0 else 0
            ),
            "avg_attempts": (
                self._stats["total_attempts"] / self._stats["total_polls"]
                if self._stats["total_polls"] > 0 else 0
            )
        }
