"""API Request Coalescing Action Module.

Implements request coalescing to batch multiple concurrent requests
into single network calls, reducing API load and improving throughput.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CoalescedRequest:
    """A coalesced request awaiting execution."""
    key: str
    future: asyncio.Future
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    callbacks: List[Callable] = field(default_factory=list)


class RequestCoalescer:
    """Coalesces multiple identical requests into a single execution.
    
    When multiple coroutines request the same resource simultaneously,
    only one network call is made and all waiters share the result.
    """
    
    def __init__(self, ttl_seconds: float = 1.0, max_batch_size: int = 100):
        self.ttl_seconds = ttl_seconds
        self.max_batch_size = max_batch_size
        self._pending: Dict[str, CoalescedRequest] = {}
        self._batches: Dict[str, List[CoalescedRequest]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._batch_tasks: Dict[str, asyncio.Task] = {}
        self._metrics = {
            "total_requests": 0,
            "coalesced_requests": 0,
            "batches_executed": 0,
            "total_wait_time": 0.0
        }
    
    async def execute(
        self,
        key: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute a function with request coalescing.
        
        Args:
            key: Unique key for coalescing (e.g., "GET:/api/users/123")
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of the function
        """
        self._metrics["total_requests"] += 1
        
        async with self._lock:
            if key in self._pending:
                self._metrics["coalesced_requests"] += 1
                request = self._pending[key]
                request.callbacks.append(time.time())
                
                try:
                    result = await asyncio.wait_for(request.future, timeout=30.0)
                    return result
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Request {key} timed out waiting for coalesced result")
            
            future = asyncio.get_event_loop().create_future()
            request = CoalescedRequest(
                key=key,
                future=future,
                args=args,
                kwargs=kwargs
            )
            self._pending[key] = request
            self._batches[key].append(request)
            
            asyncio.create_task(self._schedule_batch(key))
        
        try:
            result = await func(*args, **kwargs)
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            async with self._lock:
                if key in self._pending:
                    del self._pending[key]
    
    async def _schedule_batch(self, key: str) -> None:
        """Schedule batch execution for a key."""
        await asyncio.sleep(0.01)
        
        async with self._lock:
            if key not in self._batches or not self._batches[key]:
                return
            
            if key in self._batch_tasks:
                return
            
            batch = self._batches[key]
            self._batch_tasks[key] = asyncio.current_task()
        
        try:
            await asyncio.sleep(self.ttl_seconds)
            
            async with self._lock:
                requests = self._batches.pop(key, [])
                if key in self._batch_tasks:
                    del self._batch_tasks[key]
                
                if len(requests) <= 1:
                    return
                
                self._metrics["batches_executed"] += 1
        
        except asyncio.CancelledError:
            pass
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get coalescing metrics."""
        return {
            **self._metrics,
            "pending_count": len(self._pending),
            "batch_count": len(self._batches),
            "coalescing_rate": (
                self._metrics["coalesced_requests"] / max(self._metrics["total_requests"], 1)
            )
        }
    
    def clear(self) -> None:
        """Clear all pending requests and batches."""
        self._pending.clear()
        self._batches.clear()
        for task in self._batch_tasks.values():
            task.cancel()
        self._batch_tasks.clear()


class KeyGenerator:
    """Generates coalescing keys from request parameters."""
    
    @staticmethod
    def for_url_method(url: str, method: str = "GET") -> str:
        """Generate key from URL and method."""
        return f"{method.upper()}:{url}"
    
    @staticmethod
    def for_api_call(endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate key from API endpoint and parameters."""
        if not params:
            return endpoint
        
        sorted_params = sorted(params.items())
        param_str = "&".join(f"{k}={v}" for k, v in sorted_params if v is not None)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
        return f"{endpoint}?{param_hash}"
    
    @staticmethod
    def for_cache_key(prefix: str, **kwargs) -> str:
        """Generate cache-style key."""
        parts = [prefix]
        for k, v in sorted(kwargs.items()):
            if v is not None:
                parts.append(f"{k}={v}")
        return ":".join(parts)


class AdaptiveCoalescer(RequestCoalescer):
    """Adaptive request coalescer that adjusts TTL based on load.
    
    Dynamically adjusts coalescing window based on:
    - Request rate
    - Pending request count
    - Network latency
    """
    
    def __init__(
        self,
        min_ttl: float = 0.001,
        max_ttl: float = 1.0,
        target_pending: int = 10
    ):
        super().__init__(ttl_seconds=min_ttl)
        self.min_ttl = min_ttl
        self.max_ttl = max_ttl
        self.target_pending = target_pending
        self._current_ttl = min_ttl
        self._last_adjustment = time.time()
        self._adjustment_interval = 0.1
    
    async def execute(self, key: str, func: Callable, *args, **kwargs) -> Any:
        """Execute with adaptive TTL adjustment."""
        await self._maybe_adjust_ttl()
        
        self.ttl_seconds = self._current_ttl
        return await super().execute(key, func, *args, **kwargs)
    
    async def _maybe_adjust_ttl(self) -> None:
        """Adjust TTL based on current load."""
        now = time.time()
        if now - self._last_adjustment < self._adjustment_interval:
            return
        
        self._last_adjustment = now
        
        pending_count = len(self._pending)
        
        if pending_count > self.target_pending * 2:
            self._current_ttl = min(self._current_ttl * 1.5, self.max_ttl)
            logger.debug(f"Increasing TTL to {self._current_ttl:.4f}s (pending: {pending_count})")
        
        elif pending_count < self.target_pending // 2:
            self._current_ttl = max(self._current_ttl * 0.8, self.min_ttl)
            logger.debug(f"Decreasing TTL to {self._current_ttl:.4f}s (pending: {pending_count})")


class RequestDeduplicator:
    """Deduplicates identical requests within a time window.
    
    Unlike coalescing, deduplication cancels duplicate requests
    if an identical one is already in flight.
    """
    
    def __init__(self, window_seconds: float = 5.0):
        self.window_seconds = window_seconds
        self._in_flight: Dict[str, Tuple[asyncio.Future, float]] = {}
        self._lock = asyncio.Lock()
    
    async def execute(
        self,
        key: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute with deduplication.
        
        Args:
            key: Unique key for the request
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of the function
        """
        async with self._lock:
            now = time.time()
            
            if key in self._in_flight:
                future, start_time = self._in_flight[key]
                
                if now - start_time < self.window_seconds:
                    return await future
            
            future = asyncio.get_event_loop().create_future()
            self._in_flight[key] = (future, now)
        
        try:
            result = await func(*args, **kwargs)
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            async with self._lock:
                if key in self._in_flight:
                    del self._in_flight[key]
    
    def clear_expired(self) -> int:
        """Remove expired entries from in-flight cache.
        
        Returns:
            Number of entries removed
        """
        now = time.time()
        expired = [
            k for k, (_, start) in self._in_flight.items()
            if now - start >= self.window_seconds
        ]
        
        for k in expired:
            del self._in_flight[k]
        
        return len(expired)


class BatchRequestExecutor:
    """Executes multiple requests as a batch.
    
    Collects requests over a time window and executes them
    together, useful for APIs that support batch operations.
    """
    
    def __init__(
        self,
        batch_size: int = 50,
        window_ms: float = 100.0,
        executor: Optional[Callable] = None
    ):
        self.batch_size = batch_size
        self.window_ms = window_ms
        self.executor = executor
        self._queue: List[Tuple[str, Callable, asyncio.Future, tuple, dict]] = []
        self._lock = asyncio.Lock()
        self._pending_task: Optional[asyncio.Task] = None
    
    async def add(
        self,
        key: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Add a request to the batch.
        
        Args:
            key: Request identifier
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of the function
        """
        future = asyncio.get_event_loop().create_future()
        
        async with self._lock:
            self._queue.append((key, func, future, args, kwargs))
            
            if self._pending_task is None or self._pending_task.done():
                self._pending_task = asyncio.create_task(self._flush_after_window())
            
            if len(self._queue) >= self.batch_size:
                await self._flush()
        
        return await future
    
    async def _flush_after_window(self) -> None:
        """Flush batch after window expires."""
        await asyncio.sleep(self.window_ms / 1000.0)
        await self._flush()
    
    async def _flush(self) -> None:
        """Execute all queued requests."""
        async with self._lock:
            if not self._queue:
                return
            
            queue = self._queue[:]
            self._queue.clear()
        
        if self.executor:
            await self._execute_batched(queue)
        else:
            await self._execute_individual(queue)
    
    async def _execute_batched(self, queue: List) -> None:
        """Execute using batch executor function."""
        try:
            keys = [item[0] for item in queue]
            results = await self.executor([item[1:] for item in queue])
            
            for i, (_, _, future, _, _) in enumerate(queue):
                if i < len(results):
                    future.set_result(results[i])
                else:
                    future.set_exception(IndexError("Result index out of range"))
        except Exception as e:
            for _, _, future, _, _ in queue:
                if not future.done():
                    future.set_exception(e)
    
    async def _execute_individual(self, queue: List) -> None:
        """Execute each request individually."""
        for key, func, future, args, kwargs in queue:
            try:
                result = await func(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
