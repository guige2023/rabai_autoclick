"""
API Batch Action - Batches and queues multiple API requests.

This module provides batch processing for API calls including
request queuing, batching, deduplication, and rate limiting.
"""

from __future__ import annotations

import asyncio
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from collections import defaultdict


class BatchStrategy(Enum):
    """Strategy for batching requests."""
    FIXED_SIZE = "fixed_size"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class BatchRequest:
    """A single request to be batched."""
    request_id: str
    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    headers: dict[str, str] = field(default_factory=dict)
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    future: asyncio.Future | None = None


@dataclass
class BatchResponse:
    """Response for a batched request."""
    request_id: str
    success: bool
    status_code: int | None = None
    data: Any = None
    error: str | None = None
    duration_ms: float = 0.0


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    strategy: BatchStrategy = BatchStrategy.FIXED_SIZE
    batch_size: int = 10
    window_ms: float = 100.0
    max_queue_size: int = 1000
    rate_limit: int = 100
    rate_window: float = 60.0


class RequestDeduplicator:
    """Deduplicates identical requests."""
    
    def __init__(self, ttl: float = 60.0) -> None:
        self.ttl = ttl
        self._cache: dict[str, tuple[Any, float]] = {}
    
    def _make_key(self, request: BatchRequest) -> str:
        """Generate deduplication key."""
        data = f"{request.method}:{request.url}:{request.params}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def check(self, request: BatchRequest) -> Any | None:
        """Check if duplicate exists and is valid."""
        key = self._make_key(request)
        if key in self._cache:
            value, timestamp = self._cache[key]
            if (time.time() - timestamp) < self.ttl:
                return value
            del self._cache[key]
        return None
    
    def store(self, request: BatchRequest, response: Any) -> None:
        """Store response for deduplication."""
        key = self._make_key(request)
        self._cache[key] = (response, time.time())
    
    def clear(self) -> None:
        """Clear deduplication cache."""
        self._cache.clear()


class RateLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: int, window: float) -> None:
        self.rate = rate
        self.window = window
        self._tokens = rate
        self._last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens for request."""
        async with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.rate, self._tokens + elapsed * (self.rate / self.window))
        self._last_update = now


class APIBatcher:
    """
    Batches multiple API requests for efficient processing.
    
    Example:
        batcher = APIBatcher(BatchConfig(batch_size=20))
        await batcher.add_request("GET", "/api/users")
        await batcher.add_request("GET", "/api/orders")
        responses = await batcher.flush()
    """
    
    def __init__(self, config: BatchConfig | None = None) -> None:
        self.config = config or BatchConfig()
        self._queue: list[BatchRequest] = []
        self._lock = asyncio.Lock()
        self._rate_limiter = RateLimiter(self.config.rate_limit, self.config.rate_window)
        self._deduplicator = RequestDeduplicator()
        self._processing = False
        self._batch_callback: Callable[[list[BatchRequest]], Any] | None = None
    
    def set_batch_callback(
        self,
        callback: Callable[[list[BatchRequest]], Any],
    ) -> None:
        """Set callback for processing batches."""
        self._batch_callback = callback
    
    async def add_request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        priority: int = 0,
        deduplicate: bool = True,
    ) -> BatchResponse:
        """Add a request to the batch queue."""
        import uuid
        
        request = BatchRequest(
            request_id=str(uuid.uuid4()),
            method=method,
            url=url,
            params=params or {},
            body=body,
            headers=headers or {},
            priority=priority,
        )
        
        if deduplicate:
            cached = self._deduplicator.check(request)
            if cached is not None:
                return BatchResponse(
                    request_id=request.request_id,
                    success=True,
                    data=cached,
                    duration_ms=0.0,
                )
        
        async with self._lock:
            if len(self._queue) >= self.config.max_queue_size:
                await self.flush()
            
            self._queue.append(request)
        
        should_flush = False
        
        if self.config.strategy == BatchStrategy.FIXED_SIZE:
            should_flush = len(self._queue) >= self.config.batch_size
        elif self.config.strategy == BatchStrategy.FIXED_WINDOW:
            should_flush = True
        
        if should_flush:
            await self.flush()
        
        future = asyncio.Future()
        request.future = future
        
        return await future
    
    async def flush(self) -> list[BatchResponse]:
        """Flush the queue and process all requests."""
        async with self._lock:
            if not self._queue:
                return []
            
            batch = sorted(self._queue, key=lambda r: (-r.priority, r.timestamp))
            self._queue.clear()
        
        if self._batch_callback:
            results = await self._batch_callback(batch)
        else:
            results = await self._process_batch(batch)
        
        responses = []
        for request, result in zip(batch, results):
            if request.future and not request.future.done():
                request.future.set_result(result)
            responses.append(result)
        
        return responses
    
    async def _process_batch(
        self,
        batch: list[BatchRequest],
    ) -> list[BatchResponse]:
        """Process a batch of requests."""
        responses = []
        
        for request in batch:
            await self._rate_limiter.acquire()
            
            start_time = time.time()
            try:
                import aiohttp
                timeout = aiohttp.ClientTimeout(total=30.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.request(
                        request.method,
                        request.url,
                        params=request.params,
                        json=request.body,
                        headers=request.headers,
                    ) as response:
                        data = await response.json()
                        resp = BatchResponse(
                            request_id=request.request_id,
                            success=response.status < 400,
                            status_code=response.status,
                            data=data,
                            duration_ms=(time.time() - start_time) * 1000,
                        )
                        if resp.success:
                            self._deduplicator.store(request, data)
            except Exception as e:
                resp = BatchResponse(
                    request_id=request.request_id,
                    success=False,
                    error=str(e),
                    duration_ms=(time.time() - start_time) * 1000,
                )
            
            responses.append(resp)
        
        return responses


class APIBatchAction:
    """
    API batch processing action for automation workflows.
    
    Example:
        action = APIBatchAction(BatchConfig(batch_size=50))
        
        @action.batch_callback()
        async def process_batch(requests):
            # Custom batch processing logic
            return responses
        
        result = await action.execute_batch(requests)
    """
    
    def __init__(self, config: BatchConfig | None = None) -> None:
        self.batcher = APIBatcher(config)
    
    def batch_callback(
        self,
        callback: Callable[[list[BatchRequest]], Any],
    ) -> Callable:
        """Decorator to set batch processing callback."""
        self.batcher.set_batch_callback(callback)
        return callback
    
    async def add_request(self, method: str, url: str, **kwargs) -> BatchResponse:
        """Add a request to the batch."""
        return await self.batcher.add_request(method, url, **kwargs)
    
    async def flush(self) -> list[BatchResponse]:
        """Flush pending requests."""
        return await self.batcher.flush()
    
    def clear_cache(self) -> None:
        """Clear deduplication cache."""
        self.batcher._deduplicator.clear()


# Export public API
__all__ = [
    "BatchStrategy",
    "BatchRequest",
    "BatchResponse",
    "BatchConfig",
    "RequestDeduplicator",
    "RateLimiter",
    "APIBatcher",
    "APIBatchAction",
]
