"""
API Concurrency Action - Manages concurrent API requests with limits.

This module provides concurrency management for API calls including
semaphore-based limiting, request prioritization, and concurrent
execution control.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from collections import defaultdict


class Priority(Enum):
    """Request priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class ConcurrencyConfig:
    """Configuration for concurrency control."""
    max_concurrent: int = 10
    max_per_host: int = 5
    rate_limit: int | None = None
    rate_window: float = 1.0
    timeout: float = 30.0
    retry_attempts: int = 0


@dataclass
class ConcurrencyRequest:
    """A request with concurrency metadata."""
    request_id: str
    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    headers: dict[str, str] = field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    host: str | None = None
    timestamp: float = field(default_factory=time.time)
    future: asyncio.Future | None = None


@dataclass
class ConcurrencyResult:
    """Result of a concurrent request."""
    request_id: str
    success: bool
    response: Any = None
    error: str | None = None
    duration_ms: float = 0.0
    waited_ms: float = 0.0


class HostSemaphore:
    """Semaphore per host for connection limiting."""
    
    def __init__(self, max_per_host: int) -> None:
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._max_per_host = max_per_host
        self._lock = asyncio.Lock()
    
    async def acquire(self, host: str) -> tuple[asyncio.Semaphore, str]:
        """Acquire semaphore for host."""
        async with self._lock:
            if host not in self._semaphores:
                self._semaphores[host] = asyncio.Semaphore(self._max_per_host)
            return self._semaphores[host], host
    
    async def release(self, host: str) -> None:
        """Release semaphore for host."""
        pass


class RateLimiter:
    """Token bucket rate limiter for concurrent requests."""
    
    def __init__(self, rate: int, window: float) -> None:
        self.rate = rate
        self.window = window
        self._tokens = rate
        self._last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> bool:
        """Acquire permission to proceed."""
        async with self._lock:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.rate, self._tokens + elapsed * (self.rate / self.window))
        self._last_update = now


class APIConcurrencyManager:
    """
    Manages concurrent API requests with limits.
    
    Example:
        manager = APIConcurrencyManager(ConcurrencyConfig(max_concurrent=5))
        result = await manager.execute("GET", "/api/data")
    """
    
    def __init__(self, config: ConcurrencyConfig | None = None) -> None:
        self.config = config or ConcurrencyConfig()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._host_semaphore = HostSemaphore(self.config.max_per_host)
        self._rate_limiter = RateLimiter(
            self.config.rate_limit or 0,
            self.config.rate_window,
        ) if self.config.rate_limit else None
        self._active_requests: dict[str, asyncio.Task] = {}
        self._request_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
    
    async def execute(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        priority: Priority = Priority.NORMAL,
        request_id: str | None = None,
    ) -> ConcurrencyResult:
        """
        Execute a request with concurrency control.
        
        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters
            body: Request body
            headers: Request headers
            priority: Request priority
            request_id: Optional request ID
            
        Returns:
            ConcurrencyResult with response data
        """
        import uuid
        from urllib.parse import urlparse
        
        request_id = request_id or str(uuid.uuid4())
        parsed = urlparse(url)
        host = f"{parsed.scheme}://{parsed.netloc}"
        
        request = ConcurrencyRequest(
            request_id=request_id,
            method=method,
            url=url,
            params=params or {},
            body=body,
            headers=headers or {},
            priority=priority,
            host=host,
        )
        
        future = asyncio.Future()
        request.future = future
        
        await self._request_queue.put((4 - priority.value, request))
        
        waited_ms = 0
        start_wait = time.time()
        
        async with self._semaphore:
            waited_ms = (time.time() - start_wait) * 1000
            
            host_sem, host_key = await self._host_semaphore.acquire(request.host)
            
            async with host_sem:
                if self._rate_limiter:
                    while not await self._rate_limiter.acquire():
                        await asyncio.sleep(0.01)
                
                result = await self._execute_request(request)
                result.waited_ms = waited_ms
        
        return result
    
    async def _execute_request(
        self,
        request: ConcurrencyRequest,
    ) -> ConcurrencyResult:
        """Execute the actual HTTP request."""
        start_time = time.time()
        
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    request.method,
                    request.url,
                    params=request.params,
                    json=request.body,
                    headers=request.headers,
                ) as response:
                    try:
                        data = await response.json()
                    except Exception:
                        data = await response.text()
                    
                    return ConcurrencyResult(
                        request_id=request.request_id,
                        success=response.status < 400,
                        response=data,
                        status_code=response.status,
                        duration_ms=(time.time() - start_time) * 1000,
                    )
        
        except asyncio.TimeoutError:
            return ConcurrencyResult(
                request_id=request.request_id,
                success=False,
                error=f"Request timed out after {self.config.timeout}s",
                duration_ms=(time.time() - start_time) * 1000,
            )
        
        except Exception as e:
            return ConcurrencyResult(
                request_id=request.request_id,
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
    
    async def execute_many(
        self,
        requests: list[tuple[str, str, dict[str, Any]]],
    ) -> list[ConcurrencyResult]:
        """Execute multiple requests concurrently."""
        tasks = [
            self.execute(method, url, params)
            for method, url, params in requests
        ]
        return await asyncio.gather(*tasks)
    
    def get_active_count(self) -> int:
        """Get count of active requests."""
        return len(self._active_requests)


@dataclass 
class status_code(int): pass


class APIConcurrencyAction:
    """
    API concurrency action for automation workflows.
    
    Example:
        action = APIConcurrencyAction(ConcurrencyConfig(max_concurrent=10))
        results = await action.execute_parallel(requests)
    """
    
    def __init__(self, config: ConcurrencyConfig | None = None) -> None:
        self.manager = APIConcurrencyManager(config)
    
    async def execute(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> ConcurrencyResult:
        """Execute a single request."""
        return await self.manager.execute(method, url, **kwargs)
    
    async def execute_parallel(
        self,
        requests: list[dict[str, Any]],
    ) -> list[ConcurrencyResult]:
        """Execute multiple requests in parallel."""
        tasks = [
            self.manager.execute(
                r.get("method", "GET"),
                r.get("url"),
                params=r.get("params"),
                body=r.get("body"),
                priority=r.get("priority", Priority.NORMAL),
            )
            for r in requests
        ]
        return await asyncio.gather(*tasks)


# Export public API
__all__ = [
    "Priority",
    "ConcurrencyConfig",
    "ConcurrencyRequest",
    "ConcurrencyResult",
    "HostSemaphore",
    "RateLimiter",
    "APIConcurrencyManager",
    "APIConcurrencyAction",
]
