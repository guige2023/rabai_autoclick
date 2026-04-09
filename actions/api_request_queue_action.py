"""
API Request Queue Module.

Provides request queuing with priority, batching, deduplication,
and request coalescing for high-throughput API clients.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs
import logging

logger = logging.getLogger(__name__)


class QueuePriority(Enum):
    """Request priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class DeduplicationStrategy(Enum):
    """Deduplication strategy for requests."""
    NONE = "none"
    EXACT = "exact"
    URL_ONLY = "url_only"
    NORMALIZED = "normalized"


@dataclass
class QueuedRequest:
    """Container for a queued API request."""
    request_id: str
    url: str
    method: str = "GET"
    params: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    body: Optional[bytes] = None
    priority: QueuePriority = QueuePriority.NORMAL
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3
    
    @property
    def deduplication_key(self) -> str:
        """Generate key for deduplication."""
        if self.params:
            sorted_params = "&".join(
                f"{k}={v}" for k, v in sorted(self.params.items())
            )
            return f"{self.method}:{self.url}:{sorted_params}"
        return f"{self.method}:{self.url}"
        
    def __lt__(self, other: QueuedRequest) -> bool:
        """Compare by priority and creation time."""
        if self.priority != other.priority:
            return self.priority.value < other.priority.value
        return self.created_at < other.created_at


@dataclass
class QueueConfig:
    """Configuration for request queue."""
    max_size: int = 10000
    max_concurrent: int = 10
    rate_limit: float = 100.0  # requests per second
    burst_size: int = 20
    dedup_strategy: DeduplicationStrategy = DeduplicationStrategy.NORMALIZED
    dedup_ttl: float = 60.0  # seconds
    coalesce_window: float = 0.1  # seconds
    enable_batching: bool = True
    batch_size: int = 10
    batch_timeout: float = 1.0


@dataclass
class RequestResult:
    """Result of a processed request."""
    request: QueuedRequest
    status_code: Optional[int]
    response: Optional[bytes]
    error: Optional[str]
    latency: float
    cached: bool = False


class RequestQueue:
    """
    High-throughput API request queue with priority and deduplication.
    
    Example:
        queue = RequestQueue(RequestQueueConfig(max_concurrent=20))
        
        # Queue requests
        await queue.enqueue("https://api.example.com/data", priority=QueuePriority.HIGH)
        await queue.enqueue("https://api.example.com/search", params={"q": "test"})
        
        # Process with handler
        async def handler(req):
            async with aiohttp.ClientSession() as sess:
                return await sess.request(req.method, req.url, **kwargs)
                
        async for result in queue.process(handler):
            print(result)
    """
    
    def __init__(self, config: Optional[QueueConfig] = None) -> None:
        """
        Initialize the request queue.
        
        Args:
            config: Queue configuration.
        """
        self.config = config or QueueConfig()
        self._queue: List[QueuedRequest] = []
        self._pending: Set[str] = set()
        self._in_flight: Set[str] = set()
        self._dedup_cache: Dict[str, float] = {}
        self._results: Dict[str, RequestResult] = {}
        self._coalescing: Dict[str, List[asyncio.Future]] = {}
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._rate_limiter = asyncio.Semaphore(int(self.config.rate_limit))
        self._running = False
        self._request_counter = 0
        
    async def enqueue(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        priority: QueuePriority = QueuePriority.NORMAL,
        timeout: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Enqueue a request.
        
        Args:
            url: Request URL.
            method: HTTP method.
            params: Query parameters.
            headers: Request headers.
            body: Request body.
            priority: Request priority.
            timeout: Request timeout.
            metadata: Optional metadata.
            
        Returns:
            Request ID.
        """
        self._request_counter += 1
        request_id = f"req_{self._request_counter}_{int(time.time() * 1000)}"
        
        request = QueuedRequest(
            request_id=request_id,
            url=url,
            method=method.upper(),
            params=params,
            headers=headers,
            body=body,
            priority=priority,
            timeout=timeout,
            metadata=metadata or {},
        )
        
        # Check deduplication
        if self.config.dedup_strategy != DeduplicationStrategy.NONE:
            key = self._get_dedup_key(request)
            if await self._is_duplicate(key):
                logger.debug(f"Deduplicating request: {request_id}")
                # Return existing request ID if found
                return key[:50]  # Return dedup key as pseudo-ID
                
        async with self._lock:
            # Check queue size
            if len(self._queue) >= self.config.max_size:
                if self.config.dedup_strategy == DeduplicationStrategy.NONE:
                    raise RuntimeError("Queue is full")
                # Remove oldest low priority
                await self._evict_low_priority()
                
            self._queue.append(request)
            self._pending.add(request_id)
            self._queue.sort()
            
        logger.debug(f"Enqueued request: {request_id}")
        return request_id
        
    async def enqueue_batch(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        priority: QueuePriority = QueuePriority.NORMAL,
    ) -> List[str]:
        """
        Enqueue multiple requests in batch.
        
        Args:
            requests: List of (url, method, params, headers, body) tuples.
            priority: Request priority.
            
        Returns:
            List of request IDs.
        """
        ids = []
        for url, method, params, headers, body in requests:
            req_id = await self.enqueue(
                url, method, params, headers, body, priority
            )
            ids.append(req_id)
        return ids
        
    async def dequeue(self, timeout: Optional[float] = None) -> Optional[QueuedRequest]:
        """
        Dequeue the next request.
        
        Args:
            timeout: Wait timeout.
            
        Returns:
            Next request or None if queue is empty.
        """
        async with self._lock:
            if not self._queue:
                return None
            request = self._queue.pop(0)
            
        self._pending.discard(request.request_id)
        self._in_flight.add(request.request_id)
        
        return request
        
    async def get_result(self, request_id: str) -> Optional[RequestResult]:
        """Get result for a completed request."""
        return self._results.get(request_id)
        
    async def process(
        self,
        handler: Callable[[QueuedRequest], Any],
    ) -> AsyncIterator[RequestResult]:
        """
        Process queued requests with the given handler.
        
        Args:
            handler: Async function to process each request.
            
        Yields:
            Request results.
        """
        self._running = True
        
        while self._running:
            try:
                # Get next request with rate limiting
                async with self._rate_limiter:
                    request = await self.dequeue()
                    if not request:
                        await asyncio.sleep(0.01)
                        continue
                        
                # Process with semaphore for concurrency control
                async with self._semaphore:
                    result = await self._process_request(request, handler)
                    yield result
                    
            except Exception as e:
                logger.error(f"Process error: {e}")
                await asyncio.sleep(0.1)
                
    async def _process_request(
        self,
        request: QueuedRequest,
        handler: Callable[[QueuedRequest], Any],
    ) -> RequestResult:
        """Process a single request."""
        start_time = time.time()
        cached = False
        
        # Check coalescing
        coalesce_key = self._get_coalesce_key(request)
        future: Optional[asyncio.Future] = None
        
        async with self._lock:
            if coalesce_key in self._coalescing:
                # Wait for existing request
                futures = self._coalescing[coalesce_key]
                future = futures[0]
            else:
                # Create new future for others to wait on
                future = asyncio.get_event_loop().create_future()
                self._coalescing[coalesce_key] = [future]
                
        if future and len(self._coalescing[coalesce_key]) > 1:
            # Another request is in flight, wait for result
            try:
                result = await asyncio.wait_for(future, timeout=request.timeout)
                result.cached = True
                return result
            except asyncio.TimeoutError:
                pass
                
        # Execute request
        status_code = None
        response = None
        error = None
        
        for attempt in range(request.max_retries + 1):
            try:
                result = await asyncio.wait_for(
                    handler(request),
                    timeout=request.timeout
                )
                
                if hasattr(result, "status"):
                    status_code = result.status
                if hasattr(result, "content"):
                    response = result.content
                    
                break
            except asyncio.TimeoutError:
                error = "Request timeout"
            except Exception as e:
                error = str(e)
                if attempt < request.max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    
        latency = time.time() - start_time
        
        request_result = RequestResult(
            request=request,
            status_code=status_code,
            response=response,
            error=error,
            latency=latency,
            cached=cached,
        )
        
        # Store result
        self._results[request.request_id] = request_result
        self._in_flight.discard(request.request_id)
        
        # Resolve coalescing futures
        async with self._lock:
            if coalesce_key in self._coalescing:
                for f in self._coalescing[coalesce_key]:
                    if not f.done():
                        f.set_result(request_result)
                del self._coalescing[coalesce_key]
                
        # Cleanup old dedup cache
        await self._cleanup_dedup_cache()
        
        return request_result
        
    def _get_dedup_key(self, request: QueuedRequest) -> str:
        """Get deduplication key for request."""
        if self.config.dedup_strategy == DeduplicationStrategy.EXACT:
            key = f"{request.method}:{request.url}:{request.body}"
        elif self.config.dedup_strategy == DeduplicationStrategy.URL_ONLY:
            key = f"{request.method}:{request.url}"
        else:  # NORMALIZED
            key = request.deduplication_key
            
        return hashlib.sha256(key.encode()).hexdigest()
        
    def _get_coalesce_key(self, request: QueuedRequest) -> str:
        """Get coalescing key for request."""
        parsed = urlparse(request.url)
        return f"{request.method}:{parsed.netloc}{parsed.path}"
        
    async def _is_duplicate(self, key: str) -> bool:
        """Check if request is a duplicate."""
        now = time.time()
        if key in self._dedup_cache:
            if now - self._dedup_cache[key] < self.config.dedup_ttl:
                return True
            del self._dedup_cache[key]
        self._dedup_cache[key] = now
        return False
        
    async def _cleanup_dedup_cache(self) -> None:
        """Remove expired deduplication entries."""
        now = time.time()
        expired = [
            k for k, v in self._dedup_cache.items()
            if now - v > self.config.dedup_ttl
        ]
        for k in expired:
            del self._dedup_cache[k]
            
    async def _evict_low_priority(self) -> None:
        """Evict low priority requests when queue is full."""
        if not self._queue:
            return
        # Remove lowest priority, oldest
        self._queue.sort(key=lambda r: (r.priority.value, -r.created_at))
        removed = self._queue.pop(0)
        self._pending.discard(removed.request_id)
        logger.warning(f"Evicted low priority request: {removed.request_id}")
        
    def size(self) -> int:
        """Get current queue size."""
        return len(self._queue)
        
    def pending(self) -> int:
        """Get number of pending requests."""
        return len(self._pending)
        
    def in_flight(self) -> int:
        """Get number of in-flight requests."""
        return len(self._in_flight)
        
    def clear(self) -> None:
        """Clear all queued requests."""
        self._queue.clear()
        self._pending.clear()
        self._results.clear()
        logger.info("Request queue cleared")
        
    async def stop(self) -> None:
        """Stop the queue processing."""
        self._running = False
