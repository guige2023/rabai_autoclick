"""
API Batching Action Module

Provides request batching, coalescing, and batch processing for API operations.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import hashlib


@dataclass
class BatchConfig:
    """Configuration for batching behavior."""
    max_batch_size: int = 100
    max_wait_time: float = 0.1  # seconds
    enable_deduplication: bool = True
    dedup_window_seconds: float = 5.0
    strategy: str = "fixed_size"  # fixed_size, adaptive, time_based


@dataclass
class BatchRequest:
    """A single request in a batch."""
    request_id: str
    key: str
    payload: dict[str, Any]
    callback: Optional[Callable] = None
    timestamp: datetime = field(default_factory=datetime.now)
    retry_count: int = 0


@dataclass
class BatchResult:
    """Result of batch processing."""
    request_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None


class RequestBatcher:
    """Batches multiple requests into single operations."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self._pending: dict[str, list[BatchRequest]] = defaultdict(list)
        self._callbacks: dict[str, list[Callable]] = defaultdict(list)
        self._results: dict[str, BatchResult] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._flush_timers: dict[str, asyncio.Task] = {}
    
    async def add(
        self,
        request_id: str,
        key: str,
        payload: dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Optional[BatchResult]:
        """Add a request to the batch."""
        # Check deduplication
        if self.config.enable_deduplication:
            dedup_key = self._get_dedup_key(key, payload)
            if await self._is_duplicate(dedup_key):
                # Return cached result if available
                if dedup_key in self._results:
                    return self._results[dedup_key]
        
        batch_key = self._get_batch_key(key)
        request = BatchRequest(
            request_id=request_id,
            key=key,
            payload=payload,
            callback=callback
        )
        
        async with self._locks[batch_key]:
            self._pending[batch_key].append(request)
            
            if callback:
                self._callbacks[request_id].append(callback)
            
            # Check if batch should be flushed
            should_flush = self._should_flush(batch_key)
            
            if should_flush:
                await self._flush_batch(batch_key)
    
    def _get_batch_key(self, key: str) -> str:
        """Determine batch key for grouping."""
        if self.config.strategy == "time_based":
            # Group by time window
            now = datetime.now()
            window = int(now.timestamp() / self.config.max_wait_time)
            return f"{key}:{window}"
        return key
    
    def _get_dedup_key(self, key: str, payload: dict) -> str:
        """Generate deduplication key."""
        content = f"{key}:{json.dumps(payload, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def _is_duplicate(self, dedup_key: str) -> bool:
        """Check if request is a duplicate."""
        # Simple in-memory dedup - in production use Redis
        return dedup_key in self._results
    
    def _should_flush(self, batch_key: str) -> bool:
        """Determine if batch should be flushed."""
        batch = self._pending.get(batch_key, [])
        
        if self.config.strategy == "fixed_size":
            return len(batch) >= self.config.max_batch_size
        elif self.config.strategy == "adaptive":
            # Adaptive: smaller batches for larger requests
            return len(batch) >= max(1, self.config.max_batch_size // 2)
        
        return False
    
    async def _flush_batch(self, batch_key: str):
        """Flush a batch for processing."""
        if batch_key not in self._pending or not self._pending[batch_key]:
            return
        
        batch = self._pending[batch_key]
        self._pending[batch_key] = []
        
        # Process batch
        results = await self._process_batch(batch)
        
        # Store results and invoke callbacks
        for request, result in zip(batch, results):
            self._results[request.request_id] = result
            
            if request.callback:
                try:
                    if asyncio.iscoroutinefunction(request.callback):
                        await request.callback(result)
                    else:
                        request.callback(result)
                except Exception:
                    pass
    
    async def _process_batch(self, batch: list[BatchRequest]) -> list[BatchResult]:
        """Process a batch of requests."""
        # Override in subclass to implement actual processing
        return [
            BatchResult(
                request_id=r.request_id,
                success=True,
                result={"processed": r.payload}
            )
            for r in batch
        ]
    
    async def flush_all(self):
        """Flush all pending batches."""
        for batch_key in list(self._pending.keys()):
            async with self._locks[batch_key]:
                if self._pending[batch_key]:
                    await self._flush_batch(batch_key)


class ApiBatchingAction:
    """Main API batching action handler."""
    
    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
        self._batchers: dict[str, RequestBatcher] = {}
        self._stats: dict[str, dict] = defaultdict(lambda: {
            "batches": 0, "requests": 0, "deduped": 0, "errors": 0
        })
    
    def get_batcher(self, name: str) -> RequestBatcher:
        """Get or create a batcher for a specific operation."""
        if name not in self._batchers:
            self._batchers[name] = RequestBatcher(self.config)
        return self._batchers[name]
    
    async def batch_request(
        self,
        batcher_name: str,
        request_id: str,
        key: str,
        payload: dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Optional[BatchResult]:
        """Add a request to a batch."""
        batcher = self.get_batcher(batcher_name)
        self._stats[batcher_name]["requests"] += 1
        
        result = await batcher.add(request_id, key, payload, callback)
        
        if result is None:
            # Request was batched - will be processed later
            pass
        
        return result
    
    async def flush_batcher(self, batcher_name: str):
        """Manually flush a specific batcher."""
        if batcher_name in self._batchers:
            await self._batchers[batcher_name].flush_all()
    
    async def flush_all(self):
        """Flush all batchers."""
        for batcher in self._batchers.values():
            await batcher.flush_all()
    
    async def process_batch(
        self,
        batcher_name: str,
        key: str,
        items: list[dict[str, Any]],
        processor: Callable[[list], Awaitable[list]]
    ) -> list[BatchResult]:
        """Process a collection of items as a batch."""
        results = []
        
        # Create batcher if needed
        batcher = self.get_batcher(batcher_name)
        
        # Process items in batch
        for i in range(0, len(items), self.config.max_batch_size):
            batch_items = items[i:i + self.config.max_batch_size]
            
            try:
                batch_results = await processor(batch_items)
                
                for item, result in zip(batch_items, batch_results):
                    results.append(BatchResult(
                        request_id=item.get("id", str(i)),
                        success=True,
                        result=result
                    ))
                
                self._stats[batcher_name]["batches"] += 1
                
            except Exception as e:
                self._stats[batcher_name]["errors"] += 1
                for item in batch_items:
                    results.append(BatchResult(
                        request_id=item.get("id", str(i)),
                        success=False,
                        error=str(e)
                    ))
        
        return results
    
    async def coalesce_requests(
        self,
        requests: list[tuple[str, dict]],
        coalesce_key: Callable[[dict], str]
    ) -> list[tuple[str, list[dict]]]:
        """Coalesce multiple requests by key."""
        coalesced: dict[str, list[dict]] = defaultdict(list)
        
        for key, payload in requests:
            group_key = coalesce_key(payload)
            coalesced[group_key].append(payload)
        
        return [(k, v) for k, v in coalesced.items()]
    
    def get_stats(self, batcher_name: Optional[str] = None) -> dict[str, Any]:
        """Get batching statistics."""
        if batcher_name:
            return dict(self._stats.get(batcher_name, {}))
        return dict(self._stats)


import json
