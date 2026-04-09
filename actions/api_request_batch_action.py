"""
API Request Batch Action Module.

Batches multiple API requests into single operations
with deduplication, combining, and parallel execution.
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class BatchStrategy(Enum):
    """Batching strategy types."""
    TIME_WINDOW = "time_window"
    SIZE_BASED = "size_based"
    HYBRID = "hybrid"


@dataclass
class BatchRequest:
    """
    Individual request within a batch.

    Attributes:
        request_id: Unique identifier.
        func: Async function to call.
        args: Positional arguments.
        kwargs: Keyword arguments.
        priority: Request priority (higher = sooner).
        created_at: Timestamp when request was created.
    """
    request_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: int = 0
    created_at: float = field(default_factory=time.time, init=False)
    future: Optional[asyncio.Future] = field(default=None, init=False)


@dataclass
class BatchConfig:
    """Configuration for request batching."""
    max_batch_size: int = 100
    time_window_ms: float = 50.0
    strategy: BatchStrategy = BatchStrategy.HYBRID
    deduplication_window: float = 5.0
    max_concurrent_batches: int = 10


class APIRequestBatchAction:
    """
    Batches multiple API requests for efficient processing.

    Example:
        batcher = APIRequestBatchAction()
        batcher.configure(max_batch_size=50, time_window_ms=100)

        result1 = await batcher.submit(some_api_call, arg1, arg2)
        result2 = await batcher.submit(another_api_call, arg3)
    """

    def __init__(self, config: Optional[BatchConfig] = None):
        """
        Initialize API request batch action.

        Args:
            config: Batch configuration. Uses defaults if None.
        """
        self.config = config or BatchConfig()
        self._pending: list[BatchRequest] = []
        self._futures: dict[str, asyncio.Future] = {}
        self._dedup_cache: dict[str, tuple[Any, float]] = {}
        self._batch_lock = asyncio.Lock()
        self._running = False
        self._batch_handle: Optional[asyncio.Task] = None

    def _generate_request_id(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """Generate unique request ID with deduplication hash."""
        key_data = f"{func.__name__}:{args}:{kwargs}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:16]

    def _get_dedup_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """Get deduplication key for request."""
        key_data = f"{func.__name__}:{args}:{kwargs}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    def configure(
        self,
        max_batch_size: Optional[int] = None,
        time_window_ms: Optional[float] = None,
        strategy: Optional[BatchStrategy] = None
    ) -> None:
        """Update batch configuration."""
        if max_batch_size is not None:
            self.config.max_batch_size = max_batch_size
        if time_window_ms is not None:
            self.config.time_window_ms = time_window_ms
        if strategy is not None:
            self.config.strategy = strategy

    async def submit(
        self,
        func: Callable,
        *args: Any,
        priority: int = 0,
        **kwargs: Any
    ) -> Any:
        """
        Submit a request to be batched.

        Args:
            func: Async function to execute.
            *args: Positional arguments.
            priority: Request priority.
            **kwargs: Keyword arguments.

        Returns:
            Result from the function execution.
        """
        dedup_key = self._get_dedup_key(func, args, kwargs)

        if dedup_key in self._dedup_cache:
            cached_result, expires = self._dedup_cache[dedup_key]
            if time.time() < expires:
                logger.debug(f"Dedup hit for {func.__name__}")
                return cached_result

        request_id = self._generate_request_id(func, args, kwargs)

        future = asyncio.get_event_loop().create_future()

        request = BatchRequest(
            request_id=request_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            future=future
        )

        async with self._batch_lock:
            self._pending.append(request)
            self._futures[request_id] = future

        if not self._running:
            await self._start_batch_loop()

        return await future

    async def _start_batch_loop(self) -> None:
        """Start the batch processing loop."""
        self._running = True

        while self._pending:
            await asyncio.sleep(self.config.time_window_ms / 1000.0)

            async with self._batch_lock:
                batch = self._take_batch()

            if batch:
                asyncio.create_task(self._process_batch(batch))

        self._running = False

    def _take_batch(self) -> list[BatchRequest]:
        """Take a batch of requests based on strategy."""
        if not self._pending:
            return []

        if self.config.strategy == BatchStrategy.SIZE_BASED:
            batch = self._pending[:self.config.max_batch_size]
            self._pending = self._pending[self.config.max_batch_size:]

        elif self.config.strategy == BatchStrategy.TIME_WINDOW:
            batch = self._pending
            self._pending = []

        else:
            batch = self._pending[:self.config.max_batch_size]
            self._pending = self._pending[self.config.max_batch_size:]

        return batch

    async def _process_batch(self, batch: list[BatchRequest]) -> None:
        """Process a batch of requests."""
        if not batch:
            return

        logger.debug(f"Processing batch of {len(batch)} requests")

        for request in batch:
            request.future.set_result(None)

        batch.sort(key=lambda r: r.priority, reverse=True)

        async def execute_single(req: BatchRequest) -> None:
            try:
                if asyncio.iscoroutinefunction(req.func):
                    result = await req.func(*req.args, **req.kwargs)
                else:
                    result = req.func(*req.args, **req.kwargs)

                req.future.set_result(result)

                dedup_key = self._get_dedup_key(req.func, req.args, req.kwargs)
                self._dedup_cache[dedup_key] = (result, time.time() + self.config.deduplication_window)

            except Exception as e:
                logger.error(f"Batch request failed: {e}")
                req.future.set_exception(e)

        await asyncio.gather(*[execute_single(req) for req in batch], return_exceptions=True)

    async def flush(self) -> None:
        """Force flush all pending requests."""
        async with self._batch_lock:
            batch = self._pending.copy()
            self._pending.clear()

        if batch:
            asyncio.create_task(self._process_batch(batch))

    def get_stats(self) -> dict:
        """Get batch processing statistics."""
        return {
            "pending_requests": len(self._pending),
            "active_futures": len(self._futures),
            "dedup_cache_size": len(self._dedup_cache),
            "config": {
                "max_batch_size": self.config.max_batch_size,
                "time_window_ms": self.config.time_window_ms,
                "strategy": self.config.strategy.value
            }
        }

    def clear_dedup_cache(self) -> None:
        """Clear the deduplication cache."""
        self._dedup_cache.clear()
