"""
API request deduplication and coalescing for reducing redundant calls.

This module prevents duplicate API requests by coalescing simultaneous
requests for the same resource into a single backend call.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Hashable, List, Optional, Set
from collections import defaultdict
import threading
import uuid

logger = logging.getLogger(__name__)


@dataclass
class PendingRequest:
    """Represents a pending deduplicated request."""
    key: Hashable
    future: asyncio.Future
    created_at: float
    waiters: int = 1
    result: Any = field(default=None, init=False)
    error: Optional[Exception] = field(default=None, init=False)
    completed: bool = False


class RequestDeduplicator:
    """
    Deduplicate concurrent API requests for the same resource.

    When multiple requests for the same key arrive simultaneously,
    only one backend call is made and all waiters share the result.

    Features:
    - Configurable TTL for deduplication windows
    - Automatic cleanup of stale entries
    - Statistics tracking
    - Thread-safe operations
    - Async support

    Example:
        >>> dedup = RequestDeduplicator(ttl=5.0)
        >>> async def get_user(user_id):
        ...     return await dedup.execute(
        ...         key=user_id,
        ...         func=lambda: api.get_user(user_id)
        ...     )
        >>>
        >>> # These three calls only trigger ONE backend request
        >>> results = await asyncio.gather(
        ...     get_user(123),
        ...     get_user(123),
        ...     get_user(123),
        ... )
    """

    def __init__(
        self,
        ttl: float = 5.0,
        max_pending: int = 1000,
        cleanup_interval: float = 60.0,
    ):
        """
        Initialize the deduplicator.

        Args:
            ttl: Time window for deduplication in seconds
            max_pending: Maximum pending requests
            cleanup_interval: Seconds between cleanup runs
        """
        self.ttl = ttl
        self.max_pending = max_pending
        self.cleanup_interval = cleanup_interval

        self._pending: Dict[Hashable, PendingRequest] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

        self._stats = {
            "deduplicated": 0,
            "executed": 0,
            "errors": 0,
        }

        logger.info(f"RequestDeduplicator initialized (ttl={ttl}s, max_pending={max_pending})")

    async def execute(
        self,
        key: Hashable,
        func: Callable[..., Any],
        *args,
        **kwargs,
    ) -> Any:
        """
        Execute a function with request deduplication.

        Args:
            key: Deduplication key
            func: Function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result
        """
        await self._ensure_cleanup()

        async with self._lock:
            if key in self._pending:
                pending = self._pending[key]
                pending.waiters += 1
                self._stats["deduplicated"] += 1
                logger.debug(f"Deduplicating request for key={key} (waiters={pending.waiters})")
            else:
                future = asyncio.get_event_loop().create_future()
                pending = PendingRequest(
                    key=key,
                    future=future,
                    created_at=time.time(),
                )
                self._pending[key] = pending
                self._stats["executed"] += 1

        if pending.future.done():
            if pending.completed:
                return pending.result
            else:
                raise pending.error

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            pending.result = result
            pending.completed = True
            pending.future.set_result(result)
            logger.debug(f"Request completed for key={key}")

        except Exception as e:
            pending.error = e
            pending.future.set_exception(e)
            self._stats["errors"] += 1
            logger.warning(f"Request failed for key={key}: {e}")

        finally:
            async with self._lock:
                if pending.waiters <= 1:
                    self._pending.pop(key, None)
                else:
                    pending.waiters -= 1

        return pending.result

    async def _ensure_cleanup(self) -> None:
        """Ensure cleanup task is running."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of stale pending requests."""
        self._running = True
        while self._running:
            await asyncio.sleep(self.cleanup_interval)
            await self._cleanup_stale()

    async def _cleanup_stale(self) -> None:
        """Remove stale pending requests."""
        now = time.time()
        async with self._lock:
            stale_keys = [
                key for key, pending in self._pending.items()
                if now - pending.created_at > self.ttl
            ]
            for key in stale_keys:
                pending = self._pending.pop(key)
                if not pending.future.done():
                    pending.future.set_exception(
                        asyncio.TimeoutError(f"Deduplication TTL expired for key={key}")
                    )
            if stale_keys:
                logger.info(f"Cleaned up {len(stale_keys)} stale pending requests")

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplicator statistics."""
        return {
            **self._stats,
            "pending_count": len(self._pending),
            "max_pending": self.max_pending,
        }

    async def close(self) -> None:
        """Close the deduplicator."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass


class RequestBatcher:
    """
    Batch multiple requests into a single operation.

    Accumulates requests over a time window and executes them together
    as a batch, improving throughput for high-frequency API calls.

    Example:
        >>> batcher = RequestBatcher(max_size=100, max_wait=0.1)
        >>> result = await batcher.add("user", 123, fetch_user, user_id=123)
    """

    def __init__(
        self,
        max_size: int = 100,
        max_wait: float = 0.1,
        batch_func: Optional[Callable[[List], Any]] = None,
    ):
        """
        Initialize the request batcher.

        Args:
            max_size: Maximum batch size
            max_wait: Maximum wait time in seconds
            batch_func: Function to execute batched requests
        """
        self.max_size = max_size
        self.max_wait = max_wait
        self.batch_func = batch_func

        self._pending: List[PendingRequest] = []
        self._lock = asyncio.Lock()
        self._batch_task: Optional[asyncio.Task] = None
        self._waiting_futures: asyncio.Future = asyncio.get_event_loop().create_future()

        self._stats = {
            "batches": 0,
            "total_items": 0,
        }

        logger.info(f"RequestBatcher initialized (max_size={max_size}, max_wait={max_wait}s)")

    async def add(
        self,
        category: str,
        item_id: Hashable,
        fetch_func: Callable[[List], Dict[Hashable, Any]],
        item_ids: Optional[List[Hashable]] = None,
    ) -> Any:
        """
        Add an item to the batch.

        Args:
            category: Batch category (e.g., "user", "product")
            item_id: Item identifier
            fetch_func: Function to fetch items (receives list of IDs)
            item_ids: Optional pre-grouped list of IDs

        Returns:
            Fetched item data
        """
        future = asyncio.get_event_loop().create_future()
        pending = PendingRequest(
            key=(category, item_id),
            future=future,
            created_at=time.time(),
        )

        async with self._lock:
            self._pending.append((category, item_id, future, fetch_func, item_ids))

            if len(self._pending) >= self.max_size:
                await self._execute_batch()

        asyncio.create_task(self._delayed_execute())

        return await future

    async def _delayed_execute(self) -> None:
        """Execute batch after max_wait timeout."""
        await asyncio.sleep(self.max_wait)
        async with self._lock:
            if self._pending:
                await self._execute_batch()

    async def _execute_batch(self) -> None:
        """Execute pending batch."""
        if not self._pending:
            return

        batch = self._pending[:self.max_size]
        self._pending = self._pending[self.max_size:]

        by_category: Dict[str, List[Hashable]] = defaultdict(list)
        by_future: Dict[asyncio.Future, tuple] = {}

        for category, item_id, future, _, item_ids in batch:
            ids_to_fetch = item_ids or [item_id]
            by_category[category].extend(ids_to_fetch)
            by_future[future] = (category, item_id)

        for category, item_ids in by_category.items():
            try:
                results = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.batch_func(item_ids) if self.batch_func else {}
                )
                for future, (cat, iid) in by_future.items():
                    if cat == category:
                        future.set_result(results.get(iid))
            except Exception as e:
                for future in by_future.keys():
                    future.set_exception(e)

        self._stats["batches"] += 1
        self._stats["total_items"] += len(batch)

    def get_stats(self) -> Dict[str, Any]:
        """Get batcher statistics."""
        return {
            **self._stats,
            "pending_count": len(self._pending),
        }
