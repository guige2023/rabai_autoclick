"""
API Batch Aggregator Action Module.

Aggregates API responses with batching and merging.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple


@dataclass
class BatchConfig:
    """Configuration for batching."""
    max_batch_size: int = 100
    max_wait_ms: int = 50
    max_batch_age_ms: int = 500


@dataclass
class BatchRequest:
    """A request in a batch."""
    request_id: str
    key: str
    func: Callable[..., Coroutine[Any, Any, Any]]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    created_at: float = field(default_factory=time.time)


@dataclass
class BatchResult:
    """Result of batch execution."""
    request_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None


class ApiBatchAggregatorAction:
    """
    Batch multiple API requests into single operations.

    Reduces API calls by grouping requests with the same key.
    """

    def __init__(
        self,
        config: Optional[BatchConfig] = None,
    ) -> None:
        self.config = config or BatchConfig()
        self._pending: Dict[str, List[BatchRequest]] = defaultdict(list)
        self._futures: Dict[str, asyncio.Future] = {}
        self._running = False
        self._lock = asyncio.Lock()

    async def submit(
        self,
        request_id: str,
        key: str,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Submit a request to be batched.

        Args:
            request_id: Unique request ID
            key: Batch key (requests with same key are batched together)
            func: Async function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result from the batched call
        """
        request = BatchRequest(
            request_id=request_id,
            key=key,
            func=func,
            args=args,
            kwargs=kwargs,
        )

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._futures[request_id] = future

        async with self._lock:
            self._pending[key].append(request)

            if len(self._pending[key]) >= self.config.max_batch_size:
                await self._flush_key(key)

        try:
            return await asyncio.wait_for(
                future,
                timeout=self.config.max_batch_age_ms / 1000.0 + 5.0,
            )
        except asyncio.TimeoutError:
            future.cancel()
            raise

    async def _flush_key(self, key: str) -> None:
        """Flush all pending requests for a key."""
        if key not in self._pending or not self._pending[key]:
            return

        requests = self._pending.pop(key, [])
        if not requests:
            return

        await self._execute_batch(key, requests)

    async def _execute_batch(
        self,
        key: str,
        requests: List[BatchRequest],
    ) -> None:
        """Execute a batch of requests."""
        try:
            first_request = requests[0]

            results = await first_request.func(
                *first_request.args,
                **first_request.kwargs,
                __batch_requests__=requests,
            )

            if isinstance(results, dict):
                for request in requests:
                    if request.request_id in results:
                        self._resolve_future(request.request_id, True, results[request.request_id])
                    else:
                        self._resolve_future(request.request_id, False, None, "No result")
            else:
                for request in requests:
                    self._resolve_future(request.request_id, True, results)

        except Exception as e:
            for request in requests:
                self._resolve_future(request.request_id, False, None, str(e))

    def _resolve_future(
        self,
        request_id: str,
        success: bool,
        data: Any = None,
        error: Optional[str] = None,
    ) -> None:
        """Resolve a request future."""
        if request_id not in self._futures:
            return

        future = self._futures.pop(request_id)

        if success:
            future.set_result(data)
        else:
            future.set_exception(Exception(error or "Unknown error"))

    async def flush_all(self) -> None:
        """Flush all pending requests."""
        async with self._lock:
            keys = list(self._pending.keys())
            for key in keys:
                await self._flush_key(key)

    def get_stats(self) -> Dict[str, Any]:
        """Get batching statistics."""
        return {
            "pending_keys": list(self._pending.keys()),
            "pending_total": sum(len(v) for v in self._pending.values()),
            "pending_by_key": {k: len(v) for k, v in self._pending.items()},
            "pending_futures": len(self._futures),
            "max_batch_size": self.config.max_batch_size,
            "max_wait_ms": self.config.max_wait_ms,
        }


class ResponseAggregator:
    """
    Aggregates multiple API responses.

    Supports merging, deduplication, and conflict resolution.
    """

    def __init__(self) -> None:
        self._mergers: Dict[str, Callable[[List[Any]], Any]] = {}

    def add_merger(
        self,
        key: str,
        merger: Callable[[List[Any]], Any],
    ) -> None:
        """Add a custom merger for a key."""
        self._mergers[key] = merger

    def aggregate(
        self,
        responses: List[Dict[str, Any]],
        strategy: str = "merge",
    ) -> Dict[str, Any]:
        """
        Aggregate multiple responses.

        Args:
            responses: List of response dicts
            strategy: Aggregation strategy

        Returns:
            Aggregated response
        """
        if not responses:
            return {}

        if strategy == "first":
            return responses[0]
        elif strategy == "last":
            return responses[-1]
        elif strategy == "merge":
            return self._merge_responses(responses)
        elif strategy == "concat":
            return self._concat_responses(responses)

        return responses[0]

    def _merge_responses(
        self,
        responses: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge responses, preferring non-null values."""
        keys = set()
        for r in responses:
            keys.update(r.keys())

        result = {}
        for key in keys:
            values = [r.get(key) for r in responses if key in r]
            non_null = [v for v in values if v is not None]

            if key in self._mergers:
                result[key] = self._mergers[key](values)
            elif non_null:
                result[key] = non_null[0]

        return result

    def _concat_responses(
        self,
        responses: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Concatenate array values."""
        result = defaultdict(list)

        for response in responses:
            for key, value in response.items():
                if isinstance(value, list):
                    result[key].extend(value)
                else:
                    result[key].append(value)

        return dict(result)
