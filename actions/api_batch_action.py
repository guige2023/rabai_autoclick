"""
API Batch Action Module.

Provides batch processing for API calls with concurrency control,
partial failure handling, and result aggregation.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Optional, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 10
    max_concurrency: int = 5
    timeout: float = 30.0
    stop_on_error: bool = False
    retry_count: int = 0


@dataclass
class BatchResult(Generic[T]):
    """Result of batch operation."""
    successful: list[T] = field(default_factory=list)
    failed: list[tuple[Any, Exception]] = field(default_factory=list)
    total: int = 0
    duration: float = 0.0

    @property
    def success_count(self) -> int:
        return len(self.successful)

    @property
    def failure_count(self) -> int:
        return len(self.failed)

    @property
    def all_successful(self) -> bool:
        return self.failure_count == 0


@dataclass
class BatchItem(Generic[T]):
    """Single item in batch."""
    id: str
    data: T
    retries: int = 0


class APISemaphore:
    """Semaphore for concurrency control."""

    def __init__(self, max_concurrent: int):
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._max_concurrent = max_concurrent
        self._initialized = False

    async def _ensure_init(self) -> None:
        if not self._initialized:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)
            self._initialized = True

    async def acquire(self) -> None:
        await self._ensure_init()
        await self._semaphore.acquire()

    def release(self) -> None:
        if self._semaphore:
            self._semaphore.release()


class APIBatchAction:
    """
    Batch processor for API calls with concurrency control.

    Example:
        batcher = APIBatchAction(batch_size=10, max_concurrency=5)
        results = await batcher.execute(
            items=url_list,
            func=lambda url: api.get(url)
        )
    """

    def __init__(
        self,
        batch_size: int = 10,
        max_concurrency: int = 5,
        timeout: float = 30.0,
        stop_on_error: bool = False
    ):
        self.config = BatchConfig(
            batch_size=batch_size,
            max_concurrency=max_concurrency,
            timeout=timeout,
            stop_on_error=stop_on_error
        )
        self._semaphore = APISemaphore(max_concurrency)

    async def _process_item(
        self,
        item: BatchItem[T],
        func: Callable[[T], R],
        results: BatchResult,
        stop_event: asyncio.Event
    ) -> None:
        """Process single batch item."""
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self.config.timeout
            )
            try:
                result = func(item.data)
                results.successful.append(result)
            finally:
                self._semaphore.release()

        except Exception as e:
            if item.retries < self.config.retry_count:
                item.retries += 1
                await self._process_item(item, func, results, stop_event)
            else:
                results.failed.append((item.data, e))
                if self.config.stop_on_error:
                    stop_event.set()

    async def execute(
        self,
        items: list[T],
        func: Callable[[T], R],
        id_func: Optional[Callable[[T], str]] = None
    ) -> BatchResult[R]:
        """Execute batch operation."""
        import time
        start = time.monotonic()

        results: BatchResult[R] = BatchResult()
        results.total = len(items)

        stop_event = asyncio.Event()

        batch_items = [
            BatchItem(
                id=id_func(item) if id_func else str(i),
                data=item
            )
            for i, item in enumerate(items)
        ]

        tasks = [
            self._process_item(item, func, results, stop_event)
            for item in batch_items
        ]

        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=self.config.timeout * len(items) / self.config.max_concurrency
            )
        except asyncio.TimeoutError:
            pass

        results.duration = time.monotonic() - start
        return results

    async def execute_async(
        self,
        items: list[T],
        func: Callable[[T], R],
        id_func: Optional[Callable[[T], str]] = None
    ) -> BatchResult[R]:
        """Execute async batch operation."""
        import time
        start = time.monotonic()

        results: BatchResult[R] = BatchResult()
        results.total = len(items)

        stop_event = asyncio.Event()

        batch_items = [
            BatchItem(
                id=id_func(item) if id_func else str(i),
                data=item
            )
            for i, item in enumerate(items)
        ]

        async def process_async(item: BatchItem[T]) -> None:
            try:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=self.config.timeout
                )
                try:
                    result = await func(item.data)
                    results.successful.append(result)
                finally:
                    self._semaphore.release()
            except Exception as e:
                if item.retries < self.config.retry_count:
                    item.retries += 1
                    await process_async(item)
                else:
                    results.failed.append((item.data, e))
                    if self.config.stop_on_error:
                        stop_event.set()

        tasks = [process_async(item) for item in batch_items]

        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=self.config.timeout * len(items) / self.config.max_concurrency
            )
        except asyncio.TimeoutError:
            pass

        results.duration = time.monotonic() - start
        return results

    async def execute_batched(
        self,
        items: list[T],
        func: Callable[[list[T]], list[R]]
    ) -> BatchResult[R]:
        """Execute in true batches."""
        import time
        start = time.monotonic()

        results: BatchResult[R] = BatchResult()
        results.total = len(items)

        for i in range(0, len(items), self.config.batch_size):
            batch = items[i:i + self.config.batch_size]
            try:
                batch_results = await asyncio.wait_for(
                    asyncio.to_thread(func, batch),
                    timeout=self.config.timeout
                )
                results.successful.extend(batch_results)
            except Exception as e:
                results.failed.append((batch, e))
                if self.config.stop_on_error:
                    break

        results.duration = time.monotonic() - start
        return results
