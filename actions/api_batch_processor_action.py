# Copyright (c) 2024. coded by claude
"""API Batch Processor Action Module.

Processes API requests in batches with support for parallel execution,
rate limiting, and result aggregation.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class BatchStrategy(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    PRIORITY = "priority"


@dataclass
class BatchItem:
    item_id: str
    request: Dict[str, Any]
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResult:
    item_id: str
    success: bool
    result: Any
    error: Optional[str] = None
    execution_time_ms: float


@dataclass
class BatchProcessingResult:
    total_items: int
    successful: int
    failed: int
    results: List[BatchResult]
    total_time_ms: float


class APIBatchProcessor:
    def __init__(
        self,
        max_concurrency: int = 10,
        strategy: BatchStrategy = BatchStrategy.PARALLEL,
        timeout: Optional[float] = None,
    ):
        self.max_concurrency = max_concurrency
        self.strategy = strategy
        self.timeout = timeout

    async def process_batch(
        self,
        items: List[BatchItem],
        processor: Callable[[Dict[str, Any]], Any],
    ) -> BatchProcessingResult:
        start_time = datetime.now()
        if self.strategy == BatchStrategy.SEQUENTIAL:
            results = await self._process_sequential(items, processor)
        elif self.strategy == BatchStrategy.PRIORITY:
            results = await self._process_priority(items, processor)
        else:
            results = await self._process_parallel(items, processor)
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_time = (datetime.now() - start_time).total_seconds() * 1000
        return BatchProcessingResult(
            total_items=len(items),
            successful=successful,
            failed=failed,
            results=results,
            total_time_ms=total_time,
        )

    async def _process_parallel(self, items: List[BatchItem], processor: Callable) -> List[BatchResult]:
        semaphore = asyncio.Semaphore(self.max_concurrency)
        async def process_with_semaphore(item: BatchItem) -> BatchResult:
            async with semaphore:
                return await self._process_item(item, processor)
        return await asyncio.gather(*[process_with_semaphore(item) for item in items])

    async def _process_sequential(self, items: List[BatchItem], processor: Callable) -> List[BatchResult]:
        results = []
        for item in items:
            result = await self._process_item(item, processor)
            results.append(result)
        return results

    async def _process_priority(self, items: List[BatchItem], processor: Callable) -> List[BatchResult]:
        sorted_items = sorted(items, key=lambda x: -x.priority)
        return await self._process_sequential(sorted_items, processor)

    async def _process_item(self, item: BatchItem, processor: Callable) -> BatchResult:
        start_time = datetime.now()
        try:
            if self.timeout:
                result = await asyncio.wait_for(
                    self._execute_processor(processor, item.request),
                    timeout=self.timeout,
                )
            else:
                result = await self._execute_processor(processor, item.request)
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            return BatchResult(
                item_id=item.item_id,
                success=True,
                result=result,
                execution_time_ms=execution_time,
            )
        except asyncio.TimeoutError:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            return BatchResult(
                item_id=item.item_id,
                success=False,
                result=None,
                error="Processing timeout",
                execution_time_ms=execution_time,
            )
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            return BatchResult(
                item_id=item.item_id,
                success=False,
                result=None,
                error=str(e),
                execution_time_ms=execution_time,
            )

    async def _execute_processor(self, processor: Callable, request: Dict[str, Any]) -> Any:
        result = processor(request)
        if asyncio.iscoroutine(result):
            return await result
        return result
