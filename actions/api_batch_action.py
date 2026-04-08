"""
API Batch Action Module.

Handles batch API operations including bulk create, update, delete
 with configurable chunking and progress tracking.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class BatchOperation(Enum):
    """Type of batch operation."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


@dataclass
class BatchConfig:
    """Configuration for batch operations."""
    chunk_size: int = 100
    max_concurrent_chunks: int = 5
    retry_failed: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class BatchItem:
    """A single item in a batch operation."""
    id: Optional[str]
    data: dict[str, Any]
    operation: BatchOperation


@dataclass
class BatchResult:
    """Result of a batch operation."""
    operation: BatchOperation
    total_items: int
    successful: int
    failed: int
    errors: list[dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0
    items_per_second: float = 0.0


class APIBatchAction:
    """
    Batch API operation handler.

    Processes large datasets in configurable chunks with
    concurrency control and detailed error reporting.

    Example:
        batch = APIBatchAction(config=BatchConfig(chunk_size=50))
        result = await batch.execute(
            items=large_dataset,
            operation=BatchOperation.CREATE,
            api_func=create_record,
        )
    """

    def __init__(
        self,
        config: Optional[BatchConfig] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.config = config or BatchConfig()
        self.progress_callback = progress_callback

    async def execute(
        self,
        items: list[dict[str, Any]],
        operation: BatchOperation,
        api_func: Callable[[list[dict[str, Any]]], Any],
        id_field: str = "id",
    ) -> BatchResult:
        """Execute a batch operation on items."""
        import time
        start_time = time.monotonic()

        chunks = self._chunk_items(items)
        semaphore = asyncio.Semaphore(self.config.max_concurrent_chunks)

        successful = 0
        failed = 0
        errors: list[dict[str, Any]] = []

        async def process_chunk(chunk: list[dict[str, Any]], chunk_idx: int) -> tuple[int, int, list[dict[str, Any]]]:
            async with semaphore:
                chunk_successful = 0
                chunk_failed = 0
                chunk_errors: list[dict[str, Any]] = []

                for item in chunk:
                    for retry in range(self.config.max_retries + 1):
                        try:
                            await api_func([item])
                            chunk_successful += 1
                            break
                        except Exception as e:
                            if retry == self.config.max_retries:
                                chunk_failed += 1
                                chunk_errors.append({
                                    "item_id": item.get(id_field),
                                    "error": str(e),
                                    "operation": operation.value,
                                })
                            else:
                                await asyncio.sleep(self.config.retry_delay * (retry + 1))

                if self.progress_callback:
                    progress = sum(c.id_field for c in chunks[:chunk_idx]) + len(chunk)
                    self.progress_callback(progress, len(items))

                return chunk_successful, chunk_failed, chunk_errors

        tasks = [process_chunk(chunk, idx) for idx, chunk in enumerate(chunks)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Chunk processing failed: {result}")
                failed += len(items) // len(chunks)
            else:
                s, f, e = result
                successful += s
                failed += f
                errors.extend(e)

        duration = (time.monotonic() - start_time) * 1000
        return BatchResult(
            operation=operation,
            total_items=len(items),
            successful=successful,
            failed=failed,
            errors=errors,
            duration_ms=duration,
            items_per_second=(len(items) / duration * 1000) if duration > 0 else 0,
        )

    def _chunk_items(
        self,
        items: list[dict[str, Any]],
    ) -> list[list[dict[str, Any]]]:
        """Split items into chunks."""
        chunks: list[list[dict[str, Any]]] = []
        for i in range(0, len(items), self.config.chunk_size):
            chunks.append(items[i:i + self.config.chunk_size])
        return chunks

    async def execute_bulk_upsert(
        self,
        items: list[dict[str, Any]],
        api_func: Callable[[list[dict[str, Any]]], Any],
        id_field: str = "id",
    ) -> BatchResult:
        """Execute bulk upsert (insert or update based on presence of ID)."""
        create_items = [item for item in items if not item.get(id_field)]
        update_items = [item for item in items if item.get(id_field)]

        create_result = await self.execute(
            items=create_items,
            operation=BatchOperation.CREATE,
            api_func=api_func,
            id_field=id_field,
        )
        update_result = await self.execute(
            items=update_items,
            operation=BatchOperation.UPDATE,
            api_func=api_func,
            id_field=id_field,
        )

        total_successful = create_result.successful + update_result.successful
        total_failed = create_result.failed + update_result.failed
        total_errors = create_result.errors + update_result.errors

        return BatchResult(
            operation=BatchOperation.UPSERT,
            total_items=len(items),
            successful=total_successful,
            failed=total_failed,
            errors=total_errors,
            duration_ms=create_result.duration_ms + update_result.duration_ms,
            items_per_second=create_result.items_per_second + update_result.items_per_second,
        )
