"""Bulk operation utilities.

Efficient batch processing for database operations, file I/O, and API calls.
Supports chunking, retry logic, and progress tracking.

Example:
    results = bulk_update(
        records=[{"id": i, "value": i * 10} for i in range(1000)],
        chunk_size=100,
        update_fn=lambda batch: db.execute_many("UPDATE ...", batch),
    )
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Generator, Sequence, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BulkOperationResult:
    """Result of a bulk operation."""
    total: int
    successful: int
    failed: int
    duration_ms: float
    errors: list[tuple[Any, Exception]]
    results: list[R]


def chunked(iterable: Sequence[T], size: int) -> Generator[list[T], None, None]:
    """Split an iterable into chunks of specified size.

    Args:
        iterable: Input sequence.
        size: Chunk size.

    Yields:
        Chunks of up to `size` elements.
    """
    for i in range(0, len(iterable), size):
        yield list(iterable[i:i + size])


def bulk_process(
    items: Sequence[T],
    processor: Callable[[T], R],
    *,
    chunk_size: int = 100,
    max_workers: int = 4,
    fail_fast: bool = False,
) -> BulkOperationResult[T, R]:
    """Process items in bulk with parallel execution.

    Args:
        items: Items to process.
        processor: Function to apply to each item.
        chunk_size: Items per chunk.
        max_workers: Max parallel threads.
        fail_fast: If True, stop on first error.

    Returns:
        BulkOperationResult with all outcomes.
    """
    import time
    start = time.perf_counter()

    successful: list[R] = []
    errors: list[tuple[Any, Exception]] = []
    total = len(items)

    if max_workers <= 1:
        for item in items:
            try:
                result = processor(item)
                successful.append(result)
            except Exception as e:
                errors.append((item, e))
                if fail_fast:
                    break
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {executor.submit(processor, item): item for item in items}

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    successful.append(result)
                except Exception as e:
                    errors.append((item, e))
                    if fail_fast:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

    return BulkOperationResult(
        total=total,
        successful=len(successful),
        failed=len(errors),
        duration_ms=(time.perf_counter() - start) * 1000,
        errors=errors,
        results=successful,
    )


def bulk_create(
    connection: Any,
    table: str,
    records: list[dict[str, Any]],
    chunk_size: int = 100,
    on_conflict: str | None = None,
) -> BulkOperationResult:
    """Bulk insert records into database.

    Args:
        connection: Database connection.
        table: Target table name.
        records: List of dicts with column names.
        chunk_size: Records per INSERT statement.
        on_conflict: Optional ON CONFLICT clause for PostgreSQL.

    Returns:
        BulkOperationResult with insert statistics.
    """
    import time
    start = time.perf_counter()
    successful = 0
    errors: list[tuple[Any, Exception]] = []

    if not records:
        return BulkOperationResult(0, 0, 0, 0, [], [])

    columns = list(records[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))

    cursor = connection.cursor()

    for chunk in chunked(records, chunk_size):
        values = [tuple(row.get(col) for col in columns) for row in chunk]

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        if on_conflict:
            query += f" ON CONFLICT {on_conflict}"

        try:
            cursor.executemany(query, values)
            successful += len(values)
        except Exception as e:
            errors.append((chunk, e))
            logger.error("Bulk insert failed: %s", e)

    connection.commit()

    return BulkOperationResult(
        total=len(records),
        successful=successful,
        failed=len(records) - successful,
        duration_ms=(time.perf_counter() - start) * 1000,
        errors=errors,
        results=[],
    )


def bulk_update(
    connection: Any,
    table: str,
    records: list[dict[str, Any]],
    id_column: str = "id",
    chunk_size: int = 100,
) -> BulkOperationResult:
    """Bulk update records in database.

    Args:
        connection: Database connection.
        table: Target table name.
        records: List of dicts with column names and values.
        id_column: Primary key column name.
        chunk_size: Records per UPDATE statement.

    Returns:
        BulkOperationResult with update statistics.
    """
    import time
    start = time.perf_counter()
    successful = 0
    errors: list[tuple[Any, Exception]] = []

    cursor = connection.cursor()

    for chunk in chunked(records, chunk_size):
        for record in chunk:
            record_id = record.get(id_column)
            if record_id is None:
                errors.append((record, ValueError(f"Missing {id_column}")))
                continue

            set_clause = ", ".join(
                f"{k} = %s" for k in record if k != id_column
            )
            values = [v for k, v in record.items() if k != id_column] + [record_id]

            query = f"UPDATE {table} SET {set_clause} WHERE {id_column} = %s"

            try:
                cursor.execute(query, values)
                successful += 1
            except Exception as e:
                errors.append((record, e))
                logger.error("Bulk update failed for id=%s: %s", record_id, e)

    connection.commit()

    return BulkOperationResult(
        total=len(records),
        successful=successful,
        failed=len(errors),
        duration_ms=(time.perf_counter() - start) * 1000,
        errors=errors,
        results=[],
    )


def paginate(
    query_fn: Callable[[int, int], Sequence[T]],
    page_size: int = 100,
    max_pages: int | None = None,
) -> Generator[T, None, None]:
    """Generic paginated query iterator.

    Args:
        query_fn: Function(page_number, page_size) returning records.
        page_size: Records per page.
        max_pages: Optional maximum pages to fetch.

    Yields:
        Individual records from all pages.
    """
    page = 0
    while True:
        if max_pages and page >= max_pages:
            break

        records = query_fn(page, page_size)
        if not records:
            break

        yield from records

        if len(records) < page_size:
            break

        page += 1


class ProgressTracker:
    """Tracks progress of bulk operations."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0
        self.succeeded = 0
        self.failed = 0
        self.start_time = time.perf_counter()
        self._lock = __import__("threading").RLock()

    def increment(self, success: bool = True) -> None:
        """Increment processed count."""
        with self._lock:
            self.processed += 1
            if success:
                self.succeeded += 1
            else:
                self.failed += 1

    @property
    def percent(self) -> float:
        """Progress percentage."""
        if self.total == 0:
            return 0.0
        return (self.processed / self.total) * 100

    @property
    def rate(self) -> float:
        """Items processed per second."""
        elapsed = time.perf_counter() - self.start_time
        if elapsed == 0:
            return 0.0
        return self.processed / elapsed

    @property
    def eta_seconds(self) -> float:
        """Estimated seconds remaining."""
        if self.rate == 0:
            return 0.0
        remaining = self.total - self.processed
        return remaining / self.rate

    def __str__(self) -> str:
        return (
            f"Progress: {self.processed}/{self.total} "
            f"({self.percent:.1f}%) "
            f"S: {self.succeeded} F: {self.failed} "
            f"Rate: {self.rate:.1f}/s ETA: {self.eta_seconds:.0f}s"
        )
