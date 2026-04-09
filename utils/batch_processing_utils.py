"""
Batch Processing Utilities for UI Automation.

This module provides utilities for batch processing operations,
including batch execution, chunking, and result aggregation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Iterable, Optional


class BatchStatus(Enum):
    """Status of batch processing."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class BatchResult:
    """
    Result of batch processing.
    
    Attributes:
        total: Total items processed
        successful: Number of successful items
        failed: Number of failed items
        results: Individual results
        duration_ms: Total processing time
    """
    total: int
    successful: int
    failed: int
    results: list[Any] = field(default_factory=list)
    duration_ms: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class BatchConfig:
    """
    Configuration for batch processing.
    
    Attributes:
        batch_size: Number of items per batch
        max_workers: Maximum parallel workers
        stop_on_first_error: Stop processing on first error
        retry_count: Number of retries per item on failure
        retry_delay: Delay between retries in seconds
    """
    batch_size: int = 10
    max_workers: int = 4
    stop_on_first_error: bool = False
    retry_count: int = 0
    retry_delay: float = 0.5


class BatchProcessor:
    """
    Processes items in batches.
    
    Example:
        processor = BatchProcessor(config=BatchConfig(batch_size=5))
        result = processor.process(
            items=[1, 2, 3, 4, 5],
            processor_func=process_item
        )
    """
    
    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
    
    def process(
        self,
        items: list[Any],
        processor_func: Callable[[Any], Any],
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> BatchResult:
        """
        Process items in batches.
        
        Args:
            items: Items to process
            processor_func: Function to apply to each item
            progress_callback: Optional progress callback
            
        Returns:
            BatchResult with processing details
        """
        start_time = time.time()
        results = []
        errors = []
        successful = 0
        failed = 0
        
        total = len(items)
        processed = 0
        
        # Process in batches
        for i in range(0, total, self.config.batch_size):
            batch = items[i:i + self.config.batch_size]
            
            for item in batch:
                try:
                    result = self._process_with_retry(item, processor_func)
                    results.append(result)
                    successful += 1
                except Exception as e:
                    errors.append(f"{type(e).__name__}: {str(e)}")
                    failed += 1
                    
                    if self.config.stop_on_first_error:
                        return BatchResult(
                            total=total,
                            successful=successful,
                            failed=failed,
                            results=results,
                            duration_ms=(time.time() - start_time) * 1000,
                            errors=errors
                        )
                
                processed += 1
                
                if progress_callback:
                    progress_callback(processed, total)
        
        return BatchResult(
            total=total,
            successful=successful,
            failed=failed,
            results=results,
            duration_ms=(time.time() - start_time) * 1000,
            errors=errors
        )
    
    def _process_with_retry(
        self,
        item: Any,
        processor_func: Callable[[Any], Any]
    ) -> Any:
        """Process an item with retry logic."""
        last_error = None
        
        for attempt in range(self.config.retry_count + 1):
            try:
                return processor_func(item)
            except Exception as e:
                last_error = e
                
                if attempt < self.config.retry_count:
                    time.sleep(self.config.retry_delay)
        
        raise last_error


def chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def chunk_iterable(items: Iterable[Any], chunk_size: int) -> Iterable[list[Any]]:
    """
    Lazily chunk an iterable.
    
    Args:
        items: Iterable to chunk
        chunk_size: Size of each chunk
        
    Yields:
        Chunks of items
    """
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    
    if chunk:
        yield chunk


@dataclass
class StreamBatchConfig:
    """Configuration for stream batch processing."""
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 1000


class StreamBatchProcessor:
    """
    Processes items in streams with batching.
    
    Example:
        processor = StreamBatchProcessor(
            batch_func=lambda batch: process_batch(batch),
            config=StreamBatchConfig(batch_size=50)
        )
        
        for item in data_stream:
            processor.add(item)
        
        processor.flush()
    """
    
    def __init__(
        self,
        batch_func: Callable[[list[Any]], Any],
        config: Optional[StreamBatchConfig] = None
    ):
        self.batch_func = batch_func
        self.config = config or StreamBatchConfig()
        self._buffer: list[Any] = []
        self._last_flush = time.time()
        self._total_processed = 0
        self._results: list[Any] = []
    
    def add(self, item: Any) -> Optional[Any]:
        """
        Add an item to the batch buffer.
        
        Args:
            item: Item to add
            
        Returns:
            Result if a batch was processed, None otherwise
        """
        self._buffer.append(item)
        
        # Check if we should flush
        should_flush = (
            len(self._buffer) >= self.config.batch_size or
            len(self._buffer) >= self.config.max_buffer_size or
            (time.time() - self._last_flush) >= self.config.flush_interval
        )
        
        if should_flush:
            return self.flush()
        
        return None
    
    def add_batch(self, items: list[Any]) -> Optional[Any]:
        """
        Add multiple items.
        
        Args:
            items: Items to add
            
        Returns:
            Result if a batch was processed
        """
        result = None
        for item in items:
            result = self.add(item)
        return result
    
    def flush(self) -> Optional[Any]:
        """
        Flush the current buffer and process it.
        
        Returns:
            Batch processing result
        """
        if not self._buffer:
            return None
        
        # Get current buffer and reset
        buffer = self._buffer
        self._buffer = []
        self._last_flush = time.time()
        
        try:
            result = self.batch_func(buffer)
            self._results.append(result)
            self._total_processed += len(buffer)
            return result
        except Exception as e:
            raise BatchProcessingError(f"Batch processing failed: {e}")
    
    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)
    
    @property
    def total_processed(self) -> int:
        """Get total items processed."""
        return self._total_processed
    
    @property
    def results(self) -> list[Any]:
        """Get all batch results."""
        return list(self._results)


class BatchProcessingError(Exception):
    """Raised when batch processing fails."""
    pass


def process_parallel(
    items: list[Any],
    func: Callable[[Any], Any],
    max_workers: int = 4
) -> list[Any]:
    """
    Process items in parallel.
    
    Args:
        items: Items to process
        func: Processing function
        max_workers: Maximum parallel workers
        
    Returns:
        List of results in same order as items
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results = [None] * len(items)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(func, item): idx
            for idx, item in enumerate(items)
        }
        
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception:
                results[idx] = None
    
    return results
