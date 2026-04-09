"""Data Batcher Action Module.

Provides batching capabilities for processing large datasets
in chunks with configurable batch sizes and overlap.

Example:
    >>> from actions.data.data_batcher_action import DataBatcherAction
    >>> action = DataBatcherAction()
    >>> for batch in action.batch_process(data, batch_size=32):
    ...     process(batch)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional
import threading


class BatchingStrategy(Enum):
    """Batching strategy types."""
    FIXED_SIZE = "fixed_size"
    VARIABLE_SIZE = "variable_size"
    OVERLAPPING = "overlapping"
    ADAPTIVE = "adaptive"


@dataclass
class Batch:
    """Data batch.
    
    Attributes:
        batch_id: Unique batch identifier
        data: Batch data
        start_index: Start index in original data
        end_index: End index in original data
        size: Batch size
        is_last: Whether this is the last batch
    """
    batch_id: str
    data: List[Any]
    start_index: int
    end_index: int
    size: int
    is_last: bool = False


@dataclass
class BatcherConfig:
    """Configuration for batching.
    
    Attributes:
        strategy: Batching strategy
        batch_size: Size of each batch
        min_batch_size: Minimum batch size
        max_batch_size: Maximum batch size
        overlap: Number of overlapping items between batches
        drop_remainder: Drop incomplete last batch
    """
    strategy: BatchingStrategy = BatchingStrategy.FIXED_SIZE
    batch_size: int = 32
    min_batch_size: int = 1
    max_batch_size: int = 1000
    overlap: int = 0
    drop_remainder: bool = False


@dataclass
class BatchingResult:
    """Result of batching operation.
    
    Attributes:
        batches: List of created batches
        original_size: Original dataset size
        num_batches: Number of batches
        avg_batch_size: Average batch size
    """
    batches: List[Batch]
    original_size: int
    num_batches: int
    avg_batch_size: float
    duration: float = 0.0


class DataBatcherAction:
    """Data batcher for large dataset processing.
    
    Provides batch iteration with configurable sizes,
    overlap, and adaptive batching strategies.
    
    Attributes:
        config: Batcher configuration
        _batch_counter: Batch ID counter
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[BatcherConfig] = None,
    ) -> None:
        """Initialize batcher action.
        
        Args:
            config: Batcher configuration
        """
        self.config = config or BatcherConfig()
        self._batch_counter = 0
        self._lock = threading.Lock()
    
    def batch_process(
        self,
        data: List[Any],
        batch_size: Optional[int] = None,
    ) -> Iterator[Batch]:
        """Create batches from data.
        
        Args:
            data: Data to batch
            batch_size: Override batch size
        
        Returns:
            Iterator of Batch objects
        """
        batch_size = batch_size or self.config.batch_size
        batch_size = max(self.config.min_batch_size, min(batch_size, self.config.max_batch_size))
        
        if self.config.strategy == BatchingStrategy.OVERLAPPING:
            yield from self._create_overlapping_batches(data, batch_size)
        elif self.config.strategy == BatchingStrategy.ADAPTIVE:
            yield from self._create_adaptive_batches(data, batch_size)
        else:
            yield from self._create_fixed_batches(data, batch_size)
    
    def _create_fixed_batches(
        self,
        data: List[Any],
        batch_size: int,
    ) -> Iterator[Batch]:
        """Create fixed-size batches.
        
        Args:
            data: Data to batch
            batch_size: Batch size
        
        Yields:
            Batch objects
        """
        n = len(data)
        batch_id = 0
        
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            batch_data = data[start:end]
            
            if self.config.drop_remainder and len(batch_data) < batch_size:
                continue
            
            is_last = end >= n
            
            yield Batch(
                batch_id=f"batch_{batch_id}",
                data=batch_data,
                start_index=start,
                end_index=end,
                size=len(batch_data),
                is_last=is_last,
            )
            
            batch_id += 1
    
    def _create_overlapping_batches(
        self,
        data: List[Any],
        batch_size: int,
    ) -> Iterator[Batch]:
        """Create overlapping batches.
        
        Args:
            data: Data to batch
            batch_size: Batch size
        
        Yields:
            Batch objects
        """
        n = len(data)
        step = batch_size - self.config.overlap
        step = max(1, step)
        
        batch_id = 0
        
        for start in range(0, n, step):
            end = min(start + batch_size, n)
            batch_data = data[start:end]
            
            is_last = end >= n
            
            yield Batch(
                batch_id=f"batch_{batch_id}",
                data=batch_data,
                start_index=start,
                end_index=end,
                size=len(batch_data),
                is_last=is_last,
            )
            
            batch_id += 1
            
            if end >= n:
                break
    
    def _create_adaptive_batches(
        self,
        data: List[Any],
        base_size: int,
    ) -> Iterator[Batch]:
        """Create adaptive batches based on data characteristics.
        
        Args:
            data: Data to batch
            base_size: Base batch size
        
        Yields:
            Batch objects
        """
        n = len(data)
        
        if n <= base_size:
            yield Batch(
                batch_id="batch_0",
                data=data,
                start_index=0,
                end_index=n,
                size=n,
                is_last=True,
            )
            return
        
        import time
        start_time = time.time()
        
        estimated_batches = n / base_size
        if estimated_batches > 10:
            batch_size = max(self.config.min_batch_size, n // 10)
        else:
            batch_size = base_size
        
        yield from self._create_fixed_batches(data, batch_size)
    
    def create_batches(
        self,
        data: List[Any],
        batch_size: Optional[int] = None,
    ) -> BatchingResult:
        """Create all batches at once.
        
        Args:
            data: Data to batch
            batch_size: Override batch size
        
        Returns:
            BatchingResult
        """
        import time
        start = time.time()
        
        batch_size = batch_size or self.config.batch_size
        batches = list(self.batch_process(data, batch_size))
        
        avg_size = sum(b.size for b in batches) / len(batches) if batches else 0.0
        
        return BatchingResult(
            batches=batches,
            original_size=len(data),
            num_batches=len(batches),
            avg_batch_size=avg_size,
            duration=time.time() - start,
        )
    
    def process_batches(
        self,
        data: List[Any],
        processor: Callable[[List[Any]], Any],
        batch_size: Optional[int] = None,
    ) -> List[Any]:
        """Process data in batches.
        
        Args:
            data: Data to process
            processor: Processing function
            batch_size: Override batch size
        
        Returns:
            List of processing results
        """
        results = []
        
        for batch in self.batch_process(data, batch_size):
            result = processor(batch.data)
            results.append(result)
        
        return results
    
    def parallel_process_batches(
        self,
        data: List[Any],
        processor: Callable[[List[Any]], Any],
        batch_size: Optional[int] = None,
        num_workers: int = 4,
    ) -> List[Any]:
        """Process data in parallel batches.
        
        Args:
            data: Data to process
            processor: Processing function
            batch_size: Override batch size
            num_workers: Number of parallel workers
        
        Returns:
            List of processing results
        """
        import asyncio
        
        batches = list(self.batch_process(data, batch_size))
        
        async def process_batch(batch: Batch) -> Any:
            return processor(batch.data)
        
        async def run_all() -> List[Any]:
            semaphore = asyncio.Semaphore(num_workers)
            
            async def bounded_process(batch: Batch) -> Any:
                async with semaphore:
                    return await process_batch(batch)
            
            tasks = [bounded_process(b) for b in batches]
            return await asyncio.gather(*tasks)
        
        return asyncio.run(run_all())
    
    def get_next_batch_id(self) -> str:
        """Get next batch ID.
        
        Returns:
            Unique batch ID
        """
        with self._lock:
            batch_id = f"batch_{self._batch_counter}"
            self._batch_counter += 1
            return batch_id
