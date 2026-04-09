"""API Batch Process Action Module.

Provides batch processing capabilities for API operations including
bulk requests, batch queuing, and parallel execution with rate limiting.

Example:
    >>> from actions.api.api_batch_process_action import APIBatchProcessAction
    >>> action = APIBatchProcessAction()
    >>> results = await action.process_batch(requests)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class BatchStrategy(Enum):
    """Batch processing strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    QUEUED = "queued"
    ADAPTIVE = "adaptive"


class BatchStatus(Enum):
    """Status of a batch operation."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchItem:
    """Single item in a batch operation.
    
    Attributes:
        item_id: Unique item identifier
        data: Item data payload
        priority: Item priority (higher = first)
        status: Current status
        result: Result if completed
        error: Error if failed
    """
    item_id: str
    data: Any
    priority: int = 0
    status: BatchStatus = BatchStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class BatchConfig:
    """Configuration for batch processing.
    
    Attributes:
        batch_size: Number of items per batch
        max_concurrent: Maximum concurrent operations
        rate_limit: Maximum requests per second
        retry_count: Number of retries on failure
        retry_delay: Delay between retries in seconds
        timeout: Per-item timeout in seconds
    """
    batch_size: int = 10
    max_concurrent: int = 5
    rate_limit: float = 100.0
    retry_count: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0


@dataclass
class BatchResult:
    """Result of a batch operation.
    
    Attributes:
        batch_id: Unique batch identifier
        total_items: Total number of items
        successful: Number of successful items
        failed: Number of failed items
        results: List of results
        duration: Total duration in seconds
        errors: List of errors
    """
    batch_id: str
    total_items: int
    successful: int
    failed: int
    results: List[Any] = field(default_factory=list)
    duration: float = 0.0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIBatchProcessAction:
    """Handles batch processing of API operations.
    
    Provides efficient batch processing with support for
    concurrent execution, rate limiting, and error handling.
    
    Attributes:
        config: Current batch configuration
    
    Example:
        >>> action = APIBatchProcessAction()
        >>> result = await action.process_batch(items, processor_fn)
    """
    
    def __init__(self, config: Optional[BatchConfig] = None):
        """Initialize the batch process action.
        
        Args:
            config: Batch configuration. Uses defaults if not provided.
        """
        self.config = config or BatchConfig()
        self._active_batches: Dict[str, BatchResult] = {}
        self._rate_limiter: Optional[asyncio.Semaphore] = None
        self._lock = threading.RLock()
        self._batch_counter = 0
        self._request_times: List[float] = []
    
    def _get_rate_limiter(self) -> asyncio.Semaphore:
        """Get or create the rate limiter semaphore.
        
        Returns:
            Rate limiting semaphore
        """
        if self._rate_limiter is None:
            self._rate_limiter = asyncio.Semaphore(int(self.config.rate_limit))
        return self._rate_limiter
    
    def _generate_batch_id(self) -> str:
        """Generate a unique batch ID.
        
        Returns:
            Unique batch identifier
        """
        with self._lock:
            self._batch_counter += 1
            return f"batch_{self._batch_counter}_{int(time.time() * 1000)}"
    
    async def process_batch(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        batch_id: Optional[str] = None,
        strategy: BatchStrategy = BatchStrategy.PARALLEL
    ) -> BatchResult:
        """Process a batch of items.
        
        Args:
            items: List of items to process
            processor: Function to process each item
            batch_id: Optional batch identifier
            strategy: Processing strategy to use
        
        Returns:
            BatchResult with processing results
        """
        batch_id = batch_id or self._generate_batch_id()
        start_time = time.time()
        
        result = BatchResult(
            batch_id=batch_id,
            total_items=len(items),
            successful=0,
            failed=0
        )
        
        self._active_batches[batch_id] = result
        
        try:
            if strategy == BatchStrategy.SEQUENTIAL:
                await self._process_sequential(items, processor, result)
            elif strategy == BatchStrategy.PARALLEL:
                await self._process_parallel(items, processor, result)
            elif strategy == BatchStrategy.QUEUED:
                await self._process_queued(items, processor, result)
            elif strategy == BatchStrategy.ADAPTIVE:
                await self._process_adaptive(items, processor, result)
            else:
                await self._process_parallel(items, processor, result)
            
        except Exception as e:
            result.errors.append(f"Batch processing error: {str(e)}")
        
        result.duration = time.time() - start_time
        result.metadata["end_time"] = datetime.now().isoformat()
        
        return result
    
    async def _process_sequential(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        result: BatchResult
    ) -> None:
        """Process items sequentially.
        
        Args:
            items: Items to process
            processor: Processor function
            result: Batch result to update
        """
        for item in items:
            item_result = await self._process_item(processor, item)
            if item_result["success"]:
                result.successful += 1
                result.results.append(item_result["data"])
            else:
                result.failed += 1
                result.errors.append(item_result["error"])
    
    async def _process_parallel(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        result: BatchResult
    ) -> None:
        """Process items in parallel with concurrency limit.
        
        Args:
            items: Items to process
            processor: Processor function
            result: Batch result to update
        """
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        async def process_with_semaphore(item: Any) -> Dict[str, Any]:
            async with semaphore:
                return await self._process_item(processor, item)
        
        tasks = [process_with_semaphore(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for item_result in results:
            if isinstance(item_result, dict):
                if item_result["success"]:
                    result.successful += 1
                    result.results.append(item_result["data"])
                else:
                    result.failed += 1
                    result.errors.append(item_result["error"])
            else:
                result.failed += 1
                result.errors.append(str(item_result))
    
    async def _process_queued(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        result: BatchResult
    ) -> None:
        """Process items in batches with rate limiting.
        
        Args:
            items: Items to process
            processor: Processor function
            result: Batch result to update
        """
        rate_limiter = self._get_rate_limiter()
        
        async def process_with_rate_limit(item: Any) -> Dict[str, Any]:
            async with rate_limiter:
                # Enforce rate limit
                await self._enforce_rate_limit()
                return await self._process_item(processor, item)
        
        # Process in batches
        for i in range(0, len(items), self.config.batch_size):
            batch = items[i:i + self.config.batch_size]
            tasks = [process_with_rate_limit(item) for item in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for item_result in batch_results:
                if isinstance(item_result, dict):
                    if item_result["success"]:
                        result.successful += 1
                        result.results.append(item_result["data"])
                    else:
                        result.failed += 1
                        result.errors.append(item_result["error"])
                else:
                    result.failed += 1
                    result.errors.append(str(item_result))
    
    async def _process_adaptive(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        result: BatchResult
    ) -> None:
        """Process items with adaptive concurrency.
        
        Args:
            items: Items to process
            processor: Processor function
            result: Batch result to update
        """
        # Start with low concurrency and increase
        current_concurrency = 1
        max_concurrency = self.config.max_concurrent
        success_streak = 0
        failure_streak = 0
        
        semaphore = asyncio.Semaphore(current_concurrency)
        
        async def process_with_adaptive_semaphore(item: Any) -> Dict[str, Any]:
            async with semaphore:
                return await self._process_item(processor, item)
        
        for item in items:
            task = asyncio.create_task(process_with_adaptive_semaphore(item))
            item_result = await task
            
            if item_result["success"]:
                result.successful += 1
                result.results.append(item_result["data"])
                success_streak += 1
                failure_streak = 0
                
                # Increase concurrency on success
                if success_streak >= 5 and current_concurrency < max_concurrency:
                    current_concurrency = min(current_concurrency + 1, max_concurrency)
                    semaphore = asyncio.Semaphore(current_concurrency)
                    success_streak = 0
            else:
                result.failed += 1
                result.errors.append(item_result["error"])
                failure_streak += 1
                success_streak = 0
                
                # Decrease concurrency on failure
                if failure_streak >= 3 and current_concurrency > 1:
                    current_concurrency = max(current_concurrency - 1, 1)
                    semaphore = asyncio.Semaphore(current_concurrency)
                    failure_streak = 0
    
    async def _process_item(
        self,
        processor: Callable[[Any], Any],
        item: Any
    ) -> Dict[str, Any]:
        """Process a single item with retry logic.
        
        Args:
            processor: Processor function
            item: Item to process
        
        Returns:
            Dictionary with success status and data/error
        """
        last_error = None
        
        for attempt in range(self.config.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(processor):
                    result = await asyncio.wait_for(
                        processor(item),
                        timeout=self.config.timeout
                    )
                else:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: processor(item)
                    )
                
                return {"success": True, "data": result}
                
            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Item processing timed out after {self.config.timeout}s")
            except Exception as e:
                last_error = e
            
            if attempt < self.config.retry_count:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
        
        return {"success": False, "error": str(last_error)}
    
    async def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting by sleeping if necessary."""
        current_time = time.time()
        
        with self._lock:
            # Remove old request times
            self._request_times = [
                t for t in self._request_times
                if current_time - t < 1.0
            ]
            
            # Check if we're at the rate limit
            if len(self._request_times) >= self.config.rate_limit:
                oldest = self._request_times[0]
                sleep_time = 1.0 - (current_time - oldest)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
            
            self._request_times.append(current_time)
    
    def get_batch_status(self, batch_id: str) -> Optional[BatchResult]:
        """Get the status of a batch operation.
        
        Args:
            batch_id: Batch identifier
        
        Returns:
            BatchResult or None if not found
        """
        with self._lock:
            return self._active_batches.get(batch_id)
    
    def get_active_batches(self) -> List[BatchResult]:
        """Get list of active batch operations.
        
        Returns:
            List of active batch results
        """
        with self._lock:
            return list(self._active_batches.values())
    
    def cancel_batch(self, batch_id: str) -> bool:
        """Cancel a batch operation.
        
        Args:
            batch_id: Batch identifier
        
        Returns:
            True if batch was found and cancelled
        """
        with self._lock:
            if batch_id in self._active_batches:
                self._active_batches[batch_id].metadata["cancelled"] = True
                return True
            return False
    
    def process_batch_sync(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        batch_id: Optional[str] = None
    ) -> BatchResult:
        """Synchronous version of process_batch.
        
        Args:
            items: List of items to process
            processor: Processor function
            batch_id: Optional batch identifier
        
        Returns:
            BatchResult with processing results
        """
        return asyncio.run(self.process_batch(items, processor, batch_id))
