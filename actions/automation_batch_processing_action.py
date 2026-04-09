"""
Automation Batch Processing Action Module.

Provides batch processing capabilities for automation workflows including
chunking, parallel execution, result aggregation, and error handling.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Iterator
from dataclasses import dataclass, field
from enum import Enum
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dataclasses import dataclass


T = TypeVar('T')
R = TypeVar('R')


class BatchStrategy(Enum):
    """Batch processing strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CHUNKED_PARALLEL = "chunked_parallel"
    QUEUE_BASED = "queue_based"


class BatchError(Exception):
    """Raised when batch processing encounters errors."""
    pass


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 10
    max_workers: int = 4
    timeout: Optional[float] = None
    continue_on_error: bool = True
    strategy: BatchStrategy = BatchStrategy.CHUNKED_PARALLEL


@dataclass
class BatchItem:
    """Represents a single item in a batch."""
    index: int
    data: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResult:
    """Result of batch processing."""
    total: int
    successful: int
    failed: int
    results: List[Any]
    errors: List[Dict[str, Any]]
    duration: float


class BatchProcessor:
    """
    Configurable batch processor for automation workflows.
    
    Example:
        processor = BatchProcessor(config=BatchConfig(
            chunk_size=10,
            max_workers=4
        ))
        results = processor.process(
            items=[1, 2, 3, 4, 5],
            func=lambda x: x * 2
        )
    """
    
    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
    
    def process(
        self,
        items: List[Any],
        func: Callable[[Any], Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> BatchResult:
        """Process items in batches."""
        start_time = datetime.now()
        
        if self.config.strategy == BatchStrategy.SEQUENTIAL:
            return self._process_sequential(items, func, metadata, start_time)
        elif self.config.strategy == BatchStrategy.PARALLEL:
            return self._process_parallel(items, func, metadata, start_time)
        elif self.config.strategy == BatchStrategy.CHUNKED_PARALLEL:
            return self._process_chunked_parallel(items, func, metadata, start_time)
        else:
            return self._process_sequential(items, func, metadata, start_time)
    
    def _process_sequential(
        self,
        items: List[Any],
        func: Callable,
        metadata: Optional[Dict],
        start_time: datetime
    ) -> BatchResult:
        """Process items sequentially."""
        results = []
        errors = []
        
        for i, item in enumerate(items):
            try:
                result = func(item)
                results.append(BatchItem(index=i, data=result, metadata=metadata or {}))
            except Exception as e:
                if self.config.continue_on_error:
                    errors.append({"index": i, "error": str(e), "item": item})
                else:
                    raise BatchError(f"Error at index {i}: {e}") from e
        
        duration = (datetime.now() - start_time).total_seconds()
        return BatchResult(
            total=len(items),
            successful=len(results),
            failed=len(errors),
            results=[r.data for r in results],
            errors=errors,
            duration=duration
        )
    
    def _process_parallel(
        self,
        items: List[Any],
        func: Callable,
        metadata: Optional[Dict],
        start_time: datetime
    ) -> BatchResult:
        """Process items in parallel."""
        results = []
        errors = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_index = {
                executor.submit(func, item): i
                for i, item in enumerate(items)
            }
            
            for future in as_completed(future_to_index, timeout=self.config.timeout):
                i = future_to_index[future]
                try:
                    result = future.result()
                    results.append(BatchItem(index=i, data=result, metadata=metadata or {}))
                except Exception as e:
                    if self.config.continue_on_error:
                        errors.append({"index": i, "error": str(e)})
                    else:
                        raise BatchError(f"Error at index {i}: {e}") from e
        
        # Sort results by index
        results.sort(key=lambda x: x.index)
        results_list = [r.data for r in results]
        
        duration = (datetime.now() - start_time).total_seconds()
        return BatchResult(
            total=len(items),
            successful=len(results),
            failed=len(errors),
            results=results_list,
            errors=errors,
            duration=duration
        )
    
    def _process_chunked_parallel(
        self,
        items: List[Any],
        func: Callable,
        metadata: Optional[Dict],
        start_time: datetime
    ) -> BatchResult:
        """Process items in chunks with parallel execution within chunks."""
        chunks = self._chunk_items(items)
        all_results = []
        all_errors = []
        
        for chunk_start, chunk in chunks:
            chunk_results = []
            chunk_errors = []
            
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                future_to_original = {
                    executor.submit(func, item): i
                    for i, item in enumerate(chunk)
                }
                
                for future in as_completed(future_to_original, timeout=self.config.timeout):
                    original_index = future_to_original[future] + chunk_start
                    try:
                        result = future.result()
                        chunk_results.append(BatchItem(
                            index=original_index,
                            data=result,
                            metadata=metadata or {}
                        ))
                    except Exception as e:
                        if self.config.continue_on_error:
                            chunk_errors.append({
                                "index": original_index,
                                "error": str(e)
                            })
                        else:
                            raise BatchError(f"Error at index {original_index}: {e}") from e
            
            all_results.extend(chunk_results)
            all_errors.extend(chunk_errors)
        
        all_results.sort(key=lambda x: x.index)
        results_list = [r.data for r in all_results]
        
        duration = (datetime.now() - start_time).total_seconds()
        return BatchResult(
            total=len(items),
            successful=len(all_results),
            failed=len(all_errors),
            results=results_list,
            errors=all_errors,
            duration=duration
        )
    
    def _chunk_items(self, items: List[Any]) -> List[tuple[int, List[Any]]]:
        """Split items into chunks with starting indices."""
        chunks = []
        for i in range(0, len(items), self.config.chunk_size):
            chunks.append((i, items[i:i + self.config.chunk_size]))
        return chunks


class QueueBatchProcessor:
    """
    Queue-based batch processor for streaming data.
    
    Example:
        processor = QueueBatchProcessor(max_workers=4, batch_size=10)
        processor.start()
        
        for item in streaming_data:
            processor.submit(item, process_func)
        
        results = processor.wait_for_completion()
    """
    
    def __init__(
        self,
        max_workers: int = 4,
        batch_size: int = 10,
        timeout: Optional[float] = None
    ):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.timeout = timeout
        
        self.input_queue: queue.Queue = queue.Queue()
        self.result_queue: queue.Queue = queue.Queue()
        self.error_queue: queue.Queue = queue.Queue()
        
        self.workers: List[threading.Thread] = []
        self.running = False
    
    def start(self):
        """Start the queue processors."""
        self.running = True
        
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self.workers.append(worker)
    
    def stop(self):
        """Stop the queue processors."""
        self.running = False
        
        for worker in self.workers:
            worker.join(timeout=1.0)
        
        self.workers.clear()
    
    def submit(self, item: Any, func: Callable):
        """Submit an item for processing."""
        self.input_queue.put((item, func))
    
    def get_result(self, timeout: Optional[float] = None) -> tuple:
        """Get a single result."""
        return self.result_queue.get(timeout=timeout)
    
    def _worker_loop(self):
        """Worker loop for processing items."""
        while self.running:
            try:
                batch = []
                
                # Collect batch
                while len(batch) < self.batch_size:
                    try:
                        item = self.input_queue.get(timeout=0.1)
                        batch.append(item)
                    except queue.Empty:
                        break
                
                # Process batch
                for item, func in batch:
                    try:
                        result = func(item)
                        self.result_queue.put(("success", result))
                    except Exception as e:
                        self.error_queue.put(("error", str(e), item))
                
            except Exception:
                continue


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationBatchProcessingAction(BaseAction):
    """
    Batch processing action for automation workflows.
    
    Parameters:
        items: List of items to process
        func: Processing function
        chunk_size: Size of each chunk
        max_workers: Maximum parallel workers
        strategy: Processing strategy
    
    Example:
        action = AutomationBatchProcessingAction()
        result = action.execute({}, {
            "items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "func_name": "double",
            "chunk_size": 5,
            "max_workers": 2
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute batch processing."""
        items = params.get("items", [])
        chunk_size = params.get("chunk_size", 10)
        max_workers = params.get("max_workers", 4)
        strategy_str = params.get("strategy", "chunked_parallel")
        continue_on_error = params.get("continue_on_error", True)
        
        strategy = BatchStrategy(strategy_str)
        
        config = BatchConfig(
            chunk_size=chunk_size,
            max_workers=max_workers,
            strategy=strategy,
            continue_on_error=continue_on_error
        )
        
        processor = BatchProcessor(config)
        
        # Use a simple identity function for demo
        # In real usage, the func would be passed or looked up
        def default_func(x):
            return x
        
        batch_result = processor.process(items, default_func)
        
        return {
            "total": batch_result.total,
            "successful": batch_result.successful,
            "failed": batch_result.failed,
            "duration_seconds": batch_result.duration,
            "strategy": strategy_str,
            "chunk_size": chunk_size,
            "max_workers": max_workers,
            "processed_at": datetime.now().isoformat()
        }
