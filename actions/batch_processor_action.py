"""Batch processor action module for RabAI AutoClick.

Provides batch processing operations:
- BatchProcessor: Process items in batches
- ParallelBatchProcessor: Parallel batch processing
- PriorityBatch: Priority-based batch processing
- BatchScheduler: Schedule batch jobs
- BatchMonitor: Monitor batch progress
"""

import time
import threading
import queue
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class BatchItem:
    """Batch item."""
    id: str
    data: Any
    priority: int = 0
    metadata: Dict = field(default_factory=dict)


@dataclass
class BatchResult:
    """Batch processing result."""
    batch_id: str
    total: int
    successful: int
    failed: int
    duration: float
    results: List[Any]
    errors: List[Dict]


@dataclass
class BatchProgress:
    """Batch progress tracking."""
    batch_id: str
    total_items: int
    processed_items: int
    successful_items: int
    failed_items: int
    start_time: float
    current_item: Optional[str] = None
    status: str = "running"


class BatchProcessor:
    """Batch processor."""

    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size

    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        on_item_complete: Optional[Callable[[Any, Any], None]] = None,
        on_error: Optional[Callable[[Exception, Any], None]] = None,
    ) -> BatchResult:
        """Process items in batches."""
        batch_id = f"batch_{int(time.time() * 1000)}"
        start_time = time.time()
        results = []
        errors = []
        successful = 0
        failed = 0

        for i in range(0, len(items), self.batch_size):
            batch = items[i:i+self.batch_size]

            for item in batch:
                try:
                    result = processor(item)
                    results.append(result)
                    successful += 1
                    if on_item_complete:
                        on_item_complete(item, result)
                except Exception as e:
                    failed += 1
                    errors.append({"item": str(item)[:50], "error": str(e)})
                    if on_error:
                        on_error(e, item)

        duration = time.time() - start_time
        return BatchResult(
            batch_id=batch_id,
            total=len(items),
            successful=successful,
            failed=failed,
            duration=duration,
            results=results,
            errors=errors,
        )


class ParallelBatchProcessor:
    """Parallel batch processor."""

    def __init__(self, max_workers: int = 4, batch_size: int = 50):
        self.max_workers = max_workers
        self.batch_size = batch_size

    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        on_error: Optional[Callable[[Exception, Any], None]] = None,
    ) -> BatchResult:
        """Process items in parallel."""
        batch_id = f"batch_{int(time.time() * 1000)}"
        start_time = time.time()
        results = []
        errors = []
        successful = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {executor.submit(processor, item): item for item in items}

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    results.append(result)
                    successful += 1
                except Exception as e:
                    failed += 1
                    errors.append({"item": str(item)[:50], "error": str(e)})
                    if on_error:
                        on_error(e, item)

        duration = time.time() - start_time
        return BatchResult(
            batch_id=batch_id,
            total=len(items),
            successful=successful,
            failed=failed,
            duration=duration,
            results=results,
            errors=errors,
        )


class PriorityBatch:
    """Priority-based batch processor."""

    def __init__(self):
        self._queue = queue.PriorityQueue()
        self._lock = threading.Lock()

    def add(self, item: Any, priority: int = 0):
        """Add item to priority batch."""
        self._queue.put((priority, time.time(), item))

    def add_batch(self, items: List[Any], default_priority: int = 0):
        """Add multiple items."""
        for item in items:
            self.add(item, default_priority)

    def get_batch(self, batch_size: int) -> List[Any]:
        """Get batch of items."""
        items = []
        for _ in range(batch_size):
            try:
                _, _, item = self._queue.get_nowait()
                items.append(item)
            except queue.Empty:
                break
        return items

    def size(self) -> int:
        """Get queue size."""
        return self._queue.qsize()


class BatchScheduler:
    """Schedule batch jobs."""

    def __init__(self):
        self._scheduled: List[Dict] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def schedule(
        self,
        name: str,
        items: List[Any],
        processor: Callable,
        interval: float,
        batch_size: int = 100,
    ) -> str:
        """Schedule recurring batch job."""
        job_id = f"job_{int(time.time() * 1000)}"
        with self._lock:
            self._scheduled.append({
                "id": job_id,
                "name": name,
                "items": items,
                "processor": processor,
                "interval": interval,
                "batch_size": batch_size,
                "last_run": None,
                "next_run": time.time(),
            })
        return job_id

    def cancel(self, job_id: str) -> bool:
        """Cancel scheduled job."""
        with self._lock:
            for job in self._scheduled:
                if job["id"] == job_id:
                    self._scheduled.remove(job)
                    return True
        return False

    def start(self):
        """Start scheduler."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run_loop(self):
        """Scheduler loop."""
        while self._running:
            now = time.time()
            with self._lock:
                for job in self._scheduled:
                    if job["next_run"] <= now:
                        try:
                            processor = BatchProcessor(batch_size=job["batch_size"])
                            processor.process(job["items"], job["processor"])
                        except Exception:
                            pass
                        job["last_run"] = now
                        job["next_run"] = now + job["interval"]
            time.sleep(1.0)


class BatchProcessorAction(BaseAction):
    """Batch processor action."""
    action_type = "batch_processor"
    display_name = "批处理器"
    description = "批量数据并行处理"

    def __init__(self):
        super().__init__()
        self._scheduler = BatchScheduler()
        self._scheduler.start()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "process")

            if operation == "process":
                return self._process(params)
            elif operation == "process_parallel":
                return self._process_parallel(params)
            elif operation == "schedule":
                return self._schedule(params)
            elif operation == "cancel":
                return self._cancel(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {str(e)}")

    def _process(self, params: Dict) -> ActionResult:
        """Process items in batches."""
        items = params.get("items", [])
        processor = params.get("processor")
        batch_size = params.get("batch_size", 100)

        if not processor:
            return ActionResult(success=False, message="processor is required")

        proc = BatchProcessor(batch_size=batch_size)
        result = proc.process(items, processor)

        return ActionResult(
            success=result.failed == 0,
            message=f"Processed {result.successful}/{result.total} items in {result.duration:.2f}s",
            data={
                "batch_id": result.batch_id,
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
            },
        )

    def _process_parallel(self, params: Dict) -> ActionResult:
        """Process items in parallel."""
        items = params.get("items", [])
        processor = params.get("processor")
        max_workers = params.get("max_workers", 4)
        batch_size = params.get("batch_size", 50)

        if not processor:
            return ActionResult(success=False, message="processor is required")

        proc = ParallelBatchProcessor(max_workers=max_workers, batch_size=batch_size)
        result = proc.process(items, processor)

        return ActionResult(
            success=result.failed == 0,
            message=f"Processed {result.successful}/{result.total} items in {result.duration:.2f}s",
            data={
                "batch_id": result.batch_id,
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
            },
        )

    def _schedule(self, params: Dict) -> ActionResult:
        """Schedule batch job."""
        name = params.get("name", "batch_job")
        items = params.get("items", [])
        processor = params.get("processor")
        interval = params.get("interval", 60.0)
        batch_size = params.get("batch_size", 100)

        if not processor:
            return ActionResult(success=False, message="processor is required")

        job_id = self._scheduler.schedule(name, items, processor, interval, batch_size)
        return ActionResult(success=True, message=f"Job '{name}' scheduled with ID '{job_id}'")

    def _cancel(self, params: Dict) -> ActionResult:
        """Cancel scheduled job."""
        job_id = params.get("job_id")
        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        success = self._scheduler.cancel(job_id)
        return ActionResult(success=success, message="Job cancelled" if success else "Job not found")
