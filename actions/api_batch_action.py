"""
API Batch Action Module.

Provides batch processing capabilities for API operations including
bulk operations, batch scheduling, parallel execution, and result aggregation.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import defaultdict

logger = logging.getLogger(__name__)


class BatchStatus(Enum):
    """Batch job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class BatchStrategy(Enum):
    """Batch execution strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    PRIORITY = "priority"
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"


@dataclass
class BatchItem:
    """Single item in a batch operation."""
    item_id: str
    data: Any
    priority: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.item_id)


@dataclass
class BatchResult:
    """Result of processing a single batch item."""
    item_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    processed_at: datetime = field(default_factory=datetime.now)
    execution_time: float = 0.0
    retry_count: int = 0


@dataclass
class BatchJob:
    """A batch processing job."""
    job_id: str
    name: str
    items: List[BatchItem] = field(default_factory=list)
    status: BatchStatus = BatchStatus.PENDING
    results: List[BatchResult] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0
    error_threshold: float = 1.0

    @property
    def total_items(self) -> int:
        """Get total number of items."""
        return len(self.items)

    @property
    def processed_items(self) -> int:
        """Get number of processed items."""
        return len(self.results)

    @property
    def success_count(self) -> int:
        """Get number of successful items."""
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        """Get number of failed items."""
        return sum(1 for r in self.results if not r.success)

    @property
    def is_complete(self) -> bool:
        """Check if batch is complete."""
        return self.processed_items >= self.total_items

    @property
    def success_rate(self) -> float:
        """Get success rate."""
        if self.processed_items == 0:
            return 0.0
        return self.success_count / self.processed_items


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    name: str
    batch_size: int = 100
    max_parallel: int = 5
    strategy: BatchStrategy = BatchStrategy.PARALLEL
    timeout: float = 300.0
    retry_count: int = 0
    retry_delay: float = 1.0
    continue_on_error: bool = True
    enable_progress: bool = True


@dataclass
class BatchMetrics:
    """Metrics for batch processing."""
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0
    total_items_processed: int = 0
    total_items_failed: int = 0
    total_processing_time: float = 0.0

    @property
    def overall_success_rate(self) -> float:
        """Get overall success rate."""
        total = self.total_items_processed + self.total_items_failed
        if total == 0:
            return 0.0
        return self.total_items_processed / total


class BatchQueue:
    """Queue for managing batch items."""

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._queue: List[BatchItem] = []
        self._priority_map: Dict[int, List[BatchItem]] = defaultdict(list)
        self._item_ids: Set[str] = set()

    def add(self, item: BatchItem) -> bool:
        """Add item to queue."""
        if len(self._queue) >= self.max_size:
            return False
        if item.item_id in self._item_ids:
            return False

        self._queue.append(item)
        self._priority_map[item.priority].append(item)
        self._item_ids.add(item.item_id)
        return True

    def add_batch(self, items: List[BatchItem]) -> int:
        """Add multiple items to queue."""
        count = 0
        for item in items:
            if self.add(item):
                count += 1
        return count

    def get_next(self, strategy: BatchStrategy = BatchStrategy.PARALLEL) -> Optional[BatchItem]:
        """Get next item from queue."""
        if not self._queue:
            return None

        if strategy == BatchStrategy.PRIORITY:
            priorities = sorted(self._priority_map.keys(), reverse=True)
            for priority in priorities:
                if self._priority_map[priority]:
                    item = self._priority_map[priority].pop(0)
                    self._queue.remove(item)
                    self._item_ids.remove(item.item_id)
                    return item

        elif strategy == BatchStrategy.ROUND_ROBIN:
            item = self._queue.pop(0)
            self._item_ids.remove(item.item_id)
            if item.priority in self._priority_map and item in self._priority_map[item.priority]:
                self._priority_map[item.priority].remove(item)
            return item

        else:
            item = self._queue.pop(0)
            self._item_ids.remove(item.item_id)
            return item

    def get_batch(self, size: int, strategy: BatchStrategy = BatchStrategy.PARALLEL) -> List[BatchItem]:
        """Get batch of items."""
        items = []
        for _ in range(min(size, len(self._queue))):
            item = self.get_next(strategy)
            if item:
                items.append(item)
        return items

    def size(self) -> int:
        """Get queue size."""
        return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0


class BatchProcessor:
    """Main batch processor with execution engine."""

    def __init__(self, config: BatchConfig):
        self.config = config
        self.queue = BatchQueue()
        self.jobs: Dict[str, BatchJob] = {}
        self.handlers: Dict[str, Callable] = {}
        self._running = False
        self._cancelled_jobs: Set[str] = set()
        self._metrics = BatchMetrics()

    def register_handler(self, job_type: str, handler: Callable):
        """Register handler for job type."""
        self.handlers[job_type] = handler

    def create_job(
        self,
        name: str,
        items: List[Any],
        job_type: Optional[str] = None,
        priority: Optional[int] = None
    ) -> str:
        """Create a new batch job."""
        job_id = str(uuid.uuid4())

        batch_items = [
            BatchItem(
                item_id=str(uuid.uuid4()),
                data=item,
                priority=priority or 0
            )
            for item in items
        ]

        job = BatchJob(
            job_id=job_id,
            name=name,
            items=batch_items
        )

        self.jobs[job_id] = job
        self.queue.add_batch(batch_items)

        self._metrics.total_batches += 1
        return job_id

    def get_job(self, job_id: str) -> Optional[BatchJob]:
        """Get job by ID."""
        return self.jobs.get(job_id)

    def cancel_job(self, job_id: str):
        """Cancel a running job."""
        if job_id in self.jobs:
            self.jobs[job_id].status = BatchStatus.CANCELLED
            self._cancelled_jobs.add(job_id)

    async def _process_item(
        self,
        item: BatchItem,
        handler: Callable,
        job: BatchJob
    ) -> BatchResult:
        """Process a single batch item."""
        start_time = time.time()
        result = BatchResult(item_id=item.item_id, success=False)

        for attempt in range(self.config.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(handler):
                    processed_result = await asyncio.wait_for(
                        handler(item.data),
                        timeout=self.config.timeout
                    )
                else:
                    processed_result = await asyncio.wait_for(
                        asyncio.to_thread(handler, item.data),
                        timeout=self.config.timeout
                    )

                result.success = True
                result.result = processed_result
                break

            except asyncio.TimeoutError:
                result.error = "Processing timeout"
                result.retry_count = attempt

            except Exception as e:
                result.error = str(e)
                result.retry_count = attempt

            if attempt < self.config.retry_count:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))

        result.execution_time = time.time() - start_time
        return result

    async def _process_job(self, job_id: str, job_type: str):
        """Process a batch job."""
        job = self.jobs.get(job_id)
        if not job or job_id in self._cancelled_jobs:
            return

        handler = self.handlers.get(job_type)
        if not handler:
            job.status = BatchStatus.FAILED
            return

        job.status = BatchStatus.RUNNING
        job.started_at = datetime.now()

        try:
            while not job.is_complete and job_id not in self._cancelled_jobs:
                items = self.queue.get_batch(self.config.max_parallel)

                if not items:
                    break

                tasks = [
                    self._process_item(item, handler, job)
                    for item in items
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for i, result in enumerate(results):
                    if isinstance(result, BatchResult):
                        job.results.append(result)
                    elif isinstance(result, Exception):
                        job.results.append(BatchResult(
                            item_id=items[i].item_id,
                            success=False,
                            error=str(result)
                        ))

                    job.progress = job.processed_items / job.total_items

                self._metrics.total_items_processed += len(results)

                if not self.config.continue_on_error:
                    if job.failure_count / job.processed_items > job.error_threshold:
                        job.status = BatchStatus.PAUSED
                        break

            if job_id in self._cancelled_jobs:
                job.status = BatchStatus.CANCELLED
            elif job.is_complete:
                job.status = BatchStatus.COMPLETED
                self._metrics.completed_batches += 1
            else:
                job.status = BatchStatus.FAILED
                self._metrics.failed_batches += 1

        except Exception as e:
            job.status = BatchStatus.FAILED
            logger.error(f"Job {job_id} failed: {e}")

        finally:
            job.completed_at = datetime.now()
            self._metrics.total_processing_time += (
                (job.completed_at - job.started_at).total_seconds()
                if job.started_at else 0
            )

    async def run(self, job_id: str, job_type: Optional[str] = None):
        """Run a specific job."""
        await self._process_job(job_id, job_type or "default")

    async def run_all(self):
        """Run all pending jobs."""
        self._running = True
        tasks = []

        for job_id, job in self.jobs.items():
            if job.status == BatchStatus.PENDING:
                task = asyncio.create_task(
                    self._process_job(job_id, "default")
                )
                tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)

        self._running = False

    def get_metrics(self) -> BatchMetrics:
        """Get batch processing metrics."""
        return self._metrics

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed job status."""
        job = self.jobs.get(job_id)
        if not job:
            return None

        return {
            "job_id": job.job_id,
            "name": job.name,
            "status": job.status.value,
            "total_items": job.total_items,
            "processed_items": job.processed_items,
            "success_count": job.success_count,
            "failure_count": job.failure_count,
            "progress": job.progress,
            "success_rate": job.success_rate,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None
        }


async def demo_handler(data: Dict[str, Any]) -> Dict[str, Any]:
    """Demo batch item handler."""
    await asyncio.sleep(0.1)
    return {"processed": True, "data": data}


async def main():
    """Demonstrate batch processing."""
    config = BatchConfig(
        name="demo_batch",
        batch_size=10,
        max_parallel=3,
        retry_count=1
    )

    processor = BatchProcessor(config)
    processor.register_handler("demo", demo_handler)

    items = [{"id": i, "value": f"item_{i}"} for i in range(20)]
    job_id = processor.create_job("Demo Job", items, "demo")

    print(f"Created job: {job_id}")

    await processor.run(job_id, "demo")

    status = processor.get_job_status(job_id)
    print(f"Job status: {status}")
    print(f"Metrics: {processor.get_metrics()}")


if __name__ == "__main__":
    asyncio.run(main())
