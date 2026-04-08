"""
Data Pipeline Action Module.

Provides streaming data processing pipeline with stage composition,
backpressure handling, error recovery, and checkpointing.
"""

from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class BackpressureStrategy(Enum):
    """Backpressure handling strategies."""
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    BUFFER = "buffer"


class CheckpointStrategy(Enum):
    """Checkpoint saving strategies."""
    NONE = "none"
    ON_COMPLETE = "on_complete"
    PERIODIC = "periodic"
    ON_ERROR = "on_error"


@dataclass
class PipelineConfig:
    """Configuration for data pipeline."""
    name: str
    buffer_size: int = 1000
    max_workers: int = 4
    backpressure: BackpressureStrategy = BackpressureStrategy.BUFFER
    checkpoint_strategy: CheckpointStrategy = CheckpointStrategy.NONE
    checkpoint_interval: float = 60.0
    error_threshold: int = 10
    enable_metrics: bool = True


@dataclass
class PipelineMetrics:
    """Metrics for pipeline monitoring."""
    items_processed: int = 0
    items_failed: int = 0
    items_dropped: int = 0
    total_latency: float = 0.0
    stage_latencies: Dict[str, float] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    last_checkpoint_time: Optional[datetime] = None

    @property
    def average_latency(self) -> float:
        """Get average processing latency."""
        if self.items_processed == 0:
            return 0.0
        return self.total_latency / self.items_processed

    @property
    def throughput(self) -> float:
        """Get throughput in items per second."""
        if not self.start_time or self.items_processed == 0:
            return 0.0
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return self.items_processed / elapsed if elapsed > 0 else 0.0


@dataclass
class StageMetrics:
    """Metrics for individual pipeline stage."""
    name: str
    items_in: int = 0
    items_out: int = 0
    items_failed: int = 0
    total_latency: float = 0.0

    @property
    def average_latency(self) -> float:
        """Get average stage latency."""
        if self.items_out == 0:
            return 0.0
        return self.total_latency / self.items_out


class PipelineStage(Generic[T, U]):
    """A single stage in the data pipeline."""

    def __init__(
        self,
        name: str,
        processor: Callable[[T], U],
        batch_size: int = 1,
        timeout: float = 30.0
    ):
        self.name = name
        self.processor = processor
        self.batch_size = batch_size
        self.timeout = timeout
        self.metrics = StageMetrics(name=name)
        self._is_async = asyncio.iscoroutinefunction(processor)

    async def process(self, item: T) -> Optional[U]:
        """Process a single item."""
        self.metrics.items_in += 1
        start_time = time.monotonic()

        try:
            if self._is_async:
                result = await asyncio.wait_for(
                    self.processor(item),
                    timeout=self.timeout
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(self.processor, item),
                    timeout=self.timeout
                )
            self.metrics.items_out += 1
            self.metrics.total_latency += time.monotonic() - start_time
            return result

        except asyncio.TimeoutError:
            self.metrics.items_failed += 1
            logger.error(f"Stage {self.name} timeout processing item")
            return None
        except Exception as e:
            self.metrics.items_failed += 1
            logger.error(f"Stage {self.name} failed: {e}")
            return None


class Checkpoint:
    """Pipeline checkpoint for recovery."""

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.checkpoint_id = f"{pipeline_name}_{int(time.time())}"
        self.timestamp = datetime.now()
        self.stage_states: Dict[str, Any] = {}
        self.items_processed: int = 0
        self.metrics: Optional[PipelineMetrics] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize checkpoint to dictionary."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "pipeline_name": self.pipeline_name,
            "timestamp": self.timestamp.isoformat(),
            "stage_states": self.stage_states,
            "items_processed": self.items_processed
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """Deserialize checkpoint from dictionary."""
        checkpoint = cls(data["pipeline_name"])
        checkpoint.checkpoint_id = data["checkpoint_id"]
        checkpoint.timestamp = datetime.fromisoformat(data["timestamp"])
        checkpoint.stage_states = data.get("stage_states", {})
        checkpoint.items_processed = data.get("items_processed", 0)
        return checkpoint


class DataBuffer(Generic[T]):
    """Thread-safe data buffer with backpressure handling."""

    def __init__(
        self,
        max_size: int,
        strategy: BackpressureStrategy = BackpressureStrategy.BUFFER
    ):
        self.max_size = max_size
        self.strategy = strategy
        self._buffer: deque[T] = deque(maxlen=max_size if strategy == BackpressureStrategy.DROP_OLDEST else None)
        self._lock = asyncio.Lock()
        self._not_full = asyncio.Condition(self._lock)
        self._not_empty = asyncio.Condition(self._lock)

    async def put(self, item: T, blocking: bool = True) -> bool:
        """Put an item into the buffer."""
        async with self._not_full:
            if not blocking and len(self._buffer) >= self.max_size:
                if self.strategy == BackpressureStrategy.DROP_NEWEST:
                    return False
                elif self.strategy == BackpressureStrategy.DROP_OLDEST:
                    self._buffer.popleft()
                    self._buffer.append(item)
                    return True

            while len(self._buffer) >= self.max_size:
                if not blocking:
                    return False
                await self._not_full.wait()

            self._buffer.append(item)
            self._not_empty.notify()
            return True

    async def get(self, blocking: bool = True, timeout: Optional[float] = None) -> Optional[T]:
        """Get an item from the buffer."""
        async with self._not_empty:
            if not blocking and len(self._buffer) == 0:
                return None

            if len(self._buffer) == 0:
                try:
                    await asyncio.wait_for(
                        self._not_empty.wait(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    return None

            item = self._buffer.popleft()
            self._not_full.notify()
            return item

    def size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self._buffer) == 0

    def is_full(self) -> bool:
        """Check if buffer is full."""
        return len(self._buffer) >= self.max_size


class DataPipeline(Generic[T]):
    """Main data pipeline with multi-stage processing."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.stages: List[PipelineStage] = []
        self.buffers: List[DataBuffer] = []
        self.metrics = PipelineMetrics()
        self.checkpoints: List[Checkpoint] = []
        self._running = False
        self._cancelled = False
        self._executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self._checkpoint_callbacks: List[Callable] = []

    def add_stage(
        self,
        name: str,
        processor: Callable,
        batch_size: int = 1,
        buffer_size: Optional[int] = None
    ) -> "DataPipeline":
        """Add a processing stage to the pipeline."""
        stage = PipelineStage(name, processor, batch_size)
        self.stages.append(stage)
        self.buffers.append(DataBuffer(
            buffer_size or self.config.buffer_size,
            self.config.backpressure
        ))
        self.metrics.stage_latencies[name] = 0.0
        return self

    def on_checkpoint(self, callback: Callable[[Checkpoint], None]):
        """Register checkpoint callback."""
        self._checkpoint_callbacks.append(callback)

    async def _process_stage(
        self,
        stage_index: int,
        input_buffer: DataBuffer,
        output_buffer: Optional[DataBuffer]
    ):
        """Process items through a single stage."""
        stage = self.stages[stage_index]

        while not self._cancelled:
            item = await input_buffer.get(blocking=True, timeout=1.0)
            if item is None:
                continue

            result = await stage.process(item)

            if result is not None and output_buffer:
                await output_buffer.put(result)

            if self.config.enable_metrics:
                self.metrics.items_processed += 1

    async def _checkpoint_loop(self):
        """Periodically save checkpoints."""
        while not self._cancelled:
            await asyncio.sleep(self.config.checkpoint_interval)
            if self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
                await self._save_checkpoint()

    async def _save_checkpoint(self):
        """Save current pipeline checkpoint."""
        checkpoint = Checkpoint(self.config.name)
        checkpoint.items_processed = self.metrics.items_processed
        checkpoint.metrics = self.metrics

        for i, stage in enumerate(self.stages):
            checkpoint.stage_states[stage.name] = {
                "items_in": stage.metrics.items_in,
                "items_out": stage.metrics.items_out,
                "items_failed": stage.metrics.items_failed
            }

        self.checkpoints.append(checkpoint)
        self.metrics.last_checkpoint_time = datetime.now()

        for callback in self._checkpoint_callbacks:
            await asyncio.to_thread(callback, checkpoint)

        logger.info(f"Checkpoint saved: {checkpoint.checkpoint_id}")

    async def start(self):
        """Start the pipeline."""
        self._running = True
        self._cancelled = False
        self.metrics.start_time = datetime.now()

        workers = []
        for i in range(len(self.stages)):
            output_buffer = self.buffers[i + 1] if i + 1 < len(self.buffers) else None
            workers.append(
                asyncio.create_task(
                    self._process_stage(i, self.buffers[i], output_buffer)
                )
            )

        if self.config.checkpoint_strategy != CheckpointStrategy.NONE:
            workers.append(asyncio.create_task(self._checkpoint_loop()))

        return workers

    async def stop(self):
        """Stop the pipeline."""
        self._cancelled = True
        self._running = False
        await asyncio.sleep(0.1)

    async def push(self, item: T) -> bool:
        """Push an item into the pipeline."""
        if not self._running:
            raise RuntimeError("Pipeline not running")
        return await self.buffers[0].put(item)

    async def pull(self) -> Any:
        """Pull an item from the pipeline output."""
        if not self._running:
            raise RuntimeError("Pipeline not running")
        output_buffer = self.buffers[-1]
        return await output_buffer.get(blocking=False)

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline metrics."""
        return self.metrics


def identity(x: T) -> T:
    """Identity function for testing."""
    return x


def double(x: int) -> int:
    """Double function for testing."""
    return x * 2


async def main():
    """Demonstrate data pipeline."""
    config = PipelineConfig(
        name="test_pipeline",
        buffer_size=100,
        max_workers=2
    )

    pipeline = DataPipeline[int](config)
    pipeline.add_stage("double", double)
    pipeline.add_stage("triple", lambda x: x * 3)

    workers = await pipeline.start()

    for i in range(10):
        await pipeline.push(i)

    await asyncio.sleep(1)
    await pipeline.stop()

    print(f"Metrics: {pipeline.get_metrics()}")
    print(f"Stage metrics: {[s.metrics for s in pipeline.stages]}")


if __name__ == "__main__":
    asyncio.run(main())
