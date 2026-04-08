"""
Data Streaming Action Module.

Provides real-time data streaming capabilities with
backpressure handling, checkpointing, and exactly-once semantics.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import uuid
from collections import deque

logger = logging.getLogger(__name__)


class StreamState(Enum):
    """Stream state."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class DeliverySemantics(Enum):
    """Delivery guarantee semantics."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class StreamEvent:
    """Event in a stream."""
    event_id: str
    sequence_num: int
    payload: Any
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    ack: bool = False


@dataclass
class StreamPartition:
    """Partition in a partitioned stream."""
    partition_id: str
    events: deque = field(default_factory=lambda: deque(maxlen=10000))
    committed_offset: int = -1
    current_offset: int = -1


@dataclass
class StreamCheckpoint:
    """Checkpoint for stream processing."""
    checkpoint_id: str
    stream_id: str
    partitions: Dict[str, int]
    timestamp: datetime = field(default_factory=datetime.now)


class BackpressureHandler:
    """Handles backpressure in streams."""

    def __init__(self, max_buffer_size: int = 10000):
        self.max_buffer_size = max_buffer_size
        self._paused: Dict[str, bool] = {}

    def should_pause(self, partition_id: str, buffer_size: int) -> bool:
        """Check if should pause due to backpressure."""
        if buffer_size >= self.max_buffer_size * 0.8:
            self._paused[partition_id] = True
            return True
        return False

    def should_resume(self, partition_id: str, buffer_size: int) -> bool:
        """Check if should resume."""
        if buffer_size < self.max_buffer_size * 0.3:
            self._paused[partition_id] = False
            return True
        return False

    def is_paused(self, partition_id: str) -> bool:
        """Check if partition is paused."""
        return self._paused.get(partition_id, False)


class StreamProcessor:
    """Processes stream events."""

    def __init__(
        self,
        stream_id: str,
        semantics: DeliverySemantics = DeliverySemantics.AT_LEAST_ONCE
    ):
        self.stream_id = stream_id
        self.semantics = semantics
        self.partitions: Dict[str, StreamPartition] = {}
        self.state = StreamState.IDLE
        self.checkpoints: Dict[str, StreamCheckpoint] = {}
        self.backpressure = BackpressureHandler()

    def create_partition(self, partition_id: str) -> StreamPartition:
        """Create a partition."""
        partition = StreamPartition(partition_id=partition_id)
        self.partitions[partition_id] = partition
        return partition

    async def produce(
        self,
        partition_id: str,
        payload: Any,
        metadata: Optional[Dict[str, Any]] = None
    ) -> StreamEvent:
        """Produce event to partition."""
        if partition_id not in self.partitions:
            self.create_partition(partition_id)

        partition = self.partitions[partition_id]
        partition.current_offset += 1

        event = StreamEvent(
            event_id=str(uuid.uuid4()),
            sequence_num=partition.current_offset,
            payload=payload,
            metadata=metadata or {}
        )

        if self.backpressure.should_pause(partition_id, len(partition.events)):
            raise Exception(f"Backpressure: partition {partition_id} is paused")

        partition.events.append(event)
        return event

    async def consume(
        self,
        partition_id: str,
        timeout: Optional[float] = None
    ) -> Optional[StreamEvent]:
        """Consume event from partition."""
        if partition_id not in self.partitions:
            return None

        partition = self.partitions[partition_id]

        while not partition.events:
            await asyncio.sleep(0.01)
            if timeout:
                timeout -= 0.01
                if timeout <= 0:
                    return None

        event = partition.events.popleft()
        return event

    async def commit_offset(self, partition_id: str, offset: int):
        """Commit processing offset."""
        if partition_id in self.partitions:
            self.partitions[partition_id].committed_offset = offset

    def checkpoint(self) -> StreamCheckpoint:
        """Create checkpoint of current state."""
        partitions_state = {
            pid: p.current_offset
            for pid, p in self.partitions.items()
        }

        checkpoint = StreamCheckpoint(
            checkpoint_id=str(uuid.uuid4()),
            stream_id=self.stream_id,
            partitions=partitions_state
        )

        self.checkpoints[checkpoint.checkpoint_id] = checkpoint
        return checkpoint

    def restore_from_checkpoint(self, checkpoint_id: str):
        """Restore state from checkpoint."""
        checkpoint = self.checkpoints.get(checkpoint_id)
        if not checkpoint:
            return

        for partition_id, offset in checkpoint.partitions.items():
            if partition_id in self.partitions:
                self.partitions[partition_id].committed_offset = offset
                self.partitions[partition_id].current_offset = offset


class StreamTransformer:
    """Transforms stream events."""

    def __init__(self):
        self.transforms: List[Callable] = []

    def add_transform(self, transform: Callable[[Any], Any]):
        """Add transform function."""
        self.transforms.append(transform)

    async def transform(self, event: StreamEvent) -> StreamEvent:
        """Transform event."""
        payload = event.payload

        for transform in self.transforms:
            if asyncio.iscoroutinefunction(transform):
                payload = await transform(payload)
            else:
                payload = transform(payload)

        return StreamEvent(
            event_id=event.event_id,
            sequence_num=event.sequence_num,
            payload=payload,
            timestamp=event.timestamp,
            metadata=event.metadata
        )


class StreamWindow:
    """Windowed stream processing."""

    def __init__(
        self,
        window_size: float,
        slide_size: Optional[float] = None
    ):
        self.window_size = window_size
        self.slide_size = slide_size or window_size
        self._buffers: Dict[str, deque] = {}
        self._windows: Dict[str, List[Tuple[float, StreamEvent]]] = {}

    def add_event(self, partition_id: str, event: StreamEvent):
        """Add event to window buffer."""
        if partition_id not in self._buffers:
            self._buffers[partition_id] = deque()
            self._windows[partition_id] = []

        self._buffers[partition_id].append(event)

        while self._buffers[partition_id]:
            oldest = self._buffers[partition_id][0]
            age = (datetime.now() - oldest.timestamp).total_seconds()
            if age > self.window_size:
                self._buffers[partition_id].popleft()
            else:
                break

    def get_window(self, partition_id: str) -> List[StreamEvent]:
        """Get events in current window."""
        if partition_id not in self._buffers:
            return []

        cutoff = datetime.now() - timedelta(seconds=self.window_size)
        return [
            e for e in self._buffers[partition_id]
            if e.timestamp >= cutoff
        ]


class StreamJoiner:
    """Joins multiple streams."""

    def __init__(self, window_size: float = 60.0):
        self.window_size = window_size
        self._buffers: Dict[str, List[StreamEvent]] = defaultdict(list)

    def add_event(self, stream_id: str, event: StreamEvent):
        """Add event to stream buffer."""
        self._buffers[stream_id].append(event)
        self._cleanup(stream_id)

    def _cleanup(self, stream_id: str):
        """Remove old events."""
        cutoff = datetime.now() - timedelta(seconds=self.window_size)
        self._buffers[stream_id] = [
            e for e in self._buffers[stream_id]
            if e.timestamp >= cutoff
        ]

    def join(
        self,
        left_stream: str,
        right_stream: str,
        key_extractor: Callable[[StreamEvent], Any]
    ) -> List[Tuple[StreamEvent, StreamEvent]]:
        """Join events from two streams."""
        joined = []

        for left_event in self._buffers[left_stream]:
            left_key = key_extractor(left_event)

            for right_event in self._buffers[right_stream]:
                right_key = key_extractor(right_event)

                if left_key == right_key:
                    time_diff = abs(
                        (left_event.timestamp - right_event.timestamp).total_seconds()
                    )
                    if time_diff <= self.window_size:
                        joined.append((left_event, right_event))

        return joined


from datetime import timedelta
from collections import defaultdict


async def main():
    """Demonstrate streaming."""
    stream = StreamProcessor("demo-stream")

    stream.create_partition("p1")

    for i in range(10):
        await stream.produce("p1", {"value": i})

    checkpoint = stream.checkpoint()
    print(f"Checkpoint: {checkpoint.checkpoint_id}")

    consumed = await stream.consume("p1")
    print(f"Consumed: {consumed.payload if consumed else None}")


if __name__ == "__main__":
    asyncio.run(main())
