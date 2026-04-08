"""
Data Streaming Action Module.

Provides data streaming capabilities with
backpressure handling and flow control.
"""

from typing import Any, Callable, Dict, List, Optional, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class StreamStatus(Enum):
    """Stream status."""
    IDLE = "idle"
    STREAMING = "streaming"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class StreamConfig:
    """Stream configuration."""
    buffer_size: int = 100
    batch_size: int = 10
    timeout: float = 30.0
    enable_backpressure: bool = True


@dataclass
class StreamEvent:
    """Stream event."""
    event_type: str
    data: Any
    timestamp: datetime = field(default_factory=datetime.now)


class DataStream:
    """Data stream with buffering."""

    def __init__(self, config: StreamConfig):
        self.config = config
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=config.buffer_size)
        self.status = StreamStatus.IDLE
        self._producer_task: Optional[asyncio.Task] = None
        self._events: List[StreamEvent] = []

    async def put(self, item: Any):
        """Add item to stream."""
        if self.config.enable_backpressure:
            await self.queue.put(item)
        else:
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                logger.warning("Stream buffer full, dropping item")

    async def get(self) -> Any:
        """Get item from stream."""
        return await asyncio.wait_for(self.queue.get(), timeout=self.config.timeout)

    async def get_batch(self, batch_size: Optional[int] = None) -> List[Any]:
        """Get batch of items from stream."""
        size = batch_size or self.config.batch_size
        batch = []

        for _ in range(size):
            try:
                item = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=self.config.timeout
                )
                batch.append(item)
            except asyncio.TimeoutError:
                break

        return batch

    def _create_iterator(self) -> AsyncIterator[Any]:
        """Create async iterator."""
        async def iterator():
            while self.status == StreamStatus.STREAMING:
                try:
                    item = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )
                    yield item
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        return iterator()

    def __aiter__(self):
        return self._create_iterator()

    async def start(self):
        """Start stream."""
        self.status = StreamStatus.STREAMING

    async def pause(self):
        """Pause stream."""
        self.status = StreamStatus.PAUSED

    async def resume(self):
        """Resume stream."""
        self.status = StreamStatus.STREAMING

    async def stop(self):
        """Stop stream."""
        self.status = StreamStatus.COMPLETED

    def is_empty(self) -> bool:
        """Check if stream is empty."""
        return self.queue.empty()

    def size(self) -> int:
        """Get current size."""
        return self.queue.qsize()


class StreamProcessor:
    """Processes data streams."""

    def __init__(self, stream: DataStream):
        self.stream = stream
        self.transformers: List[Callable] = []
        self.filters: List[Callable] = []

    def add_transformer(self, transformer: Callable):
        """Add data transformer."""
        self.transformers.append(transformer)

    def add_filter(self, filter_func: Callable[[Any], bool]):
        """Add data filter."""
        self.filters.append(filter_func)

    async def process(self, handler: Callable):
        """Process stream with handler."""
        await self.stream.start()

        async for item in self.stream:
            for transformer in self.transformers:
                item = transformer(item)

            should_process = all(f(item) for f in self.filters)

            if should_process:
                if asyncio.iscoroutinefunction(handler):
                    await handler(item)
                else:
                    handler(item)


class StreamMerger:
    """Merges multiple streams."""

    def __init__(self, streams: List[DataStream]):
        self.streams = streams
        self.config = StreamConfig()

    async def merge(self) -> AsyncIterator[Any]:
        """Merge streams into single iterator."""
        stream_queues = [
            (stream, asyncio.create_task(stream.get()))
            for stream in self.streams
        ]

        while stream_queues:
            done, pending = await asyncio.wait(
                [t for _, t in stream_queues],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in done:
                for i, (stream, t) in enumerate(stream_queues):
                    if t == task:
                        try:
                            item = task.result()
                            yield item
                            new_task = asyncio.create_task(stream.get())
                            stream_queues[i] = (stream, new_task)
                        except Exception as e:
                            logger.error(f"Stream error: {e}")
                            stream_queues.pop(i)
                        break

    async def close(self):
        """Close all streams."""
        for stream in self.streams:
            await stream.stop()


class BackpressureController:
    """Controls backpressure for streams."""

    def __init__(self, high_water_mark: int = 100, low_water_mark: int = 20):
        self.high_water_mark = high_water_mark
        self.low_water_mark = low_water_mark
        self._paused_streams: Set[DataStream] = set()

    def check_pressure(self, stream: DataStream) -> bool:
        """Check if backpressure should be applied."""
        size = stream.size()

        if size >= self.high_water_mark:
            if stream not in self._paused_streams:
                stream.status = StreamStatus.PAUSED
                self._paused_streams.add(stream)
            return True

        if size <= self.low_water_mark:
            if stream in self._paused_streams:
                stream.status = StreamStatus.STREAMING
                self._paused_streams.discard(stream)
            return False

        return stream in self._paused_streams


def main():
    """Demonstrate data streaming."""
    async def producer(stream: DataStream):
        await stream.start()
        for i in range(20):
            await stream.put({"id": i, "value": i * 10})
            await asyncio.sleep(0.1)
        await stream.stop()

    async def consumer(stream: DataStream):
        await stream.start()
        count = 0
        async for item in stream:
            print(f"Received: {item}")
            count += 1
            if count >= 5:
                break

    stream = DataStream(StreamConfig(buffer_size=50))

    asyncio.run(producer(stream))


if __name__ == "__main__":
    main()
