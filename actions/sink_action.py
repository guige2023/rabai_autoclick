"""
Sink Action Module.

Provides data sink functionality for streaming data processing.
Supports batched writes, buffering, and async operations.
"""

import time
import asyncio
import threading
import queue
from typing import Any, Optional, List, Callable, Dict, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


T = TypeVar("T")


class SinkState(Enum):
    """Sink operational states."""
    IDLE = "idle"
    BUFFERING = "buffering"
    FLUSHING = "flushing"
    ERROR = "error"
    CLOSED = "closed"


@dataclass
class SinkConfig:
    """Configuration for sink behavior."""
    buffer_size: int = 100
    flush_interval: float = 5.0
    flush_threshold: int = 50
    max_retries: int = 3
    retry_delay: float = 1.0
    async_flush: bool = True


@dataclass
class SinkMetrics:
    """Sink performance metrics."""
    items_written: int = 0
    items_buffered: int = 0
    flush_count: int = 0
    error_count: int = 0
    last_flush_time: Optional[float] = None
    last_error: Optional[str] = None


class Sink(ABC, Generic[T]):
    """Abstract base class for sinks."""

    @abstractmethod
    def write(self, item: T) -> None:
        """Write a single item to the sink."""
        pass

    @abstractmethod
    def flush(self) -> None:
        """Flush any buffered data."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the sink and release resources."""
        pass


class ConsoleSink(Sink[Any]):
    """Sink that writes to console."""

    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        self._closed = False

    def write(self, item: Any) -> None:
        if self._closed:
            raise RuntimeError("Sink is closed")
        print(f"{self.prefix}{item}")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._closed = True


class BufferSink(Sink[T]):
    """Sink that buffers items before forwarding."""

    def __init__(
        self,
        inner: Sink[T],
        buffer_size: int = 100,
    ):
        self.inner = inner
        self.buffer_size = buffer_size
        self._buffer: List[T] = []
        self._lock = threading.Lock()
        self._closed = False

    def write(self, item: T) -> None:
        if self._closed:
            raise RuntimeError("Sink is closed")
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.buffer_size:
                self.flush()

    def flush(self) -> None:
        with self._lock:
            if self._buffer:
                for item in self._buffer:
                    self.inner.write(item)
                self._buffer.clear()

    def close(self) -> None:
        self.flush()
        self._closed = True


class AsyncSink(Sink[T]):
    """Async wrapper for sync sinks."""

    def __init__(self, inner: Sink[T]):
        self.inner = inner
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def write(self, item: T) -> None:
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._write_async(item),
                self._loop,
            )
        else:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        self._write_async(item),
                        loop,
                    )
                else:
                    asyncio.run(self._write_async(item))
            except RuntimeError:
                self.inner.write(item)

    async def _write_async(self, item: T) -> None:
        if asyncio.iscoroutinefunction(self.inner.write):
            await self.inner.write(item)
        else:
            self.inner.write(item)

    def flush(self) -> None:
        self.inner.flush()

    def close(self) -> None:
        self.inner.close()


class SinkAction:
    """
    Action that writes data to a sink with buffering and metrics.

    Example:
        sink = FileSink("/tmp/output.txt")
        action = SinkAction("data_writer", sink)
        action.write({"key": "value"})
        action.flush()
    """

    def __init__(
        self,
        name: str,
        sink: Sink,
        config: Optional[SinkConfig] = None,
    ):
        self.name = name
        self.sink = sink
        self.config = config or SinkConfig()
        self._metrics = SinkMetrics()
        self._state = SinkState.IDLE
        self._lock = threading.RLock()
        self._closed = False
        self._flush_task: Optional[asyncio.Task] = None

    @property
    def state(self) -> SinkState:
        """Current sink state."""
        return self._state

    @property
    def metrics(self) -> SinkMetrics:
        """Sink metrics."""
        return self._metrics

    def write(self, item: Any) -> None:
        """Write a single item to the sink."""
        if self._closed:
            raise RuntimeError("Sink action is closed")

        with self._lock:
            self._state = SinkState.BUFFERING
            try:
                self.sink.write(item)
                self._metrics.items_written += 1
                self._metrics.items_buffered += 1

                if self._metrics.items_buffered >= self.config.flush_threshold:
                    self.flush()
            except Exception as e:
                self._state = SinkState.ERROR
                self._metrics.error_count += 1
                self._metrics.last_error = str(e)
                self._handle_write_error(item, e)

    def _handle_write_error(self, item: Any, error: Exception) -> None:
        """Handle write errors with retry logic."""
        for attempt in range(self.config.max_retries):
            time.sleep(self.config.retry_delay * (attempt + 1))
            try:
                self.sink.write(item)
                self._metrics.items_written += 1
                self._state = SinkState.BUFFERING
                return
            except Exception:
                self._metrics.error_count += 1
        raise error

    def flush(self) -> None:
        """Flush buffered data to the underlying sink."""
        with self._lock:
            if self._state == SinkState.FLUSHING:
                return

            self._state = SinkState.FLUSHING
            try:
                self.sink.flush()
                self._metrics.flush_count += 1
                self._metrics.items_buffered = 0
                self._metrics.last_flush_time = time.time()
                self._state = SinkState.IDLE
            except Exception as e:
                self._state = SinkState.ERROR
                self._metrics.last_error = str(e)
                raise

    async def flush_async(self) -> None:
        """Flush buffered data asynchronously."""
        await asyncio.sleep(0)
        self.flush()

    def write_batch(self, items: List[Any]) -> None:
        """Write multiple items to the sink."""
        for item in items:
            self.write(item)

    async def write_async(self, item: Any) -> None:
        """Write a single item asynchronously."""
        if asyncio.iscoroutinefunction(self.sink.write):
            await self.sink.write(item)
        else:
            self.write(item)

    def start_auto_flush(self) -> None:
        """Start automatic periodic flushing."""
        async def _auto_flush():
            while not self._closed:
                await asyncio.sleep(self.config.flush_interval)
                if not self._closed:
                    self.flush()

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                self._flush_task = asyncio.ensure_future(_auto_flush())
            else:
                asyncio.run(_auto_flush())
        except RuntimeError:
            pass

    def stop_auto_flush(self) -> None:
        """Stop automatic flushing."""
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None

    def close(self) -> None:
        """Close the sink and stop auto-flush."""
        self._closed = True
        self.stop_auto_flush()
        self.flush()
        self.sink.close()
        self._state = SinkState.CLOSED

    def reset(self) -> None:
        """Reset metrics and state."""
        with self._lock:
            self._metrics = SinkMetrics()
            self._state = SinkState.IDLE

    def get_stats(self) -> Dict[str, Any]:
        """Get sink statistics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "items_written": self._metrics.items_written,
            "items_buffered": self._metrics.items_buffered,
            "flush_count": self._metrics.flush_count,
            "error_count": self._metrics.error_count,
            "last_flush_time": self._metrics.last_flush_time,
        }
