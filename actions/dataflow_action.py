"""dataflow action module for rabai_autoclick.

Provides dataflow programming primitives: sources, sinks, pipes,
and composable data processing pipelines with backpressure support.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar, Iterator, Optional
from concurrent.futures import Future

__all__ = [
    "Source",
    "Sink",
    "Pipe",
    "DataflowGraph",
    "dataflow_source",
    "dataflow_pipe",
    "dataflow_sink",
    "dataflow_graph",
    "BackpressureMode",
    "FlowStatus",
    "Buffer",
    "Channel",
    "busy_wait",
    "throttle",
]


T = TypeVar("T")
U = TypeVar("U")


class BackpressureMode(Enum):
    """Backpressure handling strategies."""
    BLOCK = auto()
    DROP = auto()
    CIRCULAR = auto()
    UNBOUNDED = auto()


class FlowStatus(Enum):
    """Pipeline flow status."""
    IDLE = auto()
    FLOWING = auto()
    STALLED = auto()
    COMPLETED = auto()
    ERROR = auto()


@dataclass
class Buffer(Generic[T]):
    """Thread-safe bounded buffer with configurable backpressure."""
    capacity: int
    items: deque[T] = field(default_factory=deque)
    lock: threading.Lock = field(default_factory=threading.Lock)
    not_full: threading.Condition = field(default_factory=threading.Condition)
    not_empty: threading.Condition = field(default_factory=threading.Condition)
    mode: BackpressureMode = BackpressureMode.BLOCK

    def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put item into buffer respecting backpressure mode.

        Args:
            item: Item to add.
            timeout: Max seconds to wait (None = infinite).

        Returns:
            True if item was added, False if dropped (DROP mode).
        """
        with self.not_full:
            if self.mode == BackpressureMode.DROP and len(self.items) >= self.capacity:
                return False
            if self.mode == BackpressureMode.CIRCULAR and len(self.items) >= self.capacity:
                self.items.popleft()
            while len(self.items) >= self.capacity and self.mode == BackpressureMode.BLOCK:
                if not self.not_full.wait(timeout=timeout):
                    return False
            self.items.append(item)
            self.not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get item from buffer.

        Args:
            timeout: Max seconds to wait (None = infinite).

        Returns:
            Item or None on timeout.
        """
        with self.not_empty:
            while len(self.items) == 0:
                if not self.not_empty.wait(timeout=timeout):
                    return None
            item = self.items.popleft()
            self.not_full.notify()
            return item

    def clear(self) -> int:
        """Clear all items and return count removed."""
        with self.lock:
            count = len(self.items)
            self.items.clear()
            self.not_full.notify_all()
            return count

    @property
    def size(self) -> int:
        """Current item count."""
        with self.lock:
            return len(self.items)


class Channel(Generic[T]):
    """Single-item channel for point-to-point communication."""

    def __init__(self, capacity: int = 1) -> None:
        self._capacity = capacity
        self._buffer = Buffer[T](capacity=max(1, capacity))
        self._closed = False
        self._close_lock = threading.Lock()

    def send(self, item: T, timeout: Optional[float] = None) -> bool:
        """Send item to channel."""
        with self._close_lock:
            if self._closed:
                raise ValueError("Cannot send on closed channel")
        return self._buffer.put(item, timeout=timeout)

    def recv(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive item from channel."""
        return self._buffer.get(timeout=timeout)

    def close(self) -> None:
        """Close channel, signaling no more items."""
        with self._close_lock:
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class Source(Generic[T]):
    """Data source that can be iterated or pushed."""

    def __init__(
        self,
        generator: Callable[[], Iterator[T]],
        name: str = "anonymous",
    ) -> None:
        self.generator = generator
        self.name = name
        self._iter: Optional[Iterator[T]] = None
        self._status = FlowStatus.IDLE

    def __iter__(self) -> Iterator[T]:
        self._iter = self.generator()
        self._status = FlowStatus.FLOWING
        return self

    def __next__(self) -> T:
        if self._iter is None:
            raise StopIteration
        try:
            return next(self._iter)
        except StopIteration:
            self._status = FlowStatus.COMPLETED
            raise

    @property
    def status(self) -> FlowStatus:
        return self._status


class Sink(Generic[T]):
    """Data sink that consumes items."""

    def __init__(
        self,
        consumer: Callable[[T], Any],
        name: str = "anonymous",
    ) -> None:
        self.consumer = consumer
        self.name = name
        self.consumed: int = 0
        self.errors: list[Exception] = []
        self._status = FlowStatus.IDLE

    def __call__(self, item: T) -> None:
        """Consume a single item."""
        try:
            self.consumer(item)
            self.consumed += 1
        except Exception as e:
            self.errors.append(e)
            self._status = FlowStatus.ERROR
            raise

    def accept(self, items: Iterator[T]) -> int:
        """Accept iterator of items.

        Returns:
            Count of items consumed.
        """
        self._status = FlowStatus.FLOWING
        start = self.consumed
        for item in items:
            self(item)
        self._status = FlowStatus.COMPLETED
        return self.consumed - start


class Pipe(Generic[T, U]):
    """Transform pipe that maps input to output."""

    def __init__(
        self,
        transform: Callable[[T], U],
        name: str = "anonymous",
    ) -> None:
        self.transform = transform
        self.name = name

    def __call__(self, item: T) -> U:
        return self.transform(item)

    def then(self, other: Pipe[U, Any]) -> Pipe[T, Any]:
        """Chain another pipe after this one."""
        def composed(x: T) -> Any:
            return other.transform(self.transform(x))
        return Pipe(composed, name=f"{self.name}->{other.name}")


class DataflowGraph:
    """Composable dataflow graph with sources, pipes, and sinks."""

    def __init__(self, name: str = "graph") -> None:
        self.name = name
        self._nodes: dict[str, Any] = {}
        self._edges: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def add_source(self, name: str, source: Source) -> None:
        """Add a named source node."""
        with self._lock:
            self._nodes[name] = source

    def add_pipe(self, name: str, pipe: Pipe) -> None:
        """Add a named pipe node."""
        with self._lock:
            self._nodes[name] = pipe

    def add_sink(self, name: str, sink: Sink) -> None:
        """Add a named sink node."""
        with self._lock:
            self._nodes[name] = sink

    def connect(self, from_node: str, to_node: str) -> None:
        """Connect two nodes (from -> to)."""
        with self._lock:
            self._edges.append((from_node, to_node))

    def execute(self) -> dict[str, Any]:
        """Execute the graph and return execution stats."""
        with self._lock:
            edges = list(self._edges)
            nodes = dict(self._nodes)

        results = {}
        for name, node in nodes.items():
            if isinstance(node, Sink):
                results[name] = {"consumed": node.consumed, "errors": len(node.errors)}
            elif isinstance(node, Source):
                results[name] = {"status": node.status.name}
            else:
                results[name] = {"type": type(node).__name__}

        return results


def dataflow_source(
    gen: Callable[[], Iterator[T]],
    name: str = "source",
) -> Source[T]:
    """Create a data source from a generator function."""
    return Source(gen, name=name)


def dataflow_pipe(
    fn: Callable[[T], U],
    name: str = "pipe",
) -> Pipe[T, U]:
    """Create a pipe from a transformation function."""
    return Pipe(fn, name=name)


def dataflow_sink(
    fn: Callable[[T], Any],
    name: str = "sink",
) -> Sink[T]:
    """Create a sink from a consumer function."""
    return Sink(fn, name=name)


def dataflow_graph(name: str = "graph") -> DataflowGraph:
    """Create a new dataflow graph."""
    return DataflowGraph(name=name)


def busy_wait(predicate: Callable[[], bool], interval: float = 0.01) -> None:
    """Spin-wait for a condition.

    Args:
        predicate: Function that returns True when condition is met.
        interval: Seconds between checks.
    """
    while not predicate():
        time.sleep(interval)


def throttle(delay: float) -> Callable[[Callable], Callable]:
    """Decorator to throttle function calls.

    Args:
        delay: Minimum seconds between calls.

    Returns:
        Decorated function with throttle applied.
    """
    def decorator(fn: Callable) -> Callable:
        last_call = [0.0]
        lock = threading.Lock()

        def throttled(*args: Any, **kwargs: Any) -> Any:
            with lock:
                elapsed = time.perf_counter() - last_call[0]
                if elapsed < delay:
                    time.sleep(delay - elapsed)
                last_call[0] = time.perf_counter()
            return fn(*args, **kwargs)
        return throttled
    return decorator
