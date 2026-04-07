"""Object pooling utilities for RabAI AutoClick.

Provides:
- Object pool
- Connection pool
- Worker pool
"""

import threading
import time
from queue import Queue, Empty
from typing import Any, Callable, Generic, Optional, TypeVar


T = TypeVar("T")


class ObjectPool(Generic[T]):
    """Pool of reusable objects.

    Reduces allocation overhead by reusing objects.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 10,
        idle_timeout: float = 0,
    ) -> None:
        """Initialize object pool.

        Args:
            factory: Function to create new objects.
            max_size: Maximum pool size.
            idle_timeout: Seconds before idle object is discarded.
        """
        self._factory = factory
        self._max_size = max_size
        self._idle_timeout = idle_timeout
        self._pool: list = []
        self._lock = threading.Lock()
        self._size = 0

    def acquire(self) -> T:
        """Acquire an object from pool.

        Returns:
            Object from pool or newly created.
        """
        with self._lock:
            while self._pool:
                obj = self._pool.pop()
                if self._idle_timeout > 0:
                    # Check if object is still valid
                    if hasattr(obj, "_last_used"):
                        if time.time() - obj._last_used > self._idle_timeout:
                            self._size -= 1
                            continue
                return obj

            if self._size < self._max_size:
                self._size += 1
                return self._factory()

            # Pool exhausted, create anyway
            return self._factory()

    def release(self, obj: T) -> None:
        """Return object to pool.

        Args:
            obj: Object to return.
        """
        if hasattr(obj, "_last_used"):
            obj._last_used = time.time()

        with self._lock:
            if len(self._pool) < self._max_size:
                self._pool.append(obj)
            else:
                self._size -= 1

    def clear(self) -> None:
        """Clear all objects from pool."""
        with self._lock:
            self._pool.clear()

    @property
    def size(self) -> int:
        """Get current pool size."""
        with self._lock:
            return len(self._pool)


class PooledObject:
    """Wrapper for pooled objects with context manager support."""

    def __init__(self, pool: "ObjectPool", obj: Any) -> None:
        """Initialize pooled object.

        Args:
            pool: Parent pool.
            obj: Wrapped object.
        """
        self._pool = pool
        self._obj = obj

    def __enter__(self) -> Any:
        return self._obj

    def __exit__(self, *args: Any) -> None:
        self._pool.release(self._obj)


class ConnectionPool(Generic[T]):
    """Pool for connection-like objects.

    Manages a pool of connections with health checks.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        max_connections: int = 5,
        health_check: Optional[Callable[[T], bool]] = None,
    ) -> None:
        """Initialize connection pool.

        Args:
            factory: Function to create connections.
            max_connections: Maximum connections.
            health_check: Optional health check function.
        """
        self._factory = factory
        self._max_connections = max_connections
        self._health_check = health_check or (lambda x: True)
        self._pool: Queue = Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._created = 0

        # Pre-create some connections
        for _ in range(min(2, max_connections)):
            self._pool.put(self._factory())
            self._created += 1

    def get_connection(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get a connection from pool.

        Args:
            timeout: Optional timeout.

        Returns:
            Connection or None on timeout.
        """
        try:
            conn = self._pool.get(timeout=timeout)
            if self._health_check(conn):
                return conn
            # Connection failed health check, create new
            with self._lock:
                if self._created < self._max_connections:
                    self._created += 1
                    return self._factory()
            # At max, try factory anyway
            return self._factory()
        except Empty:
            with self._lock:
                if self._created < self._max_connections:
                    self._created += 1
                    return self._factory()
            return None

    def return_connection(self, conn: T) -> None:
        """Return connection to pool.

        Args:
            conn: Connection to return.
        """
        if self._health_check(conn):
            try:
                self._pool.put_nowait(conn)
            except:
                pass
        else:
            with self._lock:
                self._created -= 1

    def close_all(self) -> None:
        """Close all connections in pool."""
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except Empty:
                break


class WorkerPool:
    """Pool of worker threads for task distribution."""

    def __init__(self, num_workers: int = 4) -> None:
        """Initialize worker pool.

        Args:
            num_workers: Number of worker threads.
        """
        self._num_workers = num_workers
        self._task_queue: Queue = Queue()
        self._workers: list = []
        self._running = False
        self._shutdown_event = threading.Event()

    def start(self) -> None:
        """Start the worker pool."""
        if self._running:
            return

        self._running = True
        self._shutdown_event.clear()

        for _ in range(self._num_workers):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self._workers.append(worker)

    def _worker_loop(self) -> None:
        """Worker thread loop."""
        while not self._shutdown_event.is_set():
            try:
                task = self._task_queue.get(timeout=0.1)
                func, args, kwargs = task
                func(*args, **kwargs)
                self._task_queue.task_done()
            except Empty:
                continue

    def submit(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        """Submit a task to the pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        self._task_queue.put((func, args, kwargs))

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the worker pool.

        Args:
            wait: Wait for tasks to complete.
        """
        self._shutdown_event.set()

        if wait:
            self._task_queue.join()

        for worker in self._workers:
            worker.join(timeout=1)

        self._workers.clear()
        self._running = False

    @property
    def pending_tasks(self) -> int:
        """Get number of pending tasks."""
        return self._task_queue.qsize()
