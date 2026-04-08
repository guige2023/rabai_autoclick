"""Queue and blocking queue utilities.

Provides thread-safe queue implementations with
blocking operations for producer-consumer patterns.
"""

import queue
import threading
import time
from typing import Any, Generic, List, Optional, TypeVar


T = TypeVar("T")


class BlockingQueue(Generic[T]):
    """Thread-safe blocking queue.

    Example:
        q = BlockingQueue(maxsize=10)
        q.put(item)  # blocks if full
        item = q.get()  # blocks if empty
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: queue.Queue[T] = queue.Queue(maxsize=maxsize)

    def put(self, item: T, block: bool = True, timeout: float = None) -> None:
        """Put item into queue.

        Args:
            item: Item to add.
            block: Block if queue is full.
            timeout: Max wait time.
        """
        self._queue.put(item, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: float = None) -> T:
        """Get item from queue.

        Args:
            block: Block if queue is empty.
            timeout: Max wait time.

        Returns:
            Item from queue.
        """
        return self._queue.get(block=block, timeout=timeout)

    def try_put(self, item: T) -> bool:
        """Try to put item without blocking.

        Returns:
            True if successful.
        """
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            return False

    def try_get(self) -> Optional[T]:
        """Try to get item without blocking.

        Returns:
            Item or None if empty.
        """
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def size(self) -> int:
        """Get queue size."""
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def full(self) -> bool:
        return self._queue.full()


class PriorityBlockingQueue(Generic[T]):
    """Thread-safe priority queue with blocking operations.

    Example:
        q = PriorityBlockingQueue()
        q.put((1, "low priority"))
        q.put((0, "high priority"))
        item = q.get()  # ("high priority", 0)
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)

    def put(self, item: T, block: bool = True, timeout: float = None) -> None:
        """Put item into priority queue."""
        self._queue.put(item, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: float = None) -> T:
        """Get lowest priority item."""
        return self._queue.get(block=block, timeout=timeout)

    def try_put(self, item: T) -> bool:
        """Try to put without blocking."""
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            return False

    def try_get(self) -> Optional[T]:
        """Try to get without blocking."""
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def size(self) -> int:
        return self._queue.qsize()


class LIFOBQueue(Generic[T]):
    """Last-In-First-Out blocking queue (stack).

    Example:
        q = LIFOBQueue()
        q.put(a)
        q.put(b)
        q.get()  # returns b
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._queue: List[T] = []
        self._maxsize = maxsize

    def put(self, item: T, block: bool = True, timeout: float = None) -> None:
        """Put item onto stack."""
        with self._not_full:
            if not block:
                if self._maxsize and len(self._queue) >= self._maxsize:
                    raise queue.Full
            elif timeout is None:
                while self._maxsize and len(self._queue) >= self._maxsize:
                    self._not_full.wait()
            else:
                end = time.time() + timeout
                while self._maxsize and len(self._queue) >= self._maxsize:
                    remaining = end - time.time()
                    if remaining <= 0:
                        raise queue.Full
                    self._not_full.wait(remaining)

            self._queue.append(item)
            self._not_empty.notify()

    def get(self, block: bool = True, timeout: float = None) -> T:
        """Get item from top of stack."""
        with self._not_empty:
            if not block:
                if not self._queue:
                    raise queue.Empty
            elif timeout is None:
                while not self._queue:
                    self._not_empty.wait()
            else:
                end = time.time() + timeout
                while not self._queue:
                    remaining = end - time.time()
                    if remaining <= 0:
                        raise queue.Empty
                    self._not_empty.wait(remaining)

            item = self._queue.pop()
            self._not_full.notify()
            return item

    def size(self) -> int:
        with self._lock:
            return len(self._queue)


class BoundedQueue(Generic[T]):
    """Queue with bounded capacity and overflow handling.

    Example:
        q = BoundedQueue(maxsize=5, overflow="drop_old")
        q.put(1)  # etc
    """

    def __init__(self, maxsize: int, overflow: str = "block") -> None:
        self._maxsize = maxsize
        self._overflow = overflow
        self._queue: List[T] = []
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)

    def put(self, item: T, block: bool = True, timeout: float = None) -> bool:
        """Put item into queue.

        Args:
            item: Item to add.
            block: Block if full.
            timeout: Max wait time.

        Returns:
            True if successful.
        """
        with self._not_full:
            if self._overflow == "drop_new" and len(self._queue) >= self._maxsize:
                return False
            elif self._overflow == "drop_old":
                if not block and len(self._queue) >= self._maxsize:
                    return False
                while len(self._queue) >= self._maxsize:
                    if not block:
                        return False
                    self._not_full.wait(timeout)
                    if timeout is not None and len(self._queue) >= self._maxsize:
                        return False
                if self._queue:
                    self._queue.pop(0)
            else:  # block
                while len(self._queue) >= self._maxsize:
                    if not block:
                        return False
                    self._not_full.wait(timeout)

            self._queue.append(item)
            self._not_empty.notify()
            return True

    def get(self, block: bool = True, timeout: float = None) -> Optional[T]:
        """Get item from queue."""
        with self._not_empty:
            while not self._queue:
                if not block:
                    return None
                if not self._not_empty.wait(timeout):
                    return None

            item = self._queue.pop(0)
            self._not_full.notify()
            return item

    def size(self) -> int:
        with self._lock:
            return len(self._queue)


class WorkQueue(Generic[T]):
    """Simple work queue for distributing tasks.

    Example:
        q = WorkQueue()
        q.submit(task, args...)
        results = q.join()
    """

    def __init__(self, num_workers: int = 2) -> None:
        self._queue: BlockingQueue[tuple] = BlockingQueue()
        self._workers: List[threading.Thread] = []
        self._num_workers = num_workers
        self._running = False
        self._results: List[Any] = []
        self._result_lock = threading.Lock()

    def start(self) -> None:
        """Start worker threads."""
        self._running = True
        for _ in range(self._num_workers):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._workers.append(t)

    def _worker(self) -> None:
        while self._running:
            try:
                func, args, kwargs = self._queue.get(timeout=0.1)
                result = func(*args, **kwargs)
                with self._result_lock:
                    self._results.append(result)
            except queue.Empty:
                pass
            except Exception as e:
                with self._result_lock:
                    self._results.append(e)

    def submit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Submit a task."""
        self._queue.put((func, args, kwargs))

    def join(self) -> List[Any]:
        """Wait for all tasks to complete."""
        self._queue.join()
        with self._result_lock:
            return list(self._results)

    def stop(self) -> None:
        """Stop worker threads."""
        self._running = False
