"""Lock and synchronization primitives.

Provides advanced locking mechanisms including
read-write locks, semaphores, and conditional locks.
"""

import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional


class ReadWriteLock:
    """Read-write lock allowing multiple readers or one writer.

    Example:
        rwlock = ReadWriteLock()
        with rwlock.read_lock():
            data = shared_resource.read()
        with rwlock.write_lock():
            shared_resource.write(new_data)
    """

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    @contextmanager
    def read_lock(self) -> Any:
        """Acquire read lock."""
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._read_ready:
                self._readers -= 1
                if self._readers == 0:
                    self._read_ready.notify_all()

    @contextmanager
    def write_lock(self) -> Any:
        """Acquire write lock."""
        with self._read_ready:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer_active = True
        try:
            yield
        finally:
            with self._read_ready:
                self._writer_active = False
                self._read_ready.notify_all()


class Semaphore:
    """Counting semaphore for resource limiting.

    Example:
        sem = Semaphore(max_value=3)
        with sem:
            process_resource()
    """

    def __init__(self, max_value: int = 1) -> None:
        self._max_value = max_value
        self._value = max_value
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    @contextmanager
    def acquire(self, count: int = 1) -> Any:
        """Acquire semaphore count."""
        with self._condition:
            while self._value < count:
                self._condition.wait()
            self._value -= count
        try:
            yield
        finally:
            with self._condition:
                self._value += count
                self._condition.notify(count)

    def release(self, count: int = 1) -> None:
        """Release semaphore count."""
        with self._condition:
            self._value = min(self._max_value, self._value + count)
            self._condition.notify(count)

    def __enter__(self) -> "Semaphore":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class BoundedSemaphore(Semaphore):
    """Semaphore that bounds resource count."""

    def __init__(self, max_value: int = 1) -> None:
        super().__init__(max_value)

    def release(self, count: int = 1) -> None:
        """Release but don't exceed initial max."""
        with self._condition:
            self._value = min(self._max_value, self._value + count)
            self._condition.notify(count)


class ConditionVariable:
    """Condition variable for thread signaling.

    Example:
        cond = ConditionVariable()
        def waiter():
            with cond.wait_for(lambda: data_ready):
                use(data)
        def setter():
            data_ready = True
            cond.notify()
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._signaled = False

    @contextmanager
    def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: Optional[float] = None,
    ) -> Any:
        """Wait until predicate returns True.

        Args:
            predicate: Function returning bool.
            timeout: Max seconds to wait.

        Returns:
            Context manager.
        """
        end_time = time.time() + timeout if timeout else None

        with self._condition:
            while not predicate():
                if end_time:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)
                else:
                    self._condition.wait()

        yield

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for notify.

        Args:
            timeout: Max seconds to wait.

        Returns:
            True if notified, False if timed out.
        """
        with self._condition:
            signaled = self._signaled
            if not signaled:
                self._condition.wait(timeout)
            self._signaled = False
            return self._signaled

    def notify(self) -> None:
        """Notify one waiting thread."""
        with self._condition:
            self._signaled = True
            self._condition.notify()

    def notify_all(self) -> None:
        """Notify all waiting threads."""
        with self._condition:
            self._signaled = True
            self._condition.notify_all()


class ThreadPool:
    """Simple thread pool for concurrent execution.

    Example:
        pool = ThreadPool(max_workers=4)
        pool.submit(task, arg1, arg2)
        pool.shutdown()
    """

    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self._work: list = []
        self._workers: list = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._shutdown = False

        for _ in range(max_workers):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._workers.append(t)

    def _worker(self) -> None:
        """Worker thread main loop."""
        while True:
            with self._lock:
                while not self._work and not self._shutdown:
                    self._condition.wait()
                if self._shutdown and not self._work:
                    return
                if self._work:
                    func, args, kwargs = self._work.pop(0)
                else:
                    continue

            try:
                func(*args, **kwargs)
            except Exception:
                pass

    def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Submit task to pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Pool is shutdown")
            self._work.append((func, args, kwargs))
            self._condition.notify()

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown pool.

        Args:
            wait: Wait for workers to finish.
        """
        with self._lock:
            self._shutdown = True
            self._condition.notify_all()

        if wait:
            for t in self._workers:
                t.join(timeout=5.0)


@contextmanager
def lock(lock_obj: threading.Lock) -> Any:
    """Context manager for lock.

    Example:
        lock_obj = threading.Lock()
        with lock(lock_obj):
            do_work()
    """
    lock_obj.acquire()
    try:
        yield
    finally:
        lock_obj.release()


def synchronized(func: Callable) -> Callable:
    """Decorator to synchronize method with instance lock.

    Example:
        class Counter:
            @synchronized
            def increment(self):
                self.count += 1
    """
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, "_sync_lock"):
            self._sync_lock = threading.Lock()
        with self._sync_lock:
            return func(self, *args, **kwargs)
    return wrapper
