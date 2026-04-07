"""Resource management utilities for RabAI AutoClick.

Provides:
- Resource cleanup
- Resource tracking
- Resource limits
"""

import gc
import os
import resource as os_resource
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar

# Type variable for generic classes
T = TypeVar("T")


class ResourceType(Enum):
    """Resource types."""
    MEMORY = "memory"
    CPU = "cpu"
    FDs = "fds"
    THREADS = "threads"


@dataclass
class ResourceUsage:
    """Resource usage snapshot."""
    timestamp: float
    memory_mb: float
    cpu_percent: float
    num_fds: int
    num_threads: int


class ResourceTracker:
    """Track resource usage."""

    def __init__(self) -> None:
        """Initialize tracker."""
        self._samples: List[ResourceUsage] = []
        self._lock = threading.Lock()

    def sample(self) -> ResourceUsage:
        """Take a resource sample.

        Returns:
            Resource usage snapshot.
        """
        usage = self._get_usage()
        with self._lock:
            self._samples.append(usage)
        return usage

    def _get_usage(self) -> ResourceUsage:
        """Get current resource usage."""
        now = time.time()

        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            cpu = process.cpu_percent(interval=0.01)
            num_threads = process.num_threads()
        except ImportError:
            mem_mb = 0
            cpu = 0
            num_threads = threading.active_count()

        try:
            num_fds = process.num_fds() if hasattr(process, "num_fds") else 0
        except Exception:
            num_fds = 0

        return ResourceUsage(
            timestamp=now,
            memory_mb=mem_mb,
            cpu_percent=cpu,
            num_fds=num_fds,
            num_threads=num_threads,
        )

    def get_samples(self, limit: int = 100) -> List[ResourceUsage]:
        """Get recent samples.

        Args:
            limit: Maximum samples.

        Returns:
            List of samples.
        """
        with self._lock:
            return self._samples[-limit:]

    def get_average(self) -> Dict[str, float]:
        """Get average resource usage.

        Returns:
            Dict of averages.
        """
        with self._lock:
            if not self._samples:
                return {}

            return {
                "avg_memory_mb": sum(s.memory_mb for s in self._samples) / len(self._samples),
                "avg_cpu_percent": sum(s.cpu_percent for s in self._samples) / len(self._samples),
                "avg_threads": sum(s.num_threads for s in self._samples) / len(self._samples),
            }

    def clear(self) -> None:
        """Clear samples."""
        with self._lock:
            self._samples.clear()


class ResourceLimiter:
    """Limit resource usage."""

    def __init__(self) -> None:
        """Initialize limiter."""
        self._limits: Dict[ResourceType, float] = {}

    def set_limit(self, resource_type: ResourceType, limit: float) -> None:
        """Set resource limit.

        Args:
            resource_type: Type of resource.
            limit: Limit value.
        """
        self._limits[resource_type] = limit

    def check_limit(self, resource_type: ResourceType) -> bool:
        """Check if limit is exceeded.

        Args:
            resource_type: Type of resource.

        Returns:
            True if within limits.
        """
        if resource_type not in self._limits:
            return True

        limit = self._limits[resource_type]

        try:
            if resource_type == ResourceType.MEMORY:
                mem_mb = self._get_memory_usage()
                return mem_mb < limit
            elif resource_type == ResourceType.FDs:
                num_fds = self._get_fd_count()
                return num_fds < limit
        except Exception:
            pass

        return True

    def _get_memory_usage(self) -> float:
        """Get memory usage in MB."""
        try:
            import psutil
            return psutil.Process().memory_info().rss / 1024 / 1024
        except Exception:
            return 0

    def _get_fd_count(self) -> int:
        """Get open file descriptor count."""
        try:
            return os_resource.getrlimit(os_resource.RLIMIT_NOFILE)[0]
        except Exception:
            return 0


class ResourceCleanup:
    """Cleanup resources on exit."""

    def __init__(self) -> None:
        """Initialize cleanup."""
        self._cleanup_funcs: List[Callable[[], None]] = []
        self._lock = threading.Lock()
        self._registered = False

    def register(self, func: Callable[[], None]) -> None:
        """Register cleanup function.

        Args:
            func: Function to call on cleanup.
        """
        with self._lock:
            self._cleanup_funcs.append(func)
            if not self._registered:
                import atexit
                atexit.register(self._cleanup)
                self._registered = True

    def _cleanup(self) -> None:
        """Run all cleanup functions."""
        with self._lock:
            for func in self._cleanup_funcs:
                try:
                    func()
                except Exception:
                    pass

    def clear(self) -> None:
        """Clear all cleanup functions."""
        with self._lock:
            self._cleanup_funcs.clear()


class ResourcePool(Generic[T]):
    """Pool of reusable resources."""

    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 10,
        cleanup: Optional[Callable[[T], None]] = None,
    ) -> None:
        """Initialize pool.

        Args:
            factory: Resource factory.
            max_size: Maximum pool size.
            cleanup: Optional cleanup function.
        """
        self._factory = factory
        self._max_size = max_size
        self._cleanup = cleanup
        self._available: List[T] = []
        self._in_use: set = set()
        self._lock = threading.Lock()

    def acquire(self) -> T:
        """Acquire resource from pool.

        Returns:
            Resource.
        """
        with self._lock:
            if self._available:
                resource = self._available.pop()
            elif len(self._available) + len(self._in_use) < self._max_size:
                resource = self._factory()
            else:
                # Pool exhausted, create anyway
                resource = self._factory()

            self._in_use.add(resource)
            return resource

    def release(self, resource: T) -> None:
        """Release resource back to pool.

        Args:
            resource: Resource to release.
        """
        with self._lock:
            if resource in self._in_use:
                self._in_use.remove(resource)
                if len(self._available) < self._max_size:
                    self._available.append(resource)

    def clear(self) -> None:
        """Clear pool."""
        with self._lock:
            if self._cleanup:
                for resource in self._available:
                    try:
                        self._cleanup(resource)
                    except Exception:
                        pass
            self._available.clear()
            self._in_use.clear()

    @property
    def available_count(self) -> int:
        """Get available resource count."""
        with self._lock:
            return len(self._available)

    @property
    def in_use_count(self) -> int:
        """Get in-use resource count."""
        with self._lock:
            return len(self._in_use)


class MemoryPressureDetector:
    """Detect memory pressure."""

    def __init__(self, threshold_mb: float = 1000) -> None:
        """Initialize detector.

        Args:
            threshold_mb: Memory threshold in MB.
        """
        self._threshold = threshold_mb
        self._callbacks: List[Callable[[], None]] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def on_pressure(self, callback: Callable[[], None]) -> None:
        """Register pressure callback.

        Args:
            callback: Function to call on pressure.
        """
        self._callbacks.append(callback)

    def start(self, interval: float = 5.0) -> None:
        """Start monitoring.

        Args:
            interval: Check interval in seconds.
        """
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._monitor, args=(interval,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _monitor(self, interval: float) -> None:
        """Monitor loop."""
        while self._running:
            try:
                import psutil
                mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
                if mem_mb > self._threshold:
                    for callback in self._callbacks:
                        try:
                            callback()
                        except Exception:
                            pass
            except Exception:
                pass
            time.sleep(interval)

    def trigger_gc(self) -> None:
        """Trigger garbage collection."""
        gc.collect()
