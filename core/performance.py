"""Performance optimization utilities for RabAI AutoClick.

Provides utilities for optimizing workflow execution performance.
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from functools import wraps

from .context import ContextManager
from .base_action import ActionResult


@dataclass
class PerformanceMetrics:
    """Performance metrics for a single operation."""
    operation: str
    start_time: float
    end_time: float = 0.0
    duration: float = 0.0
    success: bool = True
    error: Optional[str] = None

    def finish(self, success: bool = True, error: Optional[str] = None) -> None:
        """Mark the operation as finished."""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.success = success
        self.error = error


@dataclass
class ExecutionProfile:
    """Profile of a workflow execution."""
    workflow_name: str
    total_duration: float
    step_count: int
    step_metrics: List[PerformanceMetrics] = field(default_factory=list)
    bottlenecks: List[Tuple[str, float]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'workflow_name': self.workflow_name,
            'total_duration': self.total_duration,
            'step_count': self.step_count,
            'step_metrics': [
                {
                    'operation': m.operation,
                    'duration': m.duration,
                    'success': m.success,
                    'error': m.error
                }
                for m in self.step_metrics
            ],
            'bottlenecks': [
                {'operation': op, 'duration': dur}
                for op, dur in self.bottlenecks
            ]
        }


class PerformanceProfiler:
    """Profiler for tracking execution performance."""

    def __init__(self) -> None:
        self.metrics: List[PerformanceMetrics] = []
        self.current_operation: Optional[str] = None
        self.operation_stack: List[Tuple[str, float]] = []

    def start_operation(self, operation: str) -> None:
        """Start tracking an operation.

        Args:
            operation: Name of the operation.
        """
        metric = PerformanceMetrics(
            operation=operation,
            start_time=time.time()
        )
        self.metrics.append(metric)
        self.current_operation = operation
        self.operation_stack.append(operation)

    def end_operation(
        self,
        operation: Optional[str] = None,
        success: bool = True,
        error: Optional[str] = None
    ) -> None:
        """End tracking an operation.

        Args:
            operation: Name of the operation (auto-detected if None).
            success: Whether the operation succeeded.
            error: Error message if failed.
        """
        if not self.metrics:
            return

        metric = self.metrics[-1]
        metric.finish(success=success, error=error)

        if self.operation_stack:
            self.operation_stack.pop()

        self.current_operation = self.operation_stack[-1] if self.operation_stack else None

    def get_metrics(self) -> List[PerformanceMetrics]:
        """Get all recorded metrics.

        Returns:
            List of PerformanceMetrics.
        """
        return self.metrics.copy()

    def get_total_duration(self) -> float:
        """Get total duration of all operations.

        Returns:
            Total duration in seconds.
        """
        return sum(m.duration for m in self.metrics)

    def get_slowest_operations(self, count: int = 5) -> List[Tuple[str, float]]:
        """Get the slowest operations.

        Args:
            count: Number of operations to return.

        Returns:
            List of (operation, duration) tuples.
        """
        sorted_metrics = sorted(
            self.metrics,
            key=lambda m: m.duration,
            reverse=True
        )
        return [(m.operation, m.duration) for m in sorted_metrics[:count]]

    def clear(self) -> None:
        """Clear all recorded metrics."""
        self.metrics.clear()
        self.operation_stack.clear()
        self.current_operation = None


def profile_action(func: Callable) -> Callable:
    """Decorator to profile action execution.

    Args:
        func: Action execute method to profile.

    Returns:
        Wrapped function that records performance.
    """
    @wraps(func)
    def wrapper(self: Any, context: Any, params: Dict[str, Any]) -> ActionResult:
        profiler = getattr(context, '_profiler', None)

        if profiler:
            profiler.start_operation(self.action_type)
            try:
                result = func(self, context, params)
                profiler.end_operation(success=result.success)
                return result
            except Exception as e:
                profiler.end_operation(success=False, error=str(e))
                raise
        else:
            return func(self, context, params)

    return wrapper


class CachedResult:
    """Cache for action results to avoid redundant execution."""

    def __init__(self, max_size: int = 100) -> None:
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._max_size = max_size
        self._access_order: List[str] = []

    def get(self, key: str) -> Optional[Any]:
        """Get a cached result.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found or expired.
        """
        if key in self._cache:
            value, timestamp = self._cache[key]
            # Simple 5-minute cache
            if time.time() - timestamp < 300:
                return value
            else:
                del self._cache[key]
                self._access_order.remove(key)
        return None

    def set(self, key: str, value: Any) -> None:
        """Set a cached result.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        if key in self._cache:
            self._access_order.remove(key)

        # Evict oldest if at capacity
        if len(self._cache) >= self._max_size:
            oldest = self._access_order.pop(0)
            del self._cache[oldest]

        self._cache[key] = (value, time.time())
        self._access_order.append(key)

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        self._access_order.clear()

    def invalidate(self, key: str) -> None:
        """Invalidate a specific cache entry.

        Args:
            key: Cache key to invalidate.
        """
        if key in self._cache:
            del self._cache[key]
            self._access_order.remove(key)

    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)


# Global result cache
_result_cache = CachedResult()


def get_result_cache() -> CachedResult:
    """Get the global result cache.

    Returns:
        The global CachedResult instance.
    """
    return _result_cache


def cached_action(func: Callable) -> Callable:
    """Decorator to cache action results.

    Args:
        func: Action execute method to cache.

    Returns:
        Wrapped function with caching.
    """
    @wraps(func)
    def wrapper(self: Any, context: Any, params: Dict[str, Any]) -> ActionResult:
        # Generate cache key from action type and params
        cache_key = f"{self.action_type}:{hash(frozenset(params.items()))}"

        # Check cache
        cached = _result_cache.get(cache_key)
        if cached is not None:
            return cached

        # Execute and cache result
        result = func(self, context, params)
        if result.success:
            _result_cache.set(cache_key, result)

        return result

    return wrapper


class LazyLoader:
    """Lazy loader for deferring expensive imports."""

    _instances: Dict[str, Any] = {}

    @classmethod
    def get(cls, name: str, factory: Callable) -> Any:
        """Get or create a lazily-loaded instance.

        Args:
            name: Instance name.
            factory: Factory function to create instance.

        Returns:
            The lazily-loaded instance.
        """
        if name not in cls._instances:
            cls._instances[name] = factory()
        return cls._instances[name]

    @classmethod
    def clear(cls) -> None:
        """Clear all lazily-loaded instances."""
        cls._instances.clear()


def batch_operations(
    items: List[Any],
    batch_size: int,
    operation: Callable[[List[Any]], Any]
) -> List[Any]:
    """Batch operations for better performance.

    Args:
        items: List of items to process.
        batch_size: Size of each batch.
        operation: Function to apply to each batch.

    Returns:
        List of results from each batch.
    """
    results = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        results.append(operation(batch))
    return results