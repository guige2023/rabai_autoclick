"""
Performance Optimization Module for Workflow Automation

Provides comprehensive performance optimization including:
- Execution profiling and bottleneck detection
- Caching of action results
- Parallel execution of independent steps
- Lazy evaluation of expensive operations
- Memory optimization
- I/O optimization
- Algorithm optimization suggestions
- Resource pooling
- Execution planning
- Performance baselines and regression detection
"""

import time
import functools
import hashlib
import json
import threading
import queue
import os
import gc
import weakref
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Generic
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from collections import OrderedDict
from enum import Enum
import inspect
import sys
import traceback

T = TypeVar('T')


class PerformanceMetric(Enum):
    """Types of performance metrics tracked."""
    EXECUTION_TIME = "execution_time"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"
    IO_OPERATIONS = "io_operations"
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"
    PARALLEL_SPEEDUP = "parallel_speedup"
    BASELINE_DEVIATION = "baseline_deviation"


@dataclass
class ProfileResult:
    """Results from profiling a workflow or action."""
    name: str
    total_time: float
    call_count: int
    avg_time: float
    min_time: float
    max_time: float
    parent: Optional[str] = None
    children: Dict[str, 'ProfileResult'] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "total_time": self.total_time,
            "call_count": self.call_count,
            "avg_time": self.avg_time,
            "min_time": self.min_time,
            "max_time": self.max_time,
            "parent": self.parent,
            "children": {k: v.to_dict() for k, v in self.children.items()},
            "metadata": self.metadata
        }


@dataclass
class PerformanceBaseline:
    """Performance baseline for comparison and regression detection."""
    name: str
    avg_time: float
    std_dev: float
    min_time: float
    max_time: float
    sample_count: int
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_regression(self, current_time: float, threshold: float = 2.0) -> bool:
        """Check if current time represents a regression."""
        z_score = (current_time - self.avg_time) / self.std_dev if self.std_dev > 0 else 0
        return z_score > threshold


@dataclass 
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int
    size_bytes: int
    ttl: Optional[float] = None

    def is_expired(self, current_time: float) -> bool:
        """Check if cache entry has expired."""
        if self.ttl is None:
            return False
        return current_time - self.created_at > self.ttl


class LRUCache(Generic[T]):
    """Thread-safe LRU cache with size limits."""

    def __init__(self, max_size: int = 1000, max_memory_mb: int = 100):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._current_memory = 0
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                current_time = time.time()
                if entry.is_expired(current_time):
                    self._evict(key)
                    self._misses += 1
                    return None
                entry.last_accessed = current_time
                entry.access_count += 1
                self._cache.move_to_end(key)
                self._hits += 1
                return entry.value
            self._misses += 1
            return None

    def put(self, key: str, value: Any, ttl: Optional[float] = None, size_hint: int = 0) -> None:
        """Put value into cache."""
        with self._lock:
            if key in self._cache:
                self._evict(key)
            
            size = size_hint or sys.getsizeof(value)
            while self._current_memory + size > self.max_memory_bytes and self._cache:
                self._evict_oldest()
                pass
            while len(self._cache) >= self.max_size and self._cache:
                self._evict_oldest()
            
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                access_count=1,
                size_bytes=size,
                ttl=ttl
            )
            self._cache[key] = entry
            self._current_memory += size

    def _evict(self, key: str) -> None:
        """Evict a specific key."""
        if key in self._cache:
            entry = self._cache.pop(key)
            self._current_memory -= entry.size_bytes

    def _evict_oldest(self) -> None:
        """Evict the oldest entry."""
        if self._cache:
            key = next(iter(self._cache))
            self._evict(key)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._current_memory = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._cache),
                "memory_bytes": self._current_memory,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total > 0 else 0
            }


class ResourcePool(Generic[T]):
    """Generic resource pool for reusing expensive objects."""

    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 5,
        validation_fn: Optional[Callable[[T], bool]] = None,
        cleanup_fn: Optional[Callable[[T], None]] = None
    ):
        self.factory = factory
        self.max_size = max_size
        self.validation_fn = validation_fn or (lambda x: True)
        self.cleanup_fn = cleanup_fn
        
        self._lock = threading.RLock()
        self._available: queue.Queue[T] = queue.Queue()
        self._in_use: Set[T] = set()
        self._all_resources: List[T] = []
        self._created_count = 0

    def acquire(self, timeout: float = 30.0) -> T:
        """Acquire a resource from the pool."""
        start_time = time.time()
        
        while True:
            # Try to get from available pool
            try:
                resource = self._available.get_nowait()
                if self.validation_fn(resource):
                    with self._lock:
                        self._in_use.add(resource)
                    return resource
                else:
                    self._destroy_resource(resource)
            except queue.Empty:
                pass
            
            # Create new resource if under limit
            with self._lock:
                if self._created_count < self.max_size:
                    resource = self.factory()
                    self._all_resources.append(resource)
                    self._created_count += 1
                    self._in_use.add(resource)
                    return resource
            
            # Wait for available resource
            remaining = timeout - (time.time() - start_time)
            if remaining <= 0:
                raise TimeoutError("Resource pool acquisition timeout")
            
            try:
                resource = self._available.get(timeout=min(remaining, 1.0))
                if self.validation_fn(resource):
                    with self._lock:
                        self._in_use.add(resource)
                    return resource
                else:
                    self._destroy_resource(resource)
            except queue.Empty:
                continue

    def release(self, resource: T) -> None:
        """Release a resource back to the pool."""
        with self._lock:
            if resource in self._in_use:
                self._in_use.remove(resource)
                if self.validation_fn(resource):
                    self._available.put(resource)
                else:
                    self._destroy_resource(resource)

    def _destroy_resource(self, resource: T) -> None:
        """Destroy a resource."""
        with self._lock:
            if resource in self._all_resources:
                self._all_resources.remove(resource)
            if self.cleanup_fn:
                self.cleanup_fn(resource)

    def resize(self, new_size: int) -> None:
        """Resize the pool."""
        with self._lock:
            self.max_size = new_size
            while len(self._all_resources) > new_size:
                resource = self._all_resources.pop()
                self._destroy_resource(resource)

    def clear(self) -> None:
        """Clear all resources from the pool."""
        with self._lock:
            for resource in self._all_resources:
                if self.cleanup_fn:
                    self.cleanup_fn(resource)
            self._all_resources.clear()
            self._in_use.clear()
            while not self._available.empty():
                try:
                    self._available.get_nowait()
                except queue.Empty:
                    break
            self._created_count = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        with self._lock:
            return {
                "max_size": self.max_size,
                "total_created": self._created_count,
                "available": self._available.qsize(),
                "in_use": len(self._in_use)
            }


class LazyValue(Generic[T]):
    """Lazy evaluation wrapper that defers computation until needed."""

    def __init__(self, factory: Callable[[], T], cache_result: bool = True):
        self._factory = factory
        self._cache_result = cache_result
        self._value: Optional[T] = None
        self._evaluated = False
        self._lock = threading.RLock()

    def evaluate(self) -> T:
        """Force evaluation of the lazy value."""
        with self._lock:
            if not self._evaluated:
                self._value = self._factory()
                self._evaluated = True
            return self._value

    def get(self) -> T:
        """Get the value, evaluating if necessary."""
        if self._evaluated and self._cache_result:
            return self._value
        return self.evaluate()

    def is_evaluated(self) -> bool:
        """Check if value has been evaluated."""
        return self._evaluated

    def invalidate(self) -> None:
        """Invalidate cached value."""
        with self._lock:
            self._value = None
            self._evaluated = False


class Profiler:
    """Context manager for profiling code execution."""

    def __init__(self, name: str, parent_profile: Optional['Profiler'] = None):
        self.name = name
        self.parent_profile = parent_profile
        self.start_time = 0.0
        self.end_time = 0.0
        self.children: Dict[str, ProfileResult] = {}
        self._current_child: Optional[Profiler] = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        if self.parent_profile and self.parent_profile._current_child:
            self.parent_profile.children[self.name] = self.get_result()
            self.parent_profile._current_child = None

    def sub_profile(self, name: str) -> 'Profiler':
        """Create a sub-profile for nested timing."""
        profiler = Profiler(name, parent_profile=self)
        self._current_child = profiler
        return profiler

    def get_result(self) -> ProfileResult:
        """Get profiling results."""
        total_time = self.end_time - self.start_time if self.end_time > 0 else 0
        return ProfileResult(
            name=self.name,
            total_time=total_time,
            call_count=1,
            avg_time=total_time,
            min_time=total_time,
            max_time=total_time,
            children={k: v for k, v in self.children.items()}
        )


class PerformanceOptimizer:
    """
    Comprehensive performance optimization for workflow execution.

    Features:
    - Execution profiling: Profile workflow execution to find bottlenecks
    - Caching: Cache action results for repeated executions
    - Parallel execution: Identify and execute independent steps in parallel
    - Lazy evaluation: Defer expensive operations until needed
    - Memory optimization: Reduce memory footprint
    - I/O optimization: Batch and optimize file operations
    - Algorithm optimization: Suggest better algorithms for common tasks
    - Resource pooling: Reuse expensive resources (browser instances, connections)
    - Execution planning: Optimize execution order
    - Performance baselines: Track performance over time and detect regressions
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the performance optimizer."""
        self.config = config or {}
        
        # Caching
        cache_size = self.config.get("cache_size", 1000)
        cache_memory_mb = self.config.get("cache_memory_mb", 100)
        self._action_cache = LRUCache(max_size=cache_size, max_memory_mb=cache_memory_mb)
        
        # Profiling
        self._profiler_stack: List[Profiler] = []
        self._profiling_enabled = self.config.get("profiling_enabled", True)
        self._current_profile: Optional[Profiler] = None
        
        # Resource pools
        self._resource_pools: Dict[str, ResourcePool] = {}
        
        # Baselines
        self._baselines: Dict[str, List[PerformanceBaseline]] = {}
        self._baseline_lock = threading.RLock()
        
        # Parallel execution
        self._max_workers = self.config.get("max_workers", 4)
        self._executor: Optional[ThreadPoolExecutor] = None
        
        # Lazy values
        self._lazy_values: Dict[str, LazyValue] = {}
        
        # I/O batching
        self._io_batches: Dict[str, List[Tuple[str, Any]]] = {}
        self._io_batch_lock = threading.Lock()
        self._io_batch_size = self.config.get("io_batch_size", 100)
        self._io_flush_interval = self.config.get("io_flush_interval", 5.0)
        self._io_last_flush = time.time()
        
        # Memory optimization
        self._gc_threshold = self.config.get("gc_threshold", 100)
        self._action_count = 0
        
        # Execution planning
        self._execution_graph: Dict[str, Set[str]] = {}
        self._execution_lock = threading.Lock()
        
        # Stats
        self._stats = {
            "total_executions": 0,
            "total_parallel_executions": 0,
            "total_cache_hits": 0,
            "total_cache_misses": 0,
            "regressions_detected": 0
        }

    # =========================================================================
    # EXECUTION PROFILING
    # =========================================================================

    def profile(self, name: str) -> Profiler:
        """Create a profiling context for code timing."""
        if not self._profiling_enabled:
            return Profiler(name)
        
        profiler = Profiler(name, parent_profile=self._current_profile)
        if self._current_profile:
            self._current_profile._current_child = profiler
        self._profiler_stack.append(profiler)
        self._current_profile = profiler
        return profiler

    def profile_context(self, name: str) -> 'ProfileContext':
        """Create a context manager for profiling."""
        return ProfileContext(self, name)

    def get_profiling_results(self) -> Dict[str, Any]:
        """Get all profiling results."""
        if not self._profiler_stack:
            return {}
        
        results = {}
        for profiler in self._profiler_stack:
            if isinstance(profiler, Profiler) and profiler.end_time > 0:
                results[profiler.name] = profiler.get_result().to_dict()
        return results

    def analyze_bottlenecks(self, profile_results: Dict[str, Any]) -> List[Tuple[str, float]]:
        """Analyze profiling results to find bottlenecks."""
        bottlenecks = []
        for name, result in profile_results.items():
            total_time = result.get("total_time", 0)
            if total_time > 0.1:  # Threshold for significant bottlenecks
                bottlenecks.append((name, total_time))
        bottlenecks.sort(key=lambda x: x[1], reverse=True)
        return bottlenecks

    # =========================================================================
    # CACHING
    # =========================================================================

    def cache_action(self, action_id: str, params: Dict[str, Any], result: Any, ttl: Optional[float] = None) -> None:
        """Cache an action result."""
        cache_key = self._make_cache_key(action_id, params)
        size_hint = sys.getsizeof(result)
        self._action_cache.put(cache_key, result, ttl=ttl, size_hint=size_hint)

    def get_cached_action(self, action_id: str, params: Dict[str, Any]) -> Optional[Any]:
        """Get a cached action result."""
        cache_key = self._make_cache_key(action_id, params)
        result = self._action_cache.get(cache_key)
        if result is not None:
            self._stats["total_cache_hits"] += 1
        else:
            self._stats["total_cache_misses"] += 1
        return result

    def _make_cache_key(self, action_id: str, params: Dict[str, Any]) -> str:
        """Create a cache key from action ID and parameters."""
        params_str = json.dumps(params, sort_keys=True, default=str)
        return hashlib.sha256(f"{action_id}:{params_str}".encode()).hexdigest()

    def cached(self, action_id: str, ttl: Optional[float] = None):
        """Decorator for caching action results."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                params = {"args": str(args), "kwargs": str(kwargs)}
                cached_result = self.get_cached_action(action_id, params)
                if cached_result is not None:
                    return cached_result
                result = func(*args, **kwargs)
                self.cache_action(action_id, params, result, ttl=ttl)
                return result
            return wrapper
        return decorator

    def clear_cache(self) -> None:
        """Clear the action cache."""
        self._action_cache.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        stats = self._action_cache.get_stats()
        stats["total_hits"] = self._stats["total_cache_hits"]
        stats["total_misses"] = self._stats["total_cache_misses"]
        return stats

    # =========================================================================
    # PARALLEL EXECUTION
    # =========================================================================

    def find_parallel_steps(self, workflow: Dict[str, Any]) -> List[List[str]]:
        """Analyze workflow to find independent steps that can run in parallel."""
        with self._execution_lock:
            self._execution_graph.clear()
            
            steps = workflow.get("steps", [])
            for step in steps:
                step_id = step.get("id", str(id(step)))
                dependencies = set(step.get("depends_on", []))
                self._execution_graph[step_id] = dependencies
            
            return self._compute_parallel_groups()

    def _compute_parallel_groups(self) -> List[List[str]]:
        """Compute groups of steps that can execute in parallel."""
        executed = set()
        groups = []
        
        while len(executed) < len(self._execution_graph):
            # Find steps with all dependencies satisfied
            ready = []
            for step_id, deps in self._execution_graph.items():
                if step_id not in executed and deps.issubset(executed):
                    ready.append(step_id)
            
            if not ready:
                break
            
            groups.append(ready)
            executed.update(ready)
        
        return groups

    def execute_parallel(self, steps: List[Dict[str, Any]], executor: Optional[ThreadPoolExecutor] = None) -> List[Any]:
        """Execute steps in parallel."""
        if not executor:
            if not self._executor:
                self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
            executor = self._executor
        
        futures = []
        for step in steps:
            future = executor.submit(self._execute_step, step)
            futures.append((step.get("id", str(id(step))), future))
        
        results = []
        for step_id, future in futures:
            try:
                result = future.result(timeout=self.config.get("step_timeout", 300))
                results.append({"step_id": step_id, "result": result, "error": None})
            except Exception as e:
                results.append({"step_id": step_id, "result": None, "error": str(e)})
        
        self._stats["total_parallel_executions"] += len(steps)
        return results

    def _execute_step(self, step: Dict[str, Any]) -> Any:
        """Execute a single step."""
        step_type = step.get("type")
        params = step.get("params", {})
        
        # This would be connected to actual action execution
        with self.profile(f"step_{step.get('id', 'unknown')}"):
            # Placeholder for actual step execution
            return {"status": "completed", "step": step_type}

    async def execute_parallel_async(self, steps: List[Dict[str, Any]]) -> List[Any]:
        """Async version of parallel execution."""
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=self._max_workers)
        
        async def run_step(step):
            return await loop.run_in_executor(executor, self._execute_step, step)
        
        tasks = [run_step(step) for step in steps]
        return await asyncio.gather(*tasks, return_exceptions=True)

    # =========================================================================
    # LAZY EVALUATION
    # =========================================================================

    def create_lazy(self, name: str, factory: Callable[[], T], cache: bool = True) -> LazyValue[T]:
        """Create a lazy evaluated value."""
        lazy = LazyValue(factory, cache_result=cache)
        self._lazy_values[name] = lazy
        return lazy

    def get_lazy(self, name: str) -> Optional[LazyValue]:
        """Get a lazy value by name."""
        return self._lazy_values.get(name)

    def invalidate_lazy(self, name: str) -> None:
        """Invalidate a lazy value."""
        if name in self._lazy_values:
            self._lazy_values[name].invalidate()

    def invalidate_all_lazy(self) -> None:
        """Invalidate all lazy values."""
        for lazy in self._lazy_values.values():
            lazy.invalidate()

    # =========================================================================
    # MEMORY OPTIMIZATION
    # =========================================================================

    def memory_optimize(self) -> Dict[str, Any]:
        """Perform memory optimization."""
        before_memory = self._get_memory_usage()
        
        # Force garbage collection
        collected = gc.collect()
        
        # Clear caches if memory threshold exceeded
        cache_stats = self._action_cache.get_stats()
        memory_percent = cache_stats["memory_bytes"] / cache_stats.get("max_memory_bytes", 1) * 100
        
        if memory_percent > 80:
            self.clear_cache()
        
        after_memory = self._get_memory_usage()
        
        return {
            "before_bytes": before_memory,
            "after_bytes": after_memory,
            "freed_bytes": before_memory - after_memory,
            "gc_collected": collected,
            "cache_cleared": memory_percent > 80
        }

    def _get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except ImportError:
            import resource
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

    def register_cleanup(self, obj: Any, cleanup_fn: Callable) -> None:
        """Register a cleanup function for an object."""
        weakref.finalize(obj, cleanup_fn)

    # =========================================================================
    # I/O OPTIMIZATION
    # =========================================================================

    def batch_io(self, operation: str, key: str, value: Any) -> None:
        """Batch I/O operations for optimization."""
        with self._io_batch_lock:
            if operation not in self._io_batches:
                self._io_batches[operation] = []
            self._io_batches[operation].append((key, value))
            
            if len(self._io_batches[operation]) >= self._io_batch_size:
                self._flush_io_batch(operation)

    def _flush_io_batch(self, operation: str) -> None:
        """Flush a batch of I/O operations."""
        if operation not in self._io_batches:
            return
        
        batch = self._io_batches.pop(operation)
        # Process batch - actual implementation would depend on operation type
        # This is a placeholder that subclasses would override

    def flush_all_io(self) -> None:
        """Flush all pending I/O batches."""
        with self._io_batch_lock:
            for operation in list(self._io_batches.keys()):
                self._flush_io_batch(operation)
        self._io_last_flush = time.time()

    def optimize_file_operations(self, file_paths: List[str]) -> Dict[str, Any]:
        """Optimize file operations by batching and sorting."""
        sorted_paths = sorted(set(file_paths))
        
        # Group by directory for better locality
        by_directory: Dict[str, List[str]] = {}
        for path in sorted_paths:
            directory = os.path.dirname(path)
            if directory not in by_directory:
                by_directory[directory] = []
            by_directory[directory].append(path)
        
        return {
            "sorted_paths": sorted_paths,
            "by_directory": by_directory,
            "total_files": len(sorted_paths)
        }

    # =========================================================================
    # ALGORITHM OPTIMIZATION
    # =========================================================================

    def suggest_algorithm_improvements(self, workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze workflow and suggest algorithm improvements."""
        suggestions = []
        
        steps = workflow.get("steps", [])
        
        # Check for sequential loops that could be parallelized
        for i, step in enumerate(steps):
            if step.get("type") == "loop" and i > 0:
                prev_step = steps[i - 1]
                if prev_step.get("type") != "loop":
                    suggestions.append({
                        "type": "parallelization",
                        "location": f"step_{i}",
                        "description": "Loop could potentially be parallelized",
                        "potential_speedup": "2-4x"
                    })
        
        # Check for repeated operations that could be cached
        step_types = [s.get("type") for s in steps]
        for step_type in set(step_types):
            count = step_types.count(step_type)
            if count > 3 and step_type not in ["condition", "loop"]:
                suggestions.append({
                    "type": "caching",
                    "location": f"type:{step_type}",
                    "description": f"{step_type} action executed {count} times - consider caching",
                    "potential_speedup": "1.5-3x"
                })
        
        # Check for inefficient data structures
        for step in steps:
            if step.get("type") == "search" or step.get("type") == "lookup":
                suggestions.append({
                    "type": "data_structure",
                    "location": f"step_{step.get('id')}",
                    "description": "Consider using hash map or index for faster lookups",
                    "potential_speedup": "10-100x"
                })
        
        return suggestions

    def choose_best_algorithm(
        self,
        task_type: str,
        data_size: int,
        available_algorithms: List[str]
    ) -> str:
        """Choose the best algorithm based on task characteristics."""
        # Simple algorithm selection based on data size
        if task_type == "search":
            if data_size > 10000:
                return "hash_map"
            else:
                return "binary_search" if sorted(available_algorithms) else "linear_search"
        elif task_type == "sort":
            if data_size < 100:
                return "insertion_sort"
            elif data_size < 10000:
                return "quick_sort"
            else:
                return "merge_sort"
        elif task_type == "aggregation":
            return "stream_aggregation"
        
        return available_algorithms[0] if available_algorithms else "default"

    # =========================================================================
    # RESOURCE POOLING
    # =========================================================================

    def create_resource_pool(
        self,
        name: str,
        factory: Callable[[], T],
        max_size: int = 5,
        validation_fn: Optional[Callable[[T], bool]] = None,
        cleanup_fn: Optional[Callable[[T], None]] = None
    ) -> ResourcePool[T]:
        """Create a new resource pool."""
        pool = ResourcePool(
            factory=factory,
            max_size=max_size,
            validation_fn=validation_fn,
            cleanup_fn=cleanup_fn
        )
        self._resource_pools[name] = pool
        return pool

    def get_resource_pool(self, name: str) -> Optional[ResourcePool]:
        """Get a resource pool by name."""
        return self._resource_pools.get(name)

    def acquire_resource(self, pool_name: str, timeout: float = 30.0) -> Any:
        """Acquire a resource from a pool."""
        pool = self._resource_pools.get(pool_name)
        if not pool:
            raise ValueError(f"Resource pool '{pool_name}' not found")
        return pool.acquire(timeout=timeout)

    def release_resource(self, pool_name: str, resource: Any) -> None:
        """Release a resource back to its pool."""
        pool = self._resource_pools.get(pool_name)
        if pool:
            pool.release(resource)

    def clear_all_pools(self) -> None:
        """Clear all resource pools."""
        for pool in self._resource_pools.values():
            pool.clear()

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get statistics for all resource pools."""
        return {name: pool.get_stats() for name, pool in self._resource_pools.items()}

    # =========================================================================
    # EXECUTION PLANNING
    # =========================================================================

    def plan_execution(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Create an optimized execution plan for a workflow."""
        with self.profile("execution_planning"):
            # Build dependency graph
            self.find_parallel_steps(workflow)
            
            # Get parallel groups
            parallel_groups = self._compute_parallel_groups()
            
            # Estimate execution time for each step
            step_estimates = {}
            for step in workflow.get("steps", []):
                step_id = step.get("id", "unknown")
                step_estimates[step_id] = self._estimate_step_time(step)
            
            # Calculate critical path
            critical_path = self._find_critical_path(parallel_groups, step_estimates)
            
            # Generate optimized order
            execution_order = self._generate_execution_order(parallel_groups, step_estimates)
            
            return {
                "parallel_groups": parallel_groups,
                "critical_path": critical_path,
                "execution_order": execution_order,
                "step_estimates": step_estimates,
                "estimated_total_time": sum(step_estimates.values()),
                "parallelizable_time": self._calculate_parallel_time(parallel_groups, step_estimates)
            }

    def _estimate_step_time(self, step: Dict[str, Any]) -> float:
        """Estimate execution time for a step."""
        step_type = step.get("type", "unknown")
        
        # Use historical data if available
        baseline_key = f"step_{step_type}"
        with self._baseline_lock:
            if baseline_key in self._baselines and self._baselines[baseline_key]:
                baseline = self._baselines[baseline_key][-1]
                return baseline.avg_time
        
        # Default estimates based on step type
        estimates = {
            "click": 0.05,
            "type": 0.02,
            "wait": 1.0,
            "loop": 0.1,
            "condition": 0.01,
            "image_match": 0.5,
            "ocr": 1.0,
            "script": 0.5,
            "http_request": 0.3
        }
        return estimates.get(step_type, 0.1)

    def _find_critical_path(self, parallel_groups: List[List[str]], estimates: Dict[str, float]) -> List[str]:
        """Find the critical path through the workflow."""
        critical_path = []
        max_time = 0
        
        for group in parallel_groups:
            group_time = sum(estimates.get(step, 0) for step in group)
            if group_time > max_time:
                max_time = group_time
                critical_path = group
        
        return critical_path

    def _generate_execution_order(self, groups: List[List[str]], estimates: Dict[str, float]) -> List[List[str]]:
        """Generate optimized execution order."""
        # Groups are already in optimal order for parallel execution
        # Return them with timing information
        return groups

    def _calculate_parallel_time(self, groups: List[List[str]], estimates: Dict[str, float]) -> float:
        """Calculate total time if running in parallel."""
        return sum(max(estimates.get(step, 0) for step in group) for group in groups)

    # =========================================================================
    # PERFORMANCE BASELINES
    # =========================================================================

    def record_baseline(
        self,
        name: str,
        execution_time: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PerformanceBaseline:
        """Record a performance baseline."""
        baseline = PerformanceBaseline(
            name=name,
            avg_time=execution_time,
            std_dev=0.0,
            min_time=execution_time,
            max_time=execution_time,
            sample_count=1,
            timestamp=time.time(),
            metadata=metadata or {}
        )
        
        with self._baseline_lock:
            if name not in self._baselines:
                self._baselines[name] = []
            self._baselines[name].append(baseline)
            
            # Update statistics for the baseline
            self._update_baseline_stats(name)
        
        return baseline

    def _update_baseline_stats(self, name: str) -> None:
        """Update baseline statistics after new sample."""
        baselines = self._baselines.get(name, [])
        if len(baselines) < 2:
            return
        
        times = [b.avg_time for b in baselines]
        avg = sum(times) / len(times)
        variance = sum((t - avg) ** 2 for t in times) / len(times)
        std_dev = variance ** 0.5
        
        # Update all baselines with new statistics
        for baseline in baselines:
            baseline.std_dev = std_dev
        
        # Update last baseline with correct stats
        baselines[-1].sample_count = len(baselines)

    def check_regression(self, name: str, current_time: float, threshold: float = 2.0) -> Tuple[bool, Optional[PerformanceBaseline]]:
        """Check if current performance represents a regression."""
        with self._baseline_lock:
            if name not in self._baselines or not self._baselines[name]:
                return False, None
            
            baseline = self._baselines[name][-1]
            is_regression = baseline.is_regression(current_time, threshold)
            
            if is_regression:
                self._stats["regressions_detected"] += 1
            
            return is_regression, baseline

    def get_baseline(self, name: str) -> Optional[PerformanceBaseline]:
        """Get the most recent baseline for a workflow or step."""
        with self._baseline_lock:
            baselines = self._baselines.get(name, [])
            return baselines[-1] if baselines else None

    def get_all_baselines(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all baselines as dictionaries."""
        with self._baseline_lock:
            return {
                name: [
                    {
                        "avg_time": b.avg_time,
                        "std_dev": b.std_dev,
                        "min_time": b.min_time,
                        "max_time": b.max_time,
                        "sample_count": b.sample_count,
                        "timestamp": b.timestamp
                    }
                    for b in baselines
                ]
                for name, baselines in self._baselines.items()
            }

    def clear_baselines(self, name: Optional[str] = None) -> None:
        """Clear baselines for a specific workflow or all."""
        with self._baseline_lock:
            if name:
                self._baselines.pop(name, None)
            else:
                self._baselines.clear()

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def optimize_workflow(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all optimizations to a workflow."""
        with self.profile("workflow_optimization"):
            # Get execution plan
            plan = self.plan_execution(workflow)
            
            # Get algorithm suggestions
            suggestions = self.suggest_algorithm_improvements(workflow)
            
            return {
                "original_workflow": workflow,
                "optimized_plan": plan,
                "suggestions": suggestions,
                "cache_stats": self.get_cache_stats(),
                "pool_stats": self.get_pool_stats()
            }

    def get_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report."""
        return {
            "statistics": self._stats,
            "cache": self.get_cache_stats(),
            "pools": self.get_pool_stats(),
            "baselines": self.get_all_baselines(),
            "profiling": self.get_profiling_results(),
            "lazy_values": {name: lv.is_evaluated() for name, lv in self._lazy_values.items()}
        }

    def shutdown(self) -> None:
        """Shutdown optimizer and cleanup resources."""
        self.flush_all_io()
        self.clear_all_pools()
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __enter__(self) -> 'PerformanceOptimizer':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.shutdown()


class ProfileContext:
    """Context manager for profiling code blocks."""

    def __init__(self, optimizer: PerformanceOptimizer, name: str):
        self.optimizer = optimizer
        self.name = name
        self.profiler: Optional[Profiler] = None

    def __enter__(self):
        self.profiler = self.optimizer.profile(self.name)
        self.profiler.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.profiler:
            self.profiler.__exit__(exc_type, exc_val, exc_tb)


# Factory function for creating optimizer with common configurations
def create_optimized_optimizer(mode: str = "balanced") -> PerformanceOptimizer:
    """Create a pre-configured optimizer for common use cases."""
    configs = {
        "memory_saver": {
            "cache_size": 100,
            "cache_memory_mb": 50,
            "max_workers": 2,
            "gc_threshold": 50
        },
        "balanced": {
            "cache_size": 1000,
            "cache_memory_mb": 100,
            "max_workers": 4,
            "gc_threshold": 100
        },
        "performance": {
            "cache_size": 5000,
            "cache_memory_mb": 500,
            "max_workers": 8,
            "gc_threshold": 200
        }
    }
    
    config = configs.get(mode, configs["balanced"])
    return PerformanceOptimizer(config)
