"""Benchmarking utilities for performance measurement."""

from typing import Callable, TypeVar, Any, Optional, List, Dict
import time
import statistics
import threading


T = TypeVar("T")


class BenchmarkResult:
    """Result of a benchmark run."""

    def __init__(
        self,
        name: str,
        iterations: int,
        total_time: float,
        min_time: float,
        max_time: float,
        mean_time: float,
        median_time: float,
        std_dev: float,
        ops_per_second: float
    ):
        """Initialize benchmark result."""
        self.name = name
        self.iterations = iterations
        self.total_time = total_time
        self.min_time = min_time
        self.max_time = max_time
        self.mean_time = mean_time
        self.median_time = median_time
        self.std_dev = std_dev
        self.ops_per_second = ops_per_second

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_time_ms": self.total_time * 1000,
            "min_time_ms": self.min_time * 1000,
            "max_time_ms": self.max_time * 1000,
            "mean_time_ms": self.mean_time * 1000,
            "median_time_ms": self.median_time * 1000,
            "std_dev_ms": self.std_dev * 1000,
            "ops_per_second": self.ops_per_second,
        }

    def __repr__(self) -> str:
        return (f"BenchmarkResult({self.name}: "
                f"{self.mean_time*1000:.3f}ms ± {self.std_dev*1000:.3f}ms, "
                f"{self.ops_per_second:.1f} ops/s)")


def benchmark(
    func: Callable[[], T],
    iterations: int = 100,
    warmup: int = 10,
    name: Optional[str] = None
) -> BenchmarkResult:
    """Benchmark a function.
    
    Args:
        func: Function to benchmark.
        iterations: Number of iterations.
        warmup: Warmup iterations (not counted).
        name: Benchmark name.
    
    Returns:
        Benchmark result.
    """
    benchmark_name = name or func.__name__
    for _ in range(warmup):
        func()
    times: List[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    total_time = sum(times)
    mean_time = total_time / iterations
    return BenchmarkResult(
        name=benchmark_name,
        iterations=iterations,
        total_time=total_time,
        min_time=min(times),
        max_time=max(times),
        mean_time=mean_time,
        median_time=statistics.median(times),
        std_dev=statistics.stdev(times) if len(times) > 1 else 0.0,
        ops_per_second=iterations / total_time if total_time > 0 else 0.0,
    )


class BenchmarkSuite:
    """Collection of benchmarks."""

    def __init__(self, name: str = "Suite"):
        """Initialize benchmark suite.
        
        Args:
            name: Suite name.
        """
        self.name = name
        self._benchmarks: List[Callable[[], BenchmarkResult]] = []
        self._lock = threading.Lock()

    def register(self, func: Callable[[], BenchmarkResult]) -> None:
        """Register a benchmark function."""
        with self._lock:
            self._benchmarks.append(func)

    def add(
        self,
        func: Callable[[], Any],
        iterations: int = 100,
        warmup: int = 10,
        name: Optional[str] = None
    ) -> None:
        """Add a function as benchmark.
        
        Args:
            func: Function to benchmark.
            iterations: Number of iterations.
            warmup: Warmup iterations.
            name: Benchmark name.
        """
        def wrapper():
            return benchmark(func, iterations, warmup, name or func.__name__)
        self.register(wrapper)

    def run_all(self) -> List[BenchmarkResult]:
        """Run all benchmarks.
        
        Returns:
            List of benchmark results.
        """
        with self._lock:
            benchmarks = list(self._benchmarks)
        return [b() for b in benchmarks]

    def run_and_print(self) -> None:
        """Run all benchmarks and print results."""
        results = self.run_all()
        print(f"\n=== {self.name} ===")
        for r in results:
            print(f"  {r}")
        print()


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str = "Timer"):
        """Initialize timer.
        
        Args:
            name: Timer name for identification.
        """
        self.name = name
        self.elapsed: Optional[float] = None
        self._start: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.elapsed = time.perf_counter() - self._start

    def __repr__(self) -> str:
        if self.elapsed is not None:
            return f"{self.name}: {self.elapsed*1000:.3f}ms"
        return f"{self.name}: not completed"
