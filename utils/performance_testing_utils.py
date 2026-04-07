"""Performance testing utilities: load testing, latency profiling, benchmarking."""

from __future__ import annotations

import gc
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "BenchmarkResult",
    "Benchmark",
    "LoadTestConfig",
    "LoadTestResult",
    "load_test",
    "stress_test",
]


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    name: str
    iterations: int
    total_time_ms: float
    mean_ms: float
    median_ms: float
    std_dev_ms: float
    min_ms: float
    max_ms: float
    p95_ms: float
    p99_ms: float

    def __repr__(self) -> str:
        return (
            f"Benchmark({self.name}): {self.iterations} iters, "
            f"mean={self.mean_ms:.3f}ms, p95={self.p95_ms:.3f}ms"
        )


class Benchmark:
    """Statistical benchmark runner."""

    def __init__(self, name: str, warmup: int = 3, iterations: int = 100) -> None:
        self.name = name
        self.warmup = warmup
        self.iterations = iterations
        self._results: list[float] = []

    def run(self, fn: Callable[[], Any]) -> BenchmarkResult:
        for _ in range(self.warmup):
            fn()

        self._results = []
        for _ in range(self.iterations):
            gc.disable()
            start = time.perf_counter()
            fn()
            elapsed = (time.perf_counter() - start) * 1000
            gc.enable()
            self._results.append(elapsed)

        self._results.sort()
        n = len(self._results)
        return BenchmarkResult(
            name=self.name,
            iterations=n,
            total_time_ms=sum(self._results),
            mean_ms=statistics.mean(self._results),
            median_ms=statistics.median(self._results),
            std_dev_ms=statistics.stdev(self._results) if n > 1 else 0.0,
            min_ms=min(self._results),
            max_ms=max(self._results),
            p95_ms=self._results[int(n * 0.95)] if n > 0 else 0,
            p99_ms=self._results[int(n * 0.99)] if n > 0 else 0,
        )

    def compare(self, other: "Benchmark", fn: Callable[[], Any]) -> dict[str, Any]:
        result_a = self.run(fn)
        result_b = other.run(fn)
        speedup = result_a.mean_ms / result_b.mean_ms if result_b.mean_ms > 0 else 0
        return {
            "a": {"name": result_a.name, "mean_ms": result_a.mean_ms},
            "b": {"name": result_b.name, "mean_ms": result_b.mean_ms},
            "speedup": speedup,
        }


@dataclass
class LoadTestConfig:
    """Configuration for a load test."""
    num_threads: int = 10
    requests_per_thread: int = 100
    ramp_up_seconds: float = 0.0
    timeout_seconds: float = 60.0


@dataclass
class LoadTestResult:
    """Results from a load test."""
    total_requests: int
    success_count: int
    error_count: int
    total_time_seconds: float
    requests_per_second: float
    mean_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    errors: list[tuple[str, int]]

    @property
    def success_rate(self) -> float:
        return self.success_count / self.total_requests if self.total_requests > 0 else 0.0

    def summary(self) -> str:
        return (
            f"LoadTest: {self.total_requests} requests in {self.total_time_seconds:.2f}s "
            f"({self.requests_per_second:.1f} rps), "
            f"success={self.success_rate:.1%}, "
            f"p95={self.p95_latency_ms:.1f}ms"
        )


def load_test(
    fn: Callable[[], bool],
    config: LoadTestConfig,
) -> LoadTestResult:
    """Run a load test against a function that returns True on success."""
    latencies: list[float] = []
    errors: list[tuple[str, int]] = []
    success_count = 0
    error_count = 0
    error_lock = threading.Lock()
    success_lock = threading.Lock()
    latency_lock = threading.Lock()
    start_barrier = threading.Barrier(config.num_threads)

    def worker(thread_id: int) -> None:
        nonlocal success_count, error_count
        if config.ramp_up_seconds > 0:
            delay = thread_id * (config.ramp_up_seconds / config.num_threads)
            time.sleep(delay)

        start_barrier.wait()
        local_success = 0
        local_errors: list[tuple[str, int]] = []

        for i in range(config.requests_per_thread):
            req_start = time.perf_counter()
            try:
                ok = fn()
            except Exception as e:
                ok = False
                local_errors.append((type(e).__name__, 1))
            latency = (time.perf_counter() - req_start) * 1000

            with latency_lock:
                latencies.append(latency)
            if ok:
                local_success += 1
            else:
                local_errors += local_errors

        with success_lock:
            success_count += local_success
        with error_lock:
            error_count += len(local_errors)
            for e in local_errors:
                errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(config.num_threads)]
    test_start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=config.timeout_seconds)
    total_time = time.monotonic() - test_start

    latencies.sort()
    n = len(latencies)

    return LoadTestResult(
        total_requests=config.num_threads * config.requests_per_thread,
        success_count=success_count,
        error_count=error_count,
        total_time_seconds=total_time,
        requests_per_second=success_count / total_time if total_time > 0 else 0,
        mean_latency_ms=statistics.mean(latencies) if latencies else 0,
        p50_latency_ms=latencies[int(n * 0.50)] if n > 0 else 0,
        p95_latency_ms=latencies[int(n * 0.95)] if n > 0 else 0,
        p99_latency_ms=latencies[int(n * 0.99)] if n > 0 else 0,
        max_latency_ms=max(latencies) if latencies else 0,
        errors=errors,
    )


def stress_test(
    fn: Callable[[], bool],
    max_duration_seconds: float = 30.0,
    target_rps: float = 100.0,
) -> LoadTestResult:
    """Run a stress test at a target requests-per-second rate."""
    interval = 1.0 / target_rps if target_rps > 0 else 0.0

    class StressState:
        count = 0
        lock = threading.Lock()

    def target_worker() -> None:
        deadline = time.monotonic() + max_duration_seconds
        while time.monotonic() < deadline:
            fn()
            with StressState.lock:
                StressState.count += 1
            time.sleep(interval)

    threads = [threading.Thread(target=target_worker) for _ in range(int(target_rps))]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=max_duration_seconds + 1)
    elapsed = time.monotonic() - start

    return LoadTestResult(
        total_requests=StressState.count,
        success_count=StressState.count,
        error_count=0,
        total_time_seconds=elapsed,
        requests_per_second=StressState.count / elapsed if elapsed > 0 else 0,
        mean_latency_ms=0,
        p50_latency_ms=0,
        p95_latency_ms=0,
        p99_latency_ms=0,
        max_latency_ms=0,
        errors=[],
    )
