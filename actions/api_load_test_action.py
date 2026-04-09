"""API Load Test Action Module.

Provides load testing capabilities for API endpoints including
concurrent requests, rate limiting, and performance profiling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class LoadTestStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    DEGRADED = "degraded"


@dataclass
class RequestMetrics:
    latency_ms: float
    status_code: int
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class LoadTestResult:
    total_requests: int
    successful_requests: int
    failed_requests: int
    success_rate: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    min_latency_ms: float
    requests_per_second: float
    status: LoadTestStatus
    duration_seconds: float
    errors: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": self.success_rate,
            "avg_latency_ms": self.avg_latency_ms,
            "p50_latency_ms": self.p50_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "max_latency_ms": self.max_latency_ms,
            "min_latency_ms": self.min_latency_ms,
            "requests_per_second": self.requests_per_second,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors[:10],
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class LoadTestConfig:
    target_rps: int = 100
    duration_seconds: int = 60
    concurrent_users: int = 10
    timeout_seconds: float = 30.0
    success_threshold: float = 0.95
    max_latency_threshold_ms: float = 1000.0


def calculate_percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(len(sorted_values) * percentile / 100.0)
    return sorted_values[min(index, len(sorted_values) - 1)]


class LoadTestRunner:
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.metrics: List[RequestMetrics] = []
        self._running = False
        self._cancelled = False

    async def run(self, request_fn: Callable[[], Any]) -> LoadTestResult:
        self._running = True
        self._cancelled = False
        self.metrics = []
        start_time = time.time()

        async def worker(user_id: int):
            request_delay = 1.0 / (self.config.target_rps / self.config.concurrent_users)
            while self._running and not self._cancelled:
                worker_start = time.time()
                try:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(request_fn),
                        timeout=self.config.timeout_seconds,
                    )
                    success = self._is_success(result)
                    self.metrics.append(RequestMetrics(
                        latency_ms=(time.time() - worker_start) * 1000,
                        status_code=getattr(result, 'status_code', 200),
                        success=success,
                    ))
                except asyncio.TimeoutError:
                    self.metrics.append(RequestMetrics(
                        latency_ms=(time.time() - worker_start) * 1000,
                        status_code=0,
                        success=False,
                        error="Timeout",
                    ))
                except Exception as e:
                    self.metrics.append(RequestMetrics(
                        latency_ms=(time.time() - worker_start) * 1000,
                        status_code=0,
                        success=False,
                        error=str(e),
                    ))

                elapsed = time.time() - start_time
                if elapsed >= self.config.duration_seconds:
                    break

                await asyncio.sleep(max(0, request_delay - (time.time() - worker_start)))

        workers = [asyncio.create_task(worker(i)) for i in range(self.config.concurrent_users)]
        await asyncio.gather(*workers, return_exceptions=True)

        return self._generate_report(time.time() - start_time)

    def _is_success(self, result: Any) -> bool:
        if isinstance(result, dict):
            return result.get("status_code", 200) < 400
        if hasattr(result, "status_code"):
            return result.status_code < 400
        return True

    def _generate_report(self, duration: float) -> LoadTestResult:
        latencies = [m.latency_ms for m in self.metrics if m.success]
        errors = [m.error for m in self.metrics if m.error]

        success_count = sum(1 for m in self.metrics if m.success)
        total = len(self.metrics)

        status = LoadTestStatus.PASSED
        if total == 0 or success_count / total < self.config.success_threshold:
            status = LoadTestStatus.FAILED
        elif latencies and max(latencies) > self.config.max_latency_threshold_ms:
            status = LoadTestStatus.DEGRADED

        return LoadTestResult(
            total_requests=total,
            successful_requests=success_count,
            failed_requests=total - success_count,
            success_rate=success_count / total if total > 0 else 0.0,
            avg_latency_ms=sum(latencies) / len(latencies) if latencies else 0.0,
            p50_latency_ms=calculate_percentile(latencies, 50),
            p95_latency_ms=calculate_percentile(latencies, 95),
            p99_latency_ms=calculate_percentile(latencies, 99),
            max_latency_ms=max(latencies) if latencies else 0.0,
            min_latency_ms=min(latencies) if latencies else 0.0,
            requests_per_second=total / duration if duration > 0 else 0.0,
            status=status,
            duration_seconds=duration,
            errors=errors,
        )

    def cancel(self):
        self._cancelled = True
        self._running = False


async def run_load_test(
    request_fn: Callable[[], Any],
    config: Optional[LoadTestConfig] = None,
) -> LoadTestResult:
    config = config or LoadTestConfig()
    runner = LoadTestRunner(config)
    return await runner.run(request_fn)
