"""
Load testing utilities for HTTP APIs and services.

Provides load test runner, request batching, latency profiling,
throughput measurement, and result aggregation.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

import httpx

logger = logging.getLogger(__name__)


class LoadProfile(Enum):
    """Load test profile types."""
    CONSTANT = auto()
    RAMP_UP = auto()
    SPIKE = auto()
    SHAKE = auto()


@dataclass
class RequestResult:
    """Result of a single request."""
    status_code: int
    latency_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class LoadTestConfig:
    """Configuration for load testing."""
    target_url: str = "http://localhost:8000"
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: Optional[dict[str, Any]] = None
    profile: LoadProfile = LoadProfile.CONSTANT
    duration_seconds: int = 60
    concurrency: int = 10
    requests_per_second: int = 100
    ramp_up_seconds: int = 10
    timeout: float = 30.0


@dataclass
class AggregateStats:
    """Aggregated load test statistics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    mean_latency_ms: float = 0.0
    median_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    requests_per_second: float = 0.0
    errors: dict[str, int] = field(default_factory=dict)
    status_codes: dict[int, int] = field(default_factory=dict)

    def add_result(self, result: RequestResult) -> None:
        self.total_requests += 1
        if result.success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            err = result.error or "unknown"
            self.errors[err] = self.errors.get(err, 0) + 1

        code = result.status_code
        self.status_codes[code] = self.status_codes.get(code, 0) + 1

        latencies = [r.latency_ms for r in [result]]
        if latencies:
            self.min_latency_ms = min(self.min_latency_ms, *latencies)
            self.max_latency_ms = max(self.max_latency_ms, *latencies)

    def finalize(self, latencies: list[float], elapsed: float) -> None:
        if latencies:
            self.min_latency_ms = min(latencies)
            self.max_latency_ms = max(latencies)
            self.mean_latency_ms = statistics.mean(latencies)
            self.median_latency_ms = statistics.median(latencies)
            sorted_lat = sorted(latencies)
            p95_idx = int(len(sorted_lat) * 0.95)
            p99_idx = int(len(sorted_lat) * 0.99)
            self.p95_latency_ms = sorted_lat[p95_idx] if p95_idx < len(sorted_lat) else sorted_lat[-1]
            self.p99_latency_ms = sorted_lat[p99_idx] if p99_idx < len(sorted_lat) else sorted_lat[-1]
        self.requests_per_second = self.total_requests / elapsed if elapsed > 0 else 0

    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests * 100

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful": self.successful_requests,
            "failed": self.failed_requests,
            "success_rate_%": round(self.success_rate(), 2),
            "latency_ms": {
                "min": round(self.min_latency_ms, 2),
                "max": round(self.max_latency_ms, 2),
                "mean": round(self.mean_latency_ms, 2),
                "median": round(self.median_latency_ms, 2),
                "p95": round(self.p95_latency_ms, 2),
                "p99": round(self.p99_latency_ms, 2),
            },
            "rps": round(self.requests_per_second, 2),
            "status_codes": self.status_codes,
            "errors": self.errors,
        }


class LoadTestRunner:
    """Async load test runner with multiple profiles."""

    def __init__(self, config: LoadTestConfig) -> None:
        self.config = config
        self._results: list[RequestResult] = []
        self._stats = AggregateStats()
        self._running = False

    async def _make_request(self, client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> RequestResult:
        """Make a single HTTP request."""
        async with semaphore:
            start = time.perf_counter()
            try:
                if self.config.method == "GET":
                    response = await client.get(self.config.target_url, timeout=self.config.timeout)
                elif self.config.method == "POST":
                    response = await client.post(
                        self.config.target_url,
                        json=self.config.body,
                        timeout=self.config.timeout,
                    )
                elif self.config.method == "PUT":
                    response = await client.put(
                        self.config.target_url,
                        json=self.config.body,
                        timeout=self.config.timeout,
                    )
                elif self.config.method == "DELETE":
                    response = await client.delete(self.config.target_url, timeout=self.config.timeout)
                else:
                    response = await client.request(
                        self.config.method,
                        self.config.target_url,
                        json=self.config.body,
                        timeout=self.config.timeout,
                    )
                latency = (time.perf_counter() - start) * 1000
                return RequestResult(
                    status_code=response.status_code,
                    latency_ms=latency,
                    success=200 <= response.status_code < 300,
                )
            except httpx.TimeoutException:
                latency = (time.perf_counter() - start) * 1000
                return RequestResult(status_code=0, latency_ms=latency, success=False, error="timeout")
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                return RequestResult(status_code=0, latency_ms=latency, success=False, error=str(e))

    async def _worker(
        self,
        worker_id: int,
        requests: int,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        results_out: list[RequestResult],
    ) -> None:
        """Worker coroutine for generating load."""
        for _ in range(requests):
            if not self._running:
                break
            result = await self._make_request(client, semaphore)
            results_out.append(result)

    async def run(self) -> AggregateStats:
        """Run the load test."""
        self._running = True
        self._results = []
        all_results: list[RequestResult] = []

        async with httpx.AsyncClient(headers=self.config.headers) as client:
            semaphore = asyncio.Semaphore(self.config.concurrency)

            if self.config.profile == LoadProfile.CONSTANT:
                workers = self.config.concurrency
                requests_per_worker = self.config.requests_per_second * self.config.duration_seconds // workers
                remainder = (self.config.requests_per_second * self.config.duration_seconds) % workers

                tasks = [
                    self._worker(i, requests_per_worker + (1 if i < remainder else 0), client, semaphore, all_results)
                    for i in range(workers)
                ]
                await asyncio.gather(*tasks)

            elif self.config.profile == LoadProfile.RAMP_UP:
                step_duration = self.config.ramp_up_seconds
                steps = max(1, self.config.duration_seconds // step_duration)
                rps_per_step = self.config.requests_per_second // steps
                for step in range(steps):
                    if not self._running:
                        break
                    conc = max(1, self.config.concurrency * (step + 1) // steps)
                    sem = asyncio.Semaphore(conc)
                    tasks = [
                        self._worker(i, rps_per_step // conc + 1, client, sem, all_results)
                        for i in range(conc)
                    ]
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(step_duration)

        self._results = all_results
        latencies = [r.latency_ms for r in all_results]
        elapsed = self.config.duration_seconds
        self._stats = AggregateStats()
        for r in all_results:
            self._stats.add_result(r)
        self._stats.finalize(latencies, elapsed)
        return self._stats

    def stop(self) -> None:
        """Stop the load test early."""
        self._running = False

    @property
    def stats(self) -> AggregateStats:
        return self._stats


class LatencyProfiler:
    """Profiles latency at different percentiles for a given endpoint."""

    def __init__(self, url: str, samples: int = 1000) -> None:
        self.url = url
        self.samples = samples
        self._latencies: list[float] = []

    async def profile(self) -> dict[str, float]:
        """Run latency profiling."""
        async with httpx.AsyncClient() as client:
            tasks = []
            for _ in range(self.samples):
                start = time.perf_counter()
                tasks.append(client.get(self.url))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, httpx.Response):
                    latency = (time.perf_counter() - start) * 1000
                    self._latencies.append(latency)

        sorted_lat = sorted(self._latencies)
        return {
            "min": min(sorted_lat),
            "max": max(sorted_lat),
            "mean": statistics.mean(sorted_lat),
            "median": statistics.median(sorted_lat),
            "p90": sorted_lat[int(len(sorted_lat) * 0.90)],
            "p95": sorted_lat[int(len(sorted_lat) * 0.95)],
            "p99": sorted_lat[int(len(sorted_lat) * 0.99)],
        }
