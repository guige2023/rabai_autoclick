"""API Load Tester Action.

Load testing for API endpoints with configurable concurrency,
ramp-up patterns, latency percentiles, and failure injection.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class LoadPattern(Enum):
    """Load patterns for testing."""
    CONSTANT = "constant"
    RAMP_UP = "ramp_up"
    SPIKE = "spike"
    WAVE = "wave"
    RANDOM = "random"


@dataclass
class LoadTestConfig:
    """Configuration for load testing."""
    base_url: str
    endpoint: str
    method: str = "GET"
    concurrent_users: int = 10
    total_requests: int = 1000
    ramp_up_seconds: float = 10.0
    think_time_ms: int = 0
    timeout_sec: float = 30.0
    pattern: LoadPattern = LoadPattern.CONSTANT
    payload: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None


@dataclass
class RequestResult:
    """Result of a single request."""
    request_id: int
    status_code: int
    latency_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class LoadTestReport:
    """Comprehensive load test report."""
    config: Dict[str, Any]
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_duration_sec: float
    requests_per_second: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    p50_latency_ms: float
    p90_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_distribution: Dict[int, int] = field(default_factory=dict)
    time_series: List[Dict[str, Any]] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)


class APILoadTesterAction:
    """Load testing tool for API endpoints."""

    def __init__(self) -> None:
        self._results: List[RequestResult] = []
        self._request_counter = 0
        self._lock = asyncio.Lock()

    async def _make_request(
        self,
        client: Any,
        config: LoadTestConfig,
        request_id: int,
    ) -> RequestResult:
        """Make a single HTTP request."""
        start = time.time()
        try:
            method = config.method.upper()
            headers = config.headers or {}

            if method == "GET":
                response = await asyncio.wait_for(
                    client.get(config.endpoint, headers=headers, timeout=config.timeout_sec),
                    timeout=config.timeout_sec,
                )
            elif method == "POST":
                response = await asyncio.wait_for(
                    client.post(config.endpoint, json=config.payload, headers=headers, timeout=config.timeout_sec),
                    timeout=config.timeout_sec,
                )
            elif method == "PUT":
                response = await asyncio.wait_for(
                    client.put(config.endpoint, json=config.payload, headers=headers, timeout=config.timeout_sec),
                    timeout=config.timeout_sec,
                )
            elif method == "DELETE":
                response = await asyncio.wait_for(
                    client.delete(config.endpoint, headers=headers, timeout=config.timeout_sec),
                    timeout=config.timeout_sec,
                )
            else:
                response = await asyncio.wait_for(
                    client.request(method, config.endpoint, headers=headers, timeout=config.timeout_sec),
                    timeout=config.timeout_sec,
                )

            latency_ms = (time.time() - start) * 1000
            status_code = response.status_code if hasattr(response, "status_code") else 200

            return RequestResult(
                request_id=request_id,
                status_code=status_code,
                latency_ms=latency_ms,
                success=200 <= status_code < 400,
            )

        except asyncio.TimeoutError:
            latency_ms = (time.time() - start) * 1000
            return RequestResult(
                request_id=request_id,
                status_code=0,
                latency_ms=latency_ms,
                success=False,
                error="Timeout",
            )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            return RequestResult(
                request_id=request_id,
                status_code=0,
                latency_ms=latency_ms,
                success=False,
                error=str(e),
            )

    async def _worker(
        self,
        client: Any,
        config: LoadTestConfig,
        worker_id: int,
        results_queue: asyncio.Queue,
        stop_event: asyncio.Event,
    ) -> None:
        """Worker coroutine for making requests."""
        request_id = worker_id

        while not stop_event.is_set() and request_id < config.total_requests:
            result = await self._make_request(client, config, request_id)
            await results_queue.put(result)
            request_id += config.concurrent_users

            if config.think_time_ms > 0:
                await asyncio.sleep(config.think_time_ms / 1000)

    async def run_load_test_async(
        self,
        config: LoadTestConfig,
    ) -> LoadTestReport:
        """Run a load test asynchronously."""
        import httpx

        results_queue: asyncio.Queue = asyncio.Queue()
        stop_event = asyncio.Event()

        async with httpx.AsyncClient(base_url=config.base_url) as client:
            workers = [
                asyncio.create_task(
                    self._worker(client, config, i, results_queue, stop_event)
                )
                for i in range(config.concurrent_users)
            ]

            start_time = time.time()

            await asyncio.gather(*workers)
            stop_event.set()

            collected_results: List[RequestResult] = []
            while not results_queue.empty():
                collected_results.append(await results_queue.get())

        total_duration = time.time() - start_time

        return self._generate_report(config, collected_results, total_duration)

    def run_load_test(
        self,
        config: LoadTestConfig,
    ) -> LoadTestReport:
        """Run a load test synchronously."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.run_load_test_async(config))

    def _generate_report(
        self,
        config: LoadTestConfig,
        results: List[RequestResult],
        total_duration: float,
    ) -> LoadTestReport:
        """Generate a load test report from results."""
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        latencies = sorted([r.latency_ms for r in successful]) if successful else [0]
        total_requests = len(results)

        error_dist: Dict[int, int] = {}
        for r in failed:
            error_dist[r.status_code] = error_dist.get(r.status_code, 0) + 1

        def percentile(data: List[float], p: float) -> float:
            if not data:
                return 0.0
            idx = int(len(data) * p)
            return data[min(idx, len(data) - 1)]

        return LoadTestReport(
            config={
                "base_url": config.base_url,
                "endpoint": config.endpoint,
                "method": config.method,
                "concurrent_users": config.concurrent_users,
                "total_requests": config.total_requests,
                "pattern": config.pattern.value,
            },
            total_requests=total_requests,
            successful_requests=len(successful),
            failed_requests=len(failed),
            total_duration_sec=total_duration,
            requests_per_second=total_requests / total_duration if total_duration > 0 else 0,
            avg_latency_ms=sum(latencies) / len(latencies) if latencies else 0,
            min_latency_ms=min(latencies) if latencies else 0,
            max_latency_ms=max(latencies) if latencies else 0,
            p50_latency_ms=percentile(latencies, 0.50),
            p90_latency_ms=percentile(latencies, 0.90),
            p95_latency_ms=percentile(latencies, 0.95),
            p99_latency_ms=percentile(latencies, 0.99),
            error_distribution=error_dist,
            time_series=[],
        )

    def generate_html_report(self, report: LoadTestReport) -> str:
        """Generate an HTML report from results."""
        html = f"""
        <html>
        <head><title>Load Test Report</title></head>
        <body>
        <h1>Load Test Report</h1>
        <h2>Summary</h2>
        <ul>
            <li>Total Requests: {report.total_requests}</li>
            <li>Successful: {report.successful_requests}</li>
            <li>Failed: {report.failed_requests}</li>
            <li>Duration: {report.total_duration_sec:.2f}s</li>
            <li>RPS: {report.requests_per_second:.2f}</li>
        </ul>
        <h2>Latency</h2>
        <ul>
            <li>Avg: {report.avg_latency_ms:.2f}ms</li>
            <li>Min: {report.min_latency_ms:.2f}ms</li>
            <li>Max: {report.max_latency_ms:.2f}ms</li>
            <li>P50: {report.p50_latency_ms:.2f}ms</li>
            <li>P90: {report.p90_latency_ms:.2f}ms</li>
            <li>P95: {report.p95_latency_ms:.2f}ms</li>
            <li>P99: {report.p99_latency_ms:.2f}ms</li>
        </ul>
        </body>
        </html>
        """
        return html
