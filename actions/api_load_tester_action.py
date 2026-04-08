"""API load tester action module for RabAI AutoClick.

Provides API load testing with concurrent requests, latency tracking,
and performance metrics collection.
"""

import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from collections import defaultdict
import concurrent.futures

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LoadTestResult:
    """Result of a load test."""
    timestamp: float
    duration_ms: float
    status_code: int
    success: bool
    error: Optional[str] = None


class ApiLoadTesterAction(BaseAction):
    """API load tester action for performance testing.
    
    Supports concurrent request execution with configurable
    threads, duration, and latency thresholds.
    """
    action_type = "api_load_tester"
    display_name = "API负载测试"
    description = "API并发负载测试"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute load test.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                url: Target URL
                method: HTTP method
                headers: Request headers
                body: Request body
                concurrency: Number of concurrent workers
                total_requests: Total number of requests (-1 for continuous)
                duration_seconds: Test duration in seconds
                timeout: Request timeout in seconds.
        
        Returns:
            ActionResult with load test metrics.
        """
        url = params.get('url')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body')
        concurrency = params.get('concurrency', 10)
        total_requests = params.get('total_requests', 100)
        duration = params.get('duration_seconds', 0)
        timeout = params.get('timeout', 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        results: List[LoadTestResult] = []
        lock = threading.Lock()
        start_time = time.time()
        request_count = 0
        
        def make_request():
            nonlocal request_count
            test_start = time.time()
            try:
                req = Request(url, method=method, headers=headers)
                with urlopen(req, timeout=timeout) as response:
                    duration_ms = (time.time() - test_start) * 1000
                    result = LoadTestResult(
                        timestamp=time.time(),
                        duration_ms=duration_ms,
                        status_code=response.status,
                        success=response.status < 400
                    )
            except HTTPError as e:
                duration_ms = (time.time() - test_start) * 1000
                result = LoadTestResult(
                    timestamp=time.time(),
                    duration_ms=duration_ms,
                    status_code=e.code,
                    success=False,
                    error=f"HTTP {e.code}"
                )
            except Exception as e:
                duration_ms = (time.time() - test_start) * 1000
                result = LoadTestResult(
                    timestamp=time.time(),
                    duration_ms=duration_ms,
                    status_code=0,
                    success=False,
                    error=str(e)
                )
            
            with lock:
                results.append(result)
                request_count += 1
        
        if total_requests > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(make_request) for _ in range(total_requests)]
                concurrent.futures.wait(futures)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                while time.time() - start_time < duration:
                    executor.submit(make_request)
                    time.sleep(0.001)
        
        elapsed = time.time() - start_time
        
        return self._compute_metrics(results, elapsed)
    
    def _compute_metrics(
        self,
        results: List[LoadTestResult],
        elapsed: float
    ) -> ActionResult:
        """Compute load test metrics."""
        total = len(results)
        successful = sum(1 for r in results if r.success)
        failed = total - successful
        
        if not results:
            return ActionResult(success=True, message="No results", data={})
        
        latencies = [r.duration_ms for r in results]
        latencies.sort()
        
        error_types: Dict[str, int] = defaultdict(int)
        for r in results:
            if not r.success and r.error:
                error_types[r.error] += 1
        
        return ActionResult(
            success=True,
            message=f"Load test completed: {successful}/{total} successful",
            data={
                'total_requests': total,
                'successful': successful,
                'failed': failed,
                'error_rate': round(failed / total * 100, 2) if total > 0 else 0,
                'requests_per_second': round(total / elapsed, 2),
                'min_latency_ms': round(min(latencies), 2),
                'max_latency_ms': round(max(latencies), 2),
                'avg_latency_ms': round(sum(latencies) / len(latencies), 2),
                'p50_latency_ms': round(latencies[int(len(latencies) * 0.5)], 2),
                'p90_latency_ms': round(latencies[int(len(latencies) * 0.9)], 2),
                'p99_latency_ms': round(latencies[int(len(latencies) * 0.99)], 2),
                'duration_seconds': round(elapsed, 2),
                'error_breakdown': dict(error_types)
            }
        )
