"""API Load Testing Action Module. Performs load testing on APIs."""
import sys, os, time, threading, statistics
from typing import Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class LoadTestResult:
    total_requests: int = 0; successful_requests: int = 0; failed_requests: int = 0
    error_rate: float = 0.0; avg_latency_ms: float = 0.0; min_latency_ms: float = 0.0
    max_latency_ms: float = 0.0; p50_latency_ms: float = 0.0; p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0; requests_per_second: float = 0.0; duration_seconds: float = 0.0
    errors: list = field(default_factory=list)

class APILoadTestingAction(BaseAction):
    action_type = "api_load_testing"; display_name = "API负载测试"
    description = "对API执行负载测试"
    def __init__(self) -> None: super().__init__(); self._lock = threading.Lock()
    def execute(self, context: Any, params: dict) -> ActionResult:
        url = params.get("url")
        if not url: return ActionResult(success=False, message="URL is required")
        method = params.get("method", "GET").upper()
        headers = params.get("headers", {})
        body = params.get("body")
        concurrency = params.get("concurrency", 10)
        total_requests = params.get("total_requests", 100)
        ramp_up = params.get("ramp_up_seconds", 5)
        timeout = params.get("timeout", 30)
        if ramp_up <= 0: ramp_up = 1
        start_time = time.time(); latencies = []; errors = []; successful = 0; failed = 0
        lock = threading.Lock()
        def make_request(request_id: int):
            req_start = time.time()
            try:
                import urllib.request, urllib.error
                req_body = body.encode() if body else None
                req = urllib.request.Request(url, data=req_body, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    _ = response.read()
                    latency = (time.time() - req_start) * 1000
                    return True, latency, ""
            except Exception as e:
                latency = (time.time() - req_start) * 1000
                return False, latency, str(e)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            delay_per_req = ramp_up / max(total_requests / concurrency, 1)
            for batch in range(0, total_requests, concurrency):
                batch_size = min(concurrency, total_requests - batch)
                for i in range(batch_size):
                    futures.append(executor.submit(make_request, batch + i))
                    if delay_per_req > 0.01: time.sleep(delay_per_req)
                if batch + batch_size >= total_requests: break
            for future in as_completed(futures):
                success, latency, error = future.result()
                with lock:
                    if success: successful += 1; latencies.append(latency)
                    else: failed += 1; errors.append(error)
        total_time = time.time() - start_time
        total_req = successful + failed
        if latencies:
            latencies.sort()
            result = LoadTestResult(
                total_requests=total_req, successful_requests=successful, failed_requests=failed,
                error_rate=failed/total_req if total_req > 0 else 0.0,
                avg_latency_ms=statistics.mean(latencies), min_latency_ms=min(latencies),
                max_latency_ms=max(latencies),
                p50_latency_ms=latencies[int(len(latencies)*0.50)],
                p95_latency_ms=latencies[int(len(latencies)*0.95)] if len(latencies)>20 else latencies[-1],
                p99_latency_ms=latencies[int(len(latencies)*0.99)] if len(latencies)>100 else latencies[-1],
                requests_per_second=total_req/total_time if total_time > 0 else 0.0,
                duration_seconds=total_time, errors=errors[:10]
            )
            return ActionResult(success=True, message=f"Load test: {successful}/{total_req} OK", data=result)
        return ActionResult(success=False, message=f"All {failed} requests failed", data={"errors": errors[:10]})
