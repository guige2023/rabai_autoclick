"""
API Composer Action Module.

Composes multiple API calls into a single orchestrated request with
parallel execution, sequential chains, and result aggregation.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class APIEndpoint:
    """A single API endpoint to call."""
    name: str
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    depends_on: list[str] = field(default_factory=list)


@dataclass
class ComposerResult:
    """Result of API composition."""
    results: dict[str, dict[str, Any]]
    total_time_ms: float
    failed: list[str]
    success: list[str]


class APIComposerAction(BaseAction):
    """Compose and execute multiple API calls."""

    def __init__(self) -> None:
        super().__init__("api_composer")

    def execute(self, context: dict, params: dict) -> ComposerResult:
        """
        Execute composed API calls.

        Args:
            context: Execution context (may contain auth)
            params: Parameters:
                - endpoints: List of APIEndpoint configs
                - mode: parallel, sequential, or dependency (default: parallel)
                - timeout: Per-request timeout in seconds

        Returns:
            ComposerResult with all results and timing
        """
        import time
        import urllib.request
        import urllib.parse
        import urllib.error

        endpoints_config = params.get("endpoints", [])
        mode = params.get("mode", "parallel")
        timeout = params.get("timeout", 30)

        endpoints = []
        for ep in endpoints_config:
            if isinstance(ep, dict):
                endpoints.append(APIEndpoint(
                    name=ep.get("name", ""),
                    url=ep.get("url", ""),
                    method=ep.get("method", "GET"),
                    headers=ep.get("headers", {}),
                    body=ep.get("body"),
                    depends_on=ep.get("depends_on", [])
                ))
            else:
                endpoints.append(ep)

        results: dict[str, dict[str, Any]] = {}
        failed: list[str] = []
        success: list[str] = []

        start_time = time.time()

        if mode == "parallel":
            results, failed, success = self._execute_parallel(endpoints, timeout)
        elif mode == "sequential":
            results, failed, success = self._execute_sequential(endpoints, timeout)
        elif mode == "dependency":
            results, failed, success = self._execute_dependency(endpoints, timeout)
        else:
            results, failed, success = self._execute_parallel(endpoints, timeout)

        total_time_ms = (time.time() - start_time) * 1000
        return ComposerResult(results, total_time_ms, failed, success)

    def _execute_single(self, endpoint: APIEndpoint, timeout: int) -> dict[str, Any]:
        """Execute a single API call."""
        import time
        import json
        import urllib.request
        import urllib.error

        start_time = time.time()
        try:
            req = urllib.request.Request(endpoint.url, method=endpoint.method, headers=endpoint.headers)
            if endpoint.body is not None:
                body = endpoint.body
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode("utf-8")
                elif isinstance(body, str):
                    body = body.encode("utf-8")
                req.data = body
                req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = response.read()
                content_type = response.headers.get("Content-Type", "")
                result_body = body.decode("utf-8")
                if "application/json" in content_type:
                    result_body = json.loads(result_body)
                elapsed_ms = (time.time() - start_time) * 1000
                return {
                    "status_code": response.status,
                    "body": result_body,
                    "elapsed_ms": elapsed_ms,
                    "error": None
                }
        except urllib.error.HTTPError as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return {"status_code": e.code, "body": None, "elapsed_ms": elapsed_ms, "error": f"HTTP {e.code}"}
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return {"status_code": 0, "body": None, "elapsed_ms": elapsed_ms, "error": str(e)}

    def _execute_parallel(self, endpoints: list[APIEndpoint], timeout: int):
        """Execute endpoints in parallel."""
        import concurrent.futures
        results = {}
        failed = []
        success = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_ep = {executor.submit(self._execute_single, ep, timeout): ep for ep in endpoints}
            for future in concurrent.futures.as_completed(future_to_ep):
                ep = future_to_ep[future]
                try:
                    result = future.result()
                    results[ep.name] = result
                    if result["error"]:
                        failed.append(ep.name)
                    else:
                        success.append(ep.name)
                except Exception:
                    results[ep.name] = {"status_code": 0, "body": None, "elapsed_ms": 0, "error": "Execution error"}
                    failed.append(ep.name)
        return results, failed, success

    def _execute_sequential(self, endpoints: list[APIEndpoint], timeout: int):
        """Execute endpoints sequentially."""
        results = {}
        failed = []
        success = []
        for ep in endpoints:
            result = self._execute_single(ep, timeout)
            results[ep.name] = result
            if result["error"]:
                failed.append(ep.name)
            else:
                success.append(ep.name)
        return results, failed, success

    def _execute_dependency(self, endpoints: list[APIEndpoint], timeout: int):
        """Execute endpoints respecting dependencies."""
        results = {}
        failed = []
        success = []
        executed = set()

        while len(executed) < len(endpoints):
            made_progress = False
            for ep in endpoints:
                if ep.name in executed:
                    continue
                if all(dep in executed for dep in ep.depends_on):
                    result = self._execute_single(ep, timeout)
                    results[ep.name] = result
                    executed.add(ep.name)
                    made_progress = True
                    if result["error"]:
                        failed.append(ep.name)
                    else:
                        success.append(ep.name)

            if not made_progress:
                remaining = [ep.name for ep in endpoints if ep.name not in executed]
                for name in remaining:
                    results[name] = {"status_code": 0, "body": None, "elapsed_ms": 0, "error": "Dependency resolution failed"}
                    failed.append(name)
                    executed.add(name)
                break

        return results, failed, success
