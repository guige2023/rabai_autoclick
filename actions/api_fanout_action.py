"""API Request Fanout and Aggregation.

This module provides request fanout capabilities:
- Parallel multi-endpoint requests
- Result aggregation
- Timeout handling
- Partial failure handling

Example:
    >>> from actions.api_fanout_action import RequestFanout
    >>> fanout = RequestFanout()
    >>> results = await fanout.execute([
    ...     {"url": "/api/users/1", "method": "GET"},
    ...     {"url": "/api/users/2", "method": "GET"},
    ... ])
"""

from __future__ import annotations

import asyncio
import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


@dataclass
class SubRequest:
    """A single sub-request in a fanout."""
    id: str
    url: str
    method: str = "GET"
    headers: Optional[dict[str, str]] = None
    body: Optional[Any] = None
    timeout: float = 30.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubResponse:
    """Response from a sub-request."""
    request_id: str
    status: int
    data: Any
    latency_ms: float
    error: Optional[str] = None
    success: bool = True


@dataclass
class FanoutResult:
    """Result of a fanout operation."""
    total_requests: int
    successful: int
    failed: int
    total_latency_ms: float
    responses: list[SubResponse]
    aggregated_data: Any = None


class RequestFanout:
    """Fanout requests to multiple endpoints and aggregate results."""

    def __init__(
        self,
        max_concurrency: int = 10,
        timeout: float = 60.0,
        aggregation_func: Optional[Callable[[list[SubResponse]], Any]] = None,
    ) -> None:
        """Initialize the request fanout.

        Args:
            max_concurrency: Maximum parallel requests.
            timeout: Overall timeout for fanout.
            aggregation_func: Function to aggregate results.
        """
        self._max_concurrency = max_concurrency
        self._timeout = timeout
        self._aggregation_func = aggregation_func
        self._lock = threading.Lock()
        self._stats = {"fanouts": 0, "requests_sent": 0, "requests_failed": 0}

    def execute(
        self,
        requests: list[SubRequest],
        http_client: Optional[Callable] = None,
    ) -> FanoutResult:
        """Execute fanout requests synchronously.

        Args:
            requests: List of sub-requests.
            http_client: HTTP client callable. If None, uses urllib.

        Returns:
            FanoutResult with all responses.
        """
        start = time.time()
        responses = []

        with ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
            futures = {
                executor.submit(self._execute_request, req, http_client): req
                for req in requests
            }
            for future in futures:
                req = futures[future]
                try:
                    response = future.result(timeout=self._timeout)
                    responses.append(response)
                except Exception as e:
                    logger.error("Fanout request %s failed: %s", req.id, e)
                    responses.append(SubResponse(
                        request_id=req.id,
                        status=0,
                        data=None,
                        latency_ms=0.0,
                        error=str(e),
                        success=False,
                    ))

        with self._lock:
            self._stats["fanouts"] += 1
            self._stats["requests_sent"] += len(requests)
            self._stats["requests_failed"] += sum(1 for r in responses if not r.success)

        total_latency = (time.time() - start) * 1000
        successful = sum(1 for r in responses if r.success)
        failed = len(responses) - successful

        aggregated = None
        if self._aggregation_func:
            try:
                aggregated = self._aggregation_func(responses)
            except Exception as e:
                logger.error("Aggregation failed: %s", e)

        return FanoutResult(
            total_requests=len(requests),
            successful=successful,
            failed=failed,
            total_latency_ms=total_latency,
            responses=responses,
            aggregated_data=aggregated,
        )

    def _execute_request(
        self,
        request: SubRequest,
        http_client: Optional[Callable],
    ) -> SubResponse:
        """Execute a single sub-request."""
        start = time.time()

        if http_client:
            try:
                resp = http_client(
                    method=request.method,
                    url=request.url,
                    headers=request.headers,
                    json=request.body,
                    timeout=request.timeout,
                )
                latency_ms = (time.time() - start) * 1000
                return SubResponse(
                    request_id=request.id,
                    status=resp.status_code,
                    data=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text,
                    latency_ms=latency_ms,
                    success=True,
                )
            except Exception as e:
                return SubResponse(
                    request_id=request.id,
                    status=0,
                    data=None,
                    latency_ms=(time.time() - start) * 1000,
                    error=str(e),
                    success=False,
                )

        import urllib.request
        import json as json_lib

        try:
            data = json_lib.dumps(request.body).encode() if request.body else None
            req = urllib.request.Request(
                request.url,
                data=data,
                headers=request.headers or {},
                method=request.method,
            )
            with urllib.request.urlopen(req, timeout=request.timeout) as resp:
                body = resp.read()
                latency_ms = (time.time() - start) * 1000
                return SubResponse(
                    request_id=request.id,
                    status=resp.status,
                    data=json_lib.loads(body) if body else None,
                    latency_ms=latency_ms,
                    success=True,
                )
        except Exception as e:
            return SubResponse(
                request_id=request.id,
                status=0,
                data=None,
                latency_ms=(time.time() - start) * 1000,
                error=str(e),
                success=False,
            )

    def create_sub_request(
        self,
        id: str,
        url: str,
        method: str = "GET",
        **kwargs,
    ) -> SubRequest:
        """Create a sub-request.

        Args:
            id: Request identifier.
            url: Request URL.
            method: HTTP method.
            **kwargs: Additional SubRequest fields.

        Returns:
            SubRequest object.
        """
        return SubRequest(id=id, url=url, method=method, **kwargs)

    def get_stats(self) -> dict[str, int]:
        """Get fanout statistics."""
        with self._lock:
            return dict(self._stats)


def default_aggregation(responses: list[SubResponse]) -> dict[str, Any]:
    """Default aggregation function.

    Args:
        responses: List of sub-responses.

    Returns:
        Aggregated result dict.
    """
    successful = [r for r in responses if r.success]
    failed = [r for r in responses if not r.success]

    result = {
        "total": len(responses),
        "successful": len(successful),
        "failed": len(failed),
        "avg_latency_ms": sum(r.latency_ms for r in responses) / len(responses) if responses else 0,
    }

    if all(isinstance(r.data, (int, float)) for r in successful):
        result["sum"] = sum(r.data for r in successful)
        result["count"] = len(successful)

    if all(isinstance(r.data, dict) for r in successful):
        combined = {}
        for r in successful:
            if isinstance(r.data, dict):
                combined.update(r.data)
        result["combined"] = combined

    return result
