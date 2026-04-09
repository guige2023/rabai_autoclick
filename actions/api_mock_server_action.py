"""API mock server action for testing and development.

Creates mock API endpoints with configurable responses,
delay simulation, and error handling for testing.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class MockResponseType(Enum):
    """Types of mock responses."""
    STATIC = "static"
    DYNAMIC = "dynamic"
    RANDOM = "random"
    SEQUENTIAL = "sequential"
    ERROR = "error"


@dataclass
class MockEndpoint:
    """Definition of a mock endpoint."""
    path: str
    method: str
    response_type: MockResponseType
    response_data: Any
    status_code: int = 200
    delay_ms: int = 0
    probability: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MockRequest:
    """A received mock request."""
    path: str
    method: str
    headers: dict[str, str]
    body: Optional[Any]
    timestamp: float


@dataclass
class MockStats:
    """Statistics for mock server."""
    requests_received: int = 0
    requests_matched: int = 0
    requests_not_found: int = 0
    responses_sent: int = 0


class APIMockServerAction:
    """Create and manage mock API endpoints.

    Example:
        >>> mock = APIMockServerAction()
        >>> mock.add_endpoint("/api/users", "GET", response_data=[{"id": 1}])
        >>> mock.add_endpoint("/api/users", "POST", response_type=MockResponseType.STATIC)
        >>> await mock.start(port=8080)
    """

    def __init__(self) -> None:
        self._endpoints: list[MockEndpoint] = []
        self._request_history: list[MockRequest] = []
        self._stats = MockStats()
        self._running = False
        self._server: Optional[Any] = None

    def add_endpoint(
        self,
        path: str,
        method: str,
        response_data: Any,
        response_type: MockResponseType = MockResponseType.STATIC,
        status_code: int = 200,
        delay_ms: int = 0,
        probability: float = 1.0,
    ) -> "APIMockServerAction":
        """Add a mock endpoint.

        Args:
            path: URL path for the endpoint.
            method: HTTP method (GET, POST, etc.).
            response_data: Response data to return.
            response_type: How to select response.
            status_code: HTTP status code.
            delay_ms: Simulated response delay.
            probability: Probability of this endpoint matching.

        Returns:
            Self for method chaining.
        """
        endpoint = MockEndpoint(
            path=path,
            method=method.upper(),
            response_type=response_type,
            response_data=response_data,
            status_code=status_code,
            delay_ms=delay_ms,
            probability=probability,
        )
        self._endpoints.append(endpoint)
        return self

    def add_error_endpoint(
        self,
        path: str,
        method: str,
        error_message: str,
        status_code: int = 500,
    ) -> "APIMockServerAction":
        """Add an endpoint that always returns an error.

        Args:
            path: URL path for the endpoint.
            method: HTTP method.
            error_message: Error message to return.
            status_code: HTTP status code for error.

        Returns:
            Self for method chaining.
        """
        return self.add_endpoint(
            path=path,
            method=method,
            response_data={"error": error_message},
            response_type=MockResponseType.ERROR,
            status_code=status_code,
        )

    def remove_endpoint(self, path: str, method: str) -> bool:
        """Remove an endpoint.

        Args:
            path: URL path.
            method: HTTP method.

        Returns:
            True if endpoint was found and removed.
        """
        for i, ep in enumerate(self._endpoints):
            if ep.path == path and ep.method == method.upper():
                del self._endpoints[i]
                return True
        return False

    async def handle_request(
        self,
        path: str,
        method: str,
        headers: Optional[dict[str, str]] = None,
        body: Optional[Any] = None,
    ) -> tuple[int, dict[str, str], Any]:
        """Handle an incoming mock request.

        Args:
            path: Request path.
            method: HTTP method.
            headers: Request headers.
            body: Request body.

        Returns:
            Tuple of (status_code, headers, body).
        """
        self._stats.requests_received += 1

        request = MockRequest(
            path=path,
            method=method.upper(),
            headers=headers or {},
            body=body,
            timestamp=time.time(),
        )
        self._request_history.append(request)

        endpoint = self._match_endpoint(path, method)
        if not endpoint:
            self._stats.requests_not_found += 1
            return 404, {"Content-Type": "application/json"}, {"error": "Not Found"}

        self._stats.requests_matched += 1

        if endpoint.delay_ms > 0:
            await asyncio.sleep(endpoint.delay_ms / 1000.0)

        status, headers_out, response = self._get_response(endpoint)
        self._stats.responses_sent += 1

        return status, headers_out, response

    def _match_endpoint(self, path: str, method: str) -> Optional[MockEndpoint]:
        """Find matching endpoint for request.

        Args:
            path: Request path.
            method: HTTP method.

        Returns:
            Matching endpoint or None.
        """
        matching: list[MockEndpoint] = []

        for ep in self._endpoints:
            if ep.method != method.upper():
                continue
            if self._path_matches(ep.path, path):
                matching.append(ep)

        if not matching:
            return None

        for ep in matching:
            if ep.probability >= 1.0:
                return ep

        import random
        for ep in matching:
            if random.random() < ep.probability:
                return ep

        return matching[0]

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Check if path matches pattern.

        Args:
            pattern: Endpoint path pattern.
            path: Actual request path.

        Returns:
            True if matches.
        """
        if pattern == path:
            return True

        if "{" in pattern:
            import re
            regex = pattern.replace("{id}", r"[^/]+")
            regex = f"^{regex}$"
            return bool(re.match(regex, path))

        return False

    def _get_response(
        self,
        endpoint: MockEndpoint,
    ) -> tuple[int, dict[str, str], Any]:
        """Get response for endpoint.

        Args:
            endpoint: Matched endpoint.

        Returns:
            Tuple of (status, headers, body).
        """
        headers = {"Content-Type": "application/json"}

        if endpoint.response_type == MockResponseType.STATIC:
            return endpoint.status_code, headers, endpoint.response_data

        if endpoint.response_type == MockResponseType.ERROR:
            return endpoint.status_code, headers, endpoint.response_data

        if endpoint.response_type == MockResponseType.DYNAMIC:
            if callable(endpoint.response_data):
                return endpoint.status_code, headers, endpoint.response_data()
            return endpoint.status_code, headers, endpoint.response_data

        if endpoint.response_type == MockResponseType.RANDOM:
            import random
            if isinstance(endpoint.response_data, list):
                return (
                    endpoint.status_code,
                    headers,
                    random.choice(endpoint.response_data),
                )
            return endpoint.status_code, headers, endpoint.response_data

        if endpoint.response_type == MockResponseType.SEQUENTIAL:
            if isinstance(endpoint.response_data, list):
                idx = self._stats.responses_sent % len(endpoint.response_data)
                return endpoint.status_code, headers, endpoint.response_data[idx]
            return endpoint.status_code, headers, endpoint.response_data

        return endpoint.status_code, headers, endpoint.response_data

    def get_request_history(
        self,
        path: Optional[str] = None,
        method: Optional[str] = None,
        limit: int = 100,
    ) -> list[MockRequest]:
        """Get request history.

        Args:
            path: Optional filter by path.
            method: Optional filter by method.
            limit: Maximum requests to return.

        Returns:
            List of historical requests.
        """
        history = self._request_history

        if path:
            history = [r for r in history if r.path == path]
        if method:
            history = [r for r in history if r.method == method.upper()]

        return history[-limit:]

    def get_stats(self) -> MockStats:
        """Get mock server statistics.

        Returns:
            Current statistics.
        """
        return self._stats

    def clear_history(self) -> None:
        """Clear request history."""
        self._request_history.clear()

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = MockStats()

    def get_endpoints(self) -> list[MockEndpoint]:
        """Get all configured endpoints.

        Returns:
            List of endpoints.
        """
        return self._endpoints.copy()
