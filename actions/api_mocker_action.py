"""
API Mocker Action Module.

Creates mock API responses for testing with configurable
 delay, status codes, and response patterns.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MockResponseType(Enum):
    """Type of mock response."""
    FIXED = "fixed"
    RANDOM = "random"
    SEQUENTIAL = "sequential"
    CONDITIONAL = "conditional"
    ERROR = "error"


@dataclass
class MockResponse:
    """A mock API response definition."""
    status_code: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    delay_ms: int = 0
    response_type: MockResponseType = MockResponseType.FIXED


@dataclass
class MockEndpoint:
    """A mock API endpoint."""
    path: str
    method: str
    response: MockResponse
    conditions: Optional[Callable[[dict[str, Any]], bool]] = None


class APIMockerAction:
    """
    Mock API server for testing.

    Creates mock responses for API endpoints with support for
    delays, random responses, and conditional matching.

    Example:
        mocker = APIMockerAction()
        mocker.mock("GET", "/users/{id}", status_code=200, body={"name": "John"})
        mocker.mock("POST", "/users", status_code=201, delay_ms=100)
        response = await mocker.handle_request("GET", "/users/123")
    """

    def __init__(self) -> None:
        self._endpoints: dict[tuple[str, str], list[MockEndpoint]] = {}
        self._call_counts: dict[str, int] = {}

    def mock(
        self,
        method: str,
        path: str,
        status_code: int = 200,
        body: Any = None,
        headers: Optional[dict[str, str]] = None,
        delay_ms: int = 0,
        response_type: MockResponseType = MockResponseType.FIXED,
    ) -> "APIMockerAction":
        """Register a mock endpoint."""
        endpoint = MockEndpoint(
            path=path,
            method=method.upper(),
            response=MockResponse(
                status_code=status_code,
                body=body,
                headers=headers or {},
                delay_ms=delay_ms,
                response_type=response_type,
            ),
        )

        key = (method.upper(), path)
        if key not in self._endpoints:
            self._endpoints[key] = []
        self._endpoints[key].append(endpoint)

        return self

    def mock_error(
        self,
        method: str,
        path: str,
        status_code: int = 500,
        error_message: str = "Internal Server Error",
    ) -> "APIMockerAction":
        """Register a mock error response."""
        return self.mock(
            method=method,
            path=path,
            status_code=status_code,
            body={"error": error_message},
        )

    def mock_sequential(
        self,
        method: str,
        path: str,
        responses: list[tuple[int, Any]],
    ) -> "APIMockerAction":
        """Register sequential mock responses."""
        for idx, (status_code, body) in enumerate(responses):
            endpoint = MockEndpoint(
                path=path,
                method=method.upper(),
                response=MockResponse(
                    status_code=status_code,
                    body=body,
                    response_type=MockResponseType.SEQUENTIAL,
                ),
            )
            key = (method.upper(), path)
            if key not in self._endpoints:
                self._endpoints[key] = []
            self._endpoints[key].append(endpoint)

        return self

    async def handle_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        body: Any = None,
    ) -> tuple[int, Any, dict[str, str]]:
        """Handle a mock API request."""
        await asyncio.sleep(0.001)

        key = (method.upper(), path)
        endpoints = self._endpoints.get(key, [])

        if not endpoints:
            return 404, {"error": "Not Found"}, {"Content-Type": "application/json"}

        call_key = f"{method.upper()}:{path}"
        self._call_counts[call_key] = self._call_counts.get(call_key, 0) + 1

        endpoint = self._select_endpoint(endpoints, params, body)

        if endpoint.response.delay_ms > 0:
            await asyncio.sleep(endpoint.response.delay_ms / 1000)

        return (
            endpoint.response.status_code,
            endpoint.response.body,
            endpoint.response.headers,
        )

    def _select_endpoint(
        self,
        endpoints: list[MockEndpoint],
        params: Optional[dict[str, Any]],
        body: Any,
    ) -> MockEndpoint:
        """Select the appropriate endpoint based on conditions."""
        for endpoint in endpoints:
            if endpoint.conditions:
                try:
                    context = {"params": params, "body": body}
                    if endpoint.conditions(context):
                        return endpoint
                except Exception:
                    continue
            elif endpoint.response.response_type == MockResponseType.SEQUENTIAL:
                return endpoint

        if len(endpoints) == 1:
            return endpoints[0]

        for endpoint in endpoints:
            if endpoint.response.response_type == MockResponseType.FIXED:
                return endpoint

        return random.choice(endpoints)

    def get_call_count(
        self,
        method: Optional[str] = None,
        path: Optional[str] = None,
    ) -> int:
        """Get the number of times an endpoint was called."""
        if method and path:
            return self._call_counts.get(f"{method.upper()}:{path}", 0)

        if method:
            return sum(
                count for key, count in self._call_counts.items()
                if key.startswith(method.upper() + ":")
            )

        return sum(self._call_counts.values())

    def reset_counts(self) -> None:
        """Reset call counters."""
        self._call_counts.clear()

    def clear(self) -> None:
        """Clear all mock endpoints."""
        self._endpoints.clear()
        self._call_counts.clear()
