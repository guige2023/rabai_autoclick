"""
HTTP mock utilities for testing automation workflows.

Provides HTTP request/response mocking, fake servers, and interceptors
for testing UI automation without real network dependencies.

Example:
    >>> from http_mock_utils import MockServer, MockResponse, interceptor
    >>> server = MockServer()
    >>> server.add_route("GET", "/api/test", {"data": "mocked"})
    >>> server.start()
"""

from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


# =============================================================================
# Types
# =============================================================================


class HTTPMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class MockRequest:
    """Represents a mocked HTTP request."""
    method: HTTPMethod
    path: str
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[Union[dict, str, bytes]] = None


@dataclass
class MockResponse:
    """Represents a mocked HTTP response."""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Union[dict, str, bytes]] = None
    delay_ms: int = 0

    def __post_init__(self):
        if isinstance(self.body, dict):
            self.body = json.dumps(self.body)
            if "Content-Type" not in self.headers:
                self.headers["Content-Type"] = "application/json"


# =============================================================================
# Route Matching
# =============================================================================


@dataclass
class Route:
    """A registered route for mock matching."""
    method: HTTPMethod
    path_pattern: str
    response: MockResponse
    handler: Optional[Callable[[MockRequest], MockResponse]] = None
    match_count: int = 0

    def matches(self, method: str, path: str) -> bool:
        """Check if this route matches the request."""
        if self.method.value != method.upper():
            return False

        # Simple path matching with {param} support
        pattern_parts = self.path_pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return False

        for pp, rp in zip(pattern_parts, path_parts):
            if pp.startswith("{") and pp.endswith("}"):
                continue
            if pp != rp:
                return False

        return True

    def extract_params(self, path: str) -> Dict[str, str]:
        """Extract path parameters from the path."""
        params: Dict[str, str] = {}
        pattern_parts = self.path_pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        for pp, rp in zip(pattern_parts, path_parts):
            if pp.startswith("{") and pp.endswith("}"):
                key = pp[1:-1]
                params[key] = rp

        return params


# =============================================================================
# Mock Server
# =============================================================================


class MockServer:
    """
    A mock HTTP server for testing.

    Example:
        >>> server = MockServer()
        >>> server.add_route("GET", "/api/users", [{"id": 1}])
        >>> server.add_route("POST", "/api/users", {"id": 2}, status=201)
        >>> server.start(port=8888)
        >>> # make requests to http://localhost:8888/api/users
        >>> server.stop()
    """

    def __init__(self):
        self._routes: List[Route] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._port: int = 8080
        self._requests: List[Tuple[MockRequest, MockResponse]] = []
        self._lock = threading.Lock()

    def add_route(
        self,
        method: str,
        path: str,
        body: Any = None,
        status: int = 200,
        headers: Optional[Dict[str, str]] = None,
        delay_ms: int = 0,
        handler: Optional[Callable[[MockRequest], MockResponse]] = None,
    ) -> None:
        """
        Add a route to the mock server.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: URL path (e.g., /api/users/{id}).
            body: Response body.
            status: HTTP status code.
            headers: Response headers.
            delay_ms: Response delay in milliseconds.
            handler: Custom handler function.
        """
        response = MockResponse(
            status_code=status,
            headers=headers or {},
            body=body,
            delay_ms=delay_ms,
        )

        route = Route(
            method=HTTPMethod(method.upper()),
            path_pattern=path,
            response=response,
            handler=handler,
        )

        self._routes.append(route)

    def get(self, path: str, body: Any = None, **kwargs: Any) -> None:
        """Shortcut for adding a GET route."""
        self.add_route("GET", path, body, **kwargs)

    def post(self, path: str, body: Any = None, **kwargs: Any) -> None:
        """Shortcut for adding a POST route."""
        self.add_route("POST", path, body, **kwargs)

    def put(self, path: str, body: Any = None, **kwargs: Any) -> None:
        """Shortcut for adding a PUT route."""
        self.add_route("PUT", path, body, **kwargs)

    def delete(self, path: str, body: Any = None, **kwargs: Any) -> None:
        """Shortcut for adding a DELETE route."""
        self.add_route("DELETE", path, body, **kwargs)

    def start(self, port: int = 8080) -> None:
        """Start the mock server."""
        self._port = port
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the mock server."""
        self._running = False

    def get_requests(self) -> List[Tuple[MockRequest, MockResponse]]:
        """Get all received requests and their responses."""
        with self._lock:
            return list(self._requests)

    def clear_requests(self) -> None:
        """Clear recorded requests."""
        with self._lock:
            self._requests.clear()

    def _serve(self) -> None:
        """Internal serve loop (simplified - use a real HTTP library in production)."""
        # This is a simplified placeholder - real implementation would use http.server
        # or a library like Flask/Bottle for the mock server
        time.sleep(0.1)

    def handle_request(self, method: str, path: str, headers: Dict, body: Any) -> MockResponse:
        """
        Handle an incoming request and return a mock response.

        This can be called directly for testing without network.

        Args:
            method: HTTP method.
            path: URL path.
            headers: Request headers.
            body: Request body.

        Returns:
            MockResponse to send.
        """
        request = MockRequest(
            method=HTTPMethod(method.upper()),
            path=path,
            headers=dict(headers),
            body=body,
        )

        # Find matching route
        for route in self._routes:
            if route.matches(method, path):
                route.match_count += 1

                if route.handler:
                    params = route.extract_params(path)
                    for k, v in params.items():
                        request.query_params[k] = v
                    response = route.handler(request)
                else:
                    response = route.response

                # Apply delay
                if response.delay_ms > 0:
                    time.sleep(response.delay_ms / 1000)

                with self._lock:
                    self._requests.append((request, response))

                return response

        # No match - return 404
        return MockResponse(
            status_code=404,
            body={"error": "Not found", "path": path},
        )


# =============================================================================
# Request Interceptor
# =============================================================================


@dataclass
class InterceptorRule:
    """A rule for intercepting and mocking requests."""
    url_pattern: str  # regex pattern
    method: Optional[HTTPMethod] = None
    response: Optional[MockResponse] = None
    handler: Optional[Callable[[MockRequest], MockResponse]] = None
    enabled: bool = True


class RequestInterceptor:
    """
    Intercepts HTTP requests and returns mock responses.

    Can be used as a proxy or monkey-patch layer.

    Example:
        >>> interceptor = RequestInterceptor()
        >>> interceptor.add_rule(r"api\\.example\\.com/users", body=[{"id": 1}])
        >>> interceptor.enable()
    """

    def __init__(self):
        self._rules: List[InterceptorRule] = []
        self._enabled = False
        self._original_send = None
        self._lock = threading.Lock()

    def add_rule(
        self,
        url_pattern: str,
        method: Optional[str] = None,
        body: Any = None,
        status: int = 200,
        headers: Optional[Dict[str, str]] = None,
        handler: Optional[Callable[[MockRequest], MockResponse]] = None,
    ) -> None:
        """Add an interception rule."""
        rule = InterceptorRule(
            url_pattern=url_pattern,
            method=HTTPMethod(method.upper()) if method else None,
            response=MockResponse(
                status_code=status,
                headers=headers or {},
                body=body,
            ) if body is not None or headers else None,
            handler=handler,
        )
        self._rules.append(rule)

    def enable(self) -> None:
        """Enable request interception."""
        self._enabled = True

    def disable(self) -> None:
        """Disable request interception."""
        self._enabled = False

    def clear_rules(self) -> None:
        """Clear all interception rules."""
        with self._lock:
            self._rules.clear()

    def should_intercept(self, url: str, method: str) -> bool:
        """Check if a request should be intercepted."""
        if not self._enabled:
            return False

        for rule in self._rules:
            if not rule.enabled:
                continue
            if rule.method and rule.method.value != method.upper():
                continue
            if re.search(rule.url_pattern, url):
                return True

        return False

    def get_response(self, url: str, method: str) -> Optional[MockResponse]:
        """Get mock response for a request."""
        for rule in self._rules:
            if not rule.enabled:
                continue
            if rule.method and rule.method.value != method.upper():
                continue
            if re.search(rule.url_pattern, url):
                if rule.handler:
                    return rule.handler(MockRequest(
                        method=HTTPMethod(method.upper()),
                        path=url,
                    ))
                return rule.response

        return None


# =============================================================================
# Mock Response Builder
# =============================================================================


class MockResponseBuilder:
    """Builder for constructing mock responses."""

    def __init__(self):
        self._status = 200
        self._headers: Dict[str, str] = {}
        self._body: Any = None
        self._delay_ms: int = 0

    def status(self, code: int) -> "MockResponseBuilder":
        """Set status code."""
        self._status = code
        return self

    def header(self, key: str, value: str) -> "MockResponseBuilder":
        """Add a header."""
        self._headers[key] = value
        return self

    def json(self, data: Any) -> "MockResponseBuilder":
        """Set JSON body."""
        self._body = json.dumps(data)
        self._headers["Content-Type"] = "application/json"
        return self

    def text(self, data: str) -> "MockResponseBuilder":
        """Set text body."""
        self._body = data
        self._headers["Content-Type"] = "text/plain"
        return self

    def html(self, data: str) -> "MockResponseBuilder":
        """Set HTML body."""
        self._body = data
        self._headers["Content-Type"] = "text/html"
        return self

    def delay(self, ms: int) -> "MockResponseBuilder":
        """Set response delay in milliseconds."""
        self._delay_ms = ms
        return self

    def build(self) -> MockResponse:
        """Build the MockResponse."""
        return MockResponse(
            status_code=self._status,
            headers=self._headers,
            body=self._body,
            delay_ms=self._delay_ms,
        )


# =============================================================================
# Fake HTTP Client
# =============================================================================


class FakeHTTPClient:
    """
    A fake HTTP client that returns mock responses.

    Can be used as a drop-in replacement for requests/similar libraries.

    Example:
        >>> client = FakeHTTPClient()
        >>> client.mock("GET", "https://api.example.com/users", [{"id": 1}])
        >>> response = client.get("https://api.example.com/users")
    """

    def __init__(self):
        self._interceptor = RequestInterceptor()
        self._last_request: Optional[MockRequest] = None

    def mock(
        self,
        method: str,
        url: str,
        body: Any = None,
        status: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Add a mock response for a URL pattern."""
        self._interceptor.add_rule(
            url_pattern=re.escape(url),
            method=method,
            body=body,
            status=status,
            headers=headers,
        )

    def get(self, url: str, **kwargs: Any) -> "FakeResponse":
        """Perform a GET request."""
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> "FakeResponse":
        """Perform a POST request."""
        return self._request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> "FakeResponse":
        """Perform a PUT request."""
        return self._request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> "FakeResponse":
        """Perform a DELETE request."""
        return self._request("DELETE", url, **kwargs)

    def _request(self, method: str, url: str, **kwargs: Any) -> "FakeResponse":
        """Internal request handler."""
        headers = kwargs.get("headers", {})
        json_data = kwargs.get("json")
        body = kwargs.get("data")

        self._last_request = MockRequest(
            method=HTTPMethod(method.upper()),
            path=url,
            headers=headers,
            body=json_data or body,
        )

        if self._interceptor.should_intercept(url, method):
            response = self._interceptor.get_response(url, method)
            if response:
                return FakeResponse(response)
            return FakeResponse(MockResponse(status_code=404))

        return FakeResponse(MockResponse(status_code=500, body={"error": "Not mocked"}))


class FakeResponse:
    """A fake HTTP response object."""

    def __init__(self, mock_response: MockResponse):
        self.status_code = mock_response.status_code
        self.headers = mock_response.headers
        self._body = mock_response.body
        self.ok = 200 <= mock_response.status_code < 300

    @property
    def text(self) -> str:
        """Get response as text."""
        if isinstance(self._body, bytes):
            return self._body.decode("utf-8")
        return str(self._body) if self._body else ""

    @property
    def content(self) -> bytes:
        """Get response as bytes."""
        if isinstance(self._body, str):
            return self._body.encode("utf-8")
        return self._body or b""

    def json(self) -> Any:
        """Parse response as JSON."""
        if isinstance(self._body, str):
            return json.loads(self._body)
        return self._body

    def raise_for_status(self) -> None:
        """Raise an exception for bad status codes."""
        if not self.ok:
            raise Exception(f"HTTP {self.status_code}")
