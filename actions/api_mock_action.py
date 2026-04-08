"""API mock/fake server action module.

Provides mock API response generation for testing.
Supports response templating, latency simulation, and error injection.
"""

from __future__ import annotations

import time
import random
import logging
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

logger = logging.getLogger(__name__)


@dataclass
class MockResponse:
    """A mock HTTP response definition."""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    body: Any = None
    delay_ms: float = 0.0
    error_rate: float = 0.0


@dataclass
class MockEndpoint:
    """A mock API endpoint definition."""
    method: str
    path: str
    response: MockResponse
    description: str = ""


class APIMockAction:
    """Mock API response generator and server.

    Provides fake responses for API testing without network calls.
    Can run as a local mock server for integration testing.

    Example:
        mock = APIMockAction()
        mock.add_endpoint("GET", "/users", body=[{"id": 1, "name": "Alice"}])
        response = mock.request("GET", "/users")
    """

    def __init__(self, default_delay_ms: float = 0.0) -> None:
        """Initialize mock API.

        Args:
            default_delay_ms: Default response delay in milliseconds.
        """
        self.default_delay_ms = default_delay_ms
        self._endpoints: Dict[str, MockEndpoint] = {}
        self._call_log: List[Dict[str, Any]] = []

    def add_endpoint(
        self,
        method: str,
        path: str,
        body: Any = None,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        delay_ms: float = 0.0,
        description: str = "",
    ) -> None:
        """Add a mock endpoint.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: URL path (e.g., '/users/123').
            body: Response body (any JSON-serializable type).
            status_code: HTTP status code.
            headers: Response headers.
            delay_ms: Simulated latency.
            description: Endpoint description.
        """
        key = f"{method.upper()}:{path}"
        self._endpoints[key] = MockEndpoint(
            method=method.upper(),
            path=path,
            response=MockResponse(
                status_code=status_code,
                headers=headers or {"Content-Type": "application/json"},
                body=body,
                delay_ms=delay_ms or self.default_delay_ms,
            ),
            description=description,
        )
        logger.debug("Added mock endpoint: %s %s", method.upper(), path)

    def add_error_endpoint(
        self,
        method: str,
        path: str,
        status_code: int = 500,
        error_message: str = "Internal Server Error",
        error_rate: float = 1.0,
    ) -> None:
        """Add a mock error endpoint.

        Args:
            method: HTTP method.
            path: URL path.
            status_code: Error status code.
            error_message: Error body.
            error_rate: Fraction of calls that return error (0.0-1.0).
        """
        key = f"{method.upper()}:{path}"
        import json
        self._endpoints[key] = MockEndpoint(
            method=method.upper(),
            path=path,
            response=MockResponse(
                status_code=status_code,
                body={"error": error_message},
                error_rate=error_rate,
            ),
            description=f"Error endpoint ({status_code})",
        )

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Any = None,
    ) -> Dict[str, Any]:
        """Simulate an API request.

        Args:
            method: HTTP method.
            path: URL path.
            params: Query parameters.
            data: Request body data.

        Returns:
            Dict with status_code, headers, body, and latency_ms.

        Raises:
            MockNotFoundError: If no matching endpoint is defined.
        """
        key = f"{method.upper()}:{path}"
        endpoint = self._endpoints.get(key)

        if endpoint is None:
            raise MockNotFoundError(f"No mock defined for {method.upper()} {path}")

        response = endpoint.response
        if response.delay_ms > 0:
            time.sleep(response.delay_ms / 1000)

        if response.error_rate > 0 and random.random() < response.error_rate:
            return {
                "status_code": 500,
                "headers": {"Content-Type": "application/json"},
                "body": {"error": "Simulated error"},
                "latency_ms": response.delay_ms,
            }

        log_entry = {
            "method": method.upper(),
            "path": path,
            "params": params,
            "data": data,
            "timestamp": time.time(),
            "status_code": response.status_code,
        }
        self._call_log.append(log_entry)

        return {
            "status_code": response.status_code,
            "headers": response.headers,
            "body": response.body,
            "latency_ms": response.delay_ms,
        }

    def get_call_log(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get log of all mock requests.

        Args:
            limit: Return only last N entries.

        Returns:
            List of call log entries.
        """
        if limit:
            return self._call_log[-limit:]
        return list(self._call_log)

    def clear_call_log(self) -> None:
        """Clear the call log."""
        self._call_log.clear()

    def get_call_count(self, method: Optional[str] = None, path: Optional[str] = None) -> int:
        """Get number of calls matching criteria.

        Args:
            method: Filter by HTTP method.
            path: Filter by URL path.

        Returns:
            Number of matching calls.
        """
        count = 0
        for entry in self._call_log:
            if method and entry["method"] != method.upper():
                continue
            if path and entry["path"] != path:
                continue
            count += 1
        return count

    def remove_endpoint(self, method: str, path: str) -> bool:
        """Remove a mock endpoint.

        Args:
            method: HTTP method.
            path: URL path.

        Returns:
            True if removed, False if not found.
        """
        key = f"{method.upper()}:{path}"
        if key in self._endpoints:
            del self._endpoints[key]
            return True
        return False

    def reset(self) -> None:
        """Reset all endpoints and call log."""
        self._endpoints.clear()
        self._call_log.clear()


class MockNotFoundError(Exception):
    """Raised when no mock is defined for a request."""
    pass


class FakeServer:
    """A simple fake API server using the mock action.

    Runs a local HTTP server that serves mock responses.

    Example:
        fake = FakeServer(port=8888)
        fake.start()
        # Make requests to http://localhost:8888
        fake.stop()
    """

    def __init__(
        self,
        port: int = 8888,
        mock_action: Optional[APIMockAction] = None,
    ) -> None:
        """Initialize fake server.

        Args:
            port: Port to listen on.
            mock_action: APIMockAction instance to use.
        """
        self.port = port
        self.mock = mock_action or APIMockAction()
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the fake server in a background thread."""
        import json
        from urllib.parse import urlparse, parse_qs

        class Handler(BaseHTTPRequestHandler):
            mock = self.mock

            def do_GET(self):
                self._handle_request("GET")

            def do_POST(self):
                self._handle_request("POST")

            def do_PUT(self):
                self._handle_request("PUT")

            def do_DELETE(self):
                self._handle_request("DELETE")

            def _handle_request(self, method: str):
                parsed = urlparse(self.path)
                path = parsed.path
                params = parse_qs(parsed.query)
                try:
                    result = self.mock.request(method, path, params=params)
                    self.send_response(result["status_code"])
                    for k, v in result["headers"].items():
                        self.send_header(k, v)
                    self.end_headers()
                    if result["body"] is not None:
                        self.wfile.write(json.dumps(result["body"]).encode())
                except MockNotFoundError as e:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": str(e)}).encode())

            def log_message(self, format, *args):
                logger.debug(format, *args)

        self._server = HTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("Fake API server started on port %d", self.port)

    def stop(self) -> None:
        """Stop the fake server."""
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            logger.info("Fake API server stopped")
