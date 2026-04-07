"""
Integration testing utilities for service and component testing.

Provides test fixtures, service containers, database sandboxes,
HTTP mocking, message queue testing, and test data factories.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Callable, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ServiceContainer:
    """Represents a running service container for testing."""
    name: str
    host: str
    port: int
    pid: Optional[int] = None
    started_at: float = field(default_factory=time.time)
    health_check_path: str = "/health"
    max_retries: int = 10
    retry_delay: float = 0.5

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def is_healthy(self) -> bool:
        """Check if the service is healthy."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.url}{self.health_check_path}", timeout=2.0)
                return response.status_code == 200
        except Exception:
            return False

    async def wait_until_ready(self) -> bool:
        """Wait for service to be ready."""
        for i in range(self.max_retries):
            if await self.is_healthy():
                return True
            await asyncio.sleep(self.retry_delay)
        return False


class TestDatabaseManager:
    """Manages test database lifecycle (create, populate, verify, teardown)."""

    def __init__(self, dsn: str = "sqlite:///:memory:") -> None:
        self.dsn = dsn
        self._engine: Any = None
        self._session_factory: Any = None

    def setup(self) -> None:
        """Set up the test database."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        self._engine = create_engine(self.dsn)
        self._session_factory = sessionmaker(bind=self._engine)

    def teardown(self) -> None:
        """Tear down the test database."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    @property
    def session(self) -> Any:
        """Get a new database session."""
        if self._session_factory is None:
            raise RuntimeError("Database not set up. Call setup() first.")
        return self._session_factory()


class MockHTTPResponse:
    """Mock HTTP response for testing."""

    def __init__(
        self,
        status_code: int = 200,
        json_data: Optional[dict[str, Any]] = None,
        text: str = "",
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self.headers = headers or {}
        self._is_stream = False

    def json(self) -> dict[str, Any]:
        if self._json_data is None:
            raise ValueError("No JSON data set")
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=httpx.Request("GET", "http://test"),
                response=self,
            )


class MockHTTPClient:
    """Mock HTTP client for intercepting requests in tests."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., MockHTTPResponse]] = {}
        self._call_history: list[tuple[str, str, dict]] = []

    def register(self, method: str, url_pattern: str, handler: Callable[..., MockHTTPResponse]) -> None:
        """Register a mock handler for a URL pattern."""
        self._handlers[f"{method.upper()}:{url_pattern}"] = handler

    def get(self, url: str, **kwargs: Any) -> MockHTTPResponse:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> MockHTTPResponse:
        return self._request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> MockHTTPResponse:
        return self._request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> MockHTTPResponse:
        return self._request("DELETE", url, **kwargs)

    def _request(self, method: str, url: str, **kwargs: Any) -> MockHTTPResponse:
        self._call_history.append((method, url, kwargs))
        handler_key = f"{method.upper()}:{url}"
        for pattern, h in self._handlers.items():
            method_part, url_part = pattern.split(":", 1)
            if method_part == method.upper() and self._match(url, url_part):
                return h(**kwargs)
        return MockHTTPResponse(status_code=404, text="No handler registered")

    def _match(self, url: str, pattern: str) -> bool:
        if pattern.endswith("*"):
            return url.startswith(pattern[:-1])
        return url == pattern

    def assert_called(self, method: str, url: str) -> bool:
        return any(m == method and u == url for m, u, _ in self._call_history)

    def call_count(self) -> int:
        return len(self._call_history)


@dataclass
class TestFixture:
    """Base test fixture with setup/teardown hooks."""
    name: str
    setup_fn: Optional[Callable[..., Any]] = None
    teardown_fn: Optional[Callable[..., Any]] = None

    def setup(self) -> Any:
        if self.setup_fn:
            return self.setup_fn()

    def teardown(self) -> None:
        if self.teardown_fn:
            self.teardown_fn()


class TestDataFactory:
    """Factory for generating test data with customizable constraints."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self._counters: dict[str, int] = {}
        self._seed = seed

    def counter(self, name: str) -> int:
        self._counters[name] = self._counters.get(name, 0) + 1
        return self._counters[name]

    def user(self, **overrides: Any) -> dict[str, Any]:
        idx = self.counter("user")
        defaults = {
            "id": idx,
            "username": f"user_{idx}",
            "email": f"user_{idx}@test.local",
            "is_active": True,
            "created_at": "2024-01-01T00:00:00Z",
        }
        defaults.update(overrides)
        return defaults

    def post(self, user_id: Optional[int] = None, **overrides: Any) -> dict[str, Any]:
        idx = self.counter("post")
        defaults = {
            "id": idx,
            "title": f"Test Post {idx}",
            "content": f"This is test content for post {idx}",
            "user_id": user_id or self.counter("user"),
            "published": False,
        }
        defaults.update(overrides)
        return defaults

    def comment(self, post_id: Optional[int] = None, **overrides: Any) -> dict[str, Any]:
        idx = self.counter("comment")
        defaults = {
            "id": idx,
            "body": f"This is test comment {idx}",
            "post_id": post_id or self.counter("post"),
            "author_id": self.counter("user"),
        }
        defaults.update(overrides)
        return defaults


@asynccontextmanager
async def mock_service_port(
    port: int,
    handler: Callable[..., Any],
) -> AsyncGenerator[ServiceContainer, None]:
    """Start a mock HTTP service on a specific port."""
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        sock.close()
        raise RuntimeError(f"Port {port} is already in use")

    sock.close()
    container = ServiceContainer(name="mock", host="127.0.0.1", port=port)
    yield container
