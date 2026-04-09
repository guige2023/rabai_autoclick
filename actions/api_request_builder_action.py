"""API request builder and middleware chain action."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.parse import urlencode, urljoin


RequestMiddleware = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass
class RequestConfig:
    """Configuration for an API request."""

    method: str = "GET"
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: Optional[dict[str, Any]] = None
    timeout_seconds: float = 30.0
    verify_ssl: bool = True
    allow_redirects: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BuiltRequest:
    """A fully built request ready for execution."""

    method: str
    url: str
    headers: dict[str, str]
    params: dict[str, Any]
    body: Optional[dict[str, Any]]
    timeout_seconds: float
    verify_ssl: bool
    allow_redirects: bool
    middleware_applied: list[str] = field(default_factory=list)


class APIMiddleware:
    """Base class for request middleware."""

    name: str = "base"

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        """Process a request dict."""
        return request


class AuthMiddleware(APIMiddleware):
    """Add authentication headers."""

    name = "auth"

    def __init__(self, api_key: Optional[str] = None, token: Optional[str] = None):
        self.api_key = api_key
        self.token = token

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        headers = request.get("headers", {})
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request["headers"] = headers
        return request


class ContentTypeMiddleware(APIMiddleware):
    """Set content type header."""

    name = "content_type"

    def __init__(self, content_type: str = "application/json"):
        self.content_type = content_type

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        headers = request.get("headers", {})
        if request.get("body") and "Content-Type" not in headers:
            headers["Content-Type"] = self.content_type
        request["headers"] = headers
        return request


class UserAgentMiddleware(APIMiddleware):
    """Set User-Agent header."""

    name = "user_agent"

    def __init__(self, user_agent: str = "RabAiBot/1.0"):
        self.user_agent = user_agent

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        headers = request.get("headers", {})
        headers["User-Agent"] = self.user_agent
        request["headers"] = headers
        return request


class RequestIDMiddleware(APIMiddleware):
    """Add unique request ID."""

    name = "request_id"
    _counter = 0

    def __init__(self):
        import time

        self._prefix = f"req-{int(time.time() * 1000)}-"

    def process(self, request: dict[str, Any]) -> dict[str, Any]:
        import threading

        RequestIDMiddleware._counter += 1
        request_id = f"{self._prefix}{RequestIDMiddleware._counter}"
        headers = request.get("headers", {})
        headers["X-Request-ID"] = request_id
        request["headers"] = headers
        request["metadata"] = request.get("metadata", {})
        request["metadata"]["request_id"] = request_id
        return request


class APIRequestBuilderAction:
    """Builds API requests with middleware chain support."""

    def __init__(self, base_url: Optional[str] = None):
        """Initialize request builder.

        Args:
            base_url: Base URL to join with relative paths.
        """
        self._base_url = base_url
        self._middleware: list[APIMiddleware] = []
        self._default_headers: dict[str, str] = {}

    def add_middleware(self, middleware: APIMiddleware) -> "APIRequestBuilderAction":
        """Add middleware to the chain."""
        self._middleware.append(middleware)
        return self

    def set_default_headers(self, headers: dict[str, str]) -> "APIRequestBuilderAction":
        """Set default headers for all requests."""
        self._default_headers.update(headers)
        return self

    def build(self, config: RequestConfig) -> BuiltRequest:
        """Build a request from configuration.

        Args:
            config: Request configuration.

        Returns:
            BuiltRequest ready for execution.
        """
        url = config.url
        if self._base_url and not url.startswith(("http://", "https://")):
            url = urljoin(self._base_url, url)

        if config.params:
            query_string = urlencode(config.params)
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query_string}"

        request_dict: dict[str, Any] = {
            "method": config.method.upper(),
            "url": url,
            "headers": {**self._default_headers, **config.headers},
            "params": config.params,
            "body": config.body,
            "timeout_seconds": config.timeout_seconds,
            "verify_ssl": config.verify_ssl,
            "allow_redirects": config.allow_redirects,
            "metadata": config.metadata,
        }

        applied = []
        for mw in self._middleware:
            request_dict = mw.process(request_dict)
            applied.append(mw.name)

        return BuiltRequest(
            method=request_dict["method"],
            url=request_dict["url"],
            headers=request_dict["headers"],
            params=request_dict["params"],
            body=request_dict["body"],
            timeout_seconds=request_dict["timeout_seconds"],
            verify_ssl=request_dict["verify_ssl"],
            allow_redirects=request_dict["allow_redirects"],
            middleware_applied=applied,
        )

    def build_get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BuiltRequest:
        """Build a GET request."""
        return self.build(RequestConfig(method="GET", url=url, params=params or {}, **kwargs))

    def build_post(
        self,
        url: str,
        body: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BuiltRequest:
        """Build a POST request."""
        return self.build(RequestConfig(method="POST", url=url, body=body, **kwargs))

    def build_put(
        self,
        url: str,
        body: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BuiltRequest:
        """Build a PUT request."""
        return self.build(RequestConfig(method="PUT", url=url, body=body, **kwargs))

    def build_delete(
        self,
        url: str,
        **kwargs,
    ) -> BuiltRequest:
        """Build a DELETE request."""
        return self.build(RequestConfig(method="DELETE", url=url, **kwargs))

    def build_patch(
        self,
        url: str,
        body: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BuiltRequest:
        """Build a PATCH request."""
        return self.build(RequestConfig(method="PATCH", url=url, body=body, **kwargs))
