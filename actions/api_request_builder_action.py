"""
API request builder module.

Provides fluent builder for constructing HTTP requests with
authentication, headers, and retry logic.

Author: Aito Auto Agent
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Optional,
)
import json


class HttpMethod(Enum):
    """HTTP method types."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class AuthType(Enum):
    """Authentication type."""
    NONE = auto()
    BASIC = auto()
    BEARER = auto()
    API_KEY = auto()
    OAUTH2 = auto()


@dataclass
class RequestConfig:
    """Configuration for a single request."""
    method: HttpMethod
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0
    allow_redirects: bool = True
    verify_ssl: bool = True


@dataclass
class RetryConfig:
    """Retry configuration for failed requests."""
    max_attempts: int = 3
    initial_delay_ms: int = 100
    max_delay_ms: int = 10000
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on_status: list[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])


@dataclass
class AuthConfig:
    """Authentication configuration."""
    auth_type: AuthType = AuthType.NONE
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"


class RequestBuilder:
    """
    Fluent builder for HTTP requests.

    Example:
        response = (
            RequestBuilder()
            .get("https://api.example.com/users")
            .header("Accept", "application/json")
            .header("Authorization", "Bearer token123")
            .param("page", "1")
            .timeout(10.0)
            .execute()
        )
    """

    def __init__(self):
        self._method = HttpMethod.GET
        self._url = ""
        self._headers: dict[str, str] = {}
        self._params: dict[str, str] = {}
        self._body: Optional[Any] = None
        self._timeout: float = 30.0
        self._allow_redirects = True
        self._verify_ssl = True
        self._auth: Optional[AuthConfig] = None
        self._retry: Optional[RetryConfig] = None
        self._on_response: Optional[Callable] = None
        self._on_error: Optional[Callable] = None

    def method(self, method: HttpMethod) -> RequestBuilder:
        """Set HTTP method."""
        self._method = method
        return self

    def get(self, url: str) -> RequestBuilder:
        """Start GET request."""
        return self.method(HttpMethod.GET).url(url)

    def post(self, url: str) -> RequestBuilder:
        """Start POST request."""
        return self.method(HttpMethod.POST).url(url)

    def put(self, url: str) -> RequestBuilder:
        """Start PUT request."""
        return self.method(HttpMethod.PUT).url(url)

    def patch(self, url: str) -> RequestBuilder:
        """Start PATCH request."""
        return self.method(HttpMethod.PATCH).url(url)

    def delete(self, url: str) -> RequestBuilder:
        """Start DELETE request."""
        return self.method(HttpMethod.DELETE).url(url)

    def url(self, url: str) -> RequestBuilder:
        """Set request URL."""
        self._url = url
        return self

    def header(self, name: str, value: str) -> RequestBuilder:
        """Add a header."""
        self._headers[name] = value
        return self

    def headers(self, headers: dict[str, str]) -> RequestBuilder:
        """Set multiple headers."""
        self._headers.update(headers)
        return self

    def param(self, name: str, value: str) -> RequestBuilder:
        """Add query parameter."""
        self._params[name] = value
        return self

    def params(self, params: dict[str, str]) -> RequestBuilder:
        """Set multiple query parameters."""
        self._params.update(params)
        return self

    def body(self, data: Any) -> RequestBuilder:
        """Set request body."""
        self._body = data
        return self

    def json_body(self, data: dict) -> RequestBuilder:
        """Set JSON request body."""
        self._body = data
        self._headers["Content-Type"] = "application/json"
        return self

    def form_body(self, data: dict) -> RequestBuilder:
        """Set form-encoded body."""
        self._body = data
        self._headers["Content-Type"] = "application/x-www-form-urlencoded"
        return self

    def timeout(self, seconds: float) -> RequestBuilder:
        """Set request timeout."""
        self._timeout = seconds
        return self

    def allow_redirects(self, allow: bool) -> RequestBuilder:
        """Set whether to follow redirects."""
        self._allow_redirects = allow
        return self

    def verify_ssl(self, verify: bool) -> RequestBuilder:
        """Set SSL verification."""
        self._verify_ssl = verify
        return self

    def auth_basic(self, username: str, password: str) -> RequestBuilder:
        """Set basic authentication."""
        import base64
        credentials = base64.b64encode(
            f"{username}:{password}".encode()
        ).decode()
        self._headers["Authorization"] = f"Basic {credentials}"
        return self

    def auth_bearer(self, token: str) -> RequestBuilder:
        """Set bearer token authentication."""
        self._headers["Authorization"] = f"Bearer {token}"
        return self

    def auth_api_key(self, key: str, header: str = "X-API-Key") -> RequestBuilder:
        """Set API key authentication."""
        self._headers[header] = key
        return self

    def retry(
        self,
        max_attempts: int = 3,
        delay_ms: int = 100,
        max_delay_ms: int = 10000
    ) -> RequestBuilder:
        """Configure retry behavior."""
        self._retry = RetryConfig(
            max_attempts=max_attempts,
            initial_delay_ms=delay_ms,
            max_delay_ms=max_delay_ms
        )
        return self

    def on_response(self, callback: Callable) -> RequestBuilder:
        """Set response callback."""
        self._on_response = callback
        return self

    def on_error(self, callback: Callable) -> RequestBuilder:
        """Set error callback."""
        self._on_error = callback
        return self

    def build(self) -> RequestConfig:
        """Build request configuration."""
        return RequestConfig(
            method=self._method,
            url=self._url,
            headers=self._headers.copy(),
            params=self._params.copy(),
            body=self._body,
            timeout=self._timeout,
            allow_redirects=self._allow_redirects,
            verify_ssl=self._verify_ssl
        )

    def execute(self) -> dict:
        """Execute the request and return response."""
        config = self.build()

        import requests

        method_func = getattr(requests, config.method.value.lower())

        attempt = 0
        max_attempts = self._retry.max_attempts if self._retry else 1

        while attempt < max_attempts:
            try:
                response = method_func(
                    url=config.url,
                    headers=config.headers,
                    params=config.params,
                    json=config.body if isinstance(config.body, dict) else None,
                    data=config.body if not isinstance(config.body, dict) else None,
                    timeout=config.timeout,
                    allow_redirects=config.allow_redirects,
                    verify=config.verify_ssl
                )

                if self._on_response:
                    self._on_response(response)

                if response.status_code < 400:
                    return {
                        "success": True,
                        "status_code": response.status_code,
                        "data": response.json() if response.content else None,
                        "headers": dict(response.headers)
                    }
                else:
                    should_retry = (
                        self._retry and
                        response.status_code in self._retry.retry_on_status
                    )

                    if not should_retry or attempt >= max_attempts - 1:
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": response.text,
                            "headers": dict(response.headers)
                        }

            except requests.exceptions.RequestException as e:
                if self._on_error:
                    self._on_error(e)

                if attempt >= max_attempts - 1:
                    return {
                        "success": False,
                        "error": str(e)
                    }

            if self._retry and attempt < max_attempts - 1:
                delay = min(
                    self._retry.initial_delay_ms * (self._retry.exponential_base ** attempt),
                    self._retry.max_delay_ms
                )

                if self._retry.jitter:
                    import random
                    delay *= (0.5 + random.random())

                time.sleep(delay / 1000)

            attempt += 1

        return {"success": False, "error": "Max attempts exceeded"}


class BatchRequestBuilder:
    """
    Builder for batched API requests.

    Example:
        responses = (
            BatchRequestBuilder()
            .add(RequestBuilder().get("https://api.example.com/users/1"))
            .add(RequestBuilder().get("https://api.example.com/users/2"))
            .execute_all()
        )
    """

    def __init__(self):
        self._requests: list[RequestBuilder] = []
        self._concurrency: int = 5

    def add(self, builder: RequestBuilder) -> BatchRequestBuilder:
        """Add a request to the batch."""
        self._requests.append(builder)
        return self

    def add_many(self, builders: list[RequestBuilder]) -> BatchRequestBuilder:
        """Add multiple requests to the batch."""
        self._requests.extend(builders)
        return self

    def concurrency(self, max_concurrent: int) -> BatchRequestBuilder:
        """Set maximum concurrent requests."""
        self._concurrency = max_concurrent
        return self

    def execute_all(self) -> list[dict]:
        """Execute all requests."""
        import concurrent.futures

        results = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._concurrency
        ) as executor:
            futures = {
                executor.submit(req.execute): i
                for i, req in enumerate(self._requests)
            }

            results = [None] * len(self._requests)

            for future in concurrent.futures.as_completed(futures):
                index = futures[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    results[index] = {"success": False, "error": str(e)}

        return results


def create_request_builder() -> RequestBuilder:
    """Factory to create a RequestBuilder."""
    return RequestBuilder()


def create_batch_builder() -> BatchRequestBuilder:
    """Factory to create a BatchRequestBuilder."""
    return BatchRequestBuilder()
