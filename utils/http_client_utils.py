"""HTTP client utilities with retry, timeout, and connection pooling."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
from urllib.parse import urlencode

import urllib3

__all__ = ["HTTPClient", "HTTPResponse", "RetryConfig", "create_http_client"]


@dataclass
class RetryConfig:
    """Configuration for automatic retry behavior."""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_on_exception: tuple[type[Exception], ...] = (
        urllib3.exceptions.HTTPError,
        urllib3.exceptions.ConnectTimeoutError,
        urllib3.exceptions.NewConnectionError,
    )


@dataclass
class HTTPResponse:
    """Normalized HTTP response object."""

    status_code: int
    headers: dict[str, str]
    content: bytes
    url: str
    elapsed_ms: float
    attempt: int = 1

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def json(self) -> Any:
        return json.loads(self.content)

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


class HTTPClient:
    """Feature-rich HTTP client with retry, timeout, and polling support."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
        max_connections: int = 100,
    ) -> None:
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.default_timeout = timeout
        self.default_headers = headers or {}
        self.retry_config = retry_config or RetryConfig()
        self._pool_manager = urllib3.PoolManager(
            maxsize=max_connections,
            num_pools=10,
            timeout=urllib3.Timeout(total=timeout),
        )

    def _build_url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}" if self.base_url else path
        if params:
            encoded = urlencode(params, safe="")
            url = f"{url}?{encoded}"
        return url

    def _apply_retry_delay(self, attempt: int) -> float:
        cfg = self.retry_config
        delay = cfg.base_delay * (cfg.exponential_base ** (attempt - 1))
        delay = min(delay, cfg.max_delay)
        if cfg.jitter:
            import random

            delay *= 0.5 + random.random()
        return delay

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        retry_config: RetryConfig | None = None,
    ) -> HTTPResponse:
        """Make an HTTP request with automatic retry."""
        cfg = retry_config or self.retry_config
        url = self._build_url(path, params)
        req_headers = {**self.default_headers, **(headers or {})}
        body = json_body if json_body is not None else data
        timeout_val = timeout or self.default_timeout

        attempt = 0
        while True:
            attempt += 1
            start = time.monotonic()
            try:
                response = self._pool_manager.request(
                    method=method.upper(),
                    url=url,
                    headers=req_headers,
                    json=json_body,
                    fields=body if not json_body and not data else None,
                    timeout=urllib3.Timeout(total=timeout_val),
                )
                elapsed = (time.monotonic() - start) * 1000

                if response.status not in cfg.retry_on_status or attempt >= cfg.max_attempts:
                    return HTTPResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        content=response.data,
                        url=url,
                        elapsed_ms=elapsed,
                        attempt=attempt,
                    )

            except cfg.retry_on_exception as exc:
                if attempt >= cfg.max_attempts:
                    raise
                delay = self._apply_retry_delay(attempt)
                time.sleep(delay)
                continue

            if attempt < cfg.max_attempts:
                delay = self._apply_retry_delay(attempt)
                time.sleep(delay)

        raise RuntimeError("Unreachable")

    def get(self, path: str, **kwargs: Any) -> HTTPResponse:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> HTTPResponse:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> HTTPResponse:
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> HTTPResponse:
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> HTTPResponse:
        return self.request("DELETE", path, **kwargs)

    def poll_until(
        self,
        path: str,
        condition: Callable[[HTTPResponse], bool],
        interval: float = 1.0,
        timeout: float = 60.0,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Poll a URL until condition is met or timeout expires."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.get(path, **kwargs)
            if condition(response):
                return response
            time.sleep(interval)
        raise TimeoutError(f"Polling {path} timed out after {timeout}s")

    def close(self) -> None:
        self._pool_manager.clear()


def create_http_client(
    base_url: str,
    api_key: str | None = None,
    auth_header: str | None = None,
    timeout: float = 30.0,
) -> HTTPClient:
    """Factory to create a pre-configured HTTP client."""
    headers: dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif auth_header:
        headers["Authorization"] = auth_header
    return HTTPClient(base_url=base_url, headers=headers, timeout=timeout)
