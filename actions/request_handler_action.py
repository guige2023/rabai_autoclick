"""
HTTP Request Handler Action Module.

Provides high-level HTTP client with session management,
cookie handling, retry logic, and request/response interceptors.

Example:
    >>> from request_handler_action import HTTPClient, RequestConfig
    >>> client = HTTPClient()
    >>> resp = await client.get("https://api.example.com/data")
    >>> await client.close()
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class RequestConfig:
    """HTTP request configuration."""
    method: str = "GET"
    params: Optional[dict[str, Any]] = None
    headers: Optional[dict[str, str]] = None
    body: Optional[Any] = None
    json_body: Optional[Any] = None
    form_data: Optional[dict[str, Any]] = None
    timeout: float = 30.0
    allow_redirects: bool = True
    verify_ssl: bool = True


@dataclass
class HTTPResponse:
    """HTTP response wrapper."""
    status: int
    headers: dict[str, str]
    content: bytes
    elapsed_ms: float
    url: str
    error: Optional[str] = None

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.content)

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise urllib.error.HTTPError(
                self.url, self.status, "HTTP Error", self.headers, None
            )


class HTTPClient:
    """Async HTTP client with session, cookies, and retry support."""

    def __init__(
        self,
        base_url: str = "",
        default_headers: Optional[dict[str, str]] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.default_headers = default_headers or {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.default_timeout = timeout
        self._cookies: dict[str, str] = {}
        self._session_id: Optional[str] = None
        self._on_request: Optional[Callable] = None
        self._on_response: Optional[Callable] = None

    def set_auth_basic(self, username: str, password: str) -> None:
        """Set Basic authentication header."""
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        self.default_headers["Authorization"] = f"Basic {encoded}"

    def set_auth_bearer(self, token: str) -> None:
        """Set Bearer token authentication."""
        self.default_headers["Authorization"] = f"Bearer {token}"

    def set_auth_api_key(self, key: str, header_name: str = "X-API-Key") -> None:
        """Set API key authentication."""
        self.default_headers[header_name] = key

    def set_session_id(self, session_id: str) -> None:
        """Set session identifier."""
        self._session_id = session_id
        self._cookies["session_id"] = session_id

    def set_cookie(self, name: str, value: str, domain: str = "") -> None:
        """Set a cookie."""
        self._cookies[name] = value

    def get_cookies(self) -> dict[str, str]:
        """Get all cookies."""
        return dict(self._cookies)

    def clear_cookies(self) -> None:
        """Clear all cookies."""
        self._cookies.clear()

    def on_request(self, callback: Callable) -> None:
        """Register request interceptor callback."""
        self._on_request = callback

    def on_response(self, callback: Callable) -> None:
        """Register response interceptor callback."""
        self._on_response = callback

    async def request(
        self,
        path: str,
        config: Optional[RequestConfig] = None,
    ) -> HTTPResponse:
        """
        Send HTTP request with retry logic.

        Args:
            path: URL path (appended to base_url)
            config: RequestConfig with method, headers, body, etc.

        Returns:
            HTTPResponse with status, content, headers
        """
        config = config or RequestConfig()
        url = self._build_url(path, config.params)
        headers = self._build_headers(config)

        last_error: Optional[str] = None
        for attempt in range(self.max_retries):
            try:
                response = await self._do_request(url, config, headers)
                if self._on_response:
                    await self._on_response(response)
                return response
            except Exception as e:
                last_error = str(e)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        return HTTPResponse(
            status=0,
            headers={},
            content=b"",
            elapsed_ms=0,
            url=url,
            error=last_error,
        )

    async def get(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> HTTPResponse:
        """Send GET request."""
        return await self.request(path, RequestConfig(method="GET", params=params, **kwargs))

    async def post(
        self,
        path: str,
        body: Optional[Any] = None,
        json_body: Optional[Any] = None,
        **kwargs,
    ) -> HTTPResponse:
        """Send POST request."""
        return await self.request(
            path, RequestConfig(method="POST", body=body, json_body=json_body, **kwargs)
        )

    async def put(
        self,
        path: str,
        body: Optional[Any] = None,
        json_body: Optional[Any] = None,
        **kwargs,
    ) -> HTTPResponse:
        """Send PUT request."""
        return await self.request(
            path, RequestConfig(method="PUT", body=body, json_body=json_body, **kwargs)
        )

    async def patch(
        self,
        path: str,
        body: Optional[Any] = None,
        json_body: Optional[Any] = None,
        **kwargs,
    ) -> HTTPResponse:
        """Send PATCH request."""
        return await self.request(
            path, RequestConfig(method="PATCH", body=body, json_body=json_body, **kwargs)
        )

    async def delete(self, path: str, **kwargs) -> HTTPResponse:
        """Send DELETE request."""
        return await self.request(path, RequestConfig(method="DELETE", **kwargs))

    async def head(self, path: str, **kwargs) -> HTTPResponse:
        """Send HEAD request."""
        return await self.request(path, RequestConfig(method="HEAD", **kwargs))

    async def options(self, path: str, **kwargs) -> HTTPResponse:
        """Send OPTIONS request."""
        return await self.request(path, RequestConfig(method="OPTIONS", **kwargs))

    def _build_url(self, path: str, params: Optional[dict[str, Any]]) -> str:
        if path.startswith(("http://", "https://")):
            url = path
        else:
            url = f"{self.base_url}/{path}".replace("//", "/")
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}" if "?" not in url else f"{url}&{query}"
        return url

    def _build_headers(self, config: RequestConfig) -> dict[str, str]:
        headers = dict(self.default_headers)
        if config.headers:
            headers.update(config.headers)
        if self._cookies:
            cookie_str = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
            headers["Cookie"] = cookie_str
        if config.json_body is not None:
            headers["Content-Type"] = "application/json"
        elif config.form_data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        return headers

    async def _do_request(
        self,
        url: str,
        config: RequestConfig,
        headers: dict[str, str],
    ) -> HTTPResponse:
        if self._on_request:
            await self._on_request(url, config)

        start = time.monotonic()

        def _sync_request() -> HTTPResponse:
            req_headers = dict(headers)
            body_bytes = b""
            if config.json_body is not None:
                body_bytes = json.dumps(config.json_body, default=str).encode("utf-8")
            elif config.body is not None:
                body_bytes = config.body.encode("utf-8") if isinstance(config.body, str) else config.body
            elif config.form_data is not None:
                body_bytes = urllib.parse.urlencode(config.form_data).encode("utf-8")
                req_headers["Content-Type"] = "application/x-www-form-urlencoded"

            req = urllib.request.Request(
                url,
                method=config.method,
                headers=req_headers,
                data=body_bytes if body_bytes else None,
            )

            try:
                with urllib.request.urlopen(req, timeout=config.timeout) as resp:
                    content = resp.read()
                    elapsed = (time.monotonic() - start) * 1000
                    return HTTPResponse(
                        status=resp.status,
                        headers=dict(resp.headers),
                        content=content,
                        elapsed_ms=elapsed,
                        url=url,
                    )
            except urllib.error.HTTPError as e:
                content = e.read() if e.fp else b""
                elapsed = (time.monotonic() - start) * 1000
                return HTTPResponse(
                    status=e.code,
                    headers=dict(e.headers) if e.headers else {},
                    content=content,
                    elapsed_ms=elapsed,
                    url=url,
                )
            except Exception as ex:
                elapsed = (time.monotonic() - start) * 1000
                return HTTPResponse(
                    status=0,
                    headers={},
                    content=b"",
                    elapsed_ms=elapsed,
                    url=url,
                    error=str(ex),
                )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_request)

    async def close(self) -> None:
        """Close client and cleanup."""
        pass

    async def batch_get(
        self,
        paths: list[str],
        concurrency: int = 5,
    ) -> list[HTTPResponse]:
        """Fetch multiple URLs concurrently with concurrency limit."""
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_limit(path: str) -> HTTPResponse:
            async with semaphore:
                return await self.get(path)

        return await asyncio.gather(*[fetch_with_limit(p) for p in paths])

    async def batch_post(
        self,
        requests: list[tuple[str, Any]],
        concurrency: int = 5,
    ) -> list[HTTPResponse]:
        """Post to multiple URLs concurrently."""
        semaphore = asyncio.Semaphore(concurrency)

        async def post_with_limit(path: str, data: Any) -> HTTPResponse:
            async with semaphore:
                return await self.post(path, json_body=data)

        return await asyncio.gather(
            *[post_with_limit(p, d) for p, d in requests]
        )


if __name__ == "__main__":
    async def test():
        client = HTTPClient(base_url="https://jsonplaceholder.typicode.com")

        print("GET test:")
        resp = await client.get("/posts/1")
        print(f"Status: {resp.status}, Elapsed: {resp.elapsed_ms:.0f}ms")
        if resp.status == 200:
            data = resp.json()
            print(f"Title: {data.get('title', 'N/A')}")

        print("\nPOST test:")
        resp = await client.post("/posts", json_body={"title": "Test", "body": "Content", "userId": 1})
        print(f"Status: {resp.status}")

        await client.close()

    asyncio.run(test())
