"""HTTPX integration for async HTTP client operations.

Handles HTTP operations including GET/POST/PUT/DELETE,
async requests, streaming, cookies, and retries.
"""

from typing import Any, Optional, Callable
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio

try:
    import httpx
except ImportError:
    httpx = None

logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """HTTP method enumeration."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class HTTPConfig:
    """Configuration for HTTPX client."""
    base_url: Optional[str] = None
    timeout: float = 30.0
    follow_redirects: bool = True
    max_redirects: int = 10
    verify_ssl: bool = True
    headers: Optional[dict] = None
    cookies: Optional[dict] = None


@dataclass
class HTTPResponse:
    """HTTP response wrapper."""
    status_code: int
    headers: dict
    content: bytes
    text: str
    json_data: Optional[dict] = None
    elapsed_ms: float


@dataclass
class HTTPError(Exception):
    """HTTP error with status code."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[HTTPResponse] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class HTTPXAction:
    """HTTPX client for HTTP operations."""

    def __init__(self, config: Optional[HTTPConfig] = None):
        """Initialize HTTPX client with configuration.

        Args:
            config: HTTPConfig with client settings

        Raises:
            ImportError: If httpx is not installed
        """
        if httpx is None:
            raise ImportError("httpx required: pip install httpx")

        self.config = config or HTTPConfig()
        self._client: Optional[httpx.Client] = None
        self._async_client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.Client:
        """Get or create sync client."""
        if self._client is None:
            kwargs: dict[str, Any] = {
                "timeout": httpx.Timeout(self.config.timeout),
                "follow_redirects": self.config.follow_redirects,
                "max_redirects": self.config.max_redirects,
                "verify": self.config.verify_ssl
            }

            if self.config.base_url:
                kwargs["base_url"] = self.config.base_url

            self._client = httpx.Client(**kwargs)

        return self._client

    async def _get_async_client(self) -> httpx.AsyncClient:
        """Get or create async client."""
        if self._async_client is None:
            kwargs: dict[str, Any] = {
                "timeout": httpx.Timeout(self.config.timeout),
                "follow_redirects": self.config.follow_redirects,
                "max_redirects": self.config.max_redirects,
                "verify": self.config.verify_ssl
            }

            if self.config.base_url:
                kwargs["base_url"] = self.config.base_url

            self._async_client = httpx.AsyncClient(**kwargs)

        return self._async_client

    def request(self, method: HTTPMethod, url: str,
                params: Optional[dict] = None,
                data: Optional[dict] = None,
                json: Optional[dict] = None,
                headers: Optional[dict] = None,
                cookies: Optional[dict] = None,
                timeout: Optional[float] = None) -> HTTPResponse:
        """Make a synchronous HTTP request.

        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters
            data: Form data
            json: JSON body
            headers: Request headers
            cookies: Request cookies
            timeout: Request timeout override

        Returns:
            HTTPResponse object

        Raises:
            HTTPError: On request failure
        """
        client = self._get_client()

        request_headers = dict(self.config.headers or {})
        if headers:
            request_headers.update(headers)

        request_cookies = dict(self.config.cookies or {})
        if cookies:
            request_cookies.update(cookies)

        try:
            start = datetime.now()

            response = client.request(
                method=method.value,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=request_headers,
                cookies=request_cookies,
                timeout=timeout or self.config.timeout
            )

            elapsed = (datetime.now() - start).total_seconds() * 1000

            content = response.content
            text = response.text

            json_data = None
            if response.headers.get("content-type", "").startswith("application/json"):
                try:
                    json_data = response.json()
                except Exception:
                    pass

            http_response = HTTPResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                content=content,
                text=text,
                json_data=json_data,
                elapsed_ms=elapsed
            )

            if not response.is_success:
                raise HTTPError(
                    f"HTTP {response.status_code}: {text[:500]}",
                    status_code=response.status_code,
                    response=http_response
                )

            return http_response

        except httpx.HTTPError as e:
            raise HTTPError(f"Request failed: {e}")

    def get(self, url: str, **kwargs) -> HTTPResponse:
        """Make GET request."""
        return self.request(HTTPMethod.GET, url, **kwargs)

    def post(self, url: str, **kwargs) -> HTTPResponse:
        """Make POST request."""
        return self.request(HTTPMethod.POST, url, **kwargs)

    def put(self, url: str, **kwargs) -> HTTPResponse:
        """Make PUT request."""
        return self.request(HTTPMethod.PUT, url, **kwargs)

    def patch(self, url: str, **kwargs) -> HTTPResponse:
        """Make PATCH request."""
        return self.request(HTTPMethod.PATCH, url, **kwargs)

    def delete(self, url: str, **kwargs) -> HTTPResponse:
        """Make DELETE request."""
        return self.request(HTTPMethod.DELETE, url, **kwargs)

    async def request_async(self, method: HTTPMethod, url: str,
                           params: Optional[dict] = None,
                           data: Optional[dict] = None,
                           json: Optional[dict] = None,
                           headers: Optional[dict] = None,
                           cookies: Optional[dict] = None,
                           timeout: Optional[float] = None) -> HTTPResponse:
        """Make an asynchronous HTTP request.

        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters
            data: Form data
            json: JSON body
            headers: Request headers
            cookies: Request cookies
            timeout: Request timeout override

        Returns:
            HTTPResponse object
        """
        client = await self._get_async_client()

        request_headers = dict(self.config.headers or {})
        if headers:
            request_headers.update(headers)

        request_cookies = dict(self.config.cookies or {})
        if cookies:
            request_cookies.update(cookies)

        try:
            start = datetime.now()

            response = await client.request(
                method=method.value,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=request_headers,
                cookies=request_cookies,
                timeout=timeout or self.config.timeout
            )

            elapsed = (datetime.now() - start).total_seconds() * 1000

            content = response.content
            text = response.text

            json_data = None
            if response.headers.get("content-type", "").startswith("application/json"):
                try:
                    json_data = response.json()
                except Exception:
                    pass

            http_response = HTTPResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                content=content,
                text=text,
                json_data=json_data,
                elapsed_ms=elapsed
            )

            if not response.is_success:
                raise HTTPError(
                    f"HTTP {response.status_code}: {text[:500]}",
                    status_code=response.status_code,
                    response=http_response
                )

            return http_response

        except httpx.HTTPError as e:
            raise HTTPError(f"Request failed: {e}")

    async def get_async(self, url: str, **kwargs) -> HTTPResponse:
        """Make async GET request."""
        return await self.request_async(HTTPMethod.GET, url, **kwargs)

    async def post_async(self, url: str, **kwargs) -> HTTPResponse:
        """Make async POST request."""
        return await self.request_async(HTTPMethod.POST, url, **kwargs)

    async def put_async(self, url: str, **kwargs) -> HTTPResponse:
        """Make async PUT request."""
        return await self.request_async(HTTPMethod.PUT, url, **kwargs)

    async def patch_async(self, url: str, **kwargs) -> HTTPResponse:
        """Make async PATCH request."""
        return await self.request_async(HTTPMethod.PATCH, url, **kwargs)

    async def delete_async(self, url: str, **kwargs) -> HTTPResponse:
        """Make async DELETE request."""
        return await self.request_async(HTTPMethod.DELETE, url, **kwargs)

    async def gather_requests(self, requests: list[dict]) -> list[HTTPResponse]:
        """Execute multiple requests concurrently.

        Args:
            requests: List of request dicts with 'method', 'url', etc.

        Returns:
            List of HTTPResponse objects in order
        """
        async def make_request(req: dict) -> HTTPResponse:
            method = HTTPMethod[req.get("method", "GET").upper()]
            return await self.request_async(
                method=method,
                url=req["url"],
                params=req.get("params"),
                json=req.get("json"),
                headers=req.get("headers"),
                cookies=req.get("cookies")
            )

        return await asyncio.gather(*[make_request(r) for r in requests])

    def stream_get(self, url: str,
                   on_chunk: Callable[[bytes], None],
                   **kwargs) -> HTTPResponse:
        """Make streaming GET request.

        Args:
            url: Request URL
            on_chunk: Callback for each chunk received
            **kwargs: Additional request parameters

        Returns:
            HTTPResponse (after stream complete)
        """
        client = self._get_client()

        try:
            with client.stream("GET", url, **kwargs) as response:
                for chunk in response.iter_bytes():
                    on_chunk(chunk)

                return HTTPResponse(
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    content=b"",
                    text="",
                    elapsed_ms=0
                )

        except httpx.HTTPError as e:
            raise HTTPError(f"Stream request failed: {e}")

    def download_file(self, url: str, path: str, chunk_size: int = 8192) -> bool:
        """Download file from URL to local path.

        Args:
            url: File URL
            path: Local file path
            chunk_size: Download chunk size

        Returns:
            True if successful
        """
        def on_chunk(chunk: bytes) -> None:
            f.write(chunk)

        try:
            with open(path, "wb") as f:
                self.stream_get(url, on_chunk=on_chunk)
            return True

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False

    def close(self) -> None:
        """Close HTTP clients."""
        if self._client:
            self._client.close()
            self._client = None

        if self._async_client:
            asyncio.run(self._async_client.aclose())
            self._async_client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._get_async_client()
        await self._async_client.aclose()
        self._async_client = None
        return False
