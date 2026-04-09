"""
REST Client Action Module.

Provides a flexible, feature-rich REST API client with
automatic retries, timeouts, and response parsing.

Author: rabai_autoclick team
"""

import time
import logging
from typing import Optional, Dict, Any, List, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urljoin, urlencode

logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class RequestConfig:
    """Configuration for a single request."""
    timeout: float = 30.0
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    retries: int = 3
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    verify_ssl: bool = True
    follow_redirects: bool = True
    cookies: Optional[Dict[str, str]] = None


@dataclass
class Response:
    """HTTP Response wrapper."""
    status_code: int
    headers: Dict[str, str]
    body: Union[str, bytes, Dict, List]
    elapsed: float
    cookies: Dict[str, str] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Check if response indicates success (2xx)."""
        return 200 <= self.status_code < 300

    @property
    def is_client_error(self) -> bool:
        """Check if response indicates client error (4xx)."""
        return 400 <= self.status_code < 500

    @property
    def is_server_error(self) -> bool:
        """Check if response indicates server error (5xx)."""
        return 500 <= self.status_code < 600

    @property
    def is_redirect(self) -> bool:
        """Check if response is a redirect (3xx)."""
        return 300 <= self.status_code < 400


class RestClientAction:
    """
    Feature-rich REST Client.

    Supports automatic retries, exponential backoff, request/response
    interceptors, and comprehensive error handling.

    Example:
        >>> client = RestClientAction(base_url="https://api.example.com")
        >>> client.set_auth_header("Bearer", "token")
        >>> response = await client.get("/users/123")
        >>> data = response.body
    """

    DEFAULT_HEADERS = {
        "User-Agent": "rabai-autoclick/1.0",
        "Accept": "application/json",
    }

    def __init__(
        self,
        base_url: str,
        default_headers: Optional[Dict[str, str]] = None,
        default_timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.default_headers = {**self.DEFAULT_HEADERS, **(default_headers or {})}
        self.default_timeout = default_timeout
        self._session_cookies: Dict[str, str] = {}
        self._interceptors_request: List[Callable] = []
        self._interceptors_response: List[Callable] = []

    def set_auth_header(self, auth_type: str, token: str) -> None:
        """
        Set authorization header.

        Args:
            auth_type: Auth type (Bearer, Basic, etc.)
            token: Auth token
        """
        self.default_headers["Authorization"] = f"{auth_type} {token}"

    def add_header(self, key: str, value: str) -> None:
        """Add a header to all requests."""
        self.default_headers[key] = value

    def remove_header(self, key: str) -> None:
        """Remove a header from all requests."""
        self.default_headers.pop(key, None)

    def add_request_interceptor(self, interceptor: Callable) -> None:
        """
        Add request interceptor.

        Args:
            interceptor: Callable that receives RequestConfig and returns modified config
        """
        self._interceptors_request.append(interceptor)

    def add_response_interceptor(self, interceptor: Callable) -> None:
        """
        Add response interceptor.

        Args:
            interceptor: Callable that receives Response and returns modified response
        """
        self._interceptors_response.append(interceptor)

    def _build_url(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Build full URL with query parameters."""
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        if params:
            query = urlencode(params, doseq=True)
            url = f"{url}?{query}"
        return url

    def _merge_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Merge default headers with request-specific headers."""
        merged = {**self.default_headers, **headers}
        return merged

    async def request(
        self,
        method: HttpMethod,
        endpoint: str,
        config: Optional[RequestConfig] = None,
    ) -> Response:
        """
        Execute HTTP request with automatic retries.

        Args:
            method: HTTP method
            endpoint: API endpoint
            config: Optional request configuration

        Returns:
            Response object
        """
        config = config or RequestConfig(timeout=self.default_timeout)
        config.headers = self._merge_headers(config.headers)

        for interceptor in self._interceptors_request:
            config = interceptor(config) or config

        url = self._build_url(endpoint, config.params)
        last_error = None

        for attempt in range(config.retries + 1):
            try:
                response = await self._execute_request(method, url, config)
                for interceptor in self._interceptors_response:
                    response = interceptor(response) or response
                return response
            except Exception as e:
                last_error = e
                if attempt < config.retries:
                    delay = config.retry_delay * (config.retry_backoff ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"Request failed after {config.retries + 1} attempts: {e}")

        raise last_error or Exception("Request failed")

    async def _execute_request(
        self, method: HttpMethod, url: str, config: RequestConfig
    ) -> Response:
        """Execute the actual HTTP request."""
        import httpx

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout),
            verify=config.verify_ssl,
            follow_redirects=config.follow_redirects,
        ) as client:
            start_time = time.time()
            response = await client.request(
                method=method.value,
                url=url,
                headers=config.headers,
                cookies={**self._session_cookies, **(config.cookies or {})},
            )
            elapsed = time.time() - start_time

            body = self._parse_body(response)

            return Response(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=body,
                elapsed=elapsed,
                cookies=dict(response.cookies),
            )

    def _parse_body(self, response: httpx.Response) -> Union[str, bytes, Dict, List]:
        """Parse response body based on content type."""
        content_type = response.headers.get("Content-Type", "")

        if "application/json" in content_type:
            try:
                return response.json()
            except Exception:
                return response.text
        elif "application/octet-stream" in content_type:
            return response.content
        else:
            return response.text

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Response:
        """Send GET request."""
        config = RequestConfig(params=params or {}, timeout=kwargs.pop("timeout", self.default_timeout))
        return await self.request(HttpMethod.GET, endpoint, config)

    async def post(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        **kwargs
    ) -> Response:
        """Send POST request."""
        config = RequestConfig(timeout=kwargs.pop("timeout", self.default_timeout))
        config.headers["Content-Type"] = "application/json" if json else "application/x-www-form-urlencoded"
        return await self.request(HttpMethod.POST, endpoint, config)

    async def put(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        **kwargs
    ) -> Response:
        """Send PUT request."""
        config = RequestConfig(timeout=kwargs.pop("timeout", self.default_timeout))
        config.headers["Content-Type"] = "application/json" if json else "application/x-www-form-urlencoded"
        return await self.request(HttpMethod.PUT, endpoint, config)

    async def patch(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        **kwargs
    ) -> Response:
        """Send PATCH request."""
        config = RequestConfig(timeout=kwargs.pop("timeout", self.default_timeout))
        config.headers["Content-Type"] = "application/json" if json else "application/x-www-form-urlencoded"
        return await self.request(HttpMethod.PATCH, endpoint, config)

    async def delete(
        self,
        endpoint: str,
        **kwargs
    ) -> Response:
        """Send DELETE request."""
        config = RequestConfig(timeout=kwargs.pop("timeout", self.default_timeout))
        return await self.request(HttpMethod.DELETE, endpoint, config)
