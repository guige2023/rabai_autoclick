"""HTTP requests action with retry, auth, and session management.

This module provides HTTP client capabilities including GET/POST requests,
authentication, retry logic, and response parsing.

Example:
    >>> action = RequestsAction()
    >>> result = action.execute(method="GET", url="https://api.example.com")
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class RequestConfig:
    """Configuration for HTTP requests."""
    timeout: int = 30
    retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    verify_ssl: bool = True
    follow_redirects: bool = True
    max_redirects: int = 5


@dataclass
class ResponseData:
    """Parsed HTTP response."""
    status_code: int
    headers: dict[str, str]
    body: Any
    text: str
    json_data: Optional[dict] = None
    elapsed_ms: float = 0.0


class RequestsAction:
    """HTTP requests action with retry and authentication.

    Provides comprehensive HTTP client with automatic retry,
    session management, and response parsing.

    Example:
        >>> action = RequestsAction()
        >>> result = action.execute(
        ...     method="POST",
        ...     url="https://api.example.com/data",
        ...     json={"key": "value"}
        ... )
    """

    def __init__(self, config: Optional[RequestConfig] = None) -> None:
        """Initialize requests action.

        Args:
            config: Optional request configuration.
        """
        self.config = config or RequestConfig()
        self._session: Optional[Any] = None

    def execute(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        data: Optional[Any] = None,
        json: Optional[dict] = None,
        headers: Optional[dict] = None,
        auth: Optional[tuple[str, str]] = None,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute HTTP request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.).
            url: Target URL.
            params: Query parameters.
            data: Form data.
            json: JSON body.
            headers: Custom headers.
            auth: Basic auth tuple (username, password).
            timeout: Request timeout override.
            **kwargs: Additional request parameters.

        Returns:
            Dictionary with response data.

        Raises:
            ValueError: If method or URL is invalid.
            requests.RequestException: If request fails after retries.
        """
        import requests
        from requests.exceptions import RequestException

        if not url:
            raise ValueError("URL is required")

        method = method.upper()
        if method not in ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"):
            raise ValueError(f"Invalid method: {method}")

        timeout = timeout or self.config.timeout
        result: dict[str, Any] = {"method": method, "url": url, "success": True}

        # Build request kwargs
        req_kwargs: dict[str, Any] = {
            "timeout": timeout,
            "verify": self.config.verify_ssl,
            "allow_redirects": self.config.follow_redirects,
        }

        if params:
            req_kwargs["params"] = params
        if data:
            req_kwargs["data"] = data
        if json:
            req_kwargs["json"] = json
        if headers:
            req_kwargs["headers"] = headers
        if auth:
            req_kwargs["auth"] = auth
        if kwargs:
            req_kwargs.update(kwargs)

        # Execute with retry
        retries = self.config.retries
        delay = self.config.retry_delay
        last_error: Optional[Exception] = None

        for attempt in range(retries + 1):
            try:
                start_time = time.time()

                if self._session:
                    response = self._session.request(method, url, **req_kwargs)
                else:
                    response = requests.request(method, url, **req_kwargs)

                elapsed_ms = (time.time() - start_time) * 1000

                result["status_code"] = response.status_code
                result["elapsed_ms"] = elapsed_ms
                result["headers"] = dict(response.headers)

                # Parse response
                try:
                    result["json_data"] = response.json()
                    result["body"] = result["json_data"]
                except (json.JSONDecodeError, ValueError):
                    result["body"] = response.text
                    result["text"] = response.text

                # Handle status codes
                if response.status_code >= 400:
                    if attempt < retries and self._is_retryable(response.status_code):
                        time.sleep(delay)
                        delay *= self.config.backoff_factor
                        continue
                    result["success"] = False
                    result["error"] = f"HTTP {response.status_code}"

                return result

            except RequestException as e:
                last_error = e
                if attempt < retries:
                    time.sleep(delay)
                    delay *= self.config.backoff_factor
                else:
                    result["success"] = False
                    result["error"] = str(last_error)

        return result

    def _is_retryable(self, status_code: int) -> bool:
        """Check if HTTP status code is retryable.

        Args:
            status_code: HTTP status code.

        Returns:
            True if request should be retried.
        """
        return status_code in (408, 429, 500, 502, 503, 504)

    def create_session(self, cookies: Optional[dict] = None) -> None:
        """Create persistent session with cookies.

        Args:
            cookies: Optional initial cookies.
        """
        import requests
        self._session = requests.Session()
        if cookies:
            self._session.cookies.update(cookies)

    def close_session(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    def download_file(
        self,
        url: str,
        path: str,
        chunk_size: int = 8192,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Download file from URL.

        Args:
            url: File URL.
            path: Destination path.
            chunk_size: Download chunk size.
            progress_callback: Optional progress callback.
            **kwargs: Additional request parameters.

        Returns:
            Download result dictionary.
        """
        import requests

        result: dict[str, Any] = {"url": url, "path": path, "success": True}

        try:
            response = requests.get(url, stream=True, **kwargs)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(downloaded, total_size)

            result["size"] = downloaded

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def __del__(self) -> None:
        """Cleanup on destruction."""
        self.close_session()
