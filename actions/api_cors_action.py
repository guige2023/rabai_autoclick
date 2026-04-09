"""CORS (Cross-Origin Resource Sharing) handling utilities.

This module provides CORS support:
- CORS header generation
- Preflight request handling
- Origin validation
- Credentials handling

Example:
    >>> from actions.api_cors_action import CORSHandler
    >>> handler = CORSHandler(allowed_origins=["https://example.com"])
    >>> headers = handler.get_cors_headers(request)
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class CORSConfig:
    """CORS configuration."""
    allowed_origins: list[str] = None
    allowed_methods: list[str] = None
    allowed_headers: list[str] = None
    exposed_headers: list[str] = None
    max_age: int = 3600
    allow_credentials: bool = False
    allow_any_origin: bool = False


class CORSHandler:
    """Handle CORS for API requests.

    Example:
        >>> handler = CORSHandler(
        ...     allowed_origins=["https://example.com"],
        ...     allow_credentials=True
        ... )
        >>> if request.method == "OPTIONS":
        ...     return handler.handle_preflight(request)
    """

    DEFAULT_ALLOWED_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
    DEFAULT_ALLOWED_HEADERS = [
        "Content-Type", "Authorization", "X-Requested-With",
        "Accept", "Origin", "Cache-Control",
    ]
    DEFAULT_EXPOSED_HEADERS = [
        "X-Total-Count", "X-Page-Count", "X-Current-Page",
    ]

    def __init__(
        self,
        allowed_origins: Optional[list[str]] = None,
        allowed_methods: Optional[list[str]] = None,
        allowed_headers: Optional[list[str]] = None,
        exposed_headers: Optional[list[str]] = None,
        max_age: int = 3600,
        allow_credentials: bool = False,
        allow_any_origin: bool = False,
    ) -> None:
        self.config = CORSConfig(
            allowed_origins=allowed_origins or ["*"],
            allowed_methods=allowed_methods or self.DEFAULT_ALLOWED_METHODS,
            allowed_headers=allowed_headers or self.DEFAULT_ALLOWED_HEADERS,
            exposed_headers=exposed_headers or self.DEFAULT_EXPOSED_HEADERS,
            max_age=max_age,
            allow_credentials=allow_credentials,
            allow_any_origin=allow_any_origin,
        )

    def get_cors_headers(
        self,
        origin: Optional[str] = None,
    ) -> dict[str, str]:
        """Get CORS headers for a response.

        Args:
            origin: Origin header value from request.

        Returns:
            Dictionary of CORS headers.
        """
        headers = {}
        allowed_origin = self._get_allowed_origin(origin)
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
        if self.config.allow_credentials:
            headers["Access-Control-Allow-Credentials"] = "true"
        if self.config.exposed_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(self.config.exposed_headers)
        return headers

    def get_preflight_headers(
        self,
        origin: Optional[str] = None,
        request_headers: Optional[list[str]] = None,
        request_method: Optional[str] = None,
    ) -> dict[str, str]:
        """Get headers for a preflight (OPTIONS) response.

        Args:
            origin: Origin header value.
            request_headers: Access-Control-Request-Headers.
            request_method: Access-Control-Request-Method.

        Returns:
            Dictionary of CORS headers.
        """
        headers = {}
        allowed_origin = self._get_allowed_origin(origin)
        if allowed_origin:
            headers["Access-Control-Allow-Origin"] = allowed_origin
        if self.config.allow_credentials:
            headers["Access-Control-Allow-Credentials"] = "true"
        headers["Access-Control-Max-Age"] = str(self.config.max_age)
        if self.config.allowed_methods:
            methods = self.config.allowed_methods
            if request_method and request_method in methods:
                methods = [request_method]
            headers["Access-Control-Allow-Methods"] = ", ".join(methods)
        if self.config.allowed_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(self.config.allowed_headers)
        elif request_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(request_headers)
        return headers

    def _get_allowed_origin(self, origin: Optional[str]) -> Optional[str]:
        """Determine allowed origin based on configuration."""
        if self.config.allow_any_origin:
            if not self.config.allow_credentials:
                return "*"
            return origin
        if not origin:
            return None
        if "*" in self.config.allowed_origins:
            return origin
        if origin in self.config.allowed_origins:
            return origin
        return None

    def is_origin_allowed(self, origin: Optional[str]) -> bool:
        """Check if an origin is allowed.

        Args:
            origin: Origin to check.

        Returns:
            True if origin is allowed.
        """
        return self._get_allowed_origin(origin) is not None

    def handle_preflight(
        self,
        method: str,
        origin: Optional[str],
        request_headers: Optional[list[str]],
    ) -> tuple[int, dict[str, str]]:
        """Handle a preflight request.

        Args:
            method: Request method.
            origin: Origin header.
            request_headers: Requested headers.

        Returns:
            Tuple of (status_code, headers).
        """
        if method != "OPTIONS":
            return 200, self.get_cors_headers(origin)
        if not self.is_origin_allowed(origin):
            return 403, {}
        return 204, self.get_preflight_headers(origin, request_headers, None)


class CORSValidator:
    """Validate CORS requests."""

    def __init__(self, handler: CORSHandler) -> None:
        self.handler = handler

    def validate_request(
        self,
        origin: Optional[str],
        method: str,
    ) -> tuple[bool, Optional[str]]:
        """Validate a CORS request.

        Args:
            origin: Origin header.
            method: Request method.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if not self.handler.is_origin_allowed(origin):
            return False, "Origin not allowed"
        if method not in self.handler.config.allowed_methods:
            return False, f"Method {method} not allowed"
        return True, None


def add_cors_headers(
    response: dict[str, Any],
    origin: Optional[str],
    config: Optional[CORSConfig] = None,
) -> dict[str, Any]:
    """Add CORS headers to a response.

    Args:
        response: Response dictionary.
        origin: Origin header value.
        config: Optional CORS config.

    Returns:
        Response with CORS headers added.
    """
    handler = CORSHandler() if not config else CORSHandler(
        allowed_origins=config.allowed_origins,
        allow_credentials=config.allow_credentials,
    )
    headers = handler.get_cors_headers(origin)
    if "headers" not in response:
        response["headers"] = {}
    response["headers"].update(headers)
    return response
