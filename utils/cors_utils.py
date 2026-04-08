"""
CORS (Cross-Origin Resource Sharing) utilities.

Provides CORS header generation and middleware helpers.
"""

from __future__ import annotations

from typing import Literal


AllowOrigin = Literal["*", "same-origin"] | str | list[str] | None


class CORSConfig:
    """CORS configuration container."""

    def __init__(
        self,
        allow_origins: AllowOrigin = "*",
        allow_methods: list[str] | None = None,
        allow_headers: list[str] | None = None,
        allow_credentials: bool = False,
        expose_headers: list[str] | None = None,
        max_age: int = 600,
    ):
        self.allow_origins = allow_origins
        self.allow_methods = allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        self.allow_headers = allow_headers or ["Content-Type", "Authorization"]
        self.allow_credentials = allow_credentials
        self.expose_headers = expose_headers or []
        self.max_age = max_age

    def is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed."""
        if self.allow_origins == "*":
            return True
        if self.allow_origins == "same-origin":
            return False
        if isinstance(self.allow_origins, str):
            return origin == self.allow_origins
        return origin in self.allow_origins

    def get_headers(
        self,
        origin: str | None,
        request_method: str | None = None,
    ) -> dict[str, str]:
        """
        Build CORS headers for a request.

        Args:
            origin: Origin header from request
            request_method: Access-Control-Request-Method header

        Returns:
            Dictionary of CORS headers
        """
        headers: dict[str, str] = {}

        if origin is None:
            origin = ""

        if self.allow_origins == "*" and not self.allow_credentials:
            headers["Access-Control-Allow-Origin"] = "*"
        elif self.is_origin_allowed(origin):
            headers["Access-Control-Allow-Origin"] = origin
            if self.allow_credentials:
                headers["Access-Control-Allow-Credentials"] = "true"

        if self.expose_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(self.expose_headers)

        if request_method:
            headers["Access-Control-Allow-Methods"] = ", ".join(self.allow_methods)

        if self.allow_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(self.allow_headers)

        headers["Access-Control-Max-Age"] = str(self.max_age)

        return headers


def build_cors_preflight_response(
    origin: str | None = None,
    request_method: str | None = None,
    request_headers: str | None = None,
    config: CORSConfig | None = None,
) -> tuple[dict[str, str], int]:
    """
    Build full CORS preflight (OPTIONS) response.

    Args:
        origin: Origin header
        request_method: Access-Control-Request-Method
        request_headers: Access-Control-Request-Headers
        config: CORS configuration

    Returns:
        Tuple of (headers dict, status code)
    """
    if config is None:
        config = CORSConfig()
    headers = config.get_headers(origin, request_method)
    if request_headers:
        pass
    return headers, 204


def build_cors_response_headers(
    origin: str | None,
    config: CORSConfig | None = None,
) -> dict[str, str]:
    """Build CORS response headers for actual request."""
    if config is None:
        config = CORSConfig()
    return config.get_headers(origin)


DEFAULT_CORS_CONFIG = CORSConfig()


def default_preflight_response() -> dict[str, str]:
    """Get default CORS preflight headers."""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Max-Age": "600",
    }


def cors_middleware_factory(config: CORSConfig) -> callable:
    """
    Create a CORS middleware function.

    Args:
        config: CORS configuration

    Returns:
        Middleware function (origin, method) -> headers dict
    """
    def middleware(
        origin: str | None,
        method: str | None = None,
    ) -> dict[str, str]:
        return config.get_headers(origin, method)
    return middleware
