"""API CORS Action Module.

CORS (Cross-Origin Resource Sharing) handling utilities.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .api_gateway_action import GatewayRequest, GatewayResponse


class CORSCredentials(Enum):
    """CORS credentials options."""
    INCLUDE = "include"
    SAME_ORIGIN = "same-origin"
    OMIT = "omit"


@dataclass
class CORSConfig:
    """CORS configuration."""
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    allowed_methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    allowed_headers: list[str] = field(default_factory=lambda: ["Content-Type", "Authorization"])
    exposed_headers: list[str] = field(default_factory=list)
    max_age: int = 86400
    credentials: CORSCredentials = CORSCredentials.OMIT
    allow_any_origin: bool = True


class CORSHandler:
    """Handle CORS for API requests."""

    def __init__(self, config: CORSConfig | None = None) -> None:
        self.config = config or CORSConfig()

    def is_origin_allowed(self, origin: str | None) -> bool:
        """Check if origin is allowed."""
        if self.config.allow_any_origin:
            return True
        if not origin:
            return False
        return origin in self.config.allowed_origins

    def get_cors_headers(
        self,
        origin: str | None,
        request_method: str | None = None
    ) -> dict[str, str]:
        """Get CORS headers for response."""
        headers = {}
        if self.config.allow_any_origin:
            headers["Access-Control-Allow-Origin"] = origin or "*"
        elif origin and self.is_origin_allowed(origin):
            headers["Access-Control-Allow-Origin"] = origin
        if self.config.credentials == CORSCredentials.INCLUDE:
            headers["Access-Control-Allow-Credentials"] = "true"
        if self.config.exposed_headers:
            headers["Access-Control-Expose-Headers"] = ",".join(self.config.exposed_headers)
        return headers

    def get_preflight_headers(
        self,
        origin: str | None,
        request_method: str,
        request_headers: list[str] | None = None
    ) -> dict[str, str]:
        """Get headers for preflight OPTIONS request."""
        headers = self.get_cors_headers(origin)
        headers["Access-Control-Allow-Methods"] = ",".join(self.config.allowed_methods)
        headers["Access-Control-Allow-Headers"] = ",".join(self.config.allowed_headers)
        headers["Access-Control-Max-Age"] = str(self.config.max_age)
        return headers

    async def handle_preflight(self, request: GatewayRequest) -> GatewayResponse:
        """Handle CORS preflight request."""
        origin = request.headers.get("Origin")
        method = request.headers.get("Access-Control-Request-Method")
        headers = request.headers.get("Access-Control-Request-Headers")
        if not self.is_origin_allowed(origin):
            return GatewayResponse(403, {"error": "Origin not allowed"})
        response_headers = self.get_preflight_headers(
            origin,
            method or "GET",
            headers.split(",") if headers else None
        )
        return GatewayResponse(204, None, headers=response_headers)

    def wrap_response(
        self,
        response: GatewayResponse,
        request: GatewayRequest
    ) -> GatewayResponse:
        """Wrap response with CORS headers."""
        origin = request.headers.get("Origin")
        cors_headers = self.get_cors_headers(origin)
        response.headers.update(cors_headers)
        return response
