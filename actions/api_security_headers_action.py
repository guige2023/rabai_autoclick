"""API Security Headers Action Module.

Add security headers to API responses.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .api_gateway_action import GatewayRequest, GatewayResponse


@dataclass
class SecurityHeadersConfig:
    """Security headers configuration."""
    content_security_policy: str = "default-src 'self'"
    x_frame_options: str = "DENY"
    x_content_type_options: str = "nosniff"
    strict_transport_security: str = "max-age=31536000; includeSubDomains"
    x_xss_protection: str = "1; mode=block"
    referrer_policy: str = "strict-origin-when-cross-origin"


class SecurityHeadersMiddleware:
    """Add security headers to API responses."""

    def __init__(self, config: SecurityHeadersConfig | None = None) -> None:
        self.config = config or SecurityHeadersConfig()

    def get_security_headers(self) -> dict[str, str]:
        """Get all security headers."""
        return {
            "Content-Security-Policy": self.config.content_security_policy,
            "X-Frame-Options": self.config.x_frame_options,
            "X-Content-Type-Options": self.config.x_content_type_options,
            "Strict-Transport-Security": self.config.strict_transport_security,
            "X-XSS-Protection": self.config.x_xss_protection,
            "Referrer-Policy": self.config.referrer_policy,
        }

    def add_security_headers(self, response: GatewayResponse) -> GatewayResponse:
        """Add security headers to response."""
        headers = self.get_security_headers()
        response.headers.update(headers)
        return response

    def wrap_response(
        self,
        response: GatewayResponse,
        request: GatewayRequest
    ) -> GatewayResponse:
        """Wrap response with security headers."""
        return self.add_security_headers(response)
