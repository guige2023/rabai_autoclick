"""API security headers middleware.

This module provides security headers:
- Content Security Policy
- X-Frame-Options
- X-Content-Type-Options
- Strict Transport Security
- Custom security headers

Example:
    >>> from actions.api_security_headers_action import SecurityHeadersMiddleware
    >>> middleware = SecurityHeadersMiddleware()
    >>> response = middleware.add_security_headers(response)
"""

from __future__ import annotations

import logging
from typing import Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecurityHeadersConfig:
    """Configuration for security headers."""
    csp: Optional[str] = None
    x_frame_options: str = "DENY"
    x_content_type_options: str = "nosniff"
    x_xss_protection: str = "1; mode=block"
    strict_transport_security: str = "max-age=31536000; includeSubDomains"
    referrer_policy: str = "strict-origin-when-cross-origin"
    permissions_policy: Optional[str] = None
    custom_headers: Optional[dict[str, str]] = None


class SecurityHeadersMiddleware:
    """Add security headers to responses.

    Example:
        >>> middleware = SecurityHeadersMiddleware()
        >>> secured_response = middleware.add_headers(response)
    """

    DEFAULT_CONFIG = SecurityHeadersConfig()

    def __init__(self, config: Optional[SecurityHeadersConfig] = None) -> None:
        self.config = config or self.DEFAULT_CONFIG

    def add_headers(
        self,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        """Add security headers to a response.

        Args:
            response: Response dictionary.

        Returns:
            Response with security headers added.
        """
        if "headers" not in response:
            response["headers"] = {}
        headers = response["headers"]
        headers.update(self._get_security_headers())
        return response

    def _get_security_headers(self) -> dict[str, str]:
        """Get all security headers."""
        headers = {}
        if self.config.csp:
            headers["Content-Security-Policy"] = self.config.csp
        headers["X-Frame-Options"] = self.config.x_frame_options
        headers["X-Content-Type-Options"] = self.config.x_content_type_options
        headers["X-XSS-Protection"] = self.config.x_xss_protection
        headers["Strict-Transport-Security"] = self.config.strict_transport_security
        headers["Referrer-Policy"] = self.config.referrer_policy
        if self.config.permissions_policy:
            headers["Permissions-Policy"] = self.config.permissions_policy
        if self.config.custom_headers:
            headers.update(self.config.custom_headers)
        return headers


class ContentSecurityPolicyBuilder:
    """Build Content Security Policy headers.

    Example:
        >>> csp = (
        ...     ContentSecurityPolicyBuilder()
        ...     .default_src("'self'")
        ...     .script_src("'self'", "https://trusted.cdn.com")
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        self._directives: dict[str, list[str]] = {}

    def default_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set default-src directive."""
        self._directives["default-src"] = list(sources)
        return self

    def script_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set script-src directive."""
        self._directives["script-src"] = list(sources)
        return self

    def style_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set style-src directive."""
        self._directives["style-src"] = list(sources)
        return self

    def img_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set img-src directive."""
        self._directives["img-src"] = list(sources)
        return self

    def connect_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set connect-src directive."""
        self._directives["connect-src"] = list(sources)
        return self

    def font_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set font-src directive."""
        self._directives["font-src"] = list(sources)
        return self

    def frame_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set frame-src directive."""
        self._directives["frame-src"] = list(sources)
        return self

    def object_src(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set object-src directive."""
        self._directives["object-src"] = list(sources)
        return self

    def frame_ancestors(self, *sources: str) -> ContentSecurityPolicyBuilder:
        """Set frame-ancestors directive."""
        self._directives["frame-ancestors"] = list(sources)
        return self

    def build(self) -> str:
        """Build the CSP header value.

        Returns:
            CSP header string.
        """
        parts = []
        for directive, sources in self._directives.items():
            parts.append(f"{directive} {' '.join(sources)}")
        return "; ".join(parts)


def get_strict_csp() -> str:
    """Get a strict CSP for APIs.

    Returns:
        Strict CSP string.
    """
    return ContentSecurityPolicyBuilder() \
        .default_src("'none'") \
        .script_src("'none'") \
        .style_src("'self'") \
        .img_src("'self'") \
        .connect_src("'self'") \
        .frame_ancestors("'none'") \
        .build()


def get_permissive_csp() -> str:
    """Get a permissive CSP for development.

    Returns:
        Permissive CSP string.
    """
    return ContentSecurityPolicyBuilder() \
        .default_src("'self'") \
        .script_src("'self'", "'unsafe-inline'", "'unsafe-eval'") \
        .style_src("'self'", "'unsafe-inline'") \
        .img_src("*") \
        .connect_src("*") \
        .frame_ancestors("'self'") \
        .build()
