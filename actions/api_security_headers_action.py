"""
API Security Headers Action Module.

Provides comprehensive security header management
for API responses including CSP, CORS, and protection headers.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Callable, Union
)
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class SecurityLevel(Enum):
    """Security levels."""
    NONE = "none"
    BASIC = "basic"
    MODERATE = "moderate"
    STRICT = "strict"


@dataclass
class SecurityHeadersConfig:
    """Configuration for security headers."""
    level: SecurityLevel = SecurityLevel.MODERATE
    csp_policy: Optional[str] = None
    cors_origins: List[str] = field(default_factory=list)
    cors_methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"])
    cors_headers: List[str] = field(default_factory=lambda: ["Content-Type", "Authorization"])
    cors_max_age: int = 86400
    hsts_max_age: int = 31536000
    referrer_policy: str = "strict-origin-when-cross-origin"
    content_type_options: str = "nosniff"
    frame_options: str = "DENY"
    permissions_policy: Optional[str] = None
    custom_headers: Dict[str, str] = field(default_factory=dict)


class APISecurityHeadersAction:
    """
    Security Headers Manager.

    Provides comprehensive security header management
    for API responses.

    Example:
        >>> security = APISecurityHeadersAction(level=SecurityLevel.STRICT)
        >>> headers = security.get_headers(request)
    """

    DEFAULT_CSP_BASIC = "default-src 'self'; script-src 'self'; object-src 'none'"
    DEFAULT_CSP_MODERATE = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: https:; "
        "font-src 'self'; "
        "connect-src 'self' https:; "
        "frame-ancestors 'none';"
    )
    DEFAULT_CSP_STRICT = (
        "default-src 'none'; "
        "script-src 'self'; "
        "style-src 'self'; "
        "img-src 'self'; "
        "font-src 'self'; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )

    PERMISSIONS_POLICY_DEFAULT = (
        "accelerometer=(), "
        "camera=(), "
        "geolocation=(), "
        "gyroscope=(), "
        "magnetometer=(), "
        "microphone=(), "
        "payment=(), "
        "usb=()"
    )

    def __init__(self, config: Optional[SecurityHeadersConfig] = None):
        self.config = config or SecurityHeadersConfig()
        self._apply_defaults()

    def _apply_defaults(self) -> None:
        """Apply default security configurations based on level."""
        if self.config.level == SecurityLevel.BASIC:
            if not self.config.csp_policy:
                self.config.csp_policy = self.DEFAULT_CSP_BASIC

        elif self.config.level == SecurityLevel.MODERATE:
            if not self.config.csp_policy:
                self.config.csp_policy = self.DEFAULT_CSP_MODERATE

        elif self.config.level == SecurityLevel.STRICT:
            if not self.config.csp_policy:
                self.config.csp_policy = self.DEFAULT_CSP_STRICT

    def get_headers(
        self,
        request: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """
        Get security headers for a request.

        Args:
            request: Optional request context

        Returns:
            Dictionary of security headers
        """
        headers = {}

        headers.update(self._get_protection_headers())
        headers.update(self._get_content_security_headers())
        headers.update(self._get_cors_headers(request))
        headers.update(self._get_miscellaneous_headers())

        headers.update(self.config.custom_headers)

        return headers

    def _get_protection_headers(self) -> Dict[str, str]:
        """Get basic protection headers."""
        return {
            "X-Content-Type-Options": self.config.content_type_options,
            "X-Frame-Options": self.config.frame_options,
            "X-XSS-Protection": "1; mode=block",
            "X-Permitted-Cross-Domain-Policies": "none",
            "Referrer-Policy": self.config.referrer_policy,
        }

    def _get_content_security_headers(self) -> Dict[str, str]:
        """Get content security headers."""
        headers = {}

        if self.config.csp_policy:
            headers["Content-Security-Policy"] = self.config.csp_policy

        headers["Content-Security-Policy-Report-Only"] = (
            "default-src 'none'; report-uri /csp-report"
        )

        return headers

    def _get_cors_headers(
        self,
        request: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Get CORS headers."""
        headers = {}

        if not self.config.cors_origins:
            return headers

        origin = None
        if request:
            origin = request.get("headers", {}).get("Origin")

        if origin and self._is_origin_allowed(origin):
            headers["Access-Control-Allow-Origin"] = origin
            headers["Access-Control-Allow-Credentials"] = "true"

        headers["Access-Control-Allow-Methods"] = ", ".join(self.config.cors_methods)
        headers["Access-Control-Allow-Headers"] = ", ".join(self.config.cors_headers)
        headers["Access-Control-Max-Age"] = str(self.config.cors_max_age)

        return headers

    def _is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed."""
        if "*" in self.config.cors_origins:
            return True

        if origin in self.config.cors_origins:
            return True

        for allowed in self.config.cors_origins:
            if allowed.startswith("*."):
                domain = allowed[2:]
                if origin.endswith(domain) and origin.startswith("https://"):
                    return True

        return False

    def _get_miscellaneous_headers(self) -> Dict[str, str]:
        """Get miscellaneous security headers."""
        headers = {}

        headers["Strict-Transport-Security"] = (
            f"max-age={self.config.hsts_max_age}; "
            "includeSubDomains; "
            "preload"
        )

        if self.config.permissions_policy:
            headers["Permissions-Policy"] = self.config.permissions_policy
        else:
            headers["Permissions-Policy"] = self.PERMISSIONS_POLICY_DEFAULT

        headers["Cross-Origin-Embedder-Policy"] = "require-corp"
        headers["Cross-Origin-Opener-Policy"] = "same-origin"
        headers["Cross-Origin-Resource-Policy"] = "same-origin"

        return headers

    def add_custom_header(self, name: str, value: str) -> None:
        """
        Add a custom security header.

        Args:
            name: Header name
            value: Header value
        """
        self.config.custom_headers[name] = value

    def remove_custom_header(self, name: str) -> None:
        """Remove a custom header."""
        self.config.custom_headers.pop(name, None)

    def get_report_uri(self) -> str:
        """Get CSP report URI."""
        return "/csp-report"

    def validate_csp(self, csp_policy: str) -> bool:
        """
        Validate a CSP policy string.

        Args:
            csp_policy: CSP policy to validate

        Returns:
            True if valid
        """
        valid_directives = {
            "default-src", "script-src", "style-src", "img-src",
            "font-src", "connect-src", "media-src", "object-src",
            "frame-src", "frame-ancestors", "base-uri", "form-action",
            "upgrade-insecure-requests", "block-all-mixed-content",
        }

        valid_keywords = {
            "'self'", "'none'", "'unsafe-inline'", "'unsafe-eval'",
            "'strict-dynamic'", "'report-sample'", "'wasm-unsafe-eval'",
            "data:", "https:", "http:", "blob:",
        }

        parts = csp_policy.split(";")
        for part in parts:
            part = part.strip()
            if not part:
                continue

            tokens = part.split()
            if not tokens:
                return False

            directive = tokens[0]
            if directive not in valid_directives:
                return False

        return True

    def get_security_score(self) -> int:
        """
        Calculate a security score (0-100).

        Returns:
            Security score
        """
        score = 0

        if self.config.csp_policy:
            score += 30

        if self.config.cors_origins:
            score += 15

        score += 10

        if self.config.hsts_max_age >= 31536000:
            score += 15

        if self.config.frame_options == "DENY":
            score += 10

        if self.config.content_type_options == "nosniff":
            score += 10

        score += 10

        return min(score, 100)


class SecurityHeadersMiddleware:
    """Middleware for adding security headers to responses."""

    def __init__(self, config: Optional[SecurityHeadersConfig] = None):
        self.action = APISecurityHeadersAction(config)

    async def __call__(self, request, call_next):
        """Process request and add security headers."""
        response = await call_next(request)

        security_headers = self.action.get_headers(request)

        if hasattr(response, "headers"):
            response.headers.update(security_headers)
        elif isinstance(response, dict):
            response["headers"] = {**response.get("headers", {}), **security_headers}

        return response
