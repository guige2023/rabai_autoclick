"""
API Gateway CORS Action Module.

Handles Cross-Origin Resource Sharing (CORS) preflight requests,
configures allowed origins, methods, headers, and credentials.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class CORSConfig:
    """CORS configuration."""
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    allowed_methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"])
    allowed_headers: list[str] = field(default_factory=lambda: ["Content-Type", "Authorization"])
    exposed_headers: list[str] = field(default_factory=list)
    max_age: int = 86400
    allow_credentials: bool = False


@dataclass
class CORSResult:
    """Result of CORS evaluation."""
    allowed: bool
    response_headers: dict[str, str]
    error: Optional[str] = None


class APIGatewayCORSAction(BaseAction):
    """Handle CORS for API gateway."""

    def __init__(self) -> None:
        super().__init__("api_gateway_cors")
        self._config = CORSConfig()

    def execute(self, context: dict, params: dict) -> dict:
        """
        Evaluate and handle CORS request.

        Args:
            context: Execution context
            params: Parameters:
                - request: Incoming request
                - origin: Origin header value
                - method: Preflight method
                - allowed_origins: Override allowed origins
                - allowed_methods: Override allowed methods
                - allowed_headers: Override allowed headers
                - max_age: Preflight cache duration

        Returns:
            CORSResult with appropriate headers
        """
        request = params.get("request", {})
        headers = request.get("headers", {})
        origin = headers.get("Origin", headers.get("origin", ""))
        access_control_request_method = headers.get("Access-Control-Request-Method", "")
        access_control_request_headers = headers.get("Access-Control-Request-Headers", "")

        allowed_origins = params.get("allowed_origins", self._config.allowed_origins)
        allowed_methods = params.get("allowed_methods", self._config.allowed_methods)
        allowed_headers = params.get("allowed_headers", self._config.allowed_headers)
        max_age = params.get("max_age", self._config.max_age)
        allow_credentials = params.get("allow_credentials", self._config.allow_credentials)

        response_headers: dict[str, str] = {}

        if "*" in allowed_origins:
            response_headers["Access-Control-Allow-Origin"] = origin or "*"
        elif origin and origin in allowed_origins:
            response_headers["Access-Control-Allow-Origin"] = origin
        else:
            return CORSResult(
                allowed=False,
                response_headers={},
                error=f"Origin '{origin}' not allowed"
            )

        if access_control_request_method and access_control_request_method.upper() in [m.upper() for m in allowed_methods]:
            response_headers["Access-Control-Allow-Methods"] = access_control_request_method.upper()
        else:
            response_headers["Access-Control-Allow-Methods"] = ", ".join(allowed_methods)

        if access_control_request_headers:
            req_headers = [h.strip() for h in access_control_request_headers.split(",")]
            response_headers["Access-Control-Allow-Headers"] = ", ".join(req_headers)
        else:
            response_headers["Access-Control-Allow-Headers"] = ", ".join(allowed_headers)

        response_headers["Access-Control-Max-Age"] = str(max_age)

        if allow_credentials:
            response_headers["Access-Control-Allow-Credentials"] = "true"

        if self._config.exposed_headers:
            response_headers["Access-Control-Expose-Headers"] = ", ".join(self._config.exposed_headers)

        return CORSResult(allowed=True, response_headers=response_headers)

    def configure(self, allowed_origins: list[str], allowed_methods: list[str], allowed_headers: list[str],
                  max_age: int = 86400, allow_credentials: bool = False) -> None:
        """Configure CORS settings."""
        self._config.allowed_origins = allowed_origins
        self._config.allowed_methods = allowed_methods
        self._config.allowed_headers = allowed_headers
        self._config.max_age = max_age
        self._config.allow_credentials = allow_credentials
