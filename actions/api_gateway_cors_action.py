"""API Gateway CORS Action Module.

Handles Cross-Origin Resource Sharing (CORS) configuration
and preflight request handling for API gateways.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CORSConfig:
    """CORS configuration."""
    allowed_origins: List[str] = field(default_factory=list)
    allowed_methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    allowed_headers: List[str] = field(default_factory=lambda: ["Content-Type", "Authorization"])
    exposed_headers: List[str] = field(default_factory=list)
    max_age: int = 3600
    allow_credentials: bool = True


class APIGatewayCORSAction(BaseAction):
    """
    API Gateway CORS handling.

    Manages CORS configuration, validates origins,
    and handles preflight requests.

    Example:
        cors = APIGatewayCORSAction()
        result = cors.execute(ctx, {"action": "handle_preflight", "origin": "http://example.com"})
    """
    action_type = "api_gateway_cors"
    display_name = "API网关CORS"
    description = "API网关CORS配置和预检请求处理"

    def __init__(self) -> None:
        super().__init__()
        self._config = CORSConfig(allowed_origins=["*"])

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "handle_preflight":
                return self._handle_preflight(params)
            elif action == "validate_origin":
                return self._validate_origin(params)
            elif action == "set_config":
                return self._set_config(params)
            elif action == "get_config":
                return self._get_config(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"CORS error: {str(e)}")

    def _handle_preflight(self, params: Dict[str, Any]) -> ActionResult:
        origin = params.get("origin", "")
        method = params.get("method", "GET")
        request_headers = params.get("request_headers", [])

        allowed = self._is_origin_allowed(origin)

        if not allowed:
            return ActionResult(success=False, message="Origin not allowed")

        response_headers = {
            "Access-Control-Allow-Origin": origin if origin != "*" else "*",
            "Access-Control-Allow-Methods": ", ".join(self._config.allowed_methods),
            "Access-Control-Allow-Headers": ", ".join(self._config.allowed_headers),
            "Access-Control-Max-Age": str(self._config.max_age),
            "Access-Control-Allow-Credentials": "true" if self._config.allow_credentials else "false",
        }

        return ActionResult(success=True, message="Preflight handled", data={"headers": response_headers, "allowed": True})

    def _validate_origin(self, params: Dict[str, Any]) -> ActionResult:
        origin = params.get("origin", "")
        allowed = self._is_origin_allowed(origin)
        return ActionResult(success=True, data={"origin": origin, "allowed": allowed})

    def _set_config(self, params: Dict[str, Any]) -> ActionResult:
        allowed_origins = params.get("allowed_origins", ["*"])
        allowed_methods = params.get("allowed_methods", ["GET", "POST"])
        allowed_headers = params.get("allowed_headers", ["Content-Type"])
        max_age = params.get("max_age", 3600)
        allow_credentials = params.get("allow_credentials", True)

        self._config = CORSConfig(
            allowed_origins=allowed_origins,
            allowed_methods=allowed_methods,
            allowed_headers=allowed_headers,
            max_age=max_age,
            allow_credentials=allow_credentials,
        )

        return ActionResult(success=True, message="CORS config updated")

    def _get_config(self, params: Dict[str, Any]) -> ActionResult:
        return ActionResult(success=True, data={
            "allowed_origins": self._config.allowed_origins,
            "allowed_methods": self._config.allowed_methods,
            "allowed_headers": self._config.allowed_headers,
            "max_age": self._config.max_age,
            "allow_credentials": self._config.allow_credentials,
        })

    def _is_origin_allowed(self, origin: str) -> bool:
        if "*" in self._config.allowed_origins:
            return True
        return origin in self._config.allowed_origins
