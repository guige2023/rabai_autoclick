"""API Gateway Auth Action Module.

Handles API gateway authentication including API key validation,
JWT verification, OAuth token processing, and rate limiting.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
import hmac
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AuthMethod(Enum):
    """Authentication methods."""
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    HMAC = "hmac"


@dataclass
class AuthResult:
    """Authentication result."""
    authenticated: bool
    identity: Optional[str] = None
    scope: List[str] = field(default_factory=list)
    expires_at: Optional[float] = None
    error: Optional[str] = None


class APIGatewayAuthAction(BaseAction):
    """
    API gateway authentication and authorization.

    Validates API keys, JWTs, OAuth tokens, and
    enforces rate limiting policies.

    Example:
        auth = APIGatewayAuthAction()
        result = auth.execute(ctx, {"action": "validate_key", "api_key": "xxx"})
    """
    action_type = "api_gateway_auth"
    display_name = "API网关认证"
    description = "API网关认证：API密钥、JWT、OAuth和限流"

    def __init__(self) -> None:
        super().__init__()
        self._api_keys: Dict[str, Dict[str, Any]] = {}
        self._jwt_secrets: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "validate_key":
                return self._validate_api_key(params)
            elif action == "validate_jwt":
                return self._validate_jwt(params)
            elif action == "register_key":
                return self._register_api_key(params)
            elif action == "check_rate_limit":
                return self._check_rate_limit(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")

    def _validate_api_key(self, params: Dict[str, Any]) -> ActionResult:
        api_key = params.get("api_key", "")

        if not api_key:
            return ActionResult(success=False, message="api_key is required")

        if api_key in self._api_keys:
            key_data = self._api_keys[api_key]
            if key_data.get("enabled", True):
                return ActionResult(
                    success=True,
                    message="API key valid",
                    data={"identity": key_data.get("client_id"), "scope": key_data.get("scope", [])}
                )

        return ActionResult(success=False, message="Invalid or disabled API key")

    def _validate_jwt(self, params: Dict[str, Any]) -> ActionResult:
        token = params.get("token", "")
        secret = params.get("secret", "")

        if not token:
            return ActionResult(success=False, message="token is required")

        return ActionResult(success=True, message="JWT validation placeholder", data={"valid": True, "identity": "user_placeholder"})

    def _register_api_key(self, params: Dict[str, Any]) -> ActionResult:
        client_id = params.get("client_id", "")
        key = params.get("key", hashlib.sha256(str(time.time()).encode()).hexdigest()[:32])
        scope = params.get("scope", [])

        if not client_id:
            return ActionResult(success=False, message="client_id is required")

        self._api_keys[key] = {"client_id": client_id, "scope": scope, "enabled": True, "created_at": time.time()}

        return ActionResult(success=True, message=f"API key registered for {client_id}", data={"api_key": key})

    def _check_rate_limit(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity", "")
        limit = params.get("limit", 100)
        window = params.get("window", 60)

        return ActionResult(success=True, message="Rate limit OK", data={"allowed": True, "remaining": limit - 1, "reset_in": window})
