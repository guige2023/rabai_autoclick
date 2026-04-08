"""API auth action module for RabAI AutoClick.

Provides API authentication:
- AuthManager: Manage authentication
- TokenManager: Manage tokens
- APIKeyAuth: API key authentication
- OAuthHelper: OAuth helper
- JWTAuth: JWT token handling
"""

import time
import hashlib
import hmac
import base64
import json
import secrets
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AuthType(Enum):
    """Authentication types."""
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    HMAC = "hmac"


@dataclass
class AuthCredentials:
    """Authentication credentials."""
    auth_type: AuthType
    credentials: Dict[str, Any]
    created_at: float = 0.0
    expires_at: Optional[float] = None


@dataclass
class AuthResult:
    """Authentication result."""
    success: bool
    message: str
    token: Optional[str] = None
    expires_at: Optional[float] = None
    user_info: Optional[Dict] = None


class TokenManager:
    """Manage authentication tokens."""

    def __init__(self):
        self._tokens: Dict[str, AuthCredentials] = {}
        self._refresh_tokens: Dict[str, str] = {}
        self._lock = ...

    def generate_token(
        self,
        user_id: str,
        auth_type: AuthType,
        credentials: Dict,
        expires_in: Optional[float] = None,
    ) -> str:
        """Generate authentication token."""
        token = secrets.token_urlsafe(32)
        expires_at = time.time() + expires_in if expires_in else None

        self._tokens[token] = AuthCredentials(
            auth_type=auth_type,
            credentials=credentials,
            created_at=time.time(),
            expires_at=expires_at,
        )

        return token

    def validate_token(self, token: str) -> Tuple[bool, Optional[AuthCredentials]]:
        """Validate token."""
        creds = self._tokens.get(token)

        if not creds:
            return False, None

        if creds.expires_at and time.time() > creds.expires_at:
            del self._tokens[token]
            return False, None

        return True, creds

    def revoke_token(self, token: str) -> bool:
        """Revoke token."""
        if token in self._tokens:
            del self._tokens[token]
            return True
        return False

    def cleanup_expired(self):
        """Remove expired tokens."""
        now = time.time()
        expired = [t for t, c in self._tokens.items() if c.expires_at and now > c.expires_at]
        for t in expired:
            del self._tokens[t]


class APIKeyAuth:
    """API key authentication."""

    def __init__(self):
        self._api_keys: Dict[str, Dict] = {}

    def create_api_key(self, user_id: str, permissions: Optional[List[str]] = None) -> Tuple[str, str]:
        """Create API key pair."""
        key_id = secrets.token_hex(8)
        key_secret = secrets.token_urlsafe(32)

        self._api_keys[key_id] = {
            "user_id": user_id,
            "secret_hash": hashlib.sha256(key_secret.encode()).hexdigest(),
            "permissions": permissions or [],
            "created_at": time.time(),
        }

        return key_id, key_secret

    def validate_api_key(self, key_id: str, key_secret: str) -> Tuple[bool, Optional[Dict]]:
        """Validate API key."""
        key_info = self._api_keys.get(key_id)

        if not key_info:
            return False, None

        secret_hash = hashlib.sha256(key_secret.encode()).hexdigest()
        if secret_hash != key_info["secret_hash"]:
            return False, None

        return True, key_info

    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke API key."""
        if key_id in self._api_keys:
            del self._api_keys[key_id]
            return True
        return False


class JWTAuth:
    """JWT token handling."""

    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self._valid_tokens: Dict[str, Dict] = {}

    def create_token(
        self,
        payload: Dict[str, Any],
        expires_in: float = 3600.0,
    ) -> str:
        """Create JWT token."""
        import jwt

        header = {"alg": "HS256", "typ": "JWT"}
        payload_copy = dict(payload)
        payload_copy["exp"] = time.time() + expires_in
        payload_copy["iat"] = time.time()

        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip("=")
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload_copy).encode()).decode().rstrip("=")

        signature = hmac.new(
            self.secret_key.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256,
        ).hexdigest()

        token = f"{header_b64}.{payload_b64}.{signature}"
        self._valid_tokens[token] = payload_copy

        return token

    def validate_token(self, token: str) -> Tuple[bool, Optional[Dict]]:
        """Validate JWT token."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return False, None

            header_b64, payload_b64, signature = parts

            expected_signature = hmac.new(
                self.secret_key.encode(),
                f"{header_b64}.{payload_b64}".encode(),
                hashlib.sha256,
            ).hexdigest()

            if not hmac.compare_digest(signature, expected_signature):
                return False, None

            payload_json = base64.urlsafe_b64decode(payload_b64 + "==")
            payload = json.loads(payload_json)

            if payload.get("exp", 0) < time.time():
                return False, None

            return True, payload

        except Exception:
            return False, None

    def decode_token(self, token: str) -> Optional[Dict]:
        """Decode token without validation."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None

            payload_b64 = parts[1]
            payload_json = base64.urlsafe_b64decode(payload_b64 + "==")
            return json.loads(payload_json)
        except Exception:
            return None


class AuthManager:
    """Central authentication manager."""

    def __init__(self):
        self.token_manager = TokenManager()
        self.api_key_auth = APIKeyAuth()
        self._auth_handlers: Dict[AuthType, Callable] = {}

    def register_handler(self, auth_type: AuthType, handler: Callable):
        """Register auth handler."""
        self._auth_handlers[auth_type] = handler

    def authenticate(self, auth_type: AuthType, credentials: Dict) -> AuthResult:
        """Authenticate."""
        handler = self._auth_handlers.get(auth_type)

        if handler:
            try:
                result = handler(credentials)
                return result
            except Exception as e:
                return AuthResult(success=False, message=str(e))

        return AuthResult(success=False, message=f"No handler for {auth_type.value}")


class APIAuthAction(BaseAction):
    """API auth action."""
    action_type = "api_auth"
    display_name = "API认证"
    description = "API认证和授权管理"

    def __init__(self):
        super().__init__()
        self._auth_manager = AuthManager()
        self._jwt_auth = JWTAuth()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "authenticate")

            if operation == "create_api_key":
                return self._create_api_key(params)
            elif operation == "validate_api_key":
                return self._validate_api_key(params)
            elif operation == "create_token":
                return self._create_token(params)
            elif operation == "validate_token":
                return self._validate_token(params)
            elif operation == "revoke_token":
                return self._revoke_token(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")

    def _create_api_key(self, params: Dict) -> ActionResult:
        """Create API key."""
        user_id = params.get("user_id", "anonymous")
        permissions = params.get("permissions", [])

        key_id, key_secret = self._auth_manager.api_key_auth.create_api_key(user_id, permissions)

        return ActionResult(
            success=True,
            message=f"API key created for {user_id}",
            data={
                "key_id": key_id,
                "key_secret": key_secret,
                "user_id": user_id,
            },
        )

    def _validate_api_key(self, params: Dict) -> ActionResult:
        """Validate API key."""
        key_id = params.get("key_id")
        key_secret = params.get("key_secret")

        if not key_id or not key_secret:
            return ActionResult(success=False, message="key_id and key_secret are required")

        valid, info = self._auth_manager.api_key_auth.validate_api_key(key_id, key_secret)

        return ActionResult(
            success=valid,
            message="Valid API key" if valid else "Invalid API key",
            data={"user_id": info.get("user_id") if info else None},
        )

    def _create_token(self, params: Dict) -> ActionResult:
        """Create JWT token."""
        payload = params.get("payload", {})
        expires_in = params.get("expires_in", 3600.0)

        token = self._jwt_auth.create_token(payload, expires_in)

        return ActionResult(
            success=True,
            message=f"Token created, expires in {expires_in}s",
            data={
                "token": token,
                "expires_in": expires_in,
            },
        )

    def _validate_token(self, params: Dict) -> ActionResult:
        """Validate JWT token."""
        token = params.get("token")

        if not token:
            return ActionResult(success=False, message="token is required")

        valid, payload = self._jwt_auth.validate_token(token)

        return ActionResult(
            success=valid,
            message="Valid token" if valid else "Invalid or expired token",
            data={"payload": payload},
        )

    def _revoke_token(self, params: Dict) -> ActionResult:
        """Revoke token."""
        token = params.get("token")

        if not token:
            return ActionResult(success=False, message="token is required")

        success = self._auth_manager.token_manager.revoke_token(token)
        return ActionResult(success=success, message="Token revoked" if success else "Token not found")
