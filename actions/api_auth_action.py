"""
API Auth Action Module.

Provides authentication and authorization mechanisms for API
access including OAuth2, API keys, JWT, and RBAC.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class AuthScheme(Enum):
    """Authentication schemes."""
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    HMAC = "hmac"
    CUSTOM = "custom"


@dataclass
class User:
    """User identity."""
    user_id: str
    username: str
    email: Optional[str] = None
    roles: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Permission:
    """Permission definition."""
    resource: str
    actions: Set[str]  # e.g., {"read", "write", "delete"}


@dataclass
class Role:
    """Role with permissions."""
    name: str
    permissions: Set[Permission] = field(default_factory=set)


@dataclass
class AuthContext:
    """Authentication context."""
    user: Optional[User] = None
    authenticated: bool = False
    scheme: Optional[AuthScheme] = None
    token: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TokenManager:
    """JWT token generation and validation."""

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        token_expiry: int = 3600,
    ) -> None:
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_expiry = token_expiry
        self._revoked_tokens: Set[str] = set()

    def _base64_encode(self, data: bytes) -> str:
        """Base64 URL-safe encoding."""
        import base64
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    def _base64_decode(self, data: str) -> bytes:
        """Base64 URL-safe decoding."""
        import base64
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.urlsafe_b64decode(data)

    def generate_token(
        self,
        user_id: str,
        additional_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate JWT token."""
        import json

        header = {
            "alg": self.algorithm,
            "typ": "JWT",
        }
        payload = {
            "sub": user_id,
            "iat": int(time.time()),
            "exp": int(time.time()) + self.token_expiry,
        }
        if additional_claims:
            payload.update(additional_claims)

        header_encoded = self._base64_encode(json.dumps(header).encode())
        payload_encoded = self._base64_encode(json.dumps(payload).encode())

        message = f"{header_encoded}.{payload_encoded}"
        signature = hmac.new(
            self.secret_key.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()

        return f"{message}.{signature}"

    def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate JWT token and return payload."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None

            header_encoded, payload_encoded, signature = parts

            # Verify signature
            message = f"{header_encoded}.{payload_encoded}"
            expected_signature = hmac.new(
                self.secret_key.encode(),
                message.encode(),
                hashlib.sha256,
            ).hexdigest()

            if not hmac.compare_digest(signature, expected_signature):
                return None

            # Decode payload
            payload = json.loads(self._base64_decode(payload_encoded))

            # Check expiration
            if payload.get("exp", 0) < time.time():
                return None

            # Check if revoked
            if token in self._revoked_tokens:
                return None

            return payload

        except Exception:
            return None

    def revoke_token(self, token: str) -> None:
        """Revoke a token."""
        self._revoked_tokens.add(token)


class APIKeyManager:
    """API key management."""

    def __init__(self) -> None:
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._user_keys: Dict[str, Set[str]] = {}

    def generate_key(
        self,
        user_id: str,
        name: str,
        scopes: Optional[List[str]] = None,
        expires_in: Optional[int] = None,
    ) -> str:
        """Generate new API key."""
        import secrets
        key = f"sk_{secrets.token_urlsafe(32)}"

        self._keys[key] = {
            "user_id": user_id,
            "name": name,
            "scopes": set(scopes or []),
            "created_at": time.time(),
            "expires_at": time.time() + expires_in if expires_in else None,
            "active": True,
        }

        if user_id not in self._user_keys:
            self._user_keys[user_id] = set()
        self._user_keys[user_id].add(key)

        return key

    def validate_key(self, key: str) -> Optional[Dict[str, Any]]:
        """Validate API key."""
        if key not in self._keys:
            return None

        key_info = self._keys[key]

        if not key_info["active"]:
            return None

        if key_info.get("expires_at") and time.time() > key_info["expires_at"]:
            return None

        return key_info

    def revoke_key(self, key: str) -> bool:
        """Revoke an API key."""
        if key in self._keys:
            self._keys[key]["active"] = False
            return True
        return False

    def get_user_keys(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all keys for a user."""
        keys = []
        for key, info in self._keys.items():
            if info["user_id"] == user_id:
                keys.append({"key": key[:10] + "...", **info})
        return keys


class AuthProvider:
    """Main authentication provider."""

    def __init__(self, secret_key: str) -> None:
        self.token_manager = TokenManager(secret_key)
        self.api_key_manager = APIKeyManager()
        self._users: Dict[str, User] = {}
        self._roles: Dict[str, Role] = {}

    def register_user(
        self,
        user_id: str,
        username: str,
        email: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ) -> User:
        """Register a new user."""
        user = User(
            user_id=user_id,
            username=username,
            email=email,
            roles=set(roles or []),
        )
        self._users[user_id] = user
        return user

    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        return self._users.get(user_id)

    def add_role(
        self,
        role_name: str,
        permissions: List[Permission],
    ) -> Role:
        """Add a role."""
        role = Role(name=role_name, permissions=set(permissions))
        self._roles[role_name] = role
        return role

    def authenticate_api_key(self, key: str) -> Optional[AuthContext]:
        """Authenticate using API key."""
        key_info = self.api_key_manager.validate_key(key)
        if not key_info:
            return None

        user = self._users.get(key_info["user_id"])
        if not user:
            return None

        return AuthContext(
            user=user,
            authenticated=True,
            scheme=AuthScheme.API_KEY,
            token=key,
            metadata={"scopes": key_info["scopes"]},
        )

    def authenticate_basic(
        self,
        username: str,
        password: str,
        password_hash: Optional[str] = None,
    ) -> Optional[AuthContext]:
        """Authenticate using basic auth."""
        # In production, verify password hash properly
        user = None
        for u in self._users.values():
            if u.username == username:
                user = u
                break

        if not user:
            return None

        # Verify password (simplified - use proper password hashing in production)
        if password_hash and not hmac.compare_digest(
            hashlib.sha256(password.encode()).hexdigest(),
            password_hash,
        ):
            return None

        return AuthContext(
            user=user,
            authenticated=True,
            scheme=AuthScheme.BASIC,
        )

    def authenticate_bearer(self, token: str) -> Optional[AuthContext]:
        """Authenticate using bearer token."""
        payload = self.token_manager.validate_token(token)
        if not payload:
            return None

        user_id = payload.get("sub")
        user = self._users.get(user_id)
        if not user:
            return None

        return AuthContext(
            user=user,
            authenticated=True,
            scheme=AuthScheme.BEARER,
            token=token,
            metadata=payload,
        )

    def authorize(
        self,
        ctx: AuthContext,
        resource: str,
        action: str,
    ) -> bool:
        """Check if user is authorized for action on resource."""
        if not ctx.authenticated or not ctx.user:
            return False

        # Admin role has all permissions
        if "admin" in ctx.user.roles:
            return True

        # Check user direct permissions
        for role_name in ctx.user.roles:
            role = self._roles.get(role_name)
            if role:
                for perm in role.permissions:
                    if perm.resource == resource and action in perm.actions:
                        return True

        return False


class RBACEnforcer:
    """Role-Based Access Control enforcer."""

    def __init__(self, auth_provider: AuthProvider) -> None:
        self.auth_provider = auth_provider

    def check_permission(
        self,
        ctx: AuthContext,
        permission: Permission,
    ) -> bool:
        """Check if context has permission."""
        return self.auth_provider.authorize(
            ctx,
            permission.resource,
            next(iter(permission.actions)),  # Check first action
        )

    def require_permission(
        self,
        ctx: AuthContext,
        permission: Permission,
    ) -> None:
        """Raise exception if permission denied."""
        if not self.check_permission(ctx, permission):
            raise PermissionError(
                f"Permission denied: {permission.actions} on {permission.resource}"
            )
