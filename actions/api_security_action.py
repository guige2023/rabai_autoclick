"""
API Security Action Module.

Provides API security features including
authentication, authorization, and input validation.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import hmac
import logging
import secrets
import time

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    JWT = "jwt"


class Permission(Enum):
    """Permission levels."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


@dataclass
class User:
    """User account."""
    user_id: str
    username: str
    password_hash: str
    permissions: Set[Permission] = field(default_factory=set)
    api_keys: List[str] = field(default_factory=list)
    is_active: bool = True


@dataclass
class AuthToken:
    """Authentication token."""
    token: str
    user_id: str
    created_at: datetime
    expires_at: datetime
    permissions: Set[Permission]


@dataclass
class AuthResult:
    """Result of authentication."""
    success: bool
    user: Optional[User] = None
    token: Optional[AuthToken] = None
    error: Optional[str] = None


class PasswordHasher:
    """Handles password hashing."""

    @staticmethod
    def hash(password: str, salt: Optional[str] = None) -> str:
        """Hash a password."""
        if salt is None:
            salt = secrets.token_hex(16)

        hash_obj = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        )
        return f"{salt}${hash_obj.hex()}"

    @staticmethod
    def verify(password: str, hashed: str) -> bool:
        """Verify password against hash."""
        try:
            salt, hash_hex = hashed.split('$')
            expected = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode('utf-8'),
                salt.encode('utf-8'),
                100000
            ).hex()
            return hmac.compare_digest(expected, hash_hex)
        except:
            return False


class APIKeyManager:
    """Manages API keys."""

    def __init__(self):
        self.api_keys: Dict[str, str] = {}

    def generate_key(self, user_id: str) -> str:
        """Generate new API key."""
        key = f"sk_{secrets.token_urlsafe(32)}"
        self.api_keys[key] = user_id
        return key

    def validate_key(self, key: str) -> Optional[str]:
        """Validate API key and return user ID."""
        return self.api_keys.get(key)

    def revoke_key(self, key: str) -> bool:
        """Revoke an API key."""
        if key in self.api_keys:
            del self.api_keys[key]
            return True
        return False


class JWTHandler:
    """Handles JWT tokens."""

    def __init__(self, secret: str, expiry_seconds: int = 3600):
        self.secret = secret
        self.expiry_seconds = expiry_seconds

    def create_token(self, user_id: str, permissions: Set[Permission]) -> AuthToken:
        """Create JWT token."""
        import base64
        import json

        now = datetime.now()
        expires = now + timedelta(seconds=self.expiry_seconds)

        header = {"alg": "HS256", "typ": "JWT"}
        payload = {
            "sub": user_id,
            "perms": [p.value for p in permissions],
            "iat": int(now.timestamp()),
            "exp": int(expires.timestamp())
        }

        header_b64 = base64.urlsafe_b64encode(
            json.dumps(header).encode()
        ).decode().rstrip('=')
        payload_b64 = base64.urlsafe_b64encode(
            json.dumps(payload).encode()
        ).decode().rstrip('=')

        signature = hmac.new(
            self.secret.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256
        ).hexdigest()

        token_str = f"{header_b64}.{payload_b64}.{signature}"

        return AuthToken(
            token=token_str,
            user_id=user_id,
            created_at=now,
            expires_at=expires,
            permissions=permissions
        )

    def validate_token(self, token: str) -> Optional[AuthToken]:
        """Validate JWT token."""
        import base64
        import json

        try:
            parts = token.split('.')
            if len(parts) != 3:
                return None

            header_b64, payload_b64, signature = parts

            expected_sig = hmac.new(
                self.secret.encode(),
                f"{header_b64}.{payload_b64}".encode(),
                hashlib.sha256
            ).hexdigest()

            if not hmac.compare_digest(signature, expected_sig):
                return None

            payload = json.loads(
                base64.urlsafe_b64decode(payload_b64 + '==')
            )

            if payload["exp"] < int(time.time()):
                return None

            return AuthToken(
                token=token,
                user_id=payload["sub"],
                created_at=datetime.fromtimestamp(payload["iat"]),
                expires_at=datetime.fromtimestamp(payload["exp"]),
                permissions={Permission(p) for p in payload.get("perms", [])}
            )

        except Exception as e:
            logger.error(f"JWT validation error: {e}")
            return None


class AuthManager:
    """Main authentication manager."""

    def __init__(self):
        self.users: Dict[str, User] = {}
        self.api_key_manager = APIKeyManager()
        self.jwt_handler: Optional[JWTHandler] = None
        self.active_tokens: Dict[str, AuthToken] = {}

    def add_user(
        self,
        user_id: str,
        username: str,
        password: str,
        permissions: Optional[Set[Permission]] = None
    ) -> User:
        """Add a new user."""
        password_hash = PasswordHasher.hash(password)
        user = User(
            user_id=user_id,
            username=username,
            password_hash=password_hash,
            permissions=permissions or {Permission.READ}
        )
        self.users[user_id] = user
        self.users[username] = user
        return user

    def authenticate_basic(
        self,
        username: str,
        password: str
    ) -> AuthResult:
        """Authenticate with username/password."""
        user = self.users.get(username)

        if not user or not user.is_active:
            return AuthResult(success=False, error="Invalid credentials")

        if not PasswordHasher.verify(password, user.password_hash):
            return AuthResult(success=False, error="Invalid credentials")

        if self.jwt_handler:
            token = self.jwt_handler.create_token(user.user_id, user.permissions)
            self.active_tokens[token.token] = token
            return AuthResult(success=True, user=user, token=token)

        return AuthResult(success=True, user=user)

    def authenticate_api_key(self, key: str) -> AuthResult:
        """Authenticate with API key."""
        user_id = self.api_key_manager.validate_key(key)

        if not user_id:
            return AuthResult(success=False, error="Invalid API key")

        user = self.users.get(user_id)

        if not user or not user.is_active:
            return AuthResult(success=False, error="User not found")

        return AuthResult(success=True, user=user)

    def check_permission(self, token: str, required: Permission) -> bool:
        """Check if token has permission."""
        auth_token = self.active_tokens.get(token)

        if not auth_token:
            return False

        if auth_token.expires_at < datetime.now():
            return False

        return required in auth_token.permissions or Permission.ADMIN in auth_token.permissions


def main():
    """Demonstrate API security."""
    manager = AuthManager()

    manager.add_user("1", "admin", "password123", {Permission.ADMIN})

    result = manager.authenticate_basic("admin", "password123")
    print(f"Auth success: {result.success}")

    if result.token:
        print(f"Token: {result.token.token[:20]}...")


if __name__ == "__main__":
    main()
