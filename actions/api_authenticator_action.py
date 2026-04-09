"""
API Authenticator Action Module

Token-based authentication, JWT validation, OAuth2 flows,
API key management, and session handling.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""
    
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class TokenInfo:
    """Token metadata and claims."""
    
    token_id: str
    token_type: str
    user_id: Optional[str] = None
    scopes: List[str] = field(default_factory=list)
    issued_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    refresh_token: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthConfig:
    """Configuration for authentication."""
    
    auth_type: AuthType = AuthType.BEARER_TOKEN
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    token_ttl_seconds: int = 3600
    refresh_token_ttl_days: int = 30
    require_https: bool = True
    allowed_origins: List[str] = field(default_factory=list)


class TokenStore:
    """In-memory token storage."""
    
    def __init__(self):
        self._tokens: Dict[str, TokenInfo] = {}
        self._refresh_tokens: Dict[str, TokenInfo] = {}
        self._api_keys: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
    
    async def store_token(self, info: TokenInfo) -> None:
        """Store a token."""
        async with self._lock:
            self._tokens[info.token_id] = info
    
    async def get_token(self, token_id: str) -> Optional[TokenInfo]:
        """Get token info."""
        return self._tokens.get(token_id)
    
    async def revoke_token(self, token_id: str) -> bool:
        """Revoke a token."""
        async with self._lock:
            if token_id in self._tokens:
                del self._tokens[token_id]
                return True
            return False
    
    async def store_api_key(self, key: str, info: Dict) -> None:
        """Store an API key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        async with self._lock:
            self._api_keys[key_hash] = info
    
    async def get_api_key(self, key: str) -> Optional[Dict]:
        """Get API key info."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return self._api_keys.get(key_hash)
    
    async def cleanup_expired(self) -> int:
        """Remove expired tokens."""
        count = 0
        now = time.time()
        async with self._lock:
            expired = [
                tid for tid, info in self._tokens.items()
                if info.expires_at and info.expires_at < now
            ]
            for tid in expired:
                del self._tokens[tid]
                count += 1
        return count


class JWTAuthenticator:
    """JWT token creation and validation."""
    
    def __init__(self, secret: str, algorithm: str = "HS256"):
        self.secret = secret
        self.algorithm = algorithm
    
    def create_token(
        self,
        user_id: str,
        scopes: List[str],
        ttl_seconds: int,
        extra_claims: Optional[Dict] = None
    ) -> str:
        """Create a JWT token."""
        import jwt
        
        now = time.time()
        payload = {
            "sub": user_id,
            "scopes": scopes,
            "iat": now,
            "exp": now + ttl_seconds,
            "jti": str(uuid.uuid4())
        }
        
        if extra_claims:
            payload.update(extra_claims)
        
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)
    
    def validate_token(self, token: str) -> Optional[Dict]:
        """Validate a JWT token."""
        import jwt
        
        try:
            payload = jwt.decode(
                token,
                self.secret,
                algorithms=[self.algorithm]
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"JWT validation failed: {e}")
            return None
    
    def refresh_token(self, token: str, ttl_seconds: int) -> Optional[str]:
        """Refresh a JWT token."""
        payload = self.validate_token(token)
        if not payload:
            return None
        
        return self.create_token(
            user_id=payload.get("sub"),
            scopes=payload.get("scopes", []),
            ttl_seconds=ttl_seconds
        )


class APIKeyGenerator:
    """API key generation and validation."""
    
    @staticmethod
    def generate(prefix: str = "sk", length: int = 32) -> str:
        """Generate an API key."""
        import secrets
        key_body = secrets.token_urlsafe(length)
        return f"{prefix}_{key_body}"
    
    @staticmethod
    def validate_format(key: str, prefix: str = "sk") -> bool:
        """Validate API key format."""
        return key.startswith(f"{prefix}_") and len(key) > len(prefix) + 10
    
    @staticmethod
    def generate_signature(method: str, path: str, timestamp: str, body: str, secret: str) -> str:
        """Generate request signature for API key auth."""
        message = f"{method}{path}{timestamp}{body}"
        signature = hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature


class APIAuthenticatorAction:
    """
    Main API authenticator action handler.
    
    Provides multiple authentication methods with
    token management and authorization checks.
    """
    
    def __init__(self, config: Optional[AuthConfig] = None):
        self.config = config or AuthConfig()
        self.store = TokenStore()
        self._jwt_auth: Optional[JWTAuthenticator] = None
        
        if self.config.jwt_secret:
            self._jwt_auth = JWTAuthenticator(
                self.config.jwt_secret,
                self.config.jwt_algorithm
            )
        
        self._middleware: List[Callable] = []
        self._auth_handlers: Dict[AuthType, Callable] = {}
    
    def register_handler(self, auth_type: AuthType, handler: Callable) -> None:
        """Register a custom authentication handler."""
        self._auth_handlers[auth_type] = handler
    
    async def authenticate(
        self,
        auth_type: AuthType,
        credentials: Dict
    ) -> Optional[TokenInfo]:
        """Authenticate with given credentials."""
        if auth_type == AuthType.JWT:
            return await self._authenticate_jwt(credentials)
        elif auth_type == AuthType.API_KEY:
            return await self._authenticate_api_key(credentials)
        elif auth_type == AuthType.BEARER_TOKEN:
            return await self._authenticate_bearer(credentials)
        
        handler = self._auth_handlers.get(auth_type)
        if handler:
            return await handler(credentials)
        
        return None
    
    async def _authenticate_jwt(self, credentials: Dict) -> Optional[TokenInfo]:
        """Authenticate using JWT."""
        token = credentials.get("token")
        if not token or not self._jwt_auth:
            return None
        
        payload = self._jwt_auth.validate_token(token)
        if not payload:
            return None
        
        return TokenInfo(
            token_id=payload.get("jti", str(uuid.uuid4())),
            token_type="jwt",
            user_id=payload.get("sub"),
            scopes=payload.get("scopes", []),
            issued_at=payload.get("iat"),
            expires_at=payload.get("exp"),
            metadata=payload
        )
    
    async def _authenticate_api_key(self, credentials: Dict) -> Optional[TokenInfo]:
        """Authenticate using API key."""
        api_key = credentials.get("api_key")
        if not api_key:
            return None
        
        key_info = await self.store.get_api_key(api_key)
        if not key_info:
            return None
        
        return TokenInfo(
            token_id=api_key[:16],
            token_type="api_key",
            user_id=key_info.get("user_id"),
            scopes=key_info.get("scopes", []),
            metadata=key_info
        )
    
    async def _authenticate_bearer(self, credentials: Dict) -> Optional[TokenInfo]:
        """Authenticate using bearer token."""
        token = credentials.get("token")
        if not token:
            return None
        
        return await self.store.get_token(token)
    
    async def create_token(
        self,
        user_id: str,
        scopes: List[str] = None,
        auth_type: AuthType = AuthType.BEARER_TOKEN,
        extra_claims: Optional[Dict] = None
    ) -> TokenInfo:
        """Create a new authentication token."""
        scopes = scopes or []
        
        if auth_type == AuthType.JWT and self._jwt_auth:
            token = self._jwt_auth.create_token(
                user_id,
                scopes,
                self.config.token_ttl_seconds,
                extra_claims
            )
            token_id = str(uuid.uuid4())
        else:
            import secrets
            token = f"{self.config.auth_type.value}_{secrets.token_urlsafe(32)}"
            token_id = token
        
        info = TokenInfo(
            token_id=token_id,
            token_type=auth_type.value,
            user_id=user_id,
            scopes=scopes,
            issued_at=time.time(),
            expires_at=time.time() + self.config.token_ttl_seconds
        )
        
        await self.store.store_token(info)
        
        return info
    
    async def create_api_key(
        self,
        user_id: str,
        name: str,
        scopes: List[str] = None
    ) -> str:
        """Create a new API key."""
        api_key = APIKeyGenerator.generate()
        
        await self.store.store_api_key(api_key, {
            "user_id": user_id,
            "name": name,
            "scopes": scopes or [],
            "created_at": time.time()
        })
        
        return api_key
    
    async def revoke_token(self, token_id: str) -> bool:
        """Revoke a token."""
        return await self.store.revoke_token(token_id)
    
    def validate_scopes(
        self,
        token_info: TokenInfo,
        required_scopes: List[str]
    ) -> bool:
        """Validate that token has required scopes."""
        if not required_scopes:
            return True
        
        token_scopes = set(token_info.scopes)
        for scope in required_scopes:
            if scope not in token_scopes:
                return False
        return True
    
    async def middleware(
        self,
        request: Dict,
        required_scopes: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """Authentication middleware for requests."""
        headers = request.get("headers", {})
        
        auth_header = headers.get("Authorization", "")
        
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            credentials = {"token": token}
            auth_type = AuthType.BEARER_TOKEN
        elif auth_header.startswith("Basic "):
            import base64
            try:
                decoded = base64.b64decode(auth_header[6:]).decode()
                username, password = decoded.split(":", 1)
                credentials = {"username": username, "password": password}
                auth_type = AuthType.BASIC
            except Exception:
                return None
        else:
            api_key = headers.get("X-API-Key") or headers.get("api_key")
            if api_key:
                credentials = {"api_key": api_key}
                auth_type = AuthType.API_KEY
            else:
                return None
        
        token_info = await self.authenticate(auth_type, credentials)
        
        if not token_info:
            return None
        
        if token_info.expires_at and token_info.expires_at < time.time():
            return None
        
        if required_scopes and not self.validate_scopes(token_info, required_scopes):
            return None
        
        request["auth"] = token_info
        
        for mw in self._middleware:
            await mw(request, token_info)
        
        return token_info
    
    def add_middleware(self, func: Callable) -> None:
        """Add post-authentication middleware."""
        self._middleware.append(func)
    
    async def cleanup(self) -> int:
        """Clean up expired tokens."""
        return await self.store.cleanup_expired()
