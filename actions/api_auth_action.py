"""
API Authentication Module.

Provides comprehensive authentication handling including
OAuth2, API keys, Basic Auth, JWT tokens, and session management
for secure API client implementations.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, TypeVar, Union
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import base64
import hashlib
import hmac
import json
import time
import logging
import threading
import secrets

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""
    NONE = auto()
    API_KEY = auto()
    BASIC = auto()
    BEARER = auto()
    OAUTH2 = auto()
    JWT = auto()
    HMAC = auto()
    CUSTOM = auto()


class TokenStatus(Enum):
    """Token status."""
    VALID = auto()
    EXPIRED = auto()
    REVOKED = auto()
    INVALID = auto()


@dataclass
class AuthCredentials:
    """Authentication credentials."""
    auth_type: AuthType
    credentials: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    
    def apply_to_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """Apply credentials to request and return modified headers/params."""
        headers = dict(headers) if headers else {}
        params = dict(params) if params else {}
        
        if self.auth_type == AuthType.API_KEY:
            key_name = self.credentials.get("key_name", "api_key")
            location = self.credentials.get("location", "header")
            
            if location == "header":
                headers[key_name] = self.credentials.get("key", "")
            else:
                params[key_name] = self.credentials.get("key", "")
        
        elif self.auth_type == AuthType.BASIC:
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        
        elif self.auth_type == AuthType.BEARER:
            token = self.credentials.get("token", "")
            headers["Authorization"] = f"Bearer {token}"
        
        elif self.auth_type == AuthType.OAUTH2:
            token = self.credentials.get("access_token", "")
            headers["Authorization"] = f"Bearer {token}"
        
        elif self.auth_type == AuthType.JWT:
            token = self.credentials.get("token", "")
            headers["Authorization"] = f"JWT {token}"
        
        # Merge custom headers and params
        headers.update(self.headers)
        params.update(self.params)
        
        return headers, params


@dataclass
class TokenInfo:
    """Token information and metadata."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    issued_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    
    def __post_init__(self) -> None:
        if self.expires_at is None and self.expires_in:
            self.expires_at = self.issued_at + timedelta(seconds=self.expires_in)
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        if self.expires_at is None:
            return False
        return datetime.now() >= self.expires_at
    
    @property
    def is_expiring_soon(self, buffer_seconds: int = 300) -> bool:
        """Check if token is expiring within buffer time."""
        if self.expires_at is None:
            return False
        return datetime.now() >= (self.expires_at - timedelta(seconds=buffer_seconds))
    
    @property
    def status(self) -> TokenStatus:
        """Get token status."""
        if self.is_expired:
            return TokenStatus.EXPIRED
        return TokenStatus.VALID


class OAuth2Handler:
    """OAuth2 authentication handler."""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        authorization_url: Optional[str] = None,
        redirect_uri: Optional[str] = None
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.authorization_url = authorization_url
        self.redirect_uri = redirect_uri
        self._token: Optional[TokenInfo] = None
        self._refresh_lock = threading.Lock()
    
    @property
    def authorization_header(self) -> Optional[str]:
        """Get authorization header value."""
        if self._token and not self._token.is_expired:
            return f"{self._token.token_type} {self._token.access_token}"
        return None
    
    def get_auth_url(self, scope: Optional[str] = None, state: Optional[str] = None) -> str:
        """Generate authorization URL."""
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri
        }
        
        if scope:
            params["scope"] = scope
        if state:
            params["state"] = state
        
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.authorization_url}?{query}"
    
    def exchange_code_for_token(
        self,
        code: str,
        code_verifier: Optional[str] = None
    ) -> TokenInfo:
        """
        Exchange authorization code for access token.
        
        Note: In production, this would make HTTP request to token_url.
        """
        # Placeholder implementation
        token_info = TokenInfo(
            access_token=f"access_{secrets.token_hex(16)}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"refresh_{secrets.token_hex(16)}",
            scope="read write"
        )
        
        self._token = token_info
        return token_info
    
    def refresh_access_token(self) -> TokenInfo:
        """Refresh the access token using refresh token."""
        if not self._token or not self._token.refresh_token:
            raise ValueError("No refresh token available")
        
        with self._refresh_lock:
            if not self._token.is_expiring_soon:
                return self._token
            
            # Placeholder: would call token_url with grant_type=refresh_token
            self._token = TokenInfo(
                access_token=f"access_{secrets.token_hex(16)}",
                token_type="Bearer",
                expires_in=3600,
                refresh_token=self._token.refresh_token
            )
            
            return self._token
    
    def get_token(self) -> Optional[str]:
        """Get current valid access token."""
        if self._token is None:
            return None
        
        if self._token.is_expired:
            if self._token.refresh_token:
                self.refresh_access_token()
            else:
                return None
        
        return self._token.access_token


class JWTHandler:
    """JWT token handling."""
    
    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        issuer: Optional[str] = None,
        audience: Optional[str] = None
    ) -> None:
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.issuer = issuer
        self.audience = audience
    
    def create_token(
        self,
        payload: Dict[str, Any],
        expires_in: int = 3600,
        subject: Optional[str] = None
    ) -> str:
        """
        Create a JWT token.
        
        Note: In production, use PyJWT library.
        """
        header = {"alg": self.algorithm, "typ": "JWT"}
        
        now = int(time.time())
        claims = {
            "iat": now,
            "exp": now + expires_in,
            **payload
        }
        
        if subject:
            claims["sub"] = subject
        if self.issuer:
            claims["iss"] = self.issuer
        if self.audience:
            claims["aud"] = self.audience
        
        # Placeholder: would properly encode with PyJWT
        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode()
        payload_b64 = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode()
        
        signature = hmac.new(
            self.secret_key.encode(),
            f"{header_b64}.{payload_b64}".encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"{header_b64}.{payload_b64}.{signature}"
    
    def decode_token(self, token: str) -> Tuple[Dict[str, Any], bool]:
        """
        Decode and validate JWT token.
        
        Returns:
            Tuple of (payload, is_valid)
        """
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return {}, False
            
            header_b64, payload_b64, signature = parts
            
            # Verify signature
            expected_sig = hmac.new(
                self.secret_key.encode(),
                f"{header_b64}.{payload_b64}".encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, expected_sig):
                return {}, False
            
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            
            # Check expiration
            if "exp" in payload and payload["exp"] < time.time():
                return payload, False
            
            # Check issuer
            if self.issuer and payload.get("iss") != self.issuer:
                return payload, False
            
            return payload, True
        
        except Exception as e:
            logger.error(f"JWT decode error: {e}")
            return {}, False


class HMACAuthHandler:
    """HMAC-based authentication."""
    
    def __init__(
        self,
        secret_key: str,
        algorithm: str = "sha256",
        include_timestamp: bool = True,
        include_nonce: bool = True
    ) -> None:
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.include_timestamp = include_timestamp
        self.include_nonce = include_nonce
        self._used_nonces: Set[str] = set()
    
    def create_signature(
        self,
        method: str,
        url: str,
        body: Optional[str] = None,
        timestamp: Optional[int] = None,
        nonce: Optional[str] = None
    ) -> Tuple[str, int, str]:
        """
        Create HMAC signature for request.
        
        Returns:
            Tuple of (signature, timestamp, nonce)
        """
        timestamp = timestamp or int(time.time())
        nonce = nonce or secrets.token_hex(8)
        
        message = f"{method.upper()}|{url}|{timestamp}|{nonce}"
        
        if body:
            message += f"|{body}"
        
        algorithm = getattr(hashlib, self.algorithm)
        signature = hmac.new(
            self.secret_key.encode(),
            message.encode(),
            algorithm
        ).hexdigest()
        
        return signature, timestamp, nonce
    
    def create_auth_header(
        self,
        method: str,
        url: str,
        body: Optional[str] = None
    ) -> Dict[str, str]:
        """Create authorization header for request."""
        signature, timestamp, nonce = self.create_signature(method, url, body)
        
        return {
            "X-Auth-Signature": signature,
            "X-Auth-Timestamp": str(timestamp),
            "X-Auth-Nonce": nonce
        }
    
    def verify_signature(
        self,
        method: str,
        url: str,
        signature: str,
        timestamp: int,
        nonce: str,
        body: Optional[str] = None
    ) -> bool:
        """Verify incoming HMAC signature."""
        # Check timestamp freshness (5 minute window)
        now = int(time.time())
        if abs(now - timestamp) > 300:
            return False
        
        # Check nonce hasn't been used (replay protection)
        if nonce in self._used_nonces:
            return False
        
        expected_sig, _, _ = self.create_signature(
            method, url, body, timestamp, nonce
        )
        
        self._used_nonces.add(nonce)
        
        return hmac.compare_digest(signature, expected_sig)


class CompositeAuth:
    """Combines multiple authentication methods."""
    
    def __init__(self) -> None:
        self._auth_handlers: List[Tuple[AuthType, Callable]] = []
    
    def add_auth(
        self,
        auth_type: AuthType,
        handler: Callable[[], Optional[AuthCredentials]]
    ) -> "CompositeAuth":
        """Add an authentication handler."""
        self._auth_handlers.append((auth_type, handler))
        return self
    
    def get_credentials(self) -> AuthCredentials:
        """Get the first valid set of credentials."""
        for auth_type, handler in self._auth_handlers:
            try:
                creds = handler()
                if creds:
                    return creds
            except Exception as e:
                logger.warning(f"Auth handler {auth_type} failed: {e}")
        
        return AuthCredentials(auth_type=AuthType.NONE)


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # API Key example
    api_key_auth = AuthCredentials(
        auth_type=AuthType.API_KEY,
        credentials={"key": "my-api-key", "key_name": "X-API-Key", "location": "header"}
    )
    
    headers, params = api_key_auth.apply_to_request("GET", "/api/data")
    print(f"API Key Headers: {headers}")
    
    # Basic Auth example
    basic_auth = AuthCredentials(
        auth_type=AuthType.BASIC,
        credentials={"username": "user", "password": "pass"}
    )
    
    headers, _ = basic_auth.apply_to_request("GET", "/api/data")
    print(f"Basic Auth Header: {headers.get('Authorization')}")
    
    # JWT example
    jwt_handler = JWTHandler(secret_key="my-secret-key")
    token = jwt_handler.create_token(
        payload={"user_id": 123, "role": "admin"},
        expires_in=3600,
        subject="user@example.com"
    )
    print(f"\nJWT Token: {token[:50]}...")
    
    payload, valid = jwt_handler.decode_token(token)
    print(f"JWT Payload: {payload}")
    print(f"JWT Valid: {valid}")
    
    # HMAC example
    hmac_handler = HMACAuthHandler(secret_key="hmac-secret")
    auth_headers = hmac_handler.create_auth_header("POST", "/api/data", '{"key": "value"}')
    print(f"\nHMAC Headers: {auth_headers}")
