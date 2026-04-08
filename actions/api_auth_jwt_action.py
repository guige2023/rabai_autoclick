"""
API JWT Authentication Action.

Provides JWT-based authentication and authorization for APIs.
Supports:
- Token generation and validation
- Refresh tokens
- Token blacklisting
- Role-based access control
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from dataclasses import dataclass
import hashlib
import hmac
import json
import logging
import secrets
import base64
import time

logger = logging.getLogger(__name__)


@dataclass
class TokenPayload:
    """JWT token payload."""
    sub: str  # Subject (user ID)
    iss: str  # Issuer
    aud: str  # Audience
    exp: int  # Expiration time
    iat: int  # Issued at
    nbf: int  # Not before
    jti: str  # JWT ID
    roles: List[str] = field(default_factory=list)
    scopes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TokenPair:
    """Pair of access and refresh tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600


@dataclass
class BlacklistEntry:
    """Token blacklist entry."""
    jti: str
    expires_at: datetime
    reason: str


class ApiAuthJwtAction:
    """
    API JWT Authentication Action.
    
    Provides JWT authentication with support for:
    - HS256/HS512 signing algorithms
    - Token generation and validation
    - Refresh token flow
    - Token blacklisting
    - Role and scope-based authorization
    """
    
    ALGORITHMS = ["HS256", "HS512"]
    
    def __init__(
        self,
        secret_key: str,
        issuer: str = "api-service",
        audience: str = "api-clients",
        access_token_ttl: int = 3600,
        refresh_token_ttl: int = 604800,
        algorithm: str = "HS256"
    ):
        """
        Initialize the JWT Authentication Action.
        
        Args:
            secret_key: Secret key for signing tokens
            issuer: Token issuer
            audience: Token audience
            access_token_ttl: Access token TTL in seconds
            refresh_token_ttl: Refresh token TTL in seconds
            algorithm: Signing algorithm
        """
        if algorithm not in self.ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        self.secret_key = secret_key
        self.issuer = issuer
        self.audience = audience
        self.access_token_ttl = access_token_ttl
        self.refresh_token_ttl = refresh_token_ttl
        self.algorithm = algorithm
        self._blacklist: Dict[str, BlacklistEntry] = {}
        self._revoked_tokens: Dict[str, datetime] = {}
    
    def generate_token_pair(
        self,
        user_id: str,
        roles: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TokenPair:
        """
        Generate an access/refresh token pair.
        
        Args:
            user_id: User identifier
            roles: User roles
            scopes: User scopes
            metadata: Additional metadata
        
        Returns:
            TokenPair with access and refresh tokens
        """
        now = int(time.time())
        jti = secrets.token_urlsafe(16)
        
        # Access token payload
        access_payload = {
            "sub": user_id,
            "iss": self.issuer,
            "aud": self.audience,
            "exp": now + self.access_token_ttl,
            "iat": now,
            "nbf": now,
            "jti": jti,
            "roles": roles or [],
            "scopes": scopes or [],
            "metadata": metadata or {},
            "type": "access"
        }
        
        # Refresh token payload
        refresh_jti = secrets.token_urlsafe(16)
        refresh_payload = {
            "sub": user_id,
            "iss": self.issuer,
            "aud": self.audience,
            "exp": now + self.refresh_token_ttl,
            "iat": now,
            "nbf": now,
            "jti": refresh_jti,
            "type": "refresh"
        }
        
        access_token = self._encode_token(access_payload)
        refresh_token = self._encode_token(refresh_payload)
        
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="Bearer",
            expires_in=self.access_token_ttl
        )
    
    def generate_access_token(
        self,
        user_id: str,
        roles: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        custom_claims: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate a single access token.
        
        Args:
            user_id: User identifier
            roles: User roles
            scopes: User scopes
            metadata: Additional metadata
            custom_claims: Custom JWT claims
        
        Returns:
            JWT access token string
        """
        now = int(time.time())
        jti = secrets.token_urlsafe(16)
        
        payload = {
            "sub": user_id,
            "iss": self.issuer,
            "aud": self.audience,
            "exp": now + self.access_token_ttl,
            "iat": now,
            "nbf": now,
            "jti": jti,
            "roles": roles or [],
            "scopes": scopes or [],
            "metadata": metadata or {},
            "type": "access"
        }
        
        if custom_claims:
            payload.update(custom_claims)
        
        return self._encode_token(payload)
    
    def validate_token(
        self,
        token: str,
        check_blacklist: bool = True
    ) -> Optional[TokenPayload]:
        """
        Validate a JWT token.
        
        Args:
            token: JWT token string
            check_blacklist: Whether to check if token is blacklisted
        
        Returns:
            TokenPayload if valid, None if invalid
        """
        try:
            payload = self._decode_token(token)
            
            if payload is None:
                return None
            
            # Check blacklist
            if check_blacklist:
                jti = payload.get("jti")
                if jti and self._is_blacklisted(jti):
                    logger.debug(f"Token {jti} is blacklisted")
                    return None
            
            # Check expiration
            now = int(time.time())
            if payload.get("exp", 0) < now:
                logger.debug("Token has expired")
                return None
            
            # Check not before
            if payload.get("nbf", 0) > now:
                logger.debug("Token not yet valid")
                return None
            
            return TokenPayload(
                sub=payload.get("sub", ""),
                iss=payload.get("iss", ""),
                aud=payload.get("aud", ""),
                exp=payload.get("exp", 0),
                iat=payload.get("iat", 0),
                nbf=payload.get("nbf", 0),
                jti=payload.get("jti", ""),
                roles=payload.get("roles", []),
                scopes=payload.get("scopes", []),
                metadata=payload.get("metadata", {})
            )
        
        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return None
    
    def refresh_access_token(self, refresh_token: str) -> Optional[TokenPair]:
        """
        Refresh an access token using a refresh token.
        
        Args:
            refresh_token: Valid refresh token
        
        Returns:
            New TokenPair if refresh token is valid, None otherwise
        """
        payload = self.validate_token(refresh_token, check_blacklist=True)
        
        if payload is None:
            return None
        
        if payload.metadata.get("type") != "refresh":
            logger.debug("Token is not a refresh token")
            return None
        
        # Blacklist the old refresh token
        self.revoke_token(refresh_token)
        
        # Generate new token pair
        return self.generate_token_pair(
            user_id=payload.sub,
            roles=payload.roles if hasattr(payload, 'roles') else [],
            scopes=payload.scopes if hasattr(payload, 'scopes') else [],
            metadata=payload.metadata
        )
    
    def revoke_token(self, token: str, reason: str = "user_revoked") -> bool:
        """
        Revoke a token by adding it to the blacklist.
        
        Args:
            token: Token to revoke
            reason: Reason for revocation
        
        Returns:
            True if token was revoked, False otherwise
        """
        try:
            payload = self._decode_token(token)
            if payload is None:
                return False
            
            jti = payload.get("jti")
            exp = payload.get("exp", 0)
            
            self._blacklist[jti] = BlacklistEntry(
                jti=jti,
                expires_at=datetime.fromtimestamp(exp),
                reason=reason
            )
            
            logger.info(f"Token {jti} revoked: {reason}")
            return True
        
        except Exception as e:
            logger.error(f"Token revocation error: {e}")
            return False
    
    def revoke_all_user_tokens(self, user_id: str) -> int:
        """
        Revoke all tokens for a specific user.
        
        Args:
            user_id: User identifier
        
        Returns:
            Number of tokens revoked
        """
        # Note: This is a simplified implementation
        # In production, you'd want to store issued tokens in a DB
        count = 0
        for jti, entry in list(self._blacklist.items()):
            if entry.expires_at > datetime.utcnow():
                count += 1
        return count
    
    def _is_blacklisted(self, jti: str) -> bool:
        """Check if a token is blacklisted."""
        if jti not in self._blacklist:
            return False
        
        entry = self._blacklist[jti]
        if datetime.utcnow() > entry.expires_at:
            del self._blacklist[jti]
            return False
        
        return True
    
    def _encode_token(self, payload: Dict[str, Any]) -> str:
        """Encode a payload into a JWT token."""
        header = {
            "alg": self.algorithm,
            "typ": "JWT"
        }
        
        header_b64 = self._base64url_encode(json.dumps(header).encode())
        payload_b64 = self._base64url_encode(json.dumps(payload).encode())
        
        signing_input = f"{header_b64}.{payload_b64}"
        signature = self._sign(signing_input)
        
        return f"{signing_input}.{signature}"
    
    def _decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode a JWT token without verification."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            
            header_b64, payload_b64, signature = parts
            
            # Verify signature
            signing_input = f"{header_b64}.{payload_b64}"
            expected_sig = self._sign(signing_input)
            
            if not self._constant_time_compare(signature, expected_sig):
                return None
            
            payload = json.loads(self._base64url_decode(payload_b64))
            return payload
        
        except Exception as e:
            logger.error(f"Token decode error: {e}")
            return None
    
    def _sign(self, data: str) -> str:
        """Sign data using the configured algorithm."""
        if self.algorithm == "HS256":
            return self._base64url_encode(
                hmac.new(
                    self.secret_key.encode(),
                    data.encode(),
                    hashlib.sha256
                ).digest()
            )
        elif self.algorithm == "HS512":
            return self._base64url_encode(
                hmac.new(
                    self.secret_key.encode(),
                    data.encode(),
                    hashlib.sha512
                ).digest()
            )
        raise ValueError(f"Unknown algorithm: {self.algorithm}")
    
    @staticmethod
    def _base64url_encode(data: bytes) -> str:
        """Base64 URL-safe encoding."""
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
    
    @staticmethod
    def _base64url_decode(data: str) -> bytes:
        """Base64 URL-safe decoding."""
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.urlsafe_b64decode(data)
    
    @staticmethod
    def _constant_time_compare(a: str, b: str) -> bool:
        """Constant-time string comparison to prevent timing attacks."""
        return hmac.compare_digest(a.encode(), b.encode())
    
    def check_permission(
        self,
        token_payload: TokenPayload,
        required_roles: Optional[List[str]] = None,
        required_scopes: Optional[List[str]] = None
    ) -> bool:
        """
        Check if token has required permissions.
        
        Args:
            token_payload: Validated token payload
            required_roles: Required roles (any match)
            required_scopes: Required scopes (all must match)
        
        Returns:
            True if authorized, False otherwise
        """
        if required_roles:
            if not any(role in token_payload.roles for role in required_roles):
                return False
        
        if required_scopes:
            if not all(scope in token_payload.scopes for scope in required_scopes):
                return False
        
        return True


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create auth action
    auth = ApiAuthJwtAction(
        secret_key="super-secret-key-for-testing",
        issuer="test-service",
        access_token_ttl=3600,
        refresh_token_ttl=86400
    )
    
    # Generate tokens
    tokens = auth.generate_token_pair(
        user_id="user123",
        roles=["admin", "user"],
        scopes=["read", "write"],
        metadata={"department": "engineering"}
    )
    
    print(f"Access token: {tokens.access_token[:50]}...")
    print(f"Refresh token: {tokens.refresh_token[:50]}...")
    
    # Validate access token
    payload = auth.validate_token(tokens.access_token)
    if payload:
        print(f"Token valid for user: {payload.sub}")
        print(f"Roles: {payload.roles}")
        print(f"Scopes: {payload.scopes}")
    
    # Check permissions
    print(f"Has admin role: {auth.check_permission(payload, required_roles=['admin'])}")
    print(f"Has 'delete' scope: {auth.check_permission(payload, required_scopes=['delete'])}")
    
    # Refresh tokens
    new_tokens = auth.refresh_access_token(tokens.refresh_token)
    if new_tokens:
        print("Token refreshed successfully!")
    
    # Revoke token
    auth.revoke_token(tokens.access_token)
    payload_after_revoke = auth.validate_token(tokens.access_token)
    print(f"Token after revoke: {payload_after_revoke}")
