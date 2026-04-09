"""
API Token Refresh Module.

Provides automatic token management, refresh, rotation,
and OAuth 2.0 token lifecycle handling.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class TokenType(Enum):
    """Token type."""
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    JWT = "jwt"


@dataclass
class Token:
    """Container for an API token."""
    access_token: str
    token_type: TokenType
    expires_at: float
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        return time.time() >= self.expires_at
        
    @property
    def is_expiring_soon(self, buffer_seconds: float = 300) -> bool:
        """Check if token is expiring within buffer time."""
        return time.time() >= (self.expires_at - buffer_seconds)
        
    @property
    def expires_in(self) -> float:
        """Get seconds until expiration."""
        return max(0, self.expires_at - time.time())


@dataclass
class TokenConfig:
    """Configuration for token management."""
    client_id: str
    client_secret: str
    token_url: str
    refresh_url: Optional[str] = None
    token_type: TokenType = TokenType.BEARER
    default_scopes: List[str] = field(default_factory=list)
    auto_refresh: bool = True
    refresh_buffer_seconds: float = 300  # Refresh 5 min before expiry
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class RefreshResult:
    """Result of a token refresh operation."""
    success: bool
    token: Optional[Token]
    error: Optional[str] = None
    refreshed_at: float = field(default_factory=time.time)


class TokenManager:
    """
    Manages API token lifecycle with automatic refresh.
    
    Example:
        manager = TokenManager(TokenConfig(
            client_id="my_client",
            client_secret="my_secret",
            token_url="https://auth.example.com/oauth/token"
        ))
        
        # Get valid token (auto-refreshes if needed)
        token = await manager.get_valid_token()
        
        # Use in request
        headers = {"Authorization": f"Bearer {token.access_token}"}
    """
    
    def __init__(
        self,
        config: TokenConfig,
        storage: Optional[Callable[[str, Token], Any]] = None,
    ) -> None:
        """
        Initialize token manager.
        
        Args:
            config: Token configuration.
            storage: Optional storage function for persisting tokens.
        """
        self.config = config
        self.storage = storage
        self._current_token: Optional[Token] = None
        self._refresh_lock = asyncio.Lock()
        self._refresh_in_progress = False
        self._token_futures: List[asyncio.Future] = []
        
    async def get_valid_token(self) -> Token:
        """
        Get a valid token, refreshing if necessary.
        
        Returns:
            Valid Token object.
            
        Raises:
            RuntimeError: If token cannot be obtained.
        """
        # Check if we have a valid token
        if self._current_token and not self._current_token.is_expired:
            if not self._current_token.is_expiring_soon(self.config.refresh_buffer_seconds):
                return self._current_token
                
        # Need to refresh
        async with self._refresh_lock:
            # Double-check after acquiring lock
            if self._current_token and not self._current_token.is_expired:
                if not self._current_token.is_expiring_soon(self.config.refresh_buffer_seconds):
                    return self._current_token
                    
            # Perform refresh
            result = await self._refresh_token()
            
            if not result.success:
                raise RuntimeError(f"Token refresh failed: {result.error}")
                
            self._current_token = result.token
            
            # Resolve any waiting futures
            for future in self._token_futures:
                if not future.done():
                    future.set_result(result.token)
            self._token_futures.clear()
            
            return result.token
            
    async def get_token_async(self) -> Token:
        """
        Get token asynchronously, waiting if refresh is in progress.
        
        Returns:
            Valid Token object.
        """
        # Fast path: token already valid
        if self._current_token and not self._current_token.is_expired:
            if not self._current_token.is_expiring_soon(self.config.refresh_buffer_seconds):
                return self._current_token
                
        # Wait for refresh to complete
        future = asyncio.get_event_loop().create_future()
        self._token_futures.append(future)
        
        try:
            return await asyncio.wait_for(future, timeout=60.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for token refresh")
            
    async def set_token(self, token: Token) -> None:
        """
        Set the current token (e.g., from manual authentication).
        
        Args:
            token: Token to set.
        """
        self._current_token = token
        
        if self.storage:
            await self._persist_token(token)
            
        logger.info(f"Token set, expires at {token.expires_at}")
        
    async def _refresh_token(self) -> RefreshResult:
        """
        Perform token refresh.
        
        Returns:
            RefreshResult with new token or error.
        """
        logger.info("Refreshing token...")
        
        for attempt in range(self.config.max_retries):
            try:
                if self._current_token and self._current_token.refresh_token:
                    # Use refresh token
                    result = await self._do_refresh(
                        grant_type="refresh_token",
                        refresh_token=self._current_token.refresh_token
                    )
                else:
                    # Use client credentials
                    result = await self._do_refresh(
                        grant_type="client_credentials"
                    )
                    
                if result.success:
                    logger.info("Token refreshed successfully")
                    return result
                    
            except Exception as e:
                logger.warning(f"Token refresh attempt {attempt + 1} failed: {e}")
                
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                
        return RefreshResult(
            success=False,
            token=None,
            error="All refresh attempts failed"
        )
        
    async def _do_refresh(
        self,
        grant_type: str,
        refresh_token: Optional[str] = None,
    ) -> RefreshResult:
        """
        Execute refresh request.
        
        Args:
            grant_type: OAuth grant type.
            refresh_token: Optional refresh token.
            
        Returns:
            RefreshResult with new token.
        """
        import aiohttp
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        
        data = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "grant_type": grant_type,
        }
        
        if refresh_token:
            data["refresh_token"] = refresh_token
            
        if self.config.default_scopes:
            data["scope"] = " ".join(self.config.default_scopes)
            
        url = self._current_token and self._current_token.refresh_token and self.config.refresh_url \
            or self.config.token_url
            
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data, headers=headers) as resp:
                    if resp.status == 200:
                        json_data = await resp.json()
                        token = self._parse_token_response(json_data)
                        
                        if self.storage:
                            await self._persist_token(token)
                            
                        return RefreshResult(success=True, token=token)
                    else:
                        error_text = await resp.text()
                        return RefreshResult(
                            success=False,
                            token=None,
                            error=f"HTTP {resp.status}: {error_text}"
                        )
                        
        except Exception as e:
            return RefreshResult(success=False, token=None, error=str(e))
            
    def _parse_token_response(self, data: Dict[str, Any]) -> Token:
        """
        Parse OAuth token response.
        
        Args:
            data: Response JSON.
            
        Returns:
            Token object.
        """
        expires_in = data.get("expires_in", 3600)
        
        return Token(
            access_token=data["access_token"],
            token_type=self.config.token_type,
            expires_at=time.time() + expires_in,
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
            metadata=data,
        )
        
    async def _persist_token(self, token: Token) -> None:
        """Persist token to storage."""
        if self.storage:
            try:
                await self.storage(f"token_{self.config.client_id}", token)
            except Exception as e:
                logger.error(f"Failed to persist token: {e}")
                
    def revoke_token(self) -> None:
        """Revoke current token."""
        self._current_token = None
        logger.info("Token revoked")


class ApiKeyManager:
    """
    Simple API key management with rotation support.
    
    Example:
        manager = ApiKeyManager()
        manager.add_key("key_123", primary=True)
        manager.add_key("key_456", primary=False)
        
        current_key = manager.get_current_key()
    """
    
    def __init__(self) -> None:
        """Initialize API key manager."""
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._current_key_id: Optional[str] = None
        
    def add_key(
        self,
        key: str,
        primary: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add an API key.
        
        Args:
            key: The API key.
            primary: Set as primary (active) key.
            metadata: Optional key metadata.
        """
        import uuid
        key_id = str(uuid.uuid4())[:8]
        
        self._keys[key_id] = {
            "key": key,
            "primary": primary,
            "added_at": time.time(),
            "metadata": metadata or {},
        }
        
        if primary or not self._current_key_id:
            self._current_key_id = key_id
            
        logger.info(f"Added API key {key_id}")
        
    def get_current_key(self) -> Optional[str]:
        """Get the current (primary) API key."""
        if self._current_key_id and self._current_key_id in self._keys:
            return self._keys[self._current_key_id]["key"]
        return None
        
    def rotate_key(self, old_key: str) -> Optional[str]:
        """
        Rotate from old key to new primary.
        
        Args:
            old_key: The old key to replace.
            
        Returns:
            New primary key or None.
        """
        # Find key with same value
        for key_id, key_data in self._keys.items():
            if key_data["key"] == old_key:
                # Find a non-primary replacement
                for new_key_id, new_key_data in self._keys.items():
                    if new_key_id != key_id and not new_key_data["primary"]:
                        new_key_data["primary"] = True
                        self._current_key_id = new_key_id
                        logger.info(f"Rotated from key {key_id} to {new_key_id}")
                        return new_key_data["key"]
                        
        return None
        
    def remove_key(self, key_id: str) -> bool:
        """Remove an API key."""
        if key_id in self._keys:
            del self._keys[key_id]
            if self._current_key_id == key_id:
                # Promote another key
                for new_key_id, key_data in self._keys.items():
                    key_data["primary"] = True
                    self._current_key_id = new_key_id
                    break
            return True
        return False


class JWTTokenManager:
    """
    JWT token decoding, validation, and refresh.
    
    Example:
        manager = JWTTokenManager(secret="my_secret", algorithm="HS256")
        
        # Decode token
        payload = manager.decode(token)
        
        # Check expiration
        if manager.is_expired(token):
            token = await manager.refresh(token)
    """
    
    def __init__(self, secret: str, algorithm: str = "HS256") -> None:
        """
        Initialize JWT manager.
        
        Args:
            secret: JWT secret or public key.
            algorithm: Signing algorithm.
        """
        self.secret = secret
        self.algorithm = algorithm
        
    def encode(self, payload: Dict[str, Any], expires_in: int = 3600) -> str:
        """
        Create a JWT token.
        
        Args:
            payload: Token payload.
            expires_in: Seconds until expiration.
            
        Returns:
            Encoded JWT string.
        """
        import jwt
        
        payload = payload.copy()
        payload["exp"] = int(time.time()) + expires_in
        payload["iat"] = int(time.time())
        
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)
        
    def decode(self, token: str) -> Dict[str, Any]:
        """
        Decode a JWT token.
        
        Args:
            token: JWT string.
            
        Returns:
            Token payload.
            
        Raises:
            jwt.InvalidTokenError: If token is invalid.
        """
        import jwt
        
        return jwt.decode(token, self.secret, algorithms=[self.algorithm])
        
    def is_expired(self, token: str) -> bool:
        """Check if token is expired."""
        try:
            payload = self.decode(token)
            exp = payload.get("exp", 0)
            return time.time() >= exp
        except Exception:
            return True
            
    def get_expires_at(self, token: str) -> Optional[float]:
        """Get token expiration timestamp."""
        try:
            payload = self.decode(token)
            return payload.get("exp")
        except Exception:
            return None
