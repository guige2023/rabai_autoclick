"""API Auth Action Module.

Provides OAuth 2.0, API key, Bearer token, and Basic auth
implementation for API clients.
"""
from __future__ import annotations

import time
import base64
import hashlib
import secrets
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication type."""
    NONE = "none"
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class TokenInfo:
    """OAuth token information."""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"
    expires_at: Optional[float] = None
    scope: Optional[str] = None


@dataclass
class AuthConfig:
    """Authentication configuration."""
    auth_type: AuthType
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_scopes: Optional[List[str]] = None
    custom_header: Optional[str] = None
    custom_value: Optional[str] = None


class APIAuthAction:
    """API Authentication handler.

    Example:
        auth = APIAuthAction()

        auth.configure(AuthConfig(
            auth_type=AuthType.BEARER,
            token="my-access-token"
        ))

        headers = auth.get_auth_headers()
        print(headers)  # {"Authorization": "Bearer my-access-token"}
    """

    def __init__(self) -> None:
        self._config: Optional[AuthConfig] = None
        self._token_info: Optional[TokenInfo] = None
        self._refresh_callback: Optional[Callable] = None
        self._token_cache: Dict[str, TokenInfo] = {}

    def configure(self, config: AuthConfig) -> None:
        """Configure authentication."""
        self._config = config

        if config.oauth2_client_id and config.oauth2_token_url:
            self._token_info = self._load_cached_token(config.oauth2_client_id)

    def configure_api_key(
        self,
        api_key: str,
        header_name: str = "X-API-Key",
    ) -> "APIAuthAction":
        """Configure API key authentication."""
        self._config = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key=api_key,
            api_key_header=header_name,
        )
        return self

    def configure_basic(
        self,
        username: str,
        password: str,
    ) -> "APIAuthAction":
        """Configure Basic authentication."""
        self._config = AuthConfig(
            auth_type=AuthType.BASIC,
            username=username,
            password=password,
        )
        return self

    def configure_bearer(self, token: str) -> "APIAuthAction":
        """Configure Bearer token authentication."""
        self._config = AuthConfig(
            auth_type=AuthType.BEARER,
            token=token,
        )
        return self

    def configure_oauth2(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scopes: Optional[List[str]] = None,
    ) -> "APIAuthAction":
        """Configure OAuth 2.0 authentication."""
        self._config = AuthConfig(
            auth_type=AuthType.OAUTH2,
            oauth2_client_id=client_id,
            oauth2_client_secret=client_secret,
            oauth2_token_url=token_url,
            oauth2_scopes=scopes,
        )
        return self

    def set_refresh_callback(self, callback: Callable) -> None:
        """Set callback for token refresh."""
        self._refresh_callback = callback

    async def get_auth_headers(
        self,
        force_refresh: bool = False,
    ) -> Dict[str, str]:
        """Get authentication headers.

        Args:
            force_refresh: Force token refresh if OAuth2

        Returns:
            Dict of authentication headers
        """
        if self._config is None:
            return {}

        if self._config.auth_type == AuthType.API_KEY:
            return {self._config.api_key_header: self._config.api_key}

        elif self._config.auth_type == AuthType.BASIC:
            credentials = f"{self._config.username}:{self._config.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        elif self._config.auth_type == AuthType.BEARER:
            return {"Authorization": f"Bearer {self._config.token}"}

        elif self._config.auth_type == AuthType.OAUTH2:
            return await self._get_oauth2_headers(force_refresh)

        elif self._config.auth_type == AuthType.CUSTOM:
            return {self._config.custom_header: self._config.custom_value}

        return {}

    async def _get_oauth2_headers(self, force_refresh: bool) -> Dict[str, str]:
        """Get OAuth2 authentication headers."""
        if not self._config:
            return {}

        if not force_refresh and self._is_token_valid():
            token = self._token_info.access_token
            return {"Authorization": f"Bearer {token}"}

        if self._refresh_callback:
            new_token = await self._refresh_callback(self._token_info)
            if new_token:
                self._token_info = new_token
                self._cache_token(self._config.oauth2_client_id, new_token)
                return {"Authorization": f"Bearer {new_token.access_token}"}

        return {}

    def _is_token_valid(self) -> bool:
        """Check if current token is valid."""
        if not self._token_info:
            return False

        if self._token_info.expires_at is None:
            return True

        return time.time() < self._token_info.expires_at - 60

    def _load_cached_token(self, client_id: str) -> Optional[TokenInfo]:
        """Load cached token for client."""
        return self._token_cache.get(client_id)

    def _cache_token(self, client_id: str, token_info: TokenInfo) -> None:
        """Cache token for client."""
        self._token_cache[client_id] = token_info

    def clear_cache(self) -> None:
        """Clear token cache."""
        self._token_cache.clear()

    async def revoke_token(self) -> bool:
        """Revoke current OAuth2 token."""
        if not self._config or not self._token_info:
            return False

        logger.info("Token revoked")
        self._token_info = None
        return True

    def get_auth_type(self) -> AuthType:
        """Get current authentication type."""
        return self._config.auth_type if self._config else AuthType.NONE
