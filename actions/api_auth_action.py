"""
API Auth Action Module.

Handles API authentication including OAuth 2.0, API keys,
 bearer tokens, and basic auth with automatic token refresh.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication type."""
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class AuthConfig:
    """Authentication configuration."""
    auth_type: AuthType
    api_key: Optional[str] = None
    bearer_token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    scopes: list[str] = field(default_factory=list)


@dataclass
class TokenInfo:
    """OAuth2 token information."""
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: float = 0.0
    token_type: str = "Bearer"


class APIAuthAction:
    """
    API authentication handler with token management.

    Supports multiple auth types including API keys, bearer tokens,
    basic auth, and OAuth 2.0 with automatic refresh.

    Example:
        auth = APIAuthAction(config=AuthConfig(auth_type=AuthType.BEARER_TOKEN, bearer_token="xxx"))
        headers = auth.get_auth_headers()
        await auth.refresh_if_needed()
    """

    def __init__(
        self,
        config: Optional[AuthConfig] = None,
    ) -> None:
        self.config = config or AuthConfig(auth_type=AuthType.BEARER_TOKEN)
        self._current_token: Optional[TokenInfo] = None
        self._custom_auth_func: Optional[Callable[[], dict[str, str]]] = None

    def get_auth_headers(self) -> dict[str, str]:
        """Get authentication headers for requests."""
        if self.config.auth_type == AuthType.API_KEY:
            key_name = self.config.api_key.split(":")[0] if ":" in self.config.api_key else "X-API-Key"
            key_value = self.config.api_key.split(":")[1] if ":" in self.config.api_key else self.config.api_key
            return {key_name: key_value}

        elif self.config.auth_type == AuthType.BEARER_TOKEN:
            token = self._current_token.access_token if self._current_token else self.config.bearer_token
            return {"Authorization": f"Bearer {token}"}

        elif self.config.config_type == AuthType.BASIC_AUTH:
            import base64
            credentials = f"{self.config.username}:{self.config.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        elif self.config.auth_type == AuthType.OAUTH2:
            token = self._current_token.access_token if self._current_token else ""
            return {"Authorization": f"Bearer {token}"}

        elif self.config.auth_type == AuthType.CUSTOM:
            if self._custom_auth_func:
                return self._custom_auth_func()
            return {}

        return {}

    def set_custom_auth(
        self,
        auth_func: Callable[[], dict[str, str]],
    ) -> "APIAuthAction":
        """Set a custom authentication function."""
        self._custom_auth_func = auth_func
        self.config.auth_type = AuthType.CUSTOM
        return self

    async def refresh_if_needed(self) -> bool:
        """Refresh token if it's expired or about to expire."""
        if self.config.auth_type != AuthType.OAUTH2:
            return True

        if not self._current_token:
            return await self._do_token_refresh()

        buffer_seconds = 60
        if self._current_token.expires_at > time.time() + buffer_seconds:
            return True

        if self._current_token.refresh_token:
            return await self._do_token_refresh(refresh_token=self._current_token.refresh_token)

        return await self._do_token_refresh()

    async def _do_token_refresh(
        self,
        refresh_token: Optional[str] = None,
    ) -> bool:
        """Perform OAuth2 token refresh."""
        import aiohttp

        try:
            data: dict[str, Any] = {
                "grant_type": "refresh_token" if refresh_token else "client_credentials",
                "client_id": self.config.oauth2_client_id,
                "client_secret": self.config.oauth2_client_secret,
            }

            if refresh_token:
                data["refresh_token"] = refresh_token
            else:
                data["scope"] = " ".join(self.config.scopes)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.config.oauth2_token_url,
                    data=data,
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Token refresh failed: {resp.status}")
                        return False

                    token_data = await resp.json()

                    self._current_token = TokenInfo(
                        access_token=token_data["access_token"],
                        refresh_token=token_data.get("refresh_token"),
                        expires_at=time.time() + token_data.get("expires_in", 3600),
                        token_type=token_data.get("token_type", "Bearer"),
                    )

                    return True

        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            return False

    def is_token_valid(self) -> bool:
        """Check if current token is valid."""
        if not self._current_token:
            return self.config.bearer_token is not None
        return self._current_token.expires_at > time.time()
