"""OAuth2 Token Manager.

This module provides OAuth2 token management:
- Access token storage and refresh
- Token expiration tracking
- Automatic token refresh
- Multiple provider support

Example:
    >>> from actions.api_oauth2_token_action import OAuth2TokenManager
    >>> manager = OAuth2TokenManager()
    >>> manager.store_token("provider1", {"access_token": "xxx", "refresh_token": "yyy"})
"""

from __future__ import annotations

import json
import time
import logging
import threading
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class TokenData:
    """OAuth2 token data."""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"
    expires_at: Optional[float] = None
    scope: str = ""
    created_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class OAuth2TokenManager:
    """Manages OAuth2 tokens with automatic refresh."""

    def __init__(self, auto_refresh_threshold: int = 300) -> None:
        """Initialize the token manager.

        Args:
            auto_refresh_threshold: Seconds before expiry to trigger refresh.
        """
        self._tokens: dict[str, TokenData] = {}
        self._refresh_callbacks: dict[str, callable] = {}
        self._lock = threading.RLock()
        self._auto_refresh_threshold = auto_refresh_threshold

    def store_token(
        self,
        provider: str,
        token_response: dict[str, Any],
        expires_in: Optional[int] = None,
    ) -> TokenData:
        """Store a token from an OAuth2 response.

        Args:
            provider: Provider name (e.g., "google", "github").
            token_response: The token response dict from OAuth2 provider.
            expires_in: Override seconds until expiry.

        Returns:
            The stored TokenData.
        """
        access_token = token_response.get("access_token", "")
        refresh_token = token_response.get("refresh_token")
        token_type = token_response.get("token_type", "Bearer")
        scope = token_response.get("scope", "")

        expires_at = None
        if expires_in is not None:
            expires_at = time.time() + expires_in
        elif "expires_in" in token_response:
            expires_at = time.time() + token_response["expires_in"]

        metadata = {k: v for k, v in token_response.items()
                    if k not in ("access_token", "refresh_token", "expires_in")}

        token = TokenData(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=token_type,
            expires_at=expires_at,
            scope=scope,
            metadata=metadata,
        )

        with self._lock:
            self._tokens[provider] = token
            logger.info("Stored token for provider: %s", provider)

        return token

    def get_token(self, provider: str) -> Optional[TokenData]:
        """Get stored token for a provider.

        Args:
            provider: Provider name.

        Returns:
            TokenData if exists, None otherwise.
        """
        with self._lock:
            return self._tokens.get(provider)

    def get_valid_token(self, provider: str) -> Optional[str]:
        """Get a valid (non-expired) access token.

        Args:
            provider: Provider name.

        Returns:
            Access token string, or None if not available.
        """
        token = self.get_token(provider)
        if token is None:
            return None

        if token.expires_at and time.time() >= token.expires_at:
            logger.info("Token for %s expired, need refresh", provider)
            return None

        return token.access_token

    def needs_refresh(self, provider: str) -> bool:
        """Check if a token needs refresh.

        Args:
            provider: Provider name.

        Returns:
            True if token needs refresh.
        """
        token = self.get_token(provider)
        if token is None:
            return True
        if token.refresh_token is None:
            return False
        if token.expires_at is None:
            return False
        return time.time() >= (token.expires_at - self._auto_refresh_threshold)

    def register_refresh_callback(self, provider: str, callback: callable) -> None:
        """Register a callback for token refresh.

        Args:
            provider: Provider name.
            callback: Function that returns new token_response dict.
        """
        with self._lock:
            self._refresh_callbacks[provider] = callback
            logger.info("Registered refresh callback for: %s", provider)

    def refresh_if_needed(self, provider: str) -> bool:
        """Refresh token if needed, using registered callback.

        Args:
            provider: Provider name.

        Returns:
            True if refresh succeeded or not needed, False if failed.
        """
        if not self.needs_refresh(provider):
            return True

        callback = self._refresh_callbacks.get(provider)
        if callback is None:
            logger.warning("No refresh callback for provider: %s", provider)
            return False

        try:
            new_token_response = callback()
            self.store_token(provider, new_token_response)
            logger.info("Successfully refreshed token for: %s", provider)
            return True
        except Exception as e:
            logger.error("Failed to refresh token for %s: %s", provider, e)
            return False

    def revoke_token(self, provider: str) -> None:
        """Remove a stored token.

        Args:
            provider: Provider name.
        """
        with self._lock:
            if provider in self._tokens:
                del self._tokens[provider]
                logger.info("Revoked token for provider: %s", provider)

    def list_providers(self) -> list[str]:
        """List all providers with stored tokens."""
        with self._lock:
            return list(self._tokens.keys())

    def get_expiry_info(self, provider: str) -> Optional[dict[str, Any]]:
        """Get token expiry information.

        Args:
            provider: Provider name.

        Returns:
            Dict with expires_at and seconds_until_expiry, or None.
        """
        token = self.get_token(provider)
        if token is None or token.expires_at is None:
            return None
        return {
            "expires_at": token.expires_at,
            "seconds_until_expiry": token.expires_at - time.time(),
        }
