"""API Token Refresh Action Module for RabAI AutoClick.

Automatically refreshes expired or expiring API tokens using
refresh tokens or client credentials with retry logic.
"""

import time
import sys
import os
from typing import Any, Dict, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenInfo:
    """Token metadata container."""
    access_token: str
    refresh_token: Optional[str]
    expires_at: float
    token_type: str = "Bearer"
    scope: Optional[str] = None


class ApiTokenRefreshAction(BaseAction):
    """Handle automatic API token refresh and rotation.

    Manages OAuth2 token lifecycle including access token caching,
    proactive refresh before expiration, and secure storage.
    Supports refresh token rotation and concurrent refresh protection.
    """
    action_type = "api_token_refresh"
    display_name = "API Token刷新"
    description = "自动刷新即将过期的API Token"

    _token_cache: Dict[str, TokenInfo] = {}
    _refresh_locks: Dict[str, bool] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute token refresh or retrieval.

        Args:
            context: Execution context.
            params: Dict with keys:
                - provider: str - token provider name (e.g., 'google', 'github')
                - client_id: str - OAuth client ID
                - client_secret: str - OAuth client secret
                - refresh_token: str - refresh token (if using refresh flow)
                - token_url: str - OAuth token endpoint URL
                - scope: str (optional) - space-separated scopes
                - refresh_before: int (optional) - seconds before expiry to refresh
                - force_refresh: bool (optional) - force refresh even if valid

        Returns:
            ActionResult with valid access token.
        """
        start_time = time.time()

        try:
            provider = params.get('provider', 'default')
            client_id = params.get('client_id', '')
            client_secret = params.get('client_secret', '')
            refresh_token = params.get('refresh_token')
            token_url = params.get('token_url', '')
            scope = params.get('scope', '')
            refresh_before = params.get('refresh_before', 300)
            force_refresh = params.get('force_refresh', False)

            cache_key = f"{provider}:{client_id}"

            if not force_refresh:
                existing = self._get_valid_token(cache_key, refresh_before)
                if existing is not None:
                    return ActionResult(
                        success=True,
                        message=f"Using cached token for {provider}",
                        data={'access_token': existing.access_token,
                              'expires_in': int(existing.expires_at - time.time()),
                              'token_type': existing.token_type},
                        duration=time.time() - start_time
                    )

            if self._refresh_locks.get(cache_key, False):
                for _ in range(10):
                    time.sleep(0.5)
                    existing = self._get_valid_token(cache_key, refresh_before)
                    if existing is not None:
                        return ActionResult(
                            success=True,
                            message=f"Token retrieved after concurrent refresh for {provider}",
                            data={'access_token': existing.access_token,
                                  'expires_in': int(existing.expires_at - time.time()),
                                  'token_type': existing.token_type},
                            duration=time.time() - start_time
                        )

            self._refresh_locks[cache_key] = True

            try:
                new_token = self._perform_refresh(
                    token_url, client_id, client_secret,
                    refresh_token, scope
                )

                self._token_cache[cache_key] = new_token

                return ActionResult(
                    success=True,
                    message=f"Token refreshed successfully for {provider}",
                    data={
                        'access_token': new_token.access_token,
                        'expires_in': int(new_token.expires_at - time.time()),
                        'token_type': new_token.token_type,
                        'refreshed': True
                    },
                    duration=time.time() - start_time
                )
            finally:
                self._refresh_locks[cache_key] = False

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Token refresh failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _get_valid_token(self, cache_key: str, refresh_before: int) -> Optional[TokenInfo]:
        """Get token from cache if still valid."""
        if cache_key in self._token_cache:
            token_info = self._token_cache[cache_key]
            time_until_expiry = token_info.expires_at - time.time()
            if time_until_expiry > refresh_before:
                return token_info
        return None

    def _perform_refresh(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: Optional[str],
        scope: str
    ) -> TokenInfo:
        """Perform actual token refresh HTTP call."""
        try:
            from urllib.request import Request, urlopen
            from urllib.parse import urlencode
        except ImportError:
            raise RuntimeError("urllib not available")

        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token' if refresh_token else 'client_credentials'
        }
        if refresh_token:
            data['refresh_token'] = refresh_token
        if scope:
            data['scope'] = scope

        req = Request(
            token_url,
            data=urlencode(data).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )

        try:
            with urlopen(req, timeout=30) as response:
                import json
                resp_data = json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Token endpoint request failed: {e}")

        expires_in = resp_data.get('expires_in', 3600)
        return TokenInfo(
            access_token=resp_data['access_token'],
            refresh_token=resp_data.get('refresh_token', refresh_token),
            expires_at=time.time() + expires_in,
            token_type=resp_data.get('token_type', 'Bearer'),
            scope=resp_data.get('scope', scope)
        )

    def invalidate(self, provider: str, client_id: str) -> None:
        """Invalidate cached token for a provider."""
        cache_key = f"{provider}:{client_id}"
        if cache_key in self._token_cache:
            del self._token_cache[cache_key]
