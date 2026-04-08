"""OAuth2 action module for RabAI AutoClick.

Provides OAuth2 operations:
- OAuth2AuthorizeAction: Generate authorization URL
- OAuth2TokenAction: Exchange code for token
- OAuth2RefreshAction: Refresh access token
- OAuth2RevokeAction: Revoke token
"""

from __future__ import annotations

import sys
import os
import urllib.parse
from typing import Any, Dict, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OAuth2AuthorizeAction(BaseAction):
    """Generate OAuth2 authorization URL."""
    action_type = "oauth2_authorize"
    display_name = "OAuth2授权"
    description = "生成OAuth2授权URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute authorization URL generation."""
        client_id = params.get('client_id', '')
        redirect_uri = params.get('redirect_uri', '')
        scope = params.get('scope', '')
        state = params.get('state', '')
        response_type = params.get('response_type', 'code')
        output_var = params.get('output_var', 'auth_url')

        if not client_id or not redirect_uri:
            return ActionResult(success=False, message="client_id and redirect_uri are required")

        try:
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_redirect = context.resolve_value(redirect_uri) if context else redirect_uri
            resolved_scope = context.resolve_value(scope) if context else scope

            import secrets
            state = state or secrets.token_urlsafe(16)

            params_dict = {
                'client_id': resolved_client_id,
                'redirect_uri': resolved_redirect,
                'response_type': response_type,
                'state': state,
            }

            if resolved_scope:
                params_dict['scope'] = resolved_scope

            auth_url = f"https://example.com/oauth/authorize?{urllib.parse.urlencode(params_dict)}"

            result = {
                'url': auth_url,
                'state': state,
                'client_id': resolved_client_id,
                'redirect_uri': resolved_redirect,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Generated auth URL with state: {state[:10]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 authorize error: {e}")


class OAuth2TokenAction(BaseAction):
    """Exchange authorization code for token."""
    action_type = "oauth2_token"
    display_name = "OAuth2令牌"
    description = "交换授权码获取令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute token exchange."""
        code = params.get('code', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        redirect_uri = params.get('redirect_uri', '')
        token_url = params.get('token_url', 'https://example.com/oauth/token')
        output_var = params.get('output_var', 'token_result')

        if not code or not client_id:
            return ActionResult(success=False, message="code and client_id are required")

        try:
            import requests

            resolved_code = context.resolve_value(code) if context else code
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_secret = context.resolve_value(client_secret) if context else client_secret
            resolved_redirect = context.resolve_value(redirect_uri) if context else redirect_uri
            resolved_token_url = context.resolve_value(token_url) if context else token_url

            response = requests.post(
                resolved_token_url,
                data={
                    'grant_type': 'authorization_code',
                    'code': resolved_code,
                    'client_id': resolved_client_id,
                    'client_secret': resolved_secret,
                    'redirect_uri': resolved_redirect,
                },
                timeout=30
            )

            if response.ok:
                token_data = response.json()
                result = {
                    'access_token': token_data.get('access_token', ''),
                    'token_type': token_data.get('token_type', 'Bearer'),
                    'expires_in': token_data.get('expires_in', 0),
                    'refresh_token': token_data.get('refresh_token', ''),
                    'scope': token_data.get('scope', ''),
                }
                return ActionResult(
                    success=True,
                    data={output_var: result},
                    message=f"Token received: {result['token_type']}"
                )
            else:
                return ActionResult(
                    success=False,
                    data={output_var: {'error': response.text}},
                    message=f"Token exchange failed: {response.status_code}"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 token error: {e}")


class OAuth2RefreshAction(BaseAction):
    """Refresh access token."""
    action_type = "oauth2_refresh"
    display_name = "OAuth2刷新"
    description = "刷新访问令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute token refresh."""
        refresh_token = params.get('refresh_token', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        scope = params.get('scope', '')
        token_url = params.get('token_url', 'https://example.com/oauth/token')
        output_var = params.get('output_var', 'refresh_result')

        if not refresh_token or not client_id:
            return ActionResult(success=False, message="refresh_token and client_id are required")

        try:
            import requests

            resolved_refresh = context.resolve_value(refresh_token) if context else refresh_token
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_secret = context.resolve_value(client_secret) if context else client_secret
            resolved_scope = context.resolve_value(scope) if context else scope
            resolved_token_url = context.resolve_value(token_url) if context else token_url

            data = {
                'grant_type': 'refresh_token',
                'refresh_token': resolved_refresh,
                'client_id': resolved_client_id,
                'client_secret': resolved_secret,
            }

            if resolved_scope:
                data['scope'] = resolved_scope

            response = requests.post(
                resolved_token_url,
                data=data,
                timeout=30
            )

            if response.ok:
                token_data = response.json()
                result = {
                    'access_token': token_data.get('access_token', ''),
                    'expires_in': token_data.get('expires_in', 0),
                    'refresh_token': token_data.get('refresh_token', resolved_refresh),
                }
                return ActionResult(
                    success=True,
                    data={output_var: result},
                    message="Token refreshed successfully"
                )
            else:
                return ActionResult(
                    success=False,
                    data={output_var: {'error': response.text}},
                    message=f"Token refresh failed: {response.status_code}"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 refresh error: {e}")


class OAuth2RevokeAction(BaseAction):
    """Revoke OAuth2 token."""
    action_type = "oauth2_revoke"
    display_name = "OAuth2撤销"
    description = "撤销OAuth2令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute token revocation."""
        token = params.get('token', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        revoke_url = params.get('revoke_url', 'https://example.com/oauth/revoke')
        output_var = params.get('output_var', 'revoke_result')

        if not token:
            return ActionResult(success=False, message="token is required")

        try:
            import requests

            resolved_token = context.resolve_value(token) if context else token
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_secret = context.resolve_value(client_secret) if context else client_secret
            resolved_revoke_url = context.resolve_value(revoke_url) if context else revoke_url

            response = requests.post(
                resolved_revoke_url,
                data={
                    'token': resolved_token,
                    'client_id': resolved_client_id,
                    'client_secret': resolved_secret,
                },
                timeout=30
            )

            result = {
                'revoked': response.ok,
                'status_code': response.status_code,
            }

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message="Token revoked" if response.ok else "Token revocation failed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 revoke error: {e}")
