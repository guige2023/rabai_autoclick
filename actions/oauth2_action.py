"""OAuth2 action module for RabAI AutoClick.

Provides OAuth2 operations:
- OAuth2TokenRequestAction: Request OAuth2 access token
- OAuth2RefreshAction: Refresh OAuth2 token
- OAuth2RevokeAction: Revoke OAuth2 token
- OAuth2AuthorizeAction: Generate authorization URL
"""

from __future__ import annotations

import json
import sys
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OAuth2TokenRequestAction(BaseAction):
    """Request OAuth2 access token."""
    action_type = "oauth2_token_request"
    display_name = "OAuth2获取Token"
    description = "请求OAuth2访问令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OAuth2 token request."""
        token_url = params.get('token_url', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        grant_type = params.get('grant_type', 'client_credentials')  # client_credentials, authorization_code, password
        scope = params.get('scope', '')
        code = params.get('code', '')
        redirect_uri = params.get('redirect_uri', '')
        username = params.get('username', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'oauth2_token')

        if not token_url:
            return ActionResult(success=False, message="token_url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(token_url) if context else token_url
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_client_secret = context.resolve_value(client_secret) if context else client_secret
            resolved_grant = context.resolve_value(grant_type) if context else grant_type
            resolved_scope = context.resolve_value(scope) if context else scope
            resolved_code = context.resolve_value(code) if context else code
            resolved_redirect = context.resolve_value(redirect_uri) if context else redirect_uri
            resolved_username = context.resolve_value(username) if context else username
            resolved_password = context.resolve_value(password) if context else password

            data = {
                'grant_type': resolved_grant,
                'client_id': resolved_client_id,
            }

            if resolved_client_secret:
                data['client_secret'] = resolved_client_secret
            if resolved_scope:
                data['scope'] = resolved_scope
            if resolved_grant == 'authorization_code' and resolved_code:
                data['code'] = resolved_code
                if resolved_redirect:
                    data['redirect_uri'] = resolved_redirect
            elif resolved_grant == 'password' and resolved_username:
                data['username'] = resolved_username
                data['password'] = resolved_password

            body = urlencode(data).encode('utf-8')
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            request = urllib.request.Request(resolved_url, data=body, headers=headers, method='POST')

            with urllib.request.urlopen(request, timeout=30) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))

            if 'error' in token_data:
                return ActionResult(success=False, message=f"OAuth2 error: {token_data.get('error_description', token_data.get('error'))}")

            result = {
                'access_token': token_data.get('access_token'),
                'token_type': token_data.get('token_type', 'Bearer'),
                'expires_in': token_data.get('expires_in'),
                'refresh_token': token_data.get('refresh_token'),
                'scope': token_data.get('scope'),
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"OAuth2 token obtained: {result['token_type']}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 token request error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['token_url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'client_id': '', 'client_secret': '', 'grant_type': 'client_credentials',
            'scope': '', 'code': '', 'redirect_uri': '', 'username': '', 'password': '',
            'output_var': 'oauth2_token'
        }


class OAuth2RefreshAction(BaseAction):
    """Refresh OAuth2 token."""
    action_type = "oauth2_refresh"
    display_name = "OAuth2刷新Token"
    description = "刷新OAuth2访问令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OAuth2 refresh."""
        token_url = params.get('token_url', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        refresh_token = params.get('refresh_token', '')
        scope = params.get('scope', '')
        output_var = params.get('output_var', 'oauth2_refresh_result')

        if not token_url or not refresh_token:
            return ActionResult(success=False, message="token_url and refresh_token are required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(token_url) if context else token_url
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_client_secret = context.resolve_value(client_secret) if context else client_secret
            resolved_refresh = context.resolve_value(refresh_token) if context else refresh_token
            resolved_scope = context.resolve_value(scope) if context else scope

            data = {
                'grant_type': 'refresh_token',
                'refresh_token': resolved_refresh,
                'client_id': resolved_client_id,
            }
            if resolved_client_secret:
                data['client_secret'] = resolved_client_secret
            if resolved_scope:
                data['scope'] = resolved_scope

            body = urlencode(data).encode('utf-8')
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            request = urllib.request.Request(resolved_url, data=body, headers=headers, method='POST')

            with urllib.request.urlopen(request, timeout=30) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))

            result = {
                'access_token': token_data.get('access_token'),
                'token_type': token_data.get('token_type', 'Bearer'),
                'expires_in': token_data.get('expires_in'),
                'refresh_token': token_data.get('refresh_token', resolved_refresh),
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="OAuth2 token refreshed", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 refresh error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['token_url', 'refresh_token']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'client_id': '', 'client_secret': '', 'scope': '', 'output_var': 'oauth2_refresh_result'}


class OAuth2AuthorizeAction(BaseAction):
    """Generate OAuth2 authorization URL."""
    action_type = "oauth2_authorize"
    display_name = "OAuth2授权URL"
    description = "生成OAuth2授权URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OAuth2 authorize URL generation."""
        auth_url = params.get('auth_url', '')
        client_id = params.get('client_id', '')
        redirect_uri = params.get('redirect_uri', '')
        scope = params.get('scope', '')
        state = params.get('state', '')
        response_type = params.get('response_type', 'code')
        output_var = params.get('output_var', 'auth_url')

        if not auth_url or not client_id:
            return ActionResult(success=False, message="auth_url and client_id are required")

        try:
            resolved_auth_url = context.resolve_value(auth_url) if context else auth_url
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_redirect = context.resolve_value(redirect_uri) if context else redirect_uri
            resolved_scope = context.resolve_value(scope) if context else scope
            resolved_state = context.resolve_value(state) if context else state

            params_dict = {
                'client_id': resolved_client_id,
                'response_type': response_type,
            }
            if resolved_redirect:
                params_dict['redirect_uri'] = resolved_redirect
            if resolved_scope:
                params_dict['scope'] = resolved_scope
            if resolved_state:
                params_dict['state'] = resolved_state

            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(resolved_auth_url)
            query = parse_qs(parsed.query)
            query.update({k: v for k, v in params_dict.items() if v})
            from urllib.parse import urlencode
            auth_url_final = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(query, doseq=True)}"

            if context:
                context.set(output_var, auth_url_final)
            return ActionResult(success=True, message=f"Authorization URL generated", data={'auth_url': auth_url_final})
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 authorize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['auth_url', 'client_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'redirect_uri': '', 'scope': '', 'state': '', 'response_type': 'code', 'output_var': 'auth_url'}


class OAuth2RevokeAction(BaseAction):
    """Revoke OAuth2 token."""
    action_type = "oauth2_revoke"
    display_name = "OAuth2撤销Token"
    description = "撤销OAuth2访问令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OAuth2 revoke."""
        revoke_url = params.get('revoke_url', '')
        token = params.get('token', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        token_type_hint = params.get('token_type_hint', 'access_token')
        output_var = params.get('output_var', 'revoke_result')

        if not revoke_url or not token:
            return ActionResult(success=False, message="revoke_url and token are required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(revoke_url) if context else revoke_url
            resolved_token = context.resolve_value(token) if context else token
            resolved_client_id = context.resolve_value(client_id) if context else client_id
            resolved_client_secret = context.resolve_value(client_secret) if context else client_secret

            data = {'token': resolved_token, 'token_type_hint': token_type_hint}
            if resolved_client_id:
                data['client_id'] = resolved_client_id
            if resolved_client_secret:
                data['client_secret'] = resolved_client_secret

            body = urlencode(data).encode('utf-8')
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            request = urllib.request.Request(resolved_url, data=body, headers=headers, method='POST')

            try:
                with urllib.request.urlopen(request, timeout=15) as resp:
                    result = {'revoked': True, 'status_code': resp.status}
            except urllib.error.HTTPError as e:
                # Most providers return 200 even on success
                result = {'revoked': e.code in (200, 400, 401), 'status_code': e.code}

            if context:
                context.set(output_var, result)
            return ActionResult(success=result['revoked'], message="Token revoked" if result['revoked'] else "Revoke failed", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 revoke error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['revoke_url', 'token']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'client_id': '', 'client_secret': '', 'token_type_hint': 'access_token', 'output_var': 'revoke_result'}
