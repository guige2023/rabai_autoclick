"""OAuth action module for RabAI AutoClick.

Provides OAuth 2.0 token management including authorization code flow,
client credentials flow, token refresh, and token validation.
"""

import json
import time
import sys
import os
import base64
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OAuthTokenAction(BaseAction):
    """Manage OAuth 2.0 tokens with support for multiple flows.
    
    Supports authorization code flow, client credentials flow,
    token refresh, and token validation.
    """
    action_type = "oauth_token"
    display_name = "OAuth令牌管理"
    description = "OAuth 2.0令牌获取、刷新和验证"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage OAuth 2.0 tokens.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (get_token, refresh_token,
                   validate_token), config with flow-specific params.
        
        Returns:
            ActionResult with token data or validation result.
        """
        action = params.get('action', 'get_token')
        config = params.get('config', {})
        
        if action == 'get_token':
            return self._get_token(config)
        elif action == 'refresh_token':
            return self._refresh_token(config)
        elif action == 'validate_token':
            return self._validate_token(config)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _get_token(self, config: Dict[str, Any]) -> ActionResult:
        """Obtain access token using configured flow."""
        flow = config.get('flow', 'client_credentials')
        timeout = config.get('timeout', 30)
        
        if flow == 'client_credentials':
            return self._client_credentials_flow(config, timeout)
        elif flow == 'authorization_code':
            return self._authorization_code_flow(config, timeout)
        elif flow == 'password':
            return self._password_flow(config, timeout)
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported flow: {flow}"
            )
    
    def _client_credentials_flow(
        self,
        config: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute client credentials flow."""
        token_url = config.get('token_url', '')
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        scope = config.get('scope', '')
        
        if not all([token_url, client_id, client_secret]):
            return ActionResult(
                success=False,
                message="token_url, client_id, client_secret are required"
            )
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
        if scope:
            data['scope'] = scope
        
        return self._request_token(token_url, data, timeout)
    
    def _authorization_code_flow(
        self,
        config: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute authorization code flow."""
        token_url = config.get('token_url', '')
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        code = config.get('code', '')
        redirect_uri = config.get('redirect_uri', '')
        
        if not all([token_url, client_id, client_secret, code]):
            return ActionResult(
                success=False,
                message="token_url, client_id, client_secret, code are required"
            )
        
        data = {
            'grant_type': 'authorization_code',
            'client_id': client_id,
            'client_secret': client_secret,
            'code': code
        }
        if redirect_uri:
            data['redirect_uri'] = redirect_uri
        
        return self._request_token(token_url, data, timeout)
    
    def _password_flow(
        self,
        config: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute password credentials flow."""
        token_url = config.get('token_url', '')
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        username = config.get('username', '')
        password = config.get('password', '')
        scope = config.get('scope', '')
        
        if not all([token_url, client_id, client_secret, username, password]):
            return ActionResult(
                success=False,
                message="token_url, client_id, client_secret, username, password are required"
            )
        
        data = {
            'grant_type': 'password',
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password
        }
        if scope:
            data['scope'] = scope
        
        return self._request_token(token_url, data, timeout)
    
    def _refresh_token(
        self,
        config: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Refresh an existing access token."""
        token_url = config.get('token_url', '')
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        refresh_token = config.get('refresh_token', '')
        
        if not all([token_url, client_id, client_secret, refresh_token]):
            return ActionResult(
                success=False,
                message="token_url, client_id, client_secret, refresh_token are required"
            )
        
        data = {
            'grant_type': 'refresh_token',
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token
        }
        
        return self._request_token(token_url, data, timeout)
    
    def _validate_token(
        self,
        config: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Validate an access token."""
        introspect_url = config.get('introspect_url', '')
        token = config.get('token', '')
        
        if not introspect_url:
            return ActionResult(
                success=False,
                message="introspect_url is required"
            )
        
        if not token:
            return ActionResult(
                success=False,
                message="token is required"
            )
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = urlencode({'token': token}).encode('utf-8')
        
        try:
            request = Request(introspect_url, data=data, headers=headers, method='POST')
            
            with urlopen(request, timeout=timeout) as response:
                response_body = response.read().decode('utf-8')
                response_data = json.loads(response_body)
                
                active = response_data.get('active', False)
                
                return ActionResult(
                    success=active,
                    message=f"Token is {'valid' if active else 'invalid'}",
                    data=response_data
                )
                
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Connection error: {e.reason}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Validation failed: {e}"
            )
    
    def _request_token(
        self,
        token_url: str,
        data: Dict[str, str],
        timeout: int
    ) -> ActionResult:
        """Make token request and parse response."""
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        try:
            data_bytes = urlencode(data).encode('utf-8')
            request = Request(token_url, data=data_bytes, headers=headers, method='POST')
            
            with urlopen(request, timeout=timeout) as response:
                response_body = response.read().decode('utf-8')
                token_data = json.loads(response_body)
                
                if 'error' in token_data:
                    return ActionResult(
                        success=False,
                        message=token_data.get('error_description', token_data['error']),
                        data=token_data
                    )
                
                access_token = token_data.get('access_token', '')
                expires_in = token_data.get('expires_in')
                refresh_token = token_data.get('refresh_token')
                scope = token_data.get('scope', '')
                
                return ActionResult(
                    success=True,
                    message=f"Token obtained: {access_token[:20]}...",
                    data={
                        'access_token': access_token,
                        'token_type': token_data.get('token_type', 'Bearer'),
                        'expires_in': expires_in,
                        'refresh_token': refresh_token,
                        'scope': scope,
                        'expires_at': time.time() + expires_in if expires_in else None
                    }
                )
                
        except HTTPError as e:
            try:
                error_body = e.read().decode('utf-8')
                error_data = json.loads(error_body)
                return ActionResult(
                    success=False,
                    message=error_data.get('error_description', error_data.get('error', str(e))),
                    data=error_data
                )
            except:
                return ActionResult(
                    success=False,
                    message=f"HTTP {e.code}: {e.reason}"
                )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Connection error: {e.reason}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Token request failed: {e}"
            )
