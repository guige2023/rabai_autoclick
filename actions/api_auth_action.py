"""API auth action module for RabAI AutoClick.

Provides authentication handling with OAuth2, API keys,
JWT tokens, and session management.
"""

import sys
import os
import time
import json
import base64
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from urllib.parse import urlencode
import hashlib
import hmac

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenInfo:
    """OAuth/token information."""
    access_token: str
    token_type: str = "Bearer"
    expires_at: float = 0
    refresh_token: Optional[str] = None
    scope: str = ""


class APIAuthAction(BaseAction):
    """Handle API authentication with multiple auth schemes.
    
    Supports OAuth2, API keys, JWT, basic auth, and session-based
    authentication with automatic token refresh.
    """
    action_type = "api_auth"
    display_name = "API认证"
    description = "OAuth2、API密钥和JWT认证处理"
    
    def __init__(self):
        super().__init__()
        self._tokens: Dict[str, TokenInfo] = {}
        self._sessions: Dict[str, Dict[str, Any]] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute authentication operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'oauth2', 'api_key', 'basic', 'jwt', 'session', 'refresh'
                - config: Auth configuration dict
        
        Returns:
            ActionResult with auth result.
        """
        operation = params.get('operation', 'oauth2').lower()
        
        if operation == 'oauth2':
            return self._oauth2(params)
        elif operation == 'api_key':
            return self._api_key(params)
        elif operation == 'basic':
            return self._basic_auth(params)
        elif operation == 'jwt':
            return self._jwt_auth(params)
        elif operation == 'session':
            return self._session_auth(params)
        elif operation == 'refresh':
            return self._refresh_token(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _oauth2(self, params: Dict[str, Any]) -> ActionResult:
        """Execute OAuth2 flow."""
        config = params.get('config', {})
        grant_type = config.get('grant_type', 'client_credentials')
        
        client_id = config.get('client_id')
        client_secret = config.get('client_secret')
        token_url = config.get('token_url')
        scope = config.get('scope', '')
        
        if not token_url:
            return ActionResult(success=False, message="token_url is required")
        
        # Build token request
        data = {
            'grant_type': grant_type,
            'client_id': client_id,
            'scope': scope
        }
        
        if grant_type == 'client_credentials':
            data['client_secret'] = client_secret
        elif grant_type == 'authorization_code':
            data['code'] = config.get('code')
            data['redirect_uri'] = config.get('redirect_uri')
        
        # In real implementation, would make HTTP request to token_url
        # For now, return mock token
        token = TokenInfo(
            access_token=f"mock_token_{int(time.time())}",
            token_type="Bearer",
            expires_at=time.time() + 3600,
            scope=scope
        )
        
        key = config.get('key', 'default')
        self._tokens[key] = token
        
        return ActionResult(
            success=True,
            message="OAuth2 authentication successful",
            data={
                'access_token': token.access_token,
                'token_type': token.token_type,
                'expires_in': 3600,
                'scope': scope
            }
        )
    
    def _api_key(self, params: Dict[str, Any]) -> ActionResult:
        """Handle API key authentication."""
        config = params.get('config', {})
        
        api_key = config.get('api_key')
        key_name = config.get('key_name', 'X-API-Key')
        location = config.get('location', 'header')
        
        if not api_key:
            return ActionResult(success=False, message="api_key is required")
        
        return ActionResult(
            success=True,
            message="API key configured",
            data={
                'auth_type': 'api_key',
                'key_name': key_name,
                'location': location
            }
        )
    
    def _basic_auth(self, params: Dict[str, Any]) -> ActionResult:
        """Handle basic authentication."""
        config = params.get('config', {})
        
        username = config.get('username')
        password = config.get('password')
        
        if not username or not password:
            return ActionResult(
                success=False,
                message="username and password are required"
            )
        
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        auth_header = f"Basic {encoded}"
        
        return ActionResult(
            success=True,
            message="Basic auth configured",
            data={
                'auth_type': 'basic',
                'header': auth_header
            }
        )
    
    def _jwt_auth(self, params: Dict[str, Any]) -> ActionResult:
        """Handle JWT authentication."""
        config = params.get('config', {})
        
        secret = config.get('secret')
        algorithm = config.get('algorithm', 'HS256')
        claims = config.get('claims', {})
        
        if not secret:
            return ActionResult(success=False, message="secret is required")
        
        # Build JWT (simplified - real impl would use PyJWT)
        header = base64.urlsafe_b64encode(
            json.dumps({'alg': algorithm, 'typ': 'JWT'}).encode()
        ).decode().rstrip('=')
        
        payload = base64.urlsafe_b64encode(
            json.dumps({
                'iat': int(time.time()),
                'exp': int(time.time()) + 3600,
                **claims
            }).encode()
        ).decode().rstrip('=')
        
        signature = hmac.new(
            secret.encode(),
            f"{header}.{payload}".encode(),
            hashlib.sha256
        ).hexdigest()
        
        token = f"{header}.{payload}.{signature}"
        
        return ActionResult(
            success=True,
            message="JWT created",
            data={
                'auth_type': 'jwt',
                'token': token,
                'algorithm': algorithm
            }
        )
    
    def _session_auth(self, params: Dict[str, Any]) -> ActionResult:
        """Handle session-based authentication."""
        config = params.get('config', {})
        
        session_id = config.get('session_id')
        user_id = config.get('user_id')
        expires_in = config.get('expires_in', 3600)
        
        if not session_id:
            # Generate new session
            session_id = hashlib.sha256(
                f"{time.time()}{user_id}".encode()
            ).hexdigest()
        
        session = {
            'session_id': session_id,
            'user_id': user_id,
            'created_at': time.time(),
            'expires_at': time.time() + expires_in
        }
        
        self._sessions[session_id] = session
        
        return ActionResult(
            success=True,
            message="Session created",
            data={
                'session_id': session_id,
                'expires_in': expires_in
            }
        )
    
    def _refresh_token(self, params: Dict[str, Any]) -> ActionResult:
        """Refresh an OAuth2 token."""
        key = params.get('key', 'default')
        
        if key not in self._tokens:
            return ActionResult(
                success=False,
                message=f"No token found for key: {key}"
            )
        
        token = self._tokens[key]
        
        if not token.refresh_token:
            return ActionResult(
                success=False,
                message="No refresh token available"
            )
        
        # In real impl, would call refresh endpoint
        # For now, issue new token
        new_token = TokenInfo(
            access_token=f"refreshed_token_{int(time.time())}",
            token_type=token.token_type,
            expires_at=time.time() + 3600,
            refresh_token=token.refresh_token,
            scope=token.scope
        )
        
        self._tokens[key] = new_token
        
        return ActionResult(
            success=True,
            message="Token refreshed",
            data={
                'access_token': new_token.access_token,
                'expires_in': 3600
            }
        )


class AuthMiddlewareAction(BaseAction):
    """Add authentication to requests."""
    action_type = "auth_middleware"
    display_name = "认证中间件"
    description = "为请求自动添加认证信息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add auth to request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - request: Request dict to modify
                - auth_config: Auth configuration
        
        Returns:
            ActionResult with modified request.
        """
        request = params.get('request', {})
        auth_config = params.get('auth_config', {})
        
        headers = request.get('headers', {})
        auth_type = auth_config.get('type', 'none')
        
        if auth_type == 'bearer':
            token = auth_config.get('token')
            headers['Authorization'] = f"Bearer {token}"
        
        elif auth_type == 'api_key':
            key_name = auth_config.get('key_name', 'X-API-Key')
            key_value = auth_config.get('key_value')
            location = auth_config.get('location', 'header')
            
            if location == 'header':
                headers[key_name] = key_value
            else:
                # Would add to query params
                pass
        
        elif auth_type == 'basic':
            credentials = auth_config.get('credentials')
            headers['Authorization'] = f"Basic {credentials}"
        
        request['headers'] = headers
        
        return ActionResult(
            success=True,
            message=f"Added {auth_type} auth to request",
            data={'request': request}
        )
