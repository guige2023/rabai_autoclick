"""API authentication action module for RabAI AutoClick.

Provides various API authentication strategies including API key,
Bearer token, Basic auth, OAuth2 client credentials, and JWT.
"""

import base64
import hashlib
import hmac
import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiKeyAuthAction(BaseAction):
    """Authenticate API requests using API key in header or query param.
    
    Supports X-API-Key, Authorization header, and query string API keys.
    """
    action_type = "api_key_auth"
    display_name = "API密钥认证"
    description = "使用API密钥进行认证"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply API key authentication to request.
        
        Args:
            context: Execution context.
            params: Dict with keys: api_key, location (header|query),
                   header_name, query_param, prefix.
        
        Returns:
            ActionResult with auth headers or query params.
        """
        api_key = params.get('api_key', '')
        if not api_key:
            return ActionResult(
                success=False,
                message="api_key is required"
            )

        location = params.get('location', 'header')
        header_name = params.get('header_name', 'X-API-Key')
        query_param = params.get('query_param', 'api_key')
        prefix = params.get('prefix', '')

        start_time = time.time()
        auth_data = {'api_key': api_key, 'location': location}

        if location == 'header':
            value = f"{prefix} {api_key}" if prefix else api_key
            return ActionResult(
                success=True,
                message=f"API key auth configured for header '{header_name}'",
                data={'headers': {header_name: value}},
                duration=time.time() - start_time
            )
        elif location == 'query':
            return ActionResult(
                success=True,
                message=f"API key auth configured for query param '{query_param}'",
                data={'query_params': {query_param: api_key}},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Invalid location: {location}. Use 'header' or 'query'."
            )

    def _validate_key_format(self, api_key: str) -> bool:
        """Validate API key format."""
        if not api_key or len(api_key) < 8:
            return False
        return True


class BearerTokenAuthAction(BaseAction):
    """Authenticate API requests using Bearer token (OAuth2 access token).
    
    Adds Authorization: Bearer <token> header to requests.
    """
    action_type = "bearer_token_auth"
    display_name = "Bearer令牌认证"
    description = "使用Bearer令牌进行OAuth2认证"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply Bearer token authentication.
        
        Args:
            context: Execution context.
            params: Dict with keys: token, token_type, header_name.
        
        Returns:
            ActionResult with Authorization header.
        """
        token = params.get('token', '')
        if not token:
            return ActionResult(
                success=False,
                message="token is required"
            )

        token_type = params.get('token_type', 'Bearer')
        header_name = params.get('header_name', 'Authorization')
        start_time = time.time()

        return ActionResult(
            success=True,
            message=f"{token_type} token auth configured",
            data={
                'headers': {header_name: f"{token_type} {token}"}
            },
            duration=time.time() - start_time
        )


class BasicAuthAction(BaseAction):
    """Authenticate API requests using HTTP Basic Authentication.
    
    Encodes username:password as Base64 and sets Authorization header.
    """
    action_type = "basic_auth"
    display_name = "Basic认证"
    description = "使用HTTP Basic认证"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply Basic authentication.
        
        Args:
            context: Execution context.
            params: Dict with keys: username, password, header_name.
        
        Returns:
            ActionResult with Authorization header.
        """
        username = params.get('username', '')
        password = params.get('password', '')
        if not username or not password:
            return ActionResult(
                success=False,
                message="username and password are required"
            )

        header_name = params.get('header_name', 'Authorization')
        start_time = time.time()

        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return ActionResult(
            success=True,
            message="Basic auth configured",
            data={
                'headers': {header_name: f"Basic {encoded}"}
            },
            duration=time.time() - start_time
        )


class JWTAuthAction(BaseAction):
    """Authenticate API requests using JSON Web Tokens (JWT).
    
    Supports HS256, HS384, HS512 algorithms with secret key signing.
    """
    action_type = "jwt_auth"
    display_name = "JWT认证"
    description = "使用JSON Web Token进行认证"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate and apply JWT authentication.
        
        Args:
            context: Execution context.
            params: Dict with keys: secret, algorithm, payload,
                   expires_in, header_name.
        
        Returns:
            ActionResult with Authorization header containing JWT.
        """
        import jwt as _jwt

        secret = params.get('secret', '')
        if not secret:
            return ActionResult(
                success=False,
                message="secret is required for JWT signing"
            )

        algorithm = params.get('algorithm', 'HS256')
        payload = params.get('payload', {})
        expires_in = params.get('expires_in', 3600)
        header_name = params.get('header_name', 'Authorization')
        start_time = time.time()

        if algorithm not in ('HS256', 'HS384', 'HS512'):
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )

        now = int(time.time())
        claims = {
            'iat': now,
            'exp': now + expires_in,
            **payload
        }

        token = _jwt.encode(claims, secret, algorithm=algorithm)
        return ActionResult(
            success=True,
            message=f"JWT ({algorithm}) token generated",
            data={
                'headers': {header_name: f"Bearer {token}"},
                'token': token,
                'expires_at': now + expires_in
            },
            duration=time.time() - start_time
        )


class OAuth2ClientCredentialsAction(BaseAction):
    """Obtain OAuth2 access token using client credentials flow.
    
    Sends client_id and client_secret to token endpoint and returns
    access token for use in subsequent API calls.
    """
    action_type = "oauth2_client_credentials"
    display_name = "OAuth2客户端凭证"
    description = "使用OAuth2客户端凭证获取访问令牌"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Request OAuth2 access token.
        
        Args:
            context: Execution context.
            params: Dict with keys: token_url, client_id, client_secret,
                   scope, grant_type.
        
        Returns:
            ActionResult with access_token and token data.
        """
        import urllib.request
        import urllib.parse

        token_url = params.get('token_url', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        scope = params.get('scope', '')
        grant_type = params.get('grant_type', 'client_credentials')
        start_time = time.time()

        if not token_url:
            return ActionResult(
                success=False,
                message="token_url is required"
            )

        data = {
            'grant_type': grant_type,
            'client_id': client_id,
            'client_secret': client_secret,
        }
        if scope:
            data['scope'] = scope

        try:
            req_data = urllib.parse.urlencode(data).encode()
            req = urllib.request.Request(
                token_url,
                data=req_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_data = json.loads(resp.read().decode())

            return ActionResult(
                success=True,
                message="OAuth2 token obtained",
                data={
                    'access_token': token_data.get('access_token'),
                    'token_type': token_data.get('token_type', 'Bearer'),
                    'expires_in': token_data.get('expires_in'),
                    'scope': token_data.get('scope'),
                    'raw': token_data
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OAuth2 token request failed: {str(e)}",
                data={'error': str(e)}
            )


class HmacSignatureAction(BaseAction):
    """Sign API requests using HMAC signature.
    
    Computes HMAC-SHA256 (or other) signature of request data
    and includes it in the signature header.
    """
    action_type = "hmac_signature"
    display_name = "HMAC签名认证"
    description = "使用HMAC签名验证API请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate HMAC signature for request.
        
        Args:
            context: Execution context.
            params: Dict with keys: secret, algorithm, message,
                   header_name, signature_prefix.
        
        Returns:
            ActionResult with signature header.
        """
        secret = params.get('secret', '')
        if not secret:
            return ActionResult(
                success=False,
                message="secret is required for HMAC signing"
            )

        algorithm = params.get('algorithm', 'sha256')
        message = params.get('message', '')
        header_name = params.get('header_name', 'X-Signature')
        prefix = params.get('signature_prefix', 'sha256=')
        timestamp = params.get('timestamp')
        start_time = time.time()

        algo_map = {
            'sha256': hashlib.sha256,
            'sha384': hashlib.sha384,
            'sha512': hashlib.sha512,
            'md5': hashlib.md5,
        }
        if algorithm not in algo_map:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )

        if timestamp:
            sign_input = f"{timestamp}:{message}"
        else:
            sign_input = message

        signature = hmac.new(
            secret.encode(),
            sign_input.encode(),
            algo_map[algorithm]
        ).hexdigest()

        return ActionResult(
            success=True,
            message=f"HMAC-{algorithm.upper()} signature generated",
            data={
                'signature': f"{prefix}{signature}",
                'signature_header': header_name,
                'algorithm': algorithm,
                'timestamp': timestamp or int(time.time())
            },
            duration=time.time() - start_time
        )
