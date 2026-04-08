"""REST client action module for RabAI AutoClick.

Provides advanced REST client capabilities with authentication,
retry logic, request/response transformation, and error handling.
"""

import sys
import os
import json
import time
import hmac
import hashlib
import base64
import urllib.parse
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RestClientAction(BaseAction):
    """Advanced REST client with multiple auth strategies.
    
    Supports API Key, Bearer Token, Basic Auth, OAuth 2.0,
    HMAC signature, and request/response transformation.
    """
    action_type = "rest_client"
    display_name = "REST客户端"
    description = "高级REST客户端，支持多种认证方式和请求转换"

    AUTH_TYPES = ["api_key", "bearer", "basic", "oauth2", "hmac", "none"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a REST API call with advanced options.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str, required
                - method: str (GET/POST/PUT/DELETE/PATCH)
                - auth_type: str (api_key/bearer/basic/oauth2/hmac/none)
                - auth_config: dict (auth-specific config)
                - headers: dict
                - body: dict or str
                - query_params: dict
                - timeout: int (seconds)
                - retry_count: int
                - retry_delay: float
                - transform_request: callable
                - transform_response: callable
                - save_to_var: str
        
        Returns:
            ActionResult with response data.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        auth_type = params.get('auth_type', 'none')
        auth_config = params.get('auth_config', {})
        headers = params.get('headers', {})
        body = params.get('body', None)
        query_params = params.get('query_params', {})
        timeout = params.get('timeout', 30)
        retry_count = params.get('retry_count', 0)
        retry_delay = params.get('retry_delay', 1.0)
        transform_request = params.get('transform_request', None)
        transform_response = params.get('transform_response', None)
        save_to_var = params.get('save_to_var', None)

        if not url:
            return ActionResult(success=False, message="URL is required")

        # Apply auth
        headers = self._apply_auth(auth_type, auth_config, headers, method, url)

        # Apply request transformation
        if transform_request and callable(transform_request):
            try:
                body = transform_request(body)
            except Exception as e:
                return ActionResult(success=False, message=f"Request transform failed: {e}")

        # Build URL with query params
        if query_params:
            parsed = urllib.parse.urlparse(url)
            q = urllib.parse.parse_qsl(parsed.query)
            q.extend(list(query_params.items()))
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urllib.parse.urlencode(q)}"

        # Execute with retry
        last_error = None
        for attempt in range(retry_count + 1):
            try:
                response = self._do_request(method, url, headers, body, timeout)
                
                # Apply response transformation
                if transform_response and callable(transform_response):
                    response = transform_response(response)
                
                result_data = {
                    'status_code': response.get('status_code', 0),
                    'headers': response.get('headers', {}),
                    'body': response.get('body'),
                    'elapsed_ms': response.get('elapsed_ms', 0),
                    'attempt': attempt + 1,
                }
                
                if save_to_var and context:
                    context.variables[save_to_var] = result_data
                
                return ActionResult(
                    success=response.get('status_code', 0) < 400,
                    data=result_data,
                    message=f"HTTP {response.get('status_code', 0)} in {result_data['elapsed_ms']}ms"
                )
                
            except Exception as e:
                last_error = str(e)
                if attempt < retry_count:
                    time.sleep(retry_delay * (attempt + 1))
        
        return ActionResult(success=False, message=f"Request failed after {retry_count + 1} attempts: {last_error}")

    def _apply_auth(self, auth_type: str, config: Dict, headers: Dict, method: str, url: str) -> Dict:
        """Apply authentication to headers."""
        headers = dict(headers)
        
        if auth_type == 'api_key':
            key = config.get('key', 'X-API-Key')
            value = config.get('value', '')
            location = config.get('location', 'header')
            if location == 'header':
                headers[key] = value
            elif location == 'query':
                parsed = urllib.parse.urlparse(url)
                q = urllib.parse.parse_qsl(parsed.query)
                q.append((key, value))
                # This is handled in execute
        elif auth_type == 'bearer':
            token = config.get('token', '')
            headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'basic':
            username = config.get('username', '')
            password = config.get('password', '')
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers['Authorization'] = f'Basic {credentials}'
        elif auth_type == 'oauth2':
            token = config.get('access_token', '')
            token_type = config.get('token_type', 'Bearer')
            headers['Authorization'] = f'{token_type} {token}'
        elif auth_type == 'hmac':
            secret = config.get('secret', '')
            message = config.get('message', '')
            algorithm = config.get('algorithm', 'sha256')
            signature = self._hmac_sign(secret, message, algorithm)
            headers['X-HMAC-Signature'] = signature
            headers['X-HMAC-Algorithm'] = algorithm
        
        return headers

    def _hmac_sign(self, secret: str, message: str, algorithm: str) -> str:
        """Generate HMAC signature."""
        alg = algorithm.lower().replace('-', '')
        if alg == 'sha256':
            digest = hashlib.sha256
        elif alg == 'sha512':
            digest = hashlib.sha512
        elif alg == 'md5':
            digest = hashlib.md5
        else:
            digest = hashlib.sha256
        
        sig = hmac.new(secret.encode(), message.encode(), digest)
        return base64.b64encode(sig.digest()).decode()

    def _do_request(self, method: str, url: str, headers: Dict, body: Any, timeout: int) -> Dict:
        """Execute HTTP request using urllib."""
        import urllib.request
        import urllib.error
        
        start = time.time()
        body_bytes = None
        
        if body is not None:
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
                if 'Content-Type' not in headers:
                    headers['Content-Type'] = 'application/json'
            elif isinstance(body, str):
                body_bytes = body.encode('utf-8')
        
        req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
        
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status_code = resp.status
                resp_headers = dict(resp.headers)
                resp_body = resp.read()
                elapsed_ms = int((time.time() - start) * 1000)
                
                # Try to parse as JSON
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except (ValueError, UnicodeDecodeError):
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {
                    'status_code': status_code,
                    'headers': resp_headers,
                    'body': body_data,
                    'elapsed_ms': elapsed_ms,
                }
        except urllib.error.HTTPError as e:
            elapsed_ms = int((time.time() - start) * 1000)
            body_data = None
            try:
                body_data = json.loads(e.read().decode('utf-8'))
            except:
                body_data = e.read().decode('utf-8', errors='replace') if e.fp else None
            
            return {
                'status_code': e.code,
                'headers': dict(e.headers) if e.headers else {},
                'body': body_data,
                'elapsed_ms': elapsed_ms,
            }
        except Exception as e:
            raise


class OAuth2TokenAction(BaseAction):
    """Obtain or refresh OAuth 2.0 access token.
    
    Supports client credentials, password grant,
    and token refresh flows.
    """
    action_type = "oauth2_token"
    display_name = "OAuth2令牌"
    description = "获取或刷新OAuth2访问令牌"

    GRANT_TYPES = ["client_credentials", "password", "refresh_token", "authorization_code"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get or refresh OAuth2 token.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - grant_type: str
                - token_url: str
                - client_id: str
                - client_secret: str
                - username: str (for password grant)
                - password: str (for password grant)
                - refresh_token: str (for refresh)
                - scope: str
                - save_to_var: str
        
        Returns:
            ActionResult with token data.
        """
        grant_type = params.get('grant_type', 'client_credentials')
        token_url = params.get('token_url', '')
        client_id = params.get('client_id', '')
        client_secret = params.get('client_secret', '')
        username = params.get('username', '')
        password = params.get('password', '')
        refresh_token = params.get('refresh_token', '')
        scope = params.get('scope', '')
        save_to_var = params.get('save_to_var', 'oauth2_token')

        if not token_url:
            return ActionResult(success=False, message="token_url is required")

        # Build request body
        data = {
            'grant_type': grant_type,
            'client_id': client_id,
            'client_secret': client_secret,
        }
        
        if grant_type == 'password':
            data['username'] = username
            data['password'] = password
        elif grant_type == 'refresh_token':
            data['refresh_token'] = refresh_token
        elif grant_type == 'authorization_code':
            data['code'] = params.get('code', '')
            data['redirect_uri'] = params.get('redirect_uri', '')
        
        if scope:
            data['scope'] = scope

        # URL-encode the body
        encoded = urllib.parse.urlencode(data).encode('utf-8')

        try:
            import urllib.request
            req = urllib.request.Request(
                token_url,
                data=encoded,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))
                
                if save_to_var and context:
                    context.variables[save_to_var] = token_data
                
                return ActionResult(
                    success=True,
                    data=token_data,
                    message=f"Token obtained: {token_data.get('token_type', 'Bearer')}"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth2 error: {e}")


import urllib.parse
