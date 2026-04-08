"""API Client Factory action module for RabAI AutoClick.

Creates pre-configured API client instances with
authentication, retry, and middleware.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiClientFactoryAction(BaseAction):
    """Factory for creating configured API client instances.

    Produces clients with auth, retry logic, rate limiting,
    and custom interceptors.
    """
    action_type = "api_client_factory"
    display_name = "API客户端工厂"
    description = "创建预配置的API客户端实例"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create an API client.

        Args:
            context: Execution context.
            params: Dict with keys: base_url, auth_type, auth_credentials,
                   timeout, retry_count, rate_limit, interceptors.

        Returns:
            ActionResult with client instance config.
        """
        start_time = time.time()
        try:
            base_url = params.get('base_url', '')
            auth_type = params.get('auth_type', 'none')
            auth_credentials = params.get('auth_credentials', {})
            timeout = params.get('timeout', 30)
            retry_count = params.get('retry_count', 3)
            rate_limit = params.get('rate_limit', 0)
            interceptors = params.get('interceptors', [])

            if not base_url:
                return ActionResult(
                    success=False,
                    message="base_url is required",
                    duration=time.time() - start_time,
                )

            client = {
                'base_url': base_url.rstrip('/'),
                'auth_type': auth_type,
                'auth_config': self._configure_auth(auth_type, auth_credentials),
                'timeout': timeout,
                'retry_count': retry_count,
                'rate_limit': rate_limit,
                'interceptors': interceptors,
                'created_at': time.time(),
            }

            # Create actual client object
            client_instance = ApiClient(
                base_url=base_url,
                auth_config=client['auth_config'],
                timeout=timeout,
                retry_count=retry_count,
                rate_limit=rate_limit,
                interceptors=interceptors,
            )

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Created API client for {base_url}",
                data={'client': client, 'instance_id': id(client_instance)},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Client factory error: {str(e)}",
                duration=duration,
            )

    def _configure_auth(self, auth_type: str, credentials: Dict) -> Dict:
        """Configure authentication."""
        if auth_type == 'bearer':
            return {'type': 'bearer', 'token': credentials.get('token', '')}
        elif auth_type == 'basic':
            return {'type': 'basic', 'username': credentials.get('username', ''), 'password': credentials.get('password', '')}
        elif auth_type == 'apikey':
            return {'type': 'apikey', 'key': credentials.get('key', ''), 'header': credentials.get('header', 'X-API-Key')}
        elif auth_type == 'oauth2':
            return {'type': 'oauth2', 'client_id': credentials.get('client_id', ''), 'client_secret': credentials.get('client_secret', ''), 'token_url': credentials.get('token_url', '')}
        return {'type': 'none'}


class ApiClient:
    """Pre-configured API client."""

    def __init__(
        self,
        base_url: str,
        auth_config: Dict,
        timeout: int,
        retry_count: int,
        rate_limit: float,
        interceptors: List[Callable],
    ):
        self.base_url = base_url.rstrip('/')
        self.auth_config = auth_config
        self.timeout = timeout
        self.retry_count = retry_count
        self.rate_limit = rate_limit
        self.interceptors = interceptors
        self._last_request_time = 0.0

    def request(self, method: str, path: str, **kwargs) -> Dict:
        """Make an HTTP request with all configured options."""
        import json
        import time

        url = self.base_url + path
        headers = kwargs.get('headers', {})
        data = kwargs.get('data')
        params = kwargs.get('params', {})

        # Apply auth
        if self.auth_config['type'] == 'bearer':
            headers['Authorization'] = f"Bearer {self.auth_config['token']}"
        elif self.auth_config['type'] == 'basic':
            import base64
            creds = f"{self.auth_config['username']}:{self.auth_config['password']}"
            headers['Authorization'] = f"Basic {base64.b64encode(creds.encode()).decode()}"
        elif self.auth_config['type'] == 'apikey':
            headers[self.auth_config['header']] = self.auth_config['key']

        # Rate limiting
        if self.rate_limit > 0:
            min_interval = 1.0 / self.rate_limit
            elapsed = time.time() - self._last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request_time = time.time()

        # Apply interceptors (request)
        for interceptor in self.interceptors:
            if callable(interceptor):
                headers, data = interceptor({'method': method, 'url': url, 'headers': headers, 'data': data})

        body = None
        if data:
            if isinstance(data, dict):
                body = json.dumps(data).encode('utf-8')
                headers.setdefault('Content-Type', 'application/json')
            elif isinstance(data, str):
                body = data.encode('utf-8')

        req = Request(url, data=body, headers=headers, method=method)
        for attempt in range(self.retry_count):
            try:
                with urlopen(req, timeout=self.timeout) as resp:
                    return {'success': True, 'status': resp.status, 'data': json.loads(resp.read())}
            except HTTPError as e:
                if attempt == self.retry_count - 1:
                    return {'success': False, 'status': e.code, 'error': str(e)}
                time.sleep(2 ** attempt)
            except Exception as e:
                if attempt == self.retry_count - 1:
                    return {'success': False, 'error': str(e)}
                time.sleep(2 ** attempt)
        return {'success': False, 'error': 'Max retries exceeded'}


class RestClientBuilderAction(BaseAction):
    """Build typed REST clients from service definitions.

    Creates method-based clients for specific API services.
    """
    action_type = "rest_client_builder"
    display_name = "REST客户端构建器"
    description = "从服务定义构建类型化REST客户端"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build REST client.

        Args:
            context: Execution context.
            params: Dict with keys: service_name, base_url, endpoints.

        Returns:
            ActionResult with built client code.
        """
        start_time = time.time()
        try:
            service_name = params.get('service_name', 'MyService')
            base_url = params.get('base_url', 'http://localhost')
            endpoints = params.get('endpoints', [])

            class_code = self._generate_client_class(service_name, base_url, endpoints)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Generated REST client for {service_name}",
                data={'class_code': class_code, 'service_name': service_name},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Client builder error: {str(e)}",
                duration=duration,
            )

    def _generate_client_class(self, name: str, base_url: str, endpoints: List[Dict]) -> str:
        """Generate Python client class code."""
        lines = [
            f'class {name}Client:',
            f'    """Auto-generated REST client for {name}."""',
            '',
            f'    def __init__(self, base_url="{base_url}", session=None):',
            '        import requests',
            '        self.base_url = base_url',
            '        self.session = session or requests.Session()',
            '',
        ]
        for ep in endpoints:
            ep_name = ep.get('name', 'endpoint')
            method = ep.get('method', 'GET').lower()
            path = ep.get('path', '/')
            summary = ep.get('summary', '')
            lines.extend([
                '',
                f'    def {ep_name}(self, **kwargs):',
                f'        """{summary}"""',
                f'        return self.session.{method}(f"{{self.base_url}}{path}", **kwargs)',
            ])
        return '\n'.join(lines)
