"""Request builder action module for RabAI AutoClick.

Provides HTTP request building with query parameter encoding,
header management, authentication, and body serialization.
"""

import sys
import os
import json
import base64
from typing import Any, Dict, List, Optional, Union, Callable
from urllib.parse import urlencode, quote
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ContentType(Enum):
    """HTTP content types."""
    JSON = "application/json"
    FORM = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    XML = "application/xml"
    TEXT = "text/plain"
    HTML = "text/html"


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"


@dataclass
class RequestSpec:
    """HTTP request specification."""
    method: str = "GET"
    url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, Any] = field(default_factory=dict)
    body: Any = None
    content_type: Optional[str] = None
    auth: Dict[str, Any] = field(default_factory=dict)
    timeout: float = 30.0
    allow_redirects: bool = True


class RequestBuilderAction(BaseAction):
    """Build HTTP requests with various authentication and body formats.
    
    Supports query parameter encoding, header management, 
    authentication schemes, and body serialization.
    """
    action_type = "request_builder"
    display_name = "请求构建器"
    description = "构建HTTP请求，支持多种认证和格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute request building.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'build', 'build_batch'
                - method: HTTP method (default GET)
                - url: Request URL
                - headers: Request headers dict
                - query: Query parameters dict
                - body: Request body
                - content_type: Content-Type header
                - auth: Auth config dict
                - timeout: Request timeout
        
        Returns:
            ActionResult with built request spec.
        """
        operation = params.get('operation', 'build').lower()
        
        if operation == 'build':
            return self._build(params)
        elif operation == 'build_batch':
            return self._build_batch(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _build(self, params: Dict[str, Any]) -> ActionResult:
        """Build a single request."""
        spec = RequestSpec()
        
        # Method
        spec.method = params.get('method', 'GET').upper()
        
        # URL
        url = params.get('url', '')
        if not url:
            return ActionResult(success=False, message="url is required")
        
        # Query params
        query = params.get('query', {})
        if query:
            encoded_query = urlencode(self._flatten_values(query))
            url = f"{url}?{encoded_query}" if '?' not in url else f"{url}&{encoded_query}"
        
        spec.url = url
        spec.query_params = query
        
        # Headers
        headers = params.get('headers', {})
        spec.headers = dict(headers)
        
        # Content type
        content_type = params.get('content_type')
        if content_type:
            spec.content_type = content_type
            spec.headers['Content-Type'] = content_type
        
        # Body
        body = params.get('body')
        if body is not None and spec.method in ('POST', 'PUT', 'PATCH'):
            spec.body = self._serialize_body(body, spec.content_type)
            if spec.content_type and 'Content-Type' not in spec.headers:
                spec.headers['Content-Type'] = spec.content_type
            elif 'Content-Type' not in spec.headers:
                spec.headers['Content-Type'] = ContentType.JSON.value
        
        # Auth
        auth = params.get('auth', {})
        if auth:
            spec.auth = auth
            self._apply_auth(spec, auth)
        
        # Timeout
        spec.timeout = params.get('timeout', 30.0)
        
        return ActionResult(
            success=True,
            message=f"Built {spec.method} request to {spec.url}",
            data={
                'method': spec.method,
                'url': spec.url,
                'headers': spec.headers,
                'body': spec.body,
                'timeout': spec.timeout
            }
        )
    
    def _build_batch(self, params: Dict[str, Any]) -> ActionResult:
        """Build multiple requests from a template."""
        template = params.get('template', {})
        items = params.get('items', [])
        
        if not template:
            return ActionResult(success=False, message="template is required")
        
        results = []
        for item in items:
            # Merge template with item
            merged = {**template, **item}
            result = self._build(merged)
            if result.success:
                results.append(result.data)
        
        return ActionResult(
            success=True,
            message=f"Built {len(results)} requests",
            data={'requests': results, 'count': len(results)}
        )
    
    def _flatten_values(self, d: Dict, parent_key: str = '') -> Dict:
        """Flatten nested dict values for query params."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}[{k}]" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_values(v, new_key).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _serialize_body(
        self,
        body: Any,
        content_type: Optional[str]
    ) -> Optional[str]:
        """Serialize body based on content type."""
        if content_type == ContentType.JSON.value or content_type == 'application/json':
            if isinstance(body, (dict, list)):
                return json.dumps(body, ensure_ascii=False)
            return str(body)
        elif content_type == ContentType.FORM.value:
            if isinstance(body, dict):
                return urlencode(self._flatten_values(body))
            return str(body)
        else:
            if isinstance(body, (dict, list)):
                return json.dumps(body)
            return str(body) if body is not None else None
    
    def _apply_auth(self, spec: RequestSpec, auth: Dict[str, Any]) -> None:
        """Apply authentication to request."""
        auth_type = auth.get('type', 'none').lower()
        
        if auth_type == 'basic':
            username = auth.get('username', '')
            password = auth.get('password', '')
            credentials = f"{username}:{password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            spec.headers['Authorization'] = f"Basic {encoded}"
        
        elif auth_type == 'bearer':
            token = auth.get('token', '')
            spec.headers['Authorization'] = f"Bearer {token}"
        
        elif auth_type == 'api_key':
            key = auth.get('key', '')
            value = auth.get('value', '')
            location = auth.get('location', 'header').lower()
            
            if location == 'header':
                spec.headers[key] = value
            elif location == 'query':
                # Add to URL query
                separator = '&' if '?' in spec.url else '?'
                spec.url = f"{spec.url}{separator}{key}={quote(value)}"
        
        elif auth_type == 'oauth2':
            token = auth.get('access_token', '')
            token_type = auth.get('token_type', 'Bearer')
            spec.headers['Authorization'] = f"{token_type} {token}"


class GraphQLRequestAction(BaseAction):
    """Build and execute GraphQL requests."""
    action_type = "graphql_request"
    display_name = "GraphQL请求"
    description = "构建和执行GraphQL查询"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build GraphQL request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: GraphQL endpoint
                - query: GraphQL query string
                - variables: Query variables dict
                - operation_name: Optional operation name
                - headers: Additional headers
        
        Returns:
            ActionResult with GraphQL request payload.
        """
        url = params.get('url')
        query = params.get('query')
        variables = params.get('variables', {})
        operation_name = params.get('operation_name')
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="url is required")
        if not query:
            return ActionResult(success=False, message="query is required")
        
        payload = {'query': query}
        if variables:
            payload['variables'] = variables
        if operation_name:
            payload['operationName'] = operation_name
        
        request_headers = {
            'Content-Type': 'application/json',
            **headers
        }
        
        return ActionResult(
            success=True,
            message=f"Built GraphQL request",
            data={
                'url': url,
                'method': 'POST',
                'headers': request_headers,
                'body': json.dumps(payload)
            }
        )


class BatchRequestAction(BaseAction):
    """Build batched API requests."""
    action_type = "batch_request"
    display_name = "批量请求"
    description = "构建批量API请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build batch request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - requests: List of request specs
                - batch_type: 'sequential', 'parallel', 'priority'
        
        Returns:
            ActionResult with batched requests.
        """
        requests = params.get('requests', [])
        batch_type = params.get('batch_type', 'parallel')
        
        if not requests:
            return ActionResult(success=False, message="requests list required")
        
        # Validate all requests
        for i, req in enumerate(requests):
            if not req.get('url'):
                return ActionResult(
                    success=False,
                    message=f"Request {i} missing url"
                )
        
        return ActionResult(
            success=True,
            message=f"Built {len(requests)} batched requests",
            data={
                'requests': requests,
                'batch_type': batch_type,
                'count': len(requests)
            }
        )
