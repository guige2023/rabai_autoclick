"""HTTP headers action module for RabAI AutoClick.

Provides HTTP header operations:
- HttpHeadersGetAction: Get headers from URL
- HttpHeadersParseAction: Parse headers string
- HttpHeadersBuildAction: Build headers dict
- HttpHeadersFilterAction: Filter headers
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpHeadersGetAction(BaseAction):
    """Get headers from URL."""
    action_type = "http_headers_get"
    display_name = "HTTP获取头"
    description = "获取HTTP响应头"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute headers get."""
        url = params.get('url', '')
        output_var = params.get('output_var', 'http_headers')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url

            request = urllib.request.Request(resolved_url, method='HEAD')
            with urllib.request.urlopen(request, timeout=30) as resp:
                headers = dict(resp.headers)
                result = {'headers': headers, 'status_code': resp.status}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Got {len(headers)} headers from {resolved_url}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Headers get error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'http_headers'}


class HttpHeadersParseAction(BaseAction):
    """Parse headers from string."""
    action_type = "http_headers_parse"
    display_name = "HTTP解析头"
    description = "解析HTTP头字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute headers parse."""
        header_str = params.get('header_string', '')
        format = params.get('format', 'raw')  # raw, http
        output_var = params.get('output_var', 'parsed_headers')

        if not header_str:
            return ActionResult(success=False, message="header_string is required")

        try:
            import email.message

            resolved_str = context.resolve_value(header_str) if context else header_str

            if format == 'http':
                msg = email.message.Message()
                for line in resolved_str.strip().split('\n'):
                    if ':' in line:
                        k, v = line.split(':', 1)
                        msg[k.strip()] = v.strip()
                headers = dict(msg.items())
            else:
                # Raw format: "Header-Name: value"
                headers = {}
                for line in resolved_str.strip().split('\n'):
                    line = line.strip()
                    if ':' in line:
                        k, v = line.split(':', 1)
                        headers[k.strip().lower()] = v.strip()

            result = {'headers': headers, 'count': len(headers)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Parsed {len(headers)} headers", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Headers parse error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['header_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'raw', 'output_var': 'parsed_headers'}


class HttpHeadersBuildAction(BaseAction):
    """Build headers dict."""
    action_type = "http_headers_build"
    display_name = "HTTP构建头"
    description = "构建HTTP请求头"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute headers build."""
        headers_dict = params.get('headers', {})
        user_agent = params.get('user_agent', None)
        accept = params.get('accept', None)
        content_type = params.get('content_type', None)
        auth = params.get('auth', None)  # basic auth in "user:pass" format
        bearer_token = params.get('bearer_token', None)
        custom_headers = params.get('custom_headers', {})
        output_var = params.get('output_var', 'built_headers')

        try:
            resolved_dict = context.resolve_value(headers_dict) if context else headers_dict
            resolved_custom = context.resolve_value(custom_headers) if context else custom_headers

            headers = dict(resolved_dict)

            if user_agent:
                headers['User-Agent'] = context.resolve_value(user_agent) if context else user_agent
            if accept:
                headers['Accept'] = context.resolve_value(accept) if context else accept
            if content_type:
                headers['Content-Type'] = context.resolve_value(content_type) if context else content_type
            if auth:
                import base64
                auth_val = context.resolve_value(auth) if context else auth
                if isinstance(auth_val, str) and ':' in auth_val:
                    encoded = base64.b64encode(auth_val.encode()).decode()
                    headers['Authorization'] = f'Basic {encoded}'
            if bearer_token:
                bt = context.resolve_value(bearer_token) if context else bearer_token
                headers['Authorization'] = f'Bearer {bt}'

            headers.update(resolved_custom)

            result = {'headers': headers, 'count': len(headers)}
            if context:
                context.set(output_var, headers)
            return ActionResult(success=True, message=f"Built {len(headers)} headers", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Headers build error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'headers': {}, 'user_agent': None, 'accept': None, 'content_type': None,
            'auth': None, 'bearer_token': None, 'custom_headers': {}, 'output_var': 'built_headers'
        }


class HttpHeadersFilterAction(BaseAction):
    """Filter headers."""
    action_type = "http_headers_filter"
    display_name = "HTTP过滤头"
    description = "过滤HTTP头"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute headers filter."""
        headers_var = params.get('headers_var', 'http_headers')
        include = params.get('include', None)  # list of header names to include
        exclude = params.get('exclude', None)  # list of header names to exclude
        output_var = params.get('output_var', 'filtered_headers')

        try:
            resolved_include = context.resolve_value(include) if context else include
            resolved_exclude = context.resolve_value(exclude) if context else exclude

            headers_data = context.resolve_value(headers_var) if context else None
            if headers_data is None:
                headers_data = context.resolve_value(headers_var)

            if isinstance(headers_data, dict):
                headers = headers_data
            elif isinstance(headers_data, dict) and 'headers' in headers_data:
                headers = headers_data['headers']
            else:
                headers = {}

            filtered = dict(headers)

            if resolved_include:
                filtered = {k: v for k, v in filtered.items() if k.lower() in [h.lower() for h in resolved_include]}
            if resolved_exclude:
                filtered = {k: v for k, v in filtered.items() if k.lower() not in [h.lower() for h in resolved_exclude]}

            result = {'headers': filtered, 'count': len(filtered)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Filtered to {len(filtered)} headers", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Headers filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers_var': 'http_headers', 'include': None, 'exclude': None, 'output_var': 'filtered_headers'}
