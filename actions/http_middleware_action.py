"""HTTP middleware action module for RabAI AutoClick.

Provides HTTP middleware patterns: request/response
transformers, logging, auth injection, and error handling.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RequestTransformerAction(BaseAction):
    """Transform HTTP requests before sending.
    
    Apply header modifications, body transforms,
    and URL rewrites.
    """
    action_type = "request_transform"
    display_name = "请求转换"
    description = "发送前转换HTTP请求：修改头、转换体、重写URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform HTTP request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str
                - method: str
                - headers: dict
                - body: any
                - transforms: list of {type, field, value, op}
                - save_to_var: str
        
        Returns:
            ActionResult with transformed request.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        headers = dict(params.get('headers', {}))
        body = params.get('body')
        transforms = params.get('transforms', [])
        save_to_var = params.get('save_to_var', 'transformed_request')

        transformed_url = url
        transformed_headers = headers
        transformed_body = body

        for t in transforms:
            t_type = t.get('type', '')

            if t_type == 'header_add':
                key = t.get('key', '')
                value = t.get('value', '')
                transformed_headers[key] = value

            elif t_type == 'header_remove':
                key = t.get('key', '')
                transformed_headers.pop(key, None)

            elif t_type == 'header_rename':
                old_key = t.get('old_key', '')
                new_key = t.get('new_key', '')
                if old_key in transformed_headers:
                    transformed_headers[new_key] = transformed_headers.pop(old_key)

            elif t_type == 'url_query_add':
                key = t.get('key', '')
                value = t.get('value', '')
                sep = '&' if '?' in transformed_url else '?'
                transformed_url = f"{transformed_url}{sep}{key}={value}"

            elif t_type == 'url_path_replace':
                pattern = t.get('pattern', '')
                replacement = t.get('replacement', '')
                transformed_url = transformed_url.replace(pattern, replacement)

            elif t_type == 'body_json_transform':
                if isinstance(transformed_body, dict):
                    field = t.get('field', '')
                    value = t.get('value', '')
                    op = t.get('op', 'set')
                    if op == 'set':
                        transformed_body[field] = value
                    elif op == 'delete':
                        transformed_body.pop(field, None)
                    elif op == 'rename':
                        new_field = t.get('new_field', '')
                        if field in transformed_body:
                            transformed_body[new_field] = transformed_body.pop(field)

        result = {
            'url': transformed_url,
            'method': method,
            'headers': transformed_headers,
            'body': transformed_body,
            'transforms_applied': len(transforms),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Request transformed: {len(transforms)} transforms applied"
        )


class ResponseTransformerAction(BaseAction):
    """Transform HTTP responses after receiving.
    
    Extract fields, flatten nested data, rename keys,
    and filter sensitive information.
    """
    action_type = "response_transform"
    display_name = "响应转换"
    description = "接收后转换HTTP响应：提取字段、扁平化、重命名、过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform HTTP response.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - response: dict with status_code, body, headers
                - extract: list of fields to extract
                - flatten: bool (flatten nested dicts)
                - rename: dict {old: new}
                - exclude: list of fields to exclude
                - save_to_var: str
        
        Returns:
            ActionResult with transformed response.
        """
        response = params.get('response', {})
        extract_fields = params.get('extract', [])
        flatten = params.get('flatten', False)
        rename_map = params.get('rename', {})
        exclude_fields = params.get('exclude', [])
        save_to_var = params.get('save_to_var', 'transformed_response')

        body = response.get('body')

        if isinstance(body, dict):
            result_body = dict(body)

            # Extract specific fields
            if extract_fields:
                result_body = {k: result_body.get(k) for k in extract_fields if k in result_body}

            # Flatten nested dicts
            if flatten:
                result_body = self._flatten(result_body)

            # Rename fields
            if rename_map:
                for old, new in rename_map.items():
                    if old in result_body:
                        result_body[new] = result_body.pop(old)

            # Exclude fields
            if exclude_fields:
                for f in exclude_fields:
                    result_body.pop(f, None)
        else:
            result_body = body

        result = {
            'original_status': response.get('status_code'),
            'body': result_body,
            'transforms_applied': sum([bool(extract_fields), flatten, bool(rename_map), bool(exclude_fields)]),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Response transformed"
        )

    def _flatten(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self._flatten(item, f"{new_key}[{i}]", sep=sep).items())
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        return dict(items)


class AuthInjectionAction(BaseAction):
    """Inject authentication into HTTP requests.
    
    Support API key, Bearer token, Basic auth,
    and custom header injection.
    """
    action_type = "auth_injection"
    display_name = "认证注入"
    description = "向HTTP请求注入认证信息：API Key/Bearer/Basic"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Inject auth into request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - headers: dict
                - auth_type: str (api_key/bearer/basic/custom)
                - auth_config: dict
                - save_to_var: str
        
        Returns:
            ActionResult with auth-injected headers.
        """
        headers = dict(params.get('headers', {}))
        auth_type = params.get('auth_type', 'bearer')
        auth_config = params.get('auth_config', {})
        save_to_var = params.get('save_to_var', 'auth_result')

        import base64

        if auth_type == 'api_key':
            key_name = auth_config.get('key_name', 'X-API-Key')
            key_value = auth_config.get('key_value', '')
            location = auth_config.get('location', 'header')
            if location == 'header':
                headers[key_name] = key_value
            # Query param handled at URL level

        elif auth_type == 'bearer':
            token = auth_config.get('token', '')
            headers['Authorization'] = f'Bearer {token}'

        elif auth_type == 'basic':
            username = auth_config.get('username', '')
            password = auth_config.get('password', '')
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode('ascii')
            headers['Authorization'] = f'Basic {credentials}'

        elif auth_type == 'custom':
            header_name = auth_config.get('header_name', '')
            header_value = auth_config.get('header_value', '')
            if header_name:
                headers[header_name] = header_value

        result = {
            'auth_type': auth_type,
            'headers': headers,
            'injected': True,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Auth injected: {auth_type}"
        )


class HttpLoggingAction(BaseAction):
    """Log HTTP requests and responses.
    
    Capture request details, timing, status codes,
    and response bodies for debugging.
    """
    action_type = "http_logging"
    display_name = "HTTP日志"
    description = "记录HTTP请求和响应用于调试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Log HTTP interaction.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - request: dict {url, method, headers, body}
                - response: dict {status_code, body, headers, elapsed_ms}
                - log_level: str (debug/info/warn/error)
                - include_body: bool
                - save_to_var: str
        
        Returns:
            ActionResult with log entry.
        """
        request = params.get('request', {})
        response = params.get('response', {})
        log_level = params.get('log_level', 'info')
        include_body = params.get('include_body', False)
        save_to_var = params.get('save_to_var', 'http_log')

        log_entry = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'level': log_level,
            'request': {
                'method': request.get('method', 'GET'),
                'url': request.get('url', ''),
                'headers': dict(request.get('headers', {})),
            },
            'response': {
                'status_code': response.get('status_code', 0),
                'elapsed_ms': response.get('elapsed_ms', 0),
            }
        }

        if include_body:
            log_entry['request']['body'] = request.get('body')
            log_entry['response']['body'] = response.get('body')

        # Truncate body for logging
        if not include_body:
            req_body = request.get('body')
            if req_body:
                log_entry['request']['body_size'] = len(str(req_body))
            resp_body = response.get('body')
            if resp_body:
                log_entry['response']['body_size'] = len(str(resp_body))

        if context and save_to_var:
            context.variables[save_to_var] = log_entry

        return ActionResult(
            success=True,
            data=log_entry,
            message=f"HTTP {request.get('method', '?')} -> {response.get('status_code', '?')} in {response.get('elapsed_ms', 0)}ms"
        )
