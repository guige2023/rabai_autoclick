"""HTTP action module for RabAI AutoClick.

Provides HTTP request actions:
- HttpRequestAction: Make HTTP requests
- HttpGetAction: GET requests (convenience)
- HttpPostAction: POST requests (convenience)
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpRequestAction(BaseAction):
    """Make HTTP requests."""
    action_type = "http_request"
    display_name = "HTTP请求"
    description = "发送HTTP请求"

    VALID_METHODS: List[str] = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an HTTP request.

        Args:
            context: Execution context.
            params: Dict with url, method, headers, body, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        headers = params.get('headers', {})
        body = params.get('body', None)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_response')

        # Validate url
        if not url:
            return ActionResult(
                success=False,
                message="未指定URL"
            )
        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate method
        valid, msg = self.validate_in(method, self.VALID_METHODS, 'method')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate timeout
        valid, msg = self.validate_type(timeout, (int, float), 'timeout')
        if not valid:
            return ActionResult(success=False, message=msg)
        if timeout <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'timeout' must be > 0, got {timeout}"
            )

        try:
            import urllib.request
            import urllib.error

            # Prepare request
            request_headers = {}
            for key, value in headers.items():
                resolved_key = context.resolve_value(key)
                resolved_value = context.resolve_value(value)
                request_headers[str(resolved_key)] = str(resolved_value)

            # Prepare body
            request_body = None
            if body is not None:
                if isinstance(body, dict):
                    request_body = json.dumps(body).encode('utf-8')
                    request_headers['Content-Type'] = 'application/json'
                else:
                    resolved_body = context.resolve_value(body)
                    request_body = str(resolved_body).encode('utf-8')

            # Make request
            req = urllib.request.Request(
                url=url,
                data=request_body,
                headers=request_headers,
                method=method
            )

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    response_body = response.read().decode('utf-8')
                    response_headers = dict(response.headers)
                    status_code = response.status

                    # Try to parse JSON
                    try:
                        response_data = json.loads(response_body)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        response_data = response_body

                    result_data = {
                        'status_code': status_code,
                        'headers': response_headers,
                        'body': response_data,
                        'url': url,
                        'method': method
                    }

                    # Store in context
                    context.set(output_var, result_data)

                    return ActionResult(
                        success=True,
                        message=f"HTTP请求成功: {method} {url} -> {status_code}",
                        data=result_data
                    )

            except urllib.error.HTTPError as e:
                error_body = e.read().decode('utf-8') if e.fp else ''
                return ActionResult(
                    success=False,
                    message=f"HTTP错误 {e.code}: {e.reason}",
                    data={'status_code': e.code, 'error': error_body}
                )

            except urllib.error.URLError as e:
                return ActionResult(
                    success=False,
                    message=f"URL错误: {e.reason}",
                    data={'error': str(e.reason)}
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'method': 'GET',
            'headers': {},
            'body': None,
            'timeout': 30,
            'output_var': 'http_response'
        }


class HttpGetAction(BaseAction):
    """Make GET requests (convenience)."""
    action_type = "http_get"
    display_name = "HTTP GET"
    description = "发送GET请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a GET request.

        Args:
            context: Execution context.
            params: Dict with url, headers, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        params['method'] = 'GET'
        return HttpRequestAction().execute(context, params)

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'headers': {},
            'timeout': 30,
            'output_var': 'http_response'
        }


class HttpPostAction(BaseAction):
    """Make POST requests (convenience)."""
    action_type = "http_post"
    display_name = "HTTP POST"
    description = "发送POST请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a POST request.

        Args:
            context: Execution context.
            params: Dict with url, body, headers, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        params['method'] = 'POST'
        return HttpRequestAction().execute(context, params)

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'body': None,
            'headers': {},
            'timeout': 30,
            'output_var': 'http_response'
        }