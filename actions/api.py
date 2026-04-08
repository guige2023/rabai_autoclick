"""API action module for RabAI AutoClick.

Provides HTTP API request actions including GET, POST, PUT, DELETE
with JSON payload support and response parsing.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRequestAction(BaseAction):
    """Perform HTTP API requests (GET, POST, PUT, DELETE).
    
    Supports custom headers, JSON payloads, query parameters,
    timeout configuration, and response validation.
    """
    action_type = "api_request"
    display_name = "API请求"
    description = "发送HTTP请求到指定API端点，支持GET/POST/PUT/DELETE"

    VALID_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an HTTP API request.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, data, 
                   params (query), timeout, validate_status.
        
        Returns:
            ActionResult with response data or error message.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        headers = params.get('headers', {})
        data = params.get('data', None)
        query_params = params.get('params', {})
        timeout = params.get('timeout', 30)
        validate_status = params.get('validate_status', True)
        save_to_var = params.get('save_to_var', None)

        # Validate URL
        if not url:
            return ActionResult(success=False, message="Parameter 'url' is required")
        if not url.startswith(('http://', 'https://')):
            return ActionResult(
                success=False,
                message=f"Invalid URL scheme: {url}. Must start with http:// or https://"
            )

        # Validate method
        if method not in self.VALID_METHODS:
            return ActionResult(
                success=False,
                message=f"Invalid method '{method}'. Valid: {self.VALID_METHODS}"
            )

        # Validate timeout
        try:
            timeout = int(timeout)
            if timeout <= 0:
                return ActionResult(
                    success=False,
                    message=f"Timeout must be positive, got {timeout}"
                )
        except (ValueError, TypeError):
            return ActionResult(
                success=False,
                message=f"Invalid timeout value: {timeout}"
            )

        try:
            import urllib.request
            import urllib.parse
            import urllib.error

            # Build URL with query parameters
            if query_params:
                encoded_params = urllib.parse.urlencode(query_params)
                separator = '&' if '?' in url else '?'
                url = f"{url}{separator}{encoded_params}"

            # Encode data for POST/PUT
            body = None
            if data is not None and method in ('POST', 'PUT', 'PATCH'):
                if isinstance(data, dict):
                    body = json.dumps(data).encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')
                elif isinstance(data, str):
                    body = data.encode('utf-8')
                else:
                    body = str(data).encode('utf-8')

            # Build request
            req = urllib.request.Request(url, data=body, headers=headers, method=method)

            # Execute request
            start_time = time.time()
            with urllib.request.urlopen(req, timeout=timeout) as response:
                elapsed = time.time() - start_time
                status = response.status
                content_type = response.headers.get('Content-Type', '')
                
                # Read response body
                raw_body = response.read()
                
                # Parse response
                response_data = None
                if raw_body:
                    if 'application/json' in content_type:
                        try:
                            response_data = json.loads(raw_body.decode('utf-8'))
                        except json.JSONDecodeError as e:
                            response_data = raw_body.decode('utf-8', errors='replace')
                    else:
                        response_data = raw_body.decode('utf-8', errors='replace')

                # Validate status code if requested
                if validate_status and status >= 400:
                    return ActionResult(
                        success=False,
                        message=f"HTTP {status} error for {url}",
                        data={
                            'status': status,
                            'body': response_data,
                            'elapsed': elapsed
                        }
                    )

                result_data = {
                    'status': status,
                    'body': response_data,
                    'headers': dict(response.headers),
                    'elapsed': elapsed,
                    'content_type': content_type
                }

                # Save to variable in context if requested
                if save_to_var:
                    context.variables[save_to_var] = result_data

                return ActionResult(
                    success=True,
                    message=f"API请求成功: {method} {url} -> {status}",
                    data=result_data
                )

        except urllib.error.HTTPError as e:
            try:
                error_body = e.read().decode('utf-8', errors='replace')
            except Exception:
                error_body = str(e)
            return ActionResult(
                success=False,
                message=f"HTTP {e.code} error: {e.reason}",
                data={'status': e.code, 'reason': e.reason, 'body': error_body}
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"URL error: {e.reason}",
                data={'error': str(e.reason)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API请求失败: {str(e)}",
                data={'error': str(e)}
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'method': 'GET',
            'headers': {},
            'data': None,
            'params': {},
            'timeout': 30,
            'validate_status': True,
            'save_to_var': None
        }


class ApiHealthCheckAction(BaseAction):
    """Check API endpoint health status.
    
    Performs a lightweight GET request to verify API availability,
    with configurable expected status codes and timeout.
    """
    action_type = "api_health_check"
    display_name = "API健康检查"
    description = "检查API端点是否可用，支持超时和状态码验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute health check on API endpoint.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, expected_status, timeout, 
                   save_to_var.
        
        Returns:
            ActionResult with health status information.
        """
        url = params.get('url', '')
        expected_status = params.get('expected_status', 200)
        timeout = params.get('timeout', 10)
        save_to_var = params.get('save_to_var', None)

        if not url:
            return ActionResult(success=False, message="Parameter 'url' is required")
        if not url.startswith(('http://', 'https://')):
            return ActionResult(
                success=False,
                message=f"Invalid URL scheme: {url}"
            )

        try:
            import urllib.request
            import urllib.error

            start_time = time.time()
            req = urllib.request.Request(url, method='GET')
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                elapsed = time.time() - start_time
                status = response.status
                
                is_healthy = (status == expected_status)
                
                result_data = {
                    'healthy': is_healthy,
                    'status': status,
                    'expected_status': expected_status,
                    'elapsed': elapsed,
                    'url': url
                }

                if save_to_var:
                    context.variables[save_to_var] = result_data

                if is_healthy:
                    return ActionResult(
                        success=True,
                        message=f"健康检查通过: {url} ({status}) in {elapsed:.3f}s",
                        data=result_data
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"健康检查失败: 期望 {expected_status}, 实际 {status}",
                        data=result_data
                    )

        except urllib.error.URLError as e:
            result_data = {'healthy': False, 'error': str(e.reason), 'url': url}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"健康检查失败: {e.reason}",
                data=result_data
            )
        except Exception as e:
            result_data = {'healthy': False, 'error': str(e), 'url': url}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"健康检查异常: {str(e)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'expected_status': 200,
            'timeout': 10,
            'save_to_var': None
        }
