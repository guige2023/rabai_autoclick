"""HTTP10 action module for RabAI AutoClick.

Provides additional HTTP operations:
- HTTPGetAction: HTTP GET request
- HTTPPostAction: HTTP POST request
- HTTPPutAction: HTTP PUT request
- HTTPDeleteAction: HTTP DELETE request
- HTTPHeadersAction: Get HTTP headers
- HTTPStatusAction: Get HTTP status code
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HTTPGetAction(BaseAction):
    """HTTP GET request."""
    action_type = "http10_get"
    display_name = "HTTP GET请求"
    description = "发送HTTP GET请求"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP GET.

        Args:
            context: Execution context.
            params: Dict with url, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'http_response')

        try:
            import urllib.request
            import urllib.error

            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}

            if not isinstance(resolved_headers, dict):
                resolved_headers = {}

            req = urllib.request.Request(resolved_url, headers=resolved_headers)

            with urllib.request.urlopen(req, timeout=10) as response:
                result = {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP GET: {result['status']}",
                data={
                    'url': resolved_url,
                    'status': result['status'],
                    'headers': result['headers'],
                    'body': result['body'][:200] + '...' if len(result['body']) > 200 else result['body'],
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP错误: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP GET失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'output_var': 'http_response'}


class HTTPPostAction(BaseAction):
    """HTTP POST request."""
    action_type = "http10_post"
    display_name = "HTTP POST请求"
    description = "发送HTTP POST请求"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP POST.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', '')
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'http_response')

        try:
            import urllib.request
            import urllib.error
            import urllib.parse

            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data) if data else ''
            resolved_headers = context.resolve_value(headers) if headers else {}

            if not isinstance(resolved_headers, dict):
                resolved_headers = {}

            if isinstance(resolved_data, dict):
                resolved_data = urllib.parse.urlencode(resolved_data).encode('utf-8')
            elif isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            req = urllib.request.Request(resolved_url, data=resolved_data, headers=resolved_headers, method='POST')

            with urllib.request.urlopen(req, timeout=10) as response:
                result = {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP POST: {result['status']}",
                data={
                    'url': resolved_url,
                    'status': result['status'],
                    'body': result['body'][:200] + '...' if len(result['body']) > 200 else result['body'],
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP错误: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP POST失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'output_var': 'http_response'}


class HTTPPutAction(BaseAction):
    """HTTP PUT request."""
    action_type = "http10_put"
    display_name = "HTTP PUT请求"
    description = "发送HTTP PUT请求"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP PUT.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', '')
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'http_response')

        try:
            import urllib.request
            import urllib.error
            import urllib.parse

            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data) if data else ''
            resolved_headers = context.resolve_value(headers) if headers else {}

            if not isinstance(resolved_headers, dict):
                resolved_headers = {}

            if isinstance(resolved_data, dict):
                resolved_data = urllib.parse.urlencode(resolved_data).encode('utf-8')
            elif isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            req = urllib.request.Request(resolved_url, data=resolved_data, headers=resolved_headers, method='PUT')

            with urllib.request.urlopen(req, timeout=10) as response:
                result = {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP PUT: {result['status']}",
                data={
                    'url': resolved_url,
                    'status': result['status'],
                    'body': result['body'][:200] + '...' if len(result['body']) > 200 else result['body'],
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP错误: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP PUT失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'output_var': 'http_response'}


class HTTPDeleteAction(BaseAction):
    """HTTP DELETE request."""
    action_type = "http10_delete"
    display_name = "HTTP DELETE请求"
    description = "发送HTTP DELETE请求"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP DELETE.

        Args:
            context: Execution context.
            params: Dict with url, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'http_response')

        try:
            import urllib.request
            import urllib.error

            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}

            if not isinstance(resolved_headers, dict):
                resolved_headers = {}

            req = urllib.request.Request(resolved_url, headers=resolved_headers, method='DELETE')

            with urllib.request.urlopen(req, timeout=10) as response:
                result = {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP DELETE: {result['status']}",
                data={
                    'url': resolved_url,
                    'status': result['status'],
                    'body': result['body'][:200] + '...' if len(result['body']) > 200 else result['body'],
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP错误: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP DELETE失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'output_var': 'http_response'}


class HTTPHeadersAction(BaseAction):
    """Get HTTP headers."""
    action_type = "http10_headers"
    display_name = "获取HTTP头"
    description = "获取HTTP响应头"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP headers.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with headers.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'http_headers')

        try:
            import urllib.request
            import urllib.error

            resolved_url = context.resolve_value(url)

            req = urllib.request.Request(resolved_url, method='HEAD')

            with urllib.request.urlopen(req, timeout=10) as response:
                result = dict(response.headers)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取HTTP头: {len(result)}个",
                data={
                    'url': resolved_url,
                    'headers': result,
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP错误: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取HTTP头失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'http_headers'}


class HTTPStatusAction(BaseAction):
    """Get HTTP status code."""
    action_type = "http10_status"
    display_name = "获取HTTP状态"
    description = "获取HTTP状态码"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP status.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with status.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'http_status')

        try:
            import urllib.request
            import urllib.error

            resolved_url = context.resolve_value(url)

            req = urllib.request.Request(resolved_url, method='HEAD')

            with urllib.request.urlopen(req, timeout=10) as response:
                result = response.status

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP状态: {result}",
                data={
                    'url': resolved_url,
                    'status': result,
                    'output_var': output_var
                }
            )
        except urllib.error.HTTPError as e:
            context.set(output_var, e.code)
            return ActionResult(
                success=True,
                message=f"HTTP状态: {e.code}",
                data={
                    'url': resolved_url,
                    'status': e.code,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取HTTP状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'http_status'}