"""HTTP2 action module for RabAI AutoClick.

Provides additional HTTP operations:
- HttpGetHeadersAction: Get headers from URL
- HttpPostJsonAction: POST JSON data
- HttpPutAction: PUT request
- HttpDeleteAction: DELETE request
- HttpPatchAction: PATCH request
"""

import urllib.request
import urllib.parse
import json
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpGetHeadersAction(BaseAction):
    """Get headers from URL."""
    action_type = "http2_get_headers"
    display_name = "获取HTTP头"
    description = "获取URL的HTTP响应头"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get headers.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with headers.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'http_headers')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            req = urllib.request.Request(resolved_url, method='HEAD')
            with urllib.request.urlopen(req, timeout=10) as response:
                headers = dict(response.headers)

            context.set(output_var, headers)

            return ActionResult(
                success=True,
                message=f"获取HTTP头: {len(headers)} 个",
                data={
                    'url': resolved_url,
                    'headers': headers,
                    'status_code': response.status,
                    'output_var': output_var
                }
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


class HttpPostJsonAction(BaseAction):
    """POST JSON data."""
    action_type = "http2_post_json"
    display_name = "POST JSON"
    description = "发送POST请求JSON数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute POST JSON.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_response')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data)
            resolved_headers = context.resolve_value(headers) if headers else {}

            json_data = json.dumps(resolved_data).encode('utf-8')

            req_headers = {
                'Content-Type': 'application/json',
            }
            if isinstance(resolved_headers, dict):
                req_headers.update(resolved_headers)

            req = urllib.request.Request(
                resolved_url,
                data=json_data,
                headers=req_headers,
                method='POST'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read().decode('utf-8')
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': body,
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"POST JSON: {response.status}",
                data={
                    'url': resolved_url,
                    'response': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"POST JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}


class HttpPutAction(BaseAction):
    """PUT request."""
    action_type = "http2_put"
    display_name = "PUT请求"
    description = "发送PUT请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PUT.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_response')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data)

            if isinstance(resolved_data, (dict, list)):
                body_data = json.dumps(resolved_data).encode('utf-8')
                content_type = 'application/json'
            else:
                body_data = str(resolved_data).encode('utf-8')
                content_type = 'text/plain'

            req_headers = {
                'Content-Type': content_type,
            }
            if isinstance(headers, dict):
                req_headers.update(headers)

            req = urllib.request.Request(
                resolved_url,
                data=body_data,
                headers=req_headers,
                method='PUT'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read().decode('utf-8')
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': body,
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"PUT请求: {response.status}",
                data={
                    'url': resolved_url,
                    'response': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PUT请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}


class HttpDeleteAction(BaseAction):
    """DELETE request."""
    action_type = "http2_delete"
    display_name = "DELETE请求"
    description = "发送DELETE请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DELETE.

        Args:
            context: Execution context.
            params: Dict with url, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_response')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}

            req_headers = {}
            if isinstance(resolved_headers, dict):
                req_headers.update(resolved_headers)

            req = urllib.request.Request(
                resolved_url,
                headers=req_headers,
                method='DELETE'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read().decode('utf-8')
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': body,
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"DELETE请求: {response.status}",
                data={
                    'url': resolved_url,
                    'response': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DELETE请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}


class HttpPatchAction(BaseAction):
    """PATCH request."""
    action_type = "http2_patch"
    display_name = "PATCH请求"
    description = "发送PATCH请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PATCH.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_response')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data)

            if isinstance(resolved_data, (dict, list)):
                body_data = json.dumps(resolved_data).encode('utf-8')
                content_type = 'application/json'
            else:
                body_data = str(resolved_data).encode('utf-8')
                content_type = 'text/plain'

            req_headers = {
                'Content-Type': content_type,
            }
            if isinstance(headers, dict):
                req_headers.update(headers)

            req = urllib.request.Request(
                resolved_url,
                data=body_data,
                headers=req_headers,
                method='PATCH'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read().decode('utf-8')
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': body,
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"PATCH请求: {response.status}",
                data={
                    'url': resolved_url,
                    'response': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PATCH请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}