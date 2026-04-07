"""Http action module for RabAI AutoClick.

Provides HTTP operations:
- HttpGetAction: GET request
- HttpPostAction: POST request
- HttpPutAction: PUT request
- HttpDeleteAction: DELETE request
- HttpHeadAction: HEAD request
"""

import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpGetAction(BaseAction):
    """GET request."""
    action_type = "http_get"
    display_name = "HTTP GET"
    description = "发送HTTP GET请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute GET request.

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

            req = urllib.request.Request(resolved_url)
            for key, value in resolved_headers.items():
                req.add_header(key, str(value))

            with urllib.request.urlopen(req, timeout=30) as response:
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP GET: {resolved_url} - {result['status_code']}",
                data={
                    'url': resolved_url,
                    'status_code': result['status_code'],
                    'output_var': output_var
                }
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HTTP GET失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP GET失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}


class HttpPostAction(BaseAction):
    """POST request."""
    action_type = "http_post"
    display_name = "HTTP POST"
    description = "发送HTTP POST请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute POST request.

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
            resolved_data = context.resolve_value(data) if data else {}
            resolved_headers = context.resolve_value(headers) if headers else {}

            if isinstance(resolved_data, dict):
                encoded_data = urllib.parse.urlencode(resolved_data).encode('utf-8')
            else:
                encoded_data = str(resolved_data).encode('utf-8')

            req = urllib.request.Request(resolved_url, data=encoded_data)
            for key, value in resolved_headers.items():
                req.add_header(key, str(value))

            with urllib.request.urlopen(req, timeout=30) as response:
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP POST: {resolved_url} - {result['status_code']}",
                data={
                    'url': resolved_url,
                    'status_code': result['status_code'],
                    'output_var': output_var
                }
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HTTP POST失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP POST失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': {}, 'headers': {}, 'output_var': 'http_response'}


class HttpPutAction(BaseAction):
    """PUT request."""
    action_type = "http_put"
    display_name = "HTTP PUT"
    description = "发送HTTP PUT请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PUT request.

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
            resolved_data = context.resolve_value(data) if data else {}
            resolved_headers = context.resolve_value(headers) if headers else {}

            if isinstance(resolved_data, dict):
                encoded_data = urllib.parse.urlencode(resolved_data).encode('utf-8')
            else:
                encoded_data = str(resolved_data).encode('utf-8')

            req = urllib.request.Request(resolved_url, data=encoded_data, method='PUT')
            for key, value in resolved_headers.items():
                req.add_header(key, str(value))

            with urllib.request.urlopen(req, timeout=30) as response:
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP PUT: {resolved_url} - {result['status_code']}",
                data={
                    'url': resolved_url,
                    'status_code': result['status_code'],
                    'output_var': output_var
                }
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HTTP PUT失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP PUT失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': {}, 'headers': {}, 'output_var': 'http_response'}


class HttpDeleteAction(BaseAction):
    """DELETE request."""
    action_type = "http_delete"
    display_name = "HTTP DELETE"
    description = "发送HTTP DELETE请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DELETE request.

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

            req = urllib.request.Request(resolved_url, method='DELETE')
            for key, value in resolved_headers.items():
                req.add_header(key, str(value))

            with urllib.request.urlopen(req, timeout=30) as response:
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode('utf-8')
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP DELETE: {resolved_url} - {result['status_code']}",
                data={
                    'url': resolved_url,
                    'status_code': result['status_code'],
                    'output_var': output_var
                }
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HTTP DELETE失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP DELETE失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}


class HttpHeadAction(BaseAction):
    """HEAD request."""
    action_type = "http_head"
    display_name = "HTTP HEAD"
    description = "发送HTTP HEAD请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HEAD request.

        Args:
            context: Execution context.
            params: Dict with url, headers, output_var.

        Returns:
            ActionResult with response headers.
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

            req = urllib.request.Request(resolved_url, method='HEAD')
            for key, value in resolved_headers.items():
                req.add_header(key, str(value))

            with urllib.request.urlopen(req, timeout=30) as response:
                result = {
                    'status_code': response.status,
                    'headers': dict(response.headers)
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP HEAD: {resolved_url} - {result['status_code']}",
                data={
                    'url': resolved_url,
                    'status_code': result['status_code'],
                    'output_var': output_var
                }
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HTTP HEAD失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP HEAD失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_response'}