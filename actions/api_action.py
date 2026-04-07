"""API action module for RabAI AutoClick.

Provides REST API client operations:
- ApiGetAction: Perform GET request
- ApiPostAction: Perform POST request
- ApiPutAction: Perform PUT request
- ApiPatchAction: Perform PATCH request
- ApiDeleteAction: Perform DELETE request
- ApiHeadAction: Perform HEAD request
- ApiOptionsAction: Perform OPTIONS request
- ApiUploadAction: Upload file via multipart
- ApiDownloadAction: Download file from URL
- ApiHealthAction: Health check endpoint
"""

import json
import base64
import os
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiGetAction(BaseAction):
    """Perform GET request."""
    action_type = "api_get"
    display_name = "API GET请求"
    description = "发送HTTP GET请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute GET.

        Args:
            context: Execution context.
            params: Dict with url, headers, params, output_var, timeout.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        query_params = params.get('params', {})
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)
        auth = params.get('auth', '')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request
            import urllib.parse

            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_params = context.resolve_value(query_params) if query_params else {}
            resolved_timeout = context.resolve_value(timeout)
            resolved_auth = context.resolve_value(auth) if auth else ''

            # Build URL with query params
            if resolved_params:
                encoded = urllib.parse.urlencode(resolved_params)
                resolved_url = f"{resolved_url}{'&' if '?' in resolved_url else '?'}{encoded}"

            request = urllib.request.Request(resolved_url, method='GET')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            if resolved_auth:
                auth_bytes = resolved_auth.encode('utf-8')
                encoded_auth = base64.b64encode(auth_bytes).decode('ascii')
                request.add_header('Authorization', f'Basic {encoded_auth}')

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                body = resp.read().decode('utf-8')
                status = resp.status
                resp_headers = dict(resp.headers)

                # Try to parse as JSON
                try:
                    data = json.loads(body)
                except (json.JSONDecodeError, ValueError):
                    data = body

                result = {
                    'status': status,
                    'body': data,
                    'headers': resp_headers
                }

                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"GET {resolved_url} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GET请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'headers': {}, 'params': {}, 'output_var': 'api_response',
            'timeout': 30, 'auth': ''
        }


class ApiPostAction(BaseAction):
    """Perform POST request."""
    action_type = "api_post"
    display_name = "API POST请求"
    description = "发送HTTP POST请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute POST.

        Args:
            context: Execution context.
            params: Dict with url, body, headers, content_type, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        body = params.get('body', '')
        headers = params.get('headers', {})
        content_type = params.get('content_type', 'application/json')
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_body = context.resolve_value(body) if body else ''
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_ct = context.resolve_value(content_type)
            resolved_timeout = context.resolve_value(timeout)

            # Encode body
            if resolved_ct == 'application/json':
                if isinstance(resolved_body, dict):
                    encoded_body = json.dumps(resolved_body).encode('utf-8')
                else:
                    encoded_body = json.dumps({'data': resolved_body}).encode('utf-8')
            else:
                encoded_body = str(resolved_body).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', resolved_ct)

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"POST {resolved_url} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"POST请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'body': '', 'headers': {}, 'content_type': 'application/json',
            'output_var': 'api_response', 'timeout': 30
        }


class ApiPutAction(BaseAction):
    """Perform PUT request."""
    action_type = "api_put"
    display_name = "API PUT请求"
    description = "发送HTTP PUT请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PUT.

        Args:
            context: Execution context.
            params: Dict with url, body, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        body = params.get('body', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_body = context.resolve_value(body) if body else ''
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            if isinstance(resolved_body, dict):
                encoded_body = json.dumps(resolved_body).encode('utf-8')
            else:
                encoded_body = str(resolved_body).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='PUT'
            )
            request.add_header('Content-Type', 'application/json')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"PUT {resolved_url} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PUT请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'body': '', 'headers': {}, 'output_var': 'api_response', 'timeout': 30}


class ApiPatchAction(BaseAction):
    """Perform PATCH request."""
    action_type = "api_patch"
    display_name = "API PATCH请求"
    description = "发送HTTP PATCH请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PATCH.

        Args:
            context: Execution context.
            params: Dict with url, body, headers, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        body = params.get('body', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_body = context.resolve_value(body) if body else ''
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            if isinstance(resolved_body, dict):
                encoded_body = json.dumps(resolved_body).encode('utf-8')
            else:
                encoded_body = str(resolved_body).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='PATCH'
            )
            request.add_header('Content-Type', 'application/json')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"PATCH {resolved_url} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PATCH请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'body': '', 'headers': {}, 'output_var': 'api_response', 'timeout': 30}


class ApiDeleteAction(BaseAction):
    """Perform DELETE request."""
    action_type = "api_delete"
    display_name = "API DELETE请求"
    description = "发送HTTP DELETE请求"
    version = "1.0"

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
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            request = urllib.request.Request(resolved_url, method='DELETE')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"DELETE {resolved_url} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DELETE请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'api_response', 'timeout': 30}


class ApiHeadAction(BaseAction):
    """Perform HEAD request."""
    action_type = "api_head"
    display_name = "API HEAD请求"
    description = "发送HTTP HEAD请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HEAD.

        Args:
            context: Execution context.
            params: Dict with url, headers, output_var.

        Returns:
            ActionResult with headers.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            request = urllib.request.Request(resolved_url, method='HEAD')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                status = resp.status
                resp_headers = dict(resp.headers)

                result = {'status': status, 'headers': resp_headers}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"HEAD {resolved_url} -> {status}",
                    data={'status': status, 'headers': resp_headers, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HEAD请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'api_response', 'timeout': 30}


class ApiUploadAction(BaseAction):
    """Upload file via multipart POST."""
    action_type = "api_upload"
    display_name = "API文件上传"
    description = "通过multipart表单上传文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upload.

        Args:
            context: Execution context.
            params: Dict with url, file_path, field_name, extra_fields, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        file_path = params.get('file_path', '')
        field_name = params.get('field_name', 'file')
        extra_fields = params.get('extra_fields', {})
        output_var = params.get('output_var', 'api_response')
        timeout = params.get('timeout', 60)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request
            import uuid

            resolved_url = context.resolve_value(url)
            resolved_path = context.resolve_value(file_path)
            resolved_field = context.resolve_value(field_name)
            resolved_extras = context.resolve_value(extra_fields) if extra_fields else {}
            resolved_timeout = context.resolve_value(timeout)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            # Build multipart form data
            boundary = uuid.uuid4().hex
            body = b''

            # Add extra fields
            for k, v in resolved_extras.items():
                body += f'--{boundary}\r\n'.encode('utf-8')
                body += f'Content-Disposition: form-data; name="{k}"\r\n\r\n'.encode('utf-8')
                body += f'{v}\r\n'.encode('utf-8')

            # Add file
            filename = os.path.basename(resolved_path)
            with open(resolved_path, 'rb') as f:
                file_data = f.read()

            body += f'--{boundary}\r\n'.encode('utf-8')
            body += f'Content-Disposition: form-data; name="{resolved_field}"; filename="{filename}"\r\n'.encode('utf-8')
            body += b'Content-Type: application/octet-stream\r\n\r\n'
            body += file_data
            body += b'\r\n'
            body += f'--{boundary}--\r\n'.encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=body,
                method='POST'
            )
            request.add_header('Content-Type', f'multipart/form-data; boundary={boundary}')

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"上传 {filename} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"上传失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'field_name': 'file', 'extra_fields': {}, 'output_var': 'api_response', 'timeout': 60}


class ApiDownloadAction(BaseAction):
    """Download file from URL."""
    action_type = "api_download"
    display_name = "API文件下载"
    description = "从URL下载文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute download.

        Args:
            context: Execution context.
            params: Dict with url, output_path, output_var.

        Returns:
            ActionResult with file path.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        output_var = params.get('output_var', 'download_path')
        timeout = params.get('timeout', 60)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_output = context.resolve_value(output_path) if output_path else os.path.basename(urlparse(resolved_url).path)
            resolved_timeout = context.resolve_value(timeout)

            if not resolved_output or resolved_output == '/':
                return ActionResult(
                    success=False,
                    message="无法确定输出路径"
                )

            request = urllib.request.Request(resolved_url, method='GET')

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                data = resp.read()

                with open(resolved_output, 'wb') as f:
                    f.write(data)

                context.set(output_var, resolved_output)

                return ActionResult(
                    success=True,
                    message=f"已下载: {resolved_output} ({len(data)} bytes)",
                    data={'path': resolved_output, 'size': len(data), 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"下载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'output_var': 'download_path', 'timeout': 60}


class ApiHealthAction(BaseAction):
    """Health check endpoint."""
    action_type = "api_health"
    display_name = "API健康检查"
    description = "检查API端点健康状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute health check.

        Args:
            context: Execution context.
            params: Dict with url, expected_status, output_var.

        Returns:
            ActionResult with health status.
        """
        url = params.get('url', '')
        expected_status = params.get('expected_status', 200)
        output_var = params.get('output_var', 'health_status')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_expected = context.resolve_value(expected_status)

            request = urllib.request.Request(resolved_url, method='GET')

            with urllib.request.urlopen(request, timeout=10) as resp:
                status = resp.status
                healthy = status == resolved_expected

                result = {
                    'healthy': healthy,
                    'status': status,
                    'expected': resolved_expected,
                    'url': resolved_url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=healthy,
                    message=f"健康检查 {'通过' if healthy else '失败'}: {status}",
                    data=result
                )
        except Exception as e:
            result = {
                'healthy': False,
                'error': str(e),
                'url': resolved_url if 'resolved_url' in dir() else url
            }
            context.set(output_var, result)

            return ActionResult(
                success=False,
                message=f"健康检查失败: {str(e)}",
                data=result
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expected_status': 200, 'output_var': 'health_status'}
