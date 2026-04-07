"""Http4 action module for RabAI AutoClick.

Provides additional HTTP operations:
- HttpDownloadAction: Download file from URL
- HttpUploadAction: Upload file to server
- HttpHeadAction: Send HEAD request
- HttpOptionsAction: Send OPTIONS request
- HttpPatchAction: Send PATCH request
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpDownloadAction(BaseAction):
    """Download file from URL."""
    action_type = "http4_download"
    display_name = "下载文件"
    description = "从URL下载文件"
    version = "4.0"

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
            ActionResult with download status.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        output_var = params.get('output_var', 'download_status')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_output = context.resolve_value(output_path) if output_path else ''

            if not resolved_output:
                resolved_output = resolved_url.split('/')[-1]

            import urllib.request
            urllib.request.urlretrieve(resolved_url, resolved_output)

            context.set(output_var, resolved_output)

            return ActionResult(
                success=True,
                message=f"文件下载成功: {resolved_output}",
                data={
                    'url': resolved_url,
                    'output_path': resolved_output,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件下载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'output_var': 'download_status'}


class HttpUploadAction(BaseAction):
    """Upload file to server."""
    action_type = "http4_upload"
    display_name = "上传文件"
    description = "上传文件到服务器"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upload.

        Args:
            context: Execution context.
            params: Dict with url, file_path, field_name, output_var.

        Returns:
            ActionResult with upload status.
        """
        url = params.get('url', '')
        file_path = params.get('file_path', '')
        field_name = params.get('field_name', 'file')
        output_var = params.get('output_var', 'upload_status')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_file_path = context.resolve_value(file_path) if file_path else ''
            resolved_field = context.resolve_value(field_name) if field_name else 'file'

            import urllib.request
            import urllib.parse

            with open(resolved_file_path, 'rb') as f:
                data = urllib.parse.urlencode({}).encode()
                req = urllib.request.Request(resolved_url, data=data)
                urllib.request.urlopen(req, timeout=30)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件上传成功",
                data={
                    'url': resolved_url,
                    'file_path': resolved_file_path,
                    'field_name': resolved_field,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件上传失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'field_name': 'file', 'output_var': 'upload_status'}


class HttpHeadAction(BaseAction):
    """Send HEAD request."""
    action_type = "http4_head"
    display_name = "发送HEAD请求"
    description = "发送HEAD请求"
    version = "4.0"

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
            ActionResult with HEAD response headers.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'head_result')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_headers = context.resolve_value(headers) if headers else {}

            import urllib.request

            req = urllib.request.Request(resolved_url, method='HEAD')
            for key, value in resolved_headers.items():
                req.add_header(key, value)

            response = urllib.request.urlopen(req, timeout=30)

            context.set(output_var, dict(response.headers))

            return ActionResult(
                success=True,
                message=f"HEAD请求成功: {resolved_url}",
                data={
                    'url': resolved_url,
                    'headers': dict(response.headers),
                    'status_code': response.status,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HEAD请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'head_result'}


class HttpOptionsAction(BaseAction):
    """Send OPTIONS request."""
    action_type = "http4_options"
    display_name = "发送OPTIONS请求"
    description = "发送OPTIONS请求"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OPTIONS request.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with OPTIONS response.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'options_result')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            import urllib.request

            req = urllib.request.Request(resolved_url, method='OPTIONS')
            response = urllib.request.urlopen(req, timeout=30)

            allow_methods = response.headers.get('Allow', '')

            context.set(output_var, {
                'allow': allow_methods,
                'status': response.status
            })

            return ActionResult(
                success=True,
                message=f"OPTIONS请求成功: {resolved_url}",
                data={
                    'url': resolved_url,
                    'allow': allow_methods,
                    'status_code': response.status,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OPTIONS请求失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'options_result'}


class HttpPatchAction(BaseAction):
    """Send PATCH request."""
    action_type = "http4_patch"
    display_name = "发送PATCH请求"
    description = "发送PATCH请求"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PATCH request.

        Args:
            context: Execution context.
            params: Dict with url, data, headers, output_var.

        Returns:
            ActionResult with PATCH response.
        """
        url = params.get('url', '')
        data = params.get('data', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'patch_result')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)
            resolved_data = context.resolve_value(data) if data else {}
            resolved_headers = context.resolve_value(headers) if headers else {}

            import urllib.request
            import json

            json_data = json.dumps(resolved_data).encode('utf-8')

            req = urllib.request.Request(resolved_url, data=json_data, method='PATCH')
            req.add_header('Content-Type', 'application/json')
            for key, value in resolved_headers.items():
                req.add_header(key, value)

            response = urllib.request.urlopen(req, timeout=30)
            response_body = response.read().decode('utf-8')

            context.set(output_var, {
                'status': response.status,
                'body': response_body
            })

            return ActionResult(
                success=True,
                message=f"PATCH请求成功: {resolved_url}",
                data={
                    'url': resolved_url,
                    'status_code': response.status,
                    'body': response_body,
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
        return {'headers': {}, 'output_var': 'patch_result'}