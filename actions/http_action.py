"""HTTP action module for RabAI AutoClick.

Provides HTTP request operations:
- HttpGetAction: Perform GET requests
- HttpPostAction: Perform POST requests
- HttpPutAction: Perform PUT requests
- HttpDeleteAction: Perform DELETE requests
- HttpPatchAction: Perform PATCH requests
- HttpHeadAction: Perform HEAD requests
- HttpOptionsAction: Perform OPTIONS requests
- HttpDownloadAction: Download files via HTTP
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import urllib.request
    import urllib.parse
    import urllib.error
    import json as json_module
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False


class HttpGetAction(BaseAction):
    """Perform HTTP GET requests."""
    action_type = "http_get"
    display_name = "HTTP GET请求"
    description = "发送GET请求并获取响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP GET request.

        Args:
            context: Execution context.
            params: Dict with url, headers, params, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        query_params = params.get('params', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if query_params:
                encoded_params = urllib.parse.urlencode(query_params)
                separator = '&' if '?' in url else '?'
                url = f"{url}{separator}{encoded_params}"

            request = urllib.request.Request(url, headers=headers, method='GET')

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_headers = dict(response.headers)
                response_body = response.read()

                try:
                    body_text = response_body.decode('utf-8')
                    try:
                        body_json = json_module.loads(body_text)
                        body_data = body_json
                    except (json_module.JSONDecodeError, UnicodeDecodeError):
                        body_data = body_text
                except Exception:
                    body_data = None

                result = {
                    'status_code': status_code,
                    'headers': response_headers,
                    'body': body_data,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"GET请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"GET请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GET请求异常: {str(e)}"
            )


class HttpPostAction(BaseAction):
    """Perform HTTP POST requests."""
    action_type = "http_post"
    display_name = "HTTP POST请求"
    description = "发送POST请求并获取响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP POST request.

        Args:
            context: Execution context.
            params: Dict with url, headers, body, content_type, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        body_data = params.get('body', None)
        content_type = params.get('content_type', 'application/json')
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        try:
            if body_data is not None:
                if isinstance(body_data, (dict, list)):
                    body_bytes = json_module.dumps(body_data).encode('utf-8')
                    if 'Content-Type' not in headers and 'content-type' not in headers:
                        headers['Content-Type'] = content_type
                elif isinstance(body_data, str):
                    body_bytes = body_data.encode('utf-8')
                else:
                    body_bytes = body_data
            else:
                body_bytes = None

            request = urllib.request.Request(
                url,
                data=body_bytes,
                headers=headers,
                method='POST'
            )

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_headers = dict(response.headers)
                response_body = response.read()

                try:
                    body_text = response_body.decode('utf-8')
                    try:
                        body_json = json_module.loads(body_text)
                        body_result = body_json
                    except (json_module.JSONDecodeError, UnicodeDecodeError):
                        body_result = body_text
                except Exception:
                    body_result = None

                result = {
                    'status_code': status_code,
                    'headers': response_headers,
                    'body': body_result,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"POST请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"POST请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"POST请求异常: {str(e)}"
            )


class HttpPutAction(BaseAction):
    """Perform HTTP PUT requests."""
    action_type = "http_put"
    display_name = "HTTP PUT请求"
    description = "发送PUT请求并获取响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP PUT request.

        Args:
            context: Execution context.
            params: Dict with url, headers, body, content_type, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        body_data = params.get('body', None)
        content_type = params.get('content_type', 'application/json')
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        try:
            if body_data is not None:
                if isinstance(body_data, (dict, list)):
                    body_bytes = json_module.dumps(body_data).encode('utf-8')
                    if 'Content-Type' not in headers and 'content-type' not in headers:
                        headers['Content-Type'] = content_type
                elif isinstance(body_data, str):
                    body_bytes = body_data.encode('utf-8')
                else:
                    body_bytes = body_data
            else:
                body_bytes = None

            request = urllib.request.Request(
                url,
                data=body_bytes,
                headers=headers,
                method='PUT'
            )

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_body = response.read()

                try:
                    body_text = response_body.decode('utf-8')
                    body_result = json_module.loads(body_text)
                except Exception:
                    body_result = body_text

                result = {
                    'status_code': status_code,
                    'body': body_result,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"PUT请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"PUT请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PUT请求异常: {str(e)}"
            )


class HttpDeleteAction(BaseAction):
    """Perform HTTP DELETE requests."""
    action_type = "http_delete"
    display_name = "HTTP DELETE请求"
    description = "发送DELETE请求并获取响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP DELETE request.

        Args:
            context: Execution context.
            params: Dict with url, headers, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        try:
            request = urllib.request.Request(url, headers=headers, method='DELETE')

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_body = response.read()

                try:
                    body_result = json_module.loads(response_body.decode('utf-8'))
                except Exception:
                    body_result = response_body.decode('utf-8', errors='replace')

                result = {
                    'status_code': status_code,
                    'body': body_result,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"DELETE请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"DELETE请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DELETE请求异常: {str(e)}"
            )


class HttpPatchAction(BaseAction):
    """Perform HTTP PATCH requests."""
    action_type = "http_patch"
    display_name = "HTTP PATCH请求"
    description = "发送PATCH请求并获取响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP PATCH request.

        Args:
            context: Execution context.
            params: Dict with url, headers, body, timeout, output_var.

        Returns:
            ActionResult with response data.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        body_data = params.get('body', None)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        try:
            if body_data is not None:
                if isinstance(body_data, (dict, list)):
                    body_bytes = json_module.dumps(body_data).encode('utf-8')
                elif isinstance(body_data, str):
                    body_bytes = body_data.encode('utf-8')
                else:
                    body_bytes = body_data
            else:
                body_bytes = None

            request = urllib.request.Request(
                url,
                data=body_bytes,
                headers=headers,
                method='PATCH'
            )

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_body = response.read()

                try:
                    body_result = json_module.loads(response_body.decode('utf-8'))
                except Exception:
                    body_result = response_body.decode('utf-8', errors='replace')

                result = {
                    'status_code': status_code,
                    'body': body_result,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"PATCH请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"PATCH请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PATCH请求异常: {str(e)}"
            )


class HttpHeadAction(BaseAction):
    """Perform HTTP HEAD requests."""
    action_type = "http_head"
    display_name = "HTTP HEAD请求"
    description = "发送HEAD请求获取响应头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP HEAD request.

        Args:
            context: Execution context.
            params: Dict with url, headers, timeout, output_var.

        Returns:
            ActionResult with response headers.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'http_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        try:
            request = urllib.request.Request(url, headers=headers, method='HEAD')

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_headers = dict(response.headers)

                result = {
                    'status_code': status_code,
                    'headers': response_headers,
                    'url': url
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"HEAD请求成功: {status_code}",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"HEAD请求失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HEAD请求异常: {str(e)}"
            )


class HttpDownloadAction(BaseAction):
    """Download files via HTTP."""
    action_type = "http_download"
    display_name = "HTTP文件下载"
    description = "下载文件到指定路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTTP file download.

        Args:
            context: Execution context.
            params: Dict with url, save_path, headers, timeout, output_var.

        Returns:
            ActionResult with download result.
        """
        if not HTTP_AVAILABLE:
            return ActionResult(success=False, message="HTTP库不可用")

        url = params.get('url', '')
        save_path = params.get('save_path', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 300)
        output_var = params.get('output_var', 'download_result')

        if not url:
            return ActionResult(success=False, message="URL不能为空")

        if not save_path:
            return ActionResult(success=False, message="保存路径不能为空")

        try:
            request = urllib.request.Request(url, headers=headers, method='GET')

            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                content_length = response.headers.get('Content-Length')
                total_bytes = int(content_length) if content_length else 0

                downloaded_bytes = 0
                chunk_size = 8192

                with open(save_path, 'wb') as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded_bytes += len(chunk)

                result = {
                    'url': url,
                    'save_path': save_path,
                    'status_code': status_code,
                    'bytes_downloaded': downloaded_bytes,
                    'total_bytes': total_bytes
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"下载成功: {downloaded_bytes} bytes",
                    data=result
                )

        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"下载失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"下载异常: {str(e)}"
            )
