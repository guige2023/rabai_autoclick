"""HTTP utilities action module for RabAI AutoClick.

Provides HTTP operations:
- HttpHeadAction: Perform HEAD request
- HttpOptionsAction: Perform OPTIONS request
- HttpPatchAction: Perform PATCH request
- HttpUploadAction: Multipart file upload
- HttpDownloadAction: Download file from URL
- HttpHealthCheckAction: Health check endpoint
- HttpRedirectFollowAction: Follow redirects manually
- HttpBatchAction: Batch HTTP requests
"""

from __future__ import annotations

import json
import sys
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpHeadAction(BaseAction):
    """Perform HEAD request."""
    action_type = "http_head"
    display_name = "HTTP HEAD"
    description = "发送HTTP HEAD请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HEAD request."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_head_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            request = urllib.request.Request(resolved_url, method='HEAD')
            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=30) as resp:
                result = {
                    'status_code': resp.status,
                    'headers': dict(resp.headers),
                    'url': resp.url,
                }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HEAD {resolved_url} -> {resp.status}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"HEAD request error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_head_result'}


class HttpOptionsAction(BaseAction):
    """Perform OPTIONS request."""
    action_type = "http_options"
    display_name = "HTTP OPTIONS"
    description = "发送HTTP OPTIONS请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OPTIONS request."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_options_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            request = urllib.request.Request(resolved_url, method='OPTIONS')
            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=30) as resp:
                allow = resp.headers.get('Allow', '')
                result = {
                    'status_code': resp.status,
                    'allow': allow,
                    'headers': dict(resp.headers),
                }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"OPTIONS {resolved_url} -> {resp.status}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"OPTIONS request error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'http_options_result'}


class HttpPatchAction(BaseAction):
    """Perform PATCH request."""
    action_type = "http_patch"
    display_name = "HTTP PATCH"
    description = "发送HTTP PATCH请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute PATCH request."""
        url = params.get('url', '')
        data = params.get('data', None)
        json_data = params.get('json_data', None)
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_patch_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            if json_data is not None:
                resolved_json = context.resolve_value(json_data) if context else json_data
                body = json.dumps(resolved_json).encode('utf-8')
                headers_copy = {**resolved_headers, 'Content-Type': 'application/json'}
            elif data is not None:
                resolved_data = context.resolve_value(data) if context else data
                if isinstance(resolved_data, dict):
                    import urllib.parse
                    body = urllib.parse.urlencode(resolved_data).encode('utf-8')
                elif isinstance(resolved_data, str):
                    body = resolved_data.encode('utf-8')
                else:
                    body = resolved_data
                headers_copy = resolved_headers
            else:
                body = None
                headers_copy = resolved_headers

            request = urllib.request.Request(resolved_url, data=body, method='PATCH')
            for k, v in headers_copy.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=30) as resp:
                content = resp.read().decode('utf-8')
                try:
                    response_data = json.loads(content)
                except json.JSONDecodeError:
                    response_data = content

                result = {
                    'status_code': resp.status,
                    'body': response_data,
                    'headers': dict(resp.headers),
                }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"PATCH {resolved_url} -> {resp.status}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"PATCH request error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': None, 'json_data': None, 'headers': {}, 'output_var': 'http_patch_result'}


class HttpUploadAction(BaseAction):
    """Multipart file upload."""
    action_type = "http_upload"
    display_name = "HTTP上传文件"
    description = "multipart文件上传"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file upload."""
        url = params.get('url', '')
        file_path = params.get('file_path', '')
        field_name = params.get('field_name', 'file')
        extra_fields = params.get('extra_fields', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'upload_result')

        if not url or not file_path:
            return ActionResult(success=False, message="url and file_path are required")

        try:
            import urllib.request
            import mimetypes

            resolved_url = context.resolve_value(url) if context else url
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_fields = context.resolve_value(extra_fields) if context else extra_fields

            mime_type, _ = mimetypes.guess_type(resolved_path)
            if not mime_type:
                mime_type = 'application/octet-stream'

            boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
            file_name = _os.path.basename(resolved_path)

            with open(resolved_path, 'rb') as f:
                file_data = f.read()

            body_parts = []
            for k, v in resolved_fields.items():
                body_parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{str(v)}\r\n'.encode())

            body_parts.append(
                f'--{boundary}\r\nContent-Disposition: form-data; name="{field_name}"; filename="{file_name}"\r\nContent-Type: {mime_type}\r\n\r\n'.encode()
            )
            body_parts.append(file_data)
            body_parts.append(f'--{boundary}--\r\n'.encode())
            body = b''.join(body_parts)

            headers_copy = {**headers, 'Content-Type': f'multipart/form-data; boundary={boundary}'}
            request = urllib.request.Request(resolved_url, data=body, method='POST')
            for k, v in headers_copy.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=60) as resp:
                content = resp.read().decode('utf-8')
                try:
                    response_data = json.loads(content)
                except json.JSONDecodeError:
                    response_data = content

                result = {
                    'status_code': resp.status,
                    'body': response_data,
                }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Uploaded to {resolved_url}", data=result)
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Upload error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url', 'file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'field_name': 'file', 'extra_fields': {}, 'headers': {}, 'output_var': 'upload_result'}


class HttpDownloadAction(BaseAction):
    """Download file from URL."""
    action_type = "http_download"
    display_name = "HTTP下载文件"
    description = "从URL下载文件"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file download."""
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        headers = params.get('headers', {})
        chunk_size = params.get('chunk_size', 8192)
        timeout = params.get('timeout', 60)

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_headers = context.resolve_value(headers) if context else headers
            resolved_chunk = context.resolve_value(chunk_size) if context else chunk_size

            request = urllib.request.Request(resolved_url)
            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=timeout) as resp:
                total_size = int(resp.headers.get('Content-Length', 0))
                downloaded = 0

                if resolved_output:
                    _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
                    with open(resolved_output, 'wb') as f:
                        while True:
                            chunk = resp.read(resolved_chunk)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                    result = {'output_path': resolved_output, 'bytes': downloaded, 'url': resolved_url}
                    return ActionResult(success=True, message=f"Downloaded {downloaded} bytes to {resolved_output}", data=result)
                else:
                    data = b''
                    while True:
                        chunk = resp.read(resolved_chunk)
                        if not chunk:
                            break
                        data += chunk

                    import base64
                    encoded = base64.b64encode(data).decode('ascii')
                    result = {'data': encoded, 'bytes': len(data), 'url': resolved_url}
                    return ActionResult(success=True, message=f"Downloaded {len(data)} bytes", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Download error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'headers': {}, 'chunk_size': 8192, 'timeout': 60}


class HttpHealthCheckAction(BaseAction):
    """Health check endpoint."""
    action_type = "http_health"
    display_name = "HTTP健康检查"
    description = "HTTP健康检查"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute health check."""
        url = params.get('url', '')
        expected_status = params.get('expected_status', 200)
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'health_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_expected = context.resolve_value(expected_status) if context else expected_status

            start_time = time.time()
            try:
                request = urllib.request.Request(resolved_url, method='GET')
                with urllib.request.urlopen(request, timeout=timeout) as resp:
                    elapsed = round((time.time() - start_time) * 1000, 2)
                    status_ok = resp.status == resolved_expected
                    result = {
                        'healthy': status_ok,
                        'status_code': resp.status,
                        'expected_status': resolved_expected,
                        'elapsed_ms': elapsed,
                        'url': resolved_url,
                    }
            except urllib.error.HTTPError as e:
                elapsed = round((time.time() - start_time) * 1000, 2)
                result = {
                    'healthy': e.code == resolved_expected,
                    'status_code': e.code,
                    'expected_status': resolved_expected,
                    'elapsed_ms': elapsed,
                    'url': resolved_url,
                    'error': str(e),
                }

            if context:
                context.set(output_var, result)
            return ActionResult(success=result['healthy'], message=f"Health check: {'OK' if result['healthy'] else 'FAIL'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Health check error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expected_status': 200, 'timeout': 5, 'output_var': 'health_result'}


class HttpBatchAction(BaseAction):
    """Batch HTTP requests."""
    action_type = "http_batch"
    display_name = "HTTP批量请求"
    description = "批量发送HTTP请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch HTTP requests."""
        requests_list = params.get('requests', [])  # [{url, method, headers, data, json_data}]
        max_concurrent = params.get('max_concurrent', 5)
        output_var = params.get('output_var', 'batch_result')

        if not requests_list:
            return ActionResult(success=False, message="requests is required")

        try:
            import urllib.request
            import concurrent.futures

            resolved_requests = context.resolve_value(requests_list) if context else requests_list

            def do_request(req):
                url = req.get('url', '')
                method = req.get('method', 'GET').upper()
                headers = req.get('headers', {})
                data = req.get('data', None)
                json_data = req.get('json_data', None)

                body = None
                if json_data is not None:
                    body = json.dumps(json_data).encode('utf-8')
                    headers = {**headers, 'Content-Type': 'application/json'}
                elif data is not None:
                    if isinstance(data, dict):
                        import urllib.parse
                        body = urllib.parse.urlencode(data).encode('utf-8')
                    elif isinstance(data, str):
                        body = data.encode('utf-8')
                    else:
                        body = data

                request = urllib.request.Request(url, data=body, method=method)
                for k, v in headers.items():
                    request.add_header(k, str(v))

                try:
                    with urllib.request.urlopen(request, timeout=30) as resp:
                        content = resp.read().decode('utf-8')
                        try:
                            resp_data = json.loads(content)
                        except json.JSONDecodeError:
                            resp_data = content
                        return {'success': True, 'status': resp.status, 'data': resp_data, 'url': url}
                except urllib.error.HTTPError as e:
                    return {'success': False, 'status': e.code, 'error': str(e), 'url': url}
                except Exception as e:
                    return {'success': False, 'error': str(e), 'url': url}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                results = list(executor.map(do_request, resolved_requests))

            success_count = sum(1 for r in results if r.get('success', False))
            if context:
                context.set(output_var, results)
            return ActionResult(
                success=success_count == len(results),
                message=f"Batch: {success_count}/{len(results)} succeeded",
                data={'results': results, 'success_count': success_count, 'total': len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['requests']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_concurrent': 5, 'output_var': 'batch_result'}
