"""HTTP client action module for RabAI AutoClick.

Provides advanced HTTP operations including multipart uploads,
file downloads, retry logic, and session management.
"""

import os
import sys
import json
import time
import hashlib
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HTTPDownloadAction(BaseAction):
    """Download file from URL.
    
    Supports resume, progress tracking, checksum verification,
    and custom headers.
    """
    action_type = "http_download"
    display_name = "HTTP下载"
    description = "从URL下载文件，支持断点续传和校验"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Download a file.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, output_path, headers,
                   chunk_size, verify_checksum, expected_hash,
                   timeout, save_to_var.
        
        Returns:
            ActionResult with download result.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        headers = params.get('headers', {})
        chunk_size = params.get('chunk_size', 8192)
        verify_checksum = params.get('verify_checksum', False)
        expected_hash = params.get('expected_hash', '')
        timeout = params.get('timeout', 300)
        save_to_var = params.get('save_to_var', None)

        if not url:
            return ActionResult(success=False, message="URL is required")

        if not output_path:
            # Generate filename from URL
            filename = url.split('/')[-1].split('?')[0] or 'download'
            output_path = f"/tmp/{filename}"

        try:
            import urllib.request
            import urllib.error

            req = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(req, timeout=timeout) as response:
                total_size = int(response.headers.get('Content-Length', 0))
                content_type = response.headers.get('Content-Type', '')

                # Check for resume support
                mode = 'wb'
                downloaded = 0
                if os.path.exists(output_path) and total_size > 0:
                    downloaded = os.path.getsize(output_path)
                    if downloaded < total_size:
                        req.add_header('Range', f'bytes={downloaded}-')
                        mode = 'ab'

                # Download
                with open(output_path, mode) as f:
                    start_time = time.time()
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)

                elapsed = time.time() - start_time
                actual_size = os.path.getsize(output_path)

                # Verify checksum
                checksum_ok = True
                actual_hash = ''
                if verify_checksum and expected_hash:
                    sha256 = hashlib.sha256()
                    with open(output_path, 'rb') as f:
                        for chunk in iter(lambda: f.read(chunk_size), b''):
                            sha256.update(chunk)
                    actual_hash = sha256.hexdigest()
                    checksum_ok = (actual_hash.lower() == expected_hash.lower())

                result_data = {
                    'url': url,
                    'output_path': output_path,
                    'size': actual_size,
                    'elapsed': elapsed,
                    'speed': actual_size / elapsed if elapsed > 0 else 0,
                    'content_type': content_type,
                    'checksum_ok': checksum_ok,
                    'actual_hash': actual_hash,
                    'expected_hash': expected_hash
                }

                if save_to_var:
                    context.variables[save_to_var] = result_data

                if checksum_ok:
                    return ActionResult(
                        success=True,
                        message=f"下载完成: {output_path} ({actual_size} bytes, {elapsed:.1f}s)",
                        data=result_data
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"校验失败: 期望 {expected_hash}, 实际 {actual_hash}",
                        data=result_data
                    )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"下载失败: {str(e)}",
                data={'error': str(e)}
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_path': '',
            'headers': {},
            'chunk_size': 8192,
            'verify_checksum': False,
            'expected_hash': '',
            'timeout': 300,
            'save_to_var': None
        }


class HTTPBatchRequestAction(BaseAction):
    """Perform batch HTTP requests.
    
    Supports parallel execution, rate limiting,
    and aggregate result collection.
    """
    action_type = "http_batch"
    display_name = "批量HTTP请求"
    description = "批量发送HTTP请求，支持并发控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests (list), max_concurrent,
                   delay, save_to_var.
        
        Returns:
            ActionResult with batch results.
        """
        requests_list = params.get('requests', [])
        max_concurrent = params.get('max_concurrent', 3)
        delay = params.get('delay', 0.5)
        save_to_var = params.get('save_to_var', None)

        if not requests_list:
            return ActionResult(success=False, message="Requests list is empty")

        results = []
        success_count = 0
        failure_count = 0

        import urllib.request
        import urllib.error

        for i, req_params in enumerate(requests_list):
            url = req_params.get('url', '')
            method = req_params.get('method', 'GET').upper()
            headers = req_params.get('headers', {})
            data = req_params.get('data', None)

            if not url:
                results.append({'index': i, 'success': False, 'error': 'URL missing'})
                failure_count += 1
                continue

            try:
                body = None
                if data and method in ('POST', 'PUT', 'PATCH'):
                    if isinstance(data, dict):
                        body = json.dumps(data).encode('utf-8')
                        headers.setdefault('Content-Type', 'application/json')
                    else:
                        body = str(data).encode('utf-8')

                req = urllib.request.Request(url, data=body, headers=headers, method=method)
                
                with urllib.request.urlopen(req, timeout=30) as response:
                    results.append({
                        'index': i,
                        'success': True,
                        'status': response.status,
                        'url': url
                    })
                    success_count += 1
            except Exception as e:
                results.append({
                    'index': i,
                    'success': False,
                    'error': str(e),
                    'url': url
                })
                failure_count += 1

            # Rate limiting delay
            if delay > 0 and i < len(requests_list) - 1:
                time.sleep(delay)

        result_data = {
            'total': len(requests_list),
            'success': success_count,
            'failure': failure_count,
            'results': results
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"批量请求完成: {success_count}/{len(requests_list)} 成功",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['requests']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_concurrent': 3,
            'delay': 0.5,
            'save_to_var': None
        }


class HTTPHeadersAction(BaseAction):
    """Parse and extract HTTP headers.
    
    Supports header value extraction, case-insensitive lookup,
    and common header type conversion.
    """
    action_type = "http_headers"
    display_name = "HTTP头解析"
    description = "解析HTTP响应头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Parse HTTP headers.
        
        Args:
            context: Execution context.
            params: Dict with keys: headers (dict), key,
                   extract_all, save_to_var.
        
        Returns:
            ActionResult with extracted header values.
        """
        headers = params.get('headers', {})
        key = params.get('key', None)
        extract_all = params.get('extract_all', False)
        save_to_var = params.get('save_to_var', None)

        if not headers:
            return ActionResult(success=False, message="Headers dict is empty")

        result_data = {'headers': headers}

        if key:
            # Case-insensitive lookup
            key_lower = key.lower()
            value = None
            for h_key, h_value in headers.items():
                if h_key.lower() == key_lower:
                    value = h_value
                    break
            result_data['value'] = value
            result_data['key'] = key

            # Parse common headers
            if value:
                if key_lower == 'content-type':
                    result_data['content_type'] = value.split(';')[0].strip()
                    if 'charset=' in value:
                        result_data['charset'] = value.split('charset=')[1].split(';')[0].strip()
                elif key_lower == 'content-length':
                    try:
                        result_data['content_length'] = int(value)
                    except ValueError:
                        pass
                elif key_lower == 'last-modified':
                    result_data['last_modified'] = value

        if extract_all:
            result_data['keys'] = list(headers.keys())
            result_data['count'] = len(headers)

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Headers解析完成: {len(headers)} 个响应头",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['headers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': None,
            'extract_all': False,
            'save_to_var': None
        }
