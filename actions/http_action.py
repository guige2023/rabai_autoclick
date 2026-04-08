"""HTTP action module for RabAI AutoClick.

Provides HTTP client actions for web requests, downloads, and API interactions.
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpDownloadAction(BaseAction):
    """Download file from HTTP URL.
    
    Downloads files with progress tracking and resume support.
    """
    action_type = "http_download"
    display_name = "HTTP下载"
    description = "从HTTP URL下载文件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Download file.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: url, output_path, timeout, 
                   resume, headers.
        
        Returns:
            ActionResult with download status.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        timeout = params.get('timeout', 60)
        resume = params.get('resume', False)
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="url is required")
        
        if not output_path:
            # Extract filename from URL
            parsed = urllib.parse.urlparse(url)
            output_path = os.path.basename(parsed.path) or 'download'
        
        # Prepare headers
        request_headers = {}
        if isinstance(headers, dict):
            request_headers.update(headers)
        
        # Add Range header for resume
        mode = 'wb'
        if resume and os.path.exists(output_path):
            existing_size = os.path.getsize(output_path)
            request_headers['Range'] = f'bytes={existing_size}-'
            mode = 'ab'
        
        try:
            request = urllib.request.Request(url, headers=request_headers)
            
            with urllib.request.urlopen(request, timeout=timeout) as response:
                total_size = int(response.headers.get('Content-Length', 0))
                content_type = response.headers.get('Content-Type', '')
                
                downloaded_size = 0
                chunk_size = 8192
                
                with open(output_path, mode) as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded_size += len(chunk)
                
                final_size = os.path.getsize(output_path)
                
                return ActionResult(
                    success=True,
                    message=f"Downloaded {final_size} bytes",
                    data={
                        'url': url,
                        'path': output_path,
                        'size': final_size,
                        'content_type': content_type
                    }
                )
                
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}",
                data={'error': str(e), 'code': e.code}
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"URL error: {e.reason}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Download error: {e}",
                data={'error': str(e)}
            )


class HttpStatusCheckAction(BaseAction):
    """Check HTTP status code without downloading body.
    
    Performs a HEAD request to check URL availability.
    """
    action_type = "http_status_check"
    display_name = "HTTP状态检查"
    description = "检查URL的HTTP状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check HTTP status.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: url, timeout, headers.
        
        Returns:
            ActionResult with status info.
        """
        url = params.get('url', '')
        timeout = params.get('timeout', 10)
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="url is required")
        
        try:
            request_headers = {'Method': 'HEAD'}
            if isinstance(headers, dict):
                request_headers.update(headers)
            
            request = urllib.request.Request(url, headers=request_headers, method='HEAD')
            
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_headers = dict(response.headers)
                
                is_ok = 200 <= status_code < 400
                
                return ActionResult(
                    success=is_ok,
                    message=f"HTTP {status_code}",
                    data={
                        'url': url,
                        'status_code': status_code,
                        'headers': response_headers,
                        'available': is_ok
                    }
                )
                
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}",
                data={'url': url, 'status_code': e.code, 'available': False}
            )
        except urllib.error.URLError as e:
            return ActionResult(
                success=False,
                message=f"URL error: {e.reason}",
                data={'url': url, 'available': False, 'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Status check error: {e}",
                data={'error': str(e)}
            )


class UrlEncodeAction(BaseAction):
    """URL encode/decode strings.
    
    Encodes parameters for URL queries and decodes URL-encoded strings.
    """
    action_type = "url_encode"
    display_name = "URL编码解码"
    description = "URL编码和解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """URL encode or decode.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: text, operation (encode/decode), 
                   safe_chars, quote_via.
        
        Returns:
            ActionResult with encoded/decoded string.
        """
        text = params.get('text', '')
        operation = params.get('operation', 'encode')
        safe_chars = params.get('safe_chars', '')
        quote_via = params.get('quote_via', 'quote')
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        try:
            if operation == 'encode':
                if quote_via == 'quote':
                    result = urllib.parse.quote(text, safe=safe_chars)
                elif quote_via == 'quote_plus':
                    result = urllib.parse.quote_plus(text, safe=safe_chars)
                elif quote_via == 'path':
                    result = urllib.parse.quote(text, safe=safe_chars, safe='/:@!$&\'()*+,;=')
                else:
                    result = urllib.parse.quote(text, safe=safe_chars)
            elif operation == 'decode':
                if quote_via == 'quote_plus':
                    result = urllib.parse.unquote_plus(text)
                else:
                    result = urllib.parse.unquote(text)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )
            
            return ActionResult(
                success=True,
                message=f"URL {operation}d: {len(result)} chars",
                data={'result': result, 'operation': operation}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL encode/decode error: {e}",
                data={'error': str(e)}
            )
