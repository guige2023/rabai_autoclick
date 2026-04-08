"""HTTP streaming action module for RabAI AutoClick.

Provides streaming HTTP client actions for large file downloads,
server-sent events, and chunked responses.
"""

import json
import urllib.request
import urllib.error
import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable, Iterator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpStreamDownloadAction(BaseAction):
    """Stream download large files with chunked processing.
    
    Downloads files in chunks for memory efficiency with large files.
    """
    action_type = "http_stream_download"
    display_name = "流式下载"
    description = "流式下载大文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Stream download file in chunks.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, output_path, chunk_size, 
                   timeout, headers, progress_callback.
        
        Returns:
            ActionResult with download status and bytes downloaded.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '')
        chunk_size = params.get('chunk_size', 8192)
        timeout = params.get('timeout', 300)
        headers = params.get('headers', {})
        progress_callback = params.get('progress_callback', None)

        if not url:
            return ActionResult(success=False, message="url is required")
        if not output_path:
            return ActionResult(success=False, message="output_path is required")

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                total_size = int(response.headers.get('Content-Length', 0))
                downloaded = 0
                
                with open(output_path, 'wb') as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if progress_callback and total_size > 0:
                            progress = (downloaded / total_size) * 100
                            progress_callback(progress)

            return ActionResult(
                success=True,
                message=f"Downloaded {downloaded} bytes",
                data={'bytes_downloaded': downloaded, 'path': output_path}
            )
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"HTTP error: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            return ActionResult(success=False, message=f"URL error: {str(e.reason)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Download failed: {str(e)}")


class HttpSSEAction(BaseAction):
    """Parse Server-Sent Events from HTTP response.
    
    Handles SSE streams for real-time data updates.
    """
    action_type = "http_sse"
    display_name = "SSE事件流"
    description = "解析Server-Sent Events流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse SSE stream.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, headers, timeout, 
                   reconnect, event_handler.
        
        Returns:
            ActionResult with parsed events.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 60)
        reconnect = params.get('reconnect', True)
        event_handler = params.get('event_handler', None)

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            req = urllib.request.Request(url, headers=headers)
            events = []
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                current_event = {}
                
                for line in response:
                    line = line.decode('utf-8').rstrip('\n\r')
                    
                    if line == '':
                        if current_event:
                            events.append(current_event)
                            if event_handler:
                                event_handler(current_event)
                            current_event = {}
                    elif ':' in line:
                        field, _, value = line.partition(':')
                        field = field.strip()
                        value = value.strip()
                        if field == 'event':
                            current_event['event'] = value
                        elif field == 'data':
                            if 'data' in current_event:
                                current_event['data'] += '\n' + value
                            else:
                                current_event['data'] = value
                        elif field == 'id':
                            current_event['id'] = value
                        elif field == 'retry':
                            current_event['retry'] = value

            return ActionResult(
                success=True,
                message=f"Parsed {len(events)} events",
                data={'events': events, 'count': len(events)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SSE parse failed: {str(e)}")


class HttpChunkedResponseAction(BaseAction):
    """Handle chunked transfer encoding responses.
    
    Processes HTTP responses with chunked transfer encoding.
    """
    action_type = "http_chunked_response"
    display_name = "分块响应处理"
    description = "处理HTTP分块传输响应"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process chunked response.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, headers, timeout, 
                   accumulate, separator.
        
        Returns:
            ActionResult with response chunks.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 60)
        accumulate = params.get('accumulate', True)
        separator = params.get('separator', '\n')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            req = urllib.request.Request(url, headers=headers)
            chunks = []
            full_body = b''

            with urllib.request.urlopen(req, timeout=timeout) as response:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    
                    if accumulate:
                        full_body += chunk
                    
                    chunks.append(chunk)

            data = {
                'chunks': len(chunks),
                'total_bytes': sum(len(c) for c in chunks),
            }
            
            if accumulate:
                data['body'] = full_body.decode('utf-8', errors='replace')
                data['body_preview'] = data['body'][:500]

            return ActionResult(
                success=True,
                message=f"Received {data['chunks']} chunks",
                data=data
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chunked response failed: {str(e)}")


class HttpMultiPartUploadAction(BaseAction):
    """Upload files using multipart/form-data encoding.
    
    Handles file uploads to HTTP endpoints.
    """
    action_type = "http_multipart_upload"
    display_name = "多部分上传"
    description = "multipart/form-data文件上传"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Upload file via multipart form.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, file_path, field_name, 
                   additional_fields, headers, timeout.
        
        Returns:
            ActionResult with upload status.
        """
        url = params.get('url', '')
        file_path = params.get('file_path', '')
        field_name = params.get('field_name', 'file')
        additional_fields = params.get('additional_fields', {})
        headers = params.get('headers', {})
        timeout = params.get('timeout', 120)

        if not url:
            return ActionResult(success=False, message="url is required")
        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import email.mime.multipart
            import email.mime.base
            import email.encoders
            
            boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
            
            with open(file_path, 'rb') as f:
                file_data = f.read()

            filename = os.path.basename(file_path)
            
            body = b''
            for key, value in additional_fields.items():
                body += f'--{boundary}\r\n'.encode()
                body += f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode()
                body += f'{value}\r\n'.encode()

            body += f'--{boundary}\r\n'.encode()
            body += f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode()
            body += b'Content-Type: application/octet-stream\r\n\r\n'
            body += file_data
            body += f'\r\n--{boundary}--\r\n'.encode()

            headers['Content-Type'] = f'multipart/form-data; boundary={boundary}'
            
            req = urllib.request.Request(url, data=body, headers=headers)
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                response_body = response.read()
                
            return ActionResult(
                success=True,
                message="Upload successful",
                data={'response_size': len(response_body), 'filename': filename}
            )
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {file_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Multipart upload failed: {str(e)}")
