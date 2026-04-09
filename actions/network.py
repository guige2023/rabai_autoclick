"""Network action module for RabAI AutoClick.

Provides network operations:
- HttpGetAction: HTTP GET request
- HttpPostAction: HTTP POST request
- DownloadFileAction: Download file from URL
"""

import json
from typing import Any, Dict, List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode, quote

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class HttpGetAction(BaseAction):
    """Perform HTTP GET request."""
    action_type = "http_get"
    display_name = "HTTP GET"
    description = "发送HTTP GET请求"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP GET request.
        
        Args:
            context: Execution context.
            params: Dict with url, params, headers, output_var.
            
        Returns:
            ActionResult with response data.
        """
        url = params.get('url')
        query_params = params.get('params', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'http_response')
        
        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(query_params, dict, 'params')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(headers, dict, 'headers')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if query_params:
                encoded_params = urlencode(query_params)
                url = f"{url}?{encoded_params}" if '?' not in url else f"{url}&{encoded_params}"
            
            request = Request(url, headers=headers, method='GET')
            
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                response_code = response.getcode()
                response_headers = dict(response.headers)
            
            context.set(output_var, response_body)
            
            return ActionResult(
                success=True,
                message=f"GET请求成功 (状态码: {response_code})",
                data={
                    'body': response_body,
                    'status_code': response_code,
                    'headers': response_headers
                }
            )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"GET请求失败: HTTP {e.code} - {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"GET请求失败: {e.reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GET请求失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['url']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'params': {},
            'headers': {},
            'output_var': 'http_response'
        }


class HttpPostAction(BaseAction):
    """Perform HTTP POST request."""
    action_type = "http_post"
    display_name = "HTTP POST"
    description = "发送HTTP POST请求"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP POST request.
        
        Args:
            context: Execution context.
            params: Dict with url, data, headers, content_type, output_var.
            
        Returns:
            ActionResult with response data.
        """
        url = params.get('url')
        data = params.get('data', {})
        headers = params.get('headers', {})
        content_type = params.get('content_type', 'application/json')
        output_var = params.get('output_var', 'http_response')
        
        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(data, dict, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(headers, dict, 'headers')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if content_type == 'application/json':
                json_data = json.dumps(data).encode('utf-8')
            else:
                encoded_data = urlencode(data).encode('utf-8')
                json_data = encoded_data
            
            headers['Content-Type'] = content_type
            
            request = Request(url, data=json_data, headers=headers, method='POST')
            
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                response_code = response.getcode()
                response_headers = dict(response.headers)
            
            context.set(output_var, response_body)
            
            return ActionResult(
                success=True,
                message=f"POST请求成功 (状态码: {response_code})",
                data={
                    'body': response_body,
                    'status_code': response_code,
                    'headers': response_headers
                }
            )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"POST请求失败: HTTP {e.code} - {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"POST请求失败: {e.reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"POST请求失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['url']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'data': {},
            'headers': {},
            'content_type': 'application/json',
            'output_var': 'http_response'
        }


class DownloadFileAction(BaseAction):
    """Download a file from URL."""
    action_type = "download_file"
    display_name = "下载文件"
    description = "从URL下载文件"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file download.
        
        Args:
            context: Execution context.
            params: Dict with url, dest_path, overwrite.
            
        Returns:
            ActionResult indicating success or failure.
        """
        url = params.get('url')
        dest_path = params.get('dest_path')
        overwrite = params.get('overwrite', False)
        
        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(dest_path, str, 'dest_path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(overwrite, bool, 'overwrite')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            from pathlib import Path
            path_obj = Path(dest_path)
            
            if path_obj.exists() and not overwrite:
                return ActionResult(success=False, message=f"目标文件已存在: {dest_path}")
            
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            request = Request(url, method='GET')
            
            with urlopen(request, timeout=60) as response:
                content = response.read()
                response_code = response.getcode()
                content_length = response.headers.get('Content-Length')
            
            path_obj.write_bytes(content)
            
            return ActionResult(
                success=True,
                message=f"文件下载成功: {url} -> {dest_path}",
                data={
                    'url': url,
                    'dest': str(path_obj.absolute()),
                    'size': len(content),
                    'content_length': int(content_length) if content_length else None,
                    'status_code': response_code
                }
            )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"下载失败: HTTP {e.code} - {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"下载失败: {e.reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"下载失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['url', 'dest_path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'overwrite': False
        }
