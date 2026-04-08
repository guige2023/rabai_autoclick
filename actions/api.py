"""API action module for RabAI AutoClick.

Provides HTTP API request actions including GET, POST, PUT, DELETE methods
with support for headers, JSON body, authentication, and response parsing.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRequestAction(BaseAction):
    """Perform HTTP API requests with configurable method, headers, and body.
    
    Supports GET, POST, PUT, PATCH, DELETE methods with JSON body,
    custom headers, basic authentication, and response validation.
    """
    action_type = "api_request"
    display_name = "API请求"
    description = "执行HTTP API请求，支持GET/POST/PUT/DELETE方法"
    VALID_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an HTTP API request.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: url, method, headers, body, 
                   auth_username, auth_password, timeout, expected_status.
        
        Returns:
            ActionResult with success status, response body, and status code.
        """
        # Extract and validate URL
        url = params.get('url', '')
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        # Validate method
        method = params.get('method', 'GET').upper()
        valid, msg = self.validate_in(method, self.VALID_METHODS, 'method')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Extract parameters
        headers = params.get('headers', {})
        body = params.get('body', None)
        auth_username = params.get('auth_username')
        auth_password = params.get('auth_password')
        timeout = params.get('timeout', 30)
        expected_status = params.get('expected_status', 200)
        
        # Prepare headers
        if isinstance(headers, dict):
            headers = {str(k): str(v) for k, v in headers.items()}
        else:
            headers = {}
        
        # Add Content-Type for requests with body
        if body and 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        
        # Prepare body
        request_body = None
        if body:
            if isinstance(body, dict):
                try:
                    request_body = json.dumps(body).encode('utf-8')
                except (TypeError, ValueError) as e:
                    return ActionResult(
                        success=False,
                        message=f"Failed to serialize body: {e}"
                    )
            elif isinstance(body, str):
                request_body = body.encode('utf-8')
            elif isinstance(body, bytes):
                request_body = body
        
        # Build request
        try:
            request = Request(url, data=request_body, headers=headers, method=method)
        except ValueError as e:
            return ActionResult(success=False, message=f"Invalid URL: {e}")
        
        # Add basic auth if provided
        if auth_username and auth_password:
            import base64
            credentials = f"{auth_username}:{auth_password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            request.add_header('Authorization', f'Basic {encoded}')
        
        # Execute request
        start_time = time.time()
        try:
            with urlopen(request, timeout=timeout) as response:
                status_code = response.status
                response_headers = dict(response.headers)
                response_body = response.read().decode('utf-8')
                elapsed = time.time() - start_time
                
                # Parse JSON if content-type is JSON
                content_type = response_headers.get('Content-Type', '')
                parsed_body = response_body
                if 'application/json' in content_type:
                    try:
                        parsed_body = json.loads(response_body)
                    except json.JSONDecodeError:
                        pass
                
                # Check expected status
                if isinstance(expected_status, list):
                    status_ok = status_code in expected_status
                else:
                    status_ok = status_code == expected_status
                
                return ActionResult(
                    success=status_ok,
                    message=f"HTTP {status_code} in {elapsed:.2f}s",
                    data={
                        'status_code': status_code,
                        'headers': response_headers,
                        'body': parsed_body,
                        'elapsed': elapsed
                    }
                )
                
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}",
                data={'status_code': e.code, 'error': str(e)}
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"URL Error: {e.reason}",
                data={'error': str(e)}
            )
        except TimeoutError:
            return ActionResult(
                success=False,
                message=f"Request timeout after {timeout}s",
                data={'timeout': timeout}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Request failed: {e}",
                data={'error': str(e)}
            )
