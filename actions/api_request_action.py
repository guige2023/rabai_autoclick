"""API Request Builder action module for RabAI AutoClick.

Constructs API requests with full control over headers,
authentication, and body encoding.
"""

import json
import time
import sys
import os
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRequestAction(BaseAction):
    """Build and execute HTTP API requests.

    Full control over HTTP method, headers, authentication,
    body encoding, and response parsing.
    """
    action_type = "api_request"
    display_name = "API请求"
    description = "构建和执行HTTP API请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API request.

        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   auth_type, auth_credentials, timeout,
                   expected_status, parse_response.

        Returns:
            ActionResult with response data.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            method = params.get('method', 'GET').upper()
            headers = params.get('headers', {})
            body = params.get('body')
            auth_type = params.get('auth_type', 'none')
            auth_credentials = params.get('auth_credentials', {})
            timeout = params.get('timeout', 30)
            expected_status = params.get('expected_status', 200)
            parse_response = params.get('parse_response', True)

            if not url:
                return ActionResult(success=False, message="url is required", duration=time.time() - start_time)

            # Apply authentication
            if auth_type == 'bearer':
                token = auth_credentials.get('token', '')
                headers['Authorization'] = f"Bearer {token}"
            elif auth_type == 'basic':
                import base64
                creds = f"{auth_credentials.get('username', '')}:{auth_credentials.get('password', '')}"
                headers['Authorization'] = f"Basic {base64.b64encode(creds.encode()).decode()}"
            elif auth_type == 'apikey':
                key_name = auth_credentials.get('key_name', 'X-API-Key')
                key_value = auth_credentials.get('key_value', '')
                headers[key_name] = key_value

            # Encode body
            body_bytes = None
            if body:
                if isinstance(body, str):
                    body_bytes = body.encode('utf-8')
                    headers.setdefault('Content-Type', 'text/plain')
                elif isinstance(body, dict):
                    body_bytes = json.dumps(body).encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')
                elif isinstance(body, bytes):
                    body_bytes = body

            req = Request(url, data=body_bytes, headers=headers, method=method)

            try:
                with urlopen(req, timeout=timeout) as resp:
                    latency_ms = int((time.time() - start_time) * 1000)
                    status_ok = resp.status == expected_status

                    if parse_response:
                        try:
                            response_data = json.loads(resp.read())
                        except Exception:
                            response_data = resp.read().decode('utf-8', errors='ignore')
                    else:
                        response_data = resp.read()

                    return ActionResult(
                        success=status_ok,
                        message=f"{method} {url} -> {resp.status} ({latency_ms}ms)",
                        data={
                            'status': resp.status,
                            'headers': dict(resp.headers),
                            'body': response_data,
                            'latency_ms': latency_ms,
                        },
                        duration=time.time() - start_time,
                    )

            except HTTPError as e:
                latency_ms = int((time.time() - start_time) * 1000)
                error_body = e.read().decode('utf-8', errors='ignore') if e.fp else str(e)
                try:
                    error_data = json.loads(error_body)
                except Exception:
                    error_data = error_body

                return ActionResult(
                    success=e.code == expected_status,
                    message=f"{method} {url} -> {e.code} ({latency_ms}ms)",
                    data={
                        'status': e.code,
                        'error': error_data,
                        'latency_ms': latency_ms,
                    },
                    duration=time.time() - start_time,
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Request error: {str(e)}", duration=time.time() - start_time)
