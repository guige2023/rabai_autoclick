"""API builder action module for RabAI AutoClick.

Provides API building:
- APIBuilderAction: Build API requests
- RequestBuilderAction: Build requests
- ResponseBuilderAction: Build responses
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIBuilderAction(BaseAction):
    """Build API requests."""
    action_type = "api_builder"
    display_name = "API构建器"
    description = "构建API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "build")
            api_spec = params.get("api_spec", {})

            if operation == "build":
                endpoint = params.get("endpoint", "/api/default")
                method = params.get("method", "GET")
                headers = params.get("headers", {})
                body = params.get("body", None)
                query_params = params.get("query_params", {})

                request = {
                    "endpoint": endpoint,
                    "method": method,
                    "headers": headers,
                    "body": body,
                    "query_params": query_params,
                    "url": self._build_url(endpoint, query_params)
                }

                return ActionResult(
                    success=True,
                    data={
                        "request": request,
                        "built_at": datetime.now().isoformat()
                    },
                    message=f"API built: {method} {request['url']}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"API builder error: {str(e)}")

    def _build_url(self, endpoint: str, query_params: Dict) -> str:
        if not query_params:
            return endpoint
        params_str = "&".join(f"{k}={v}" for k, v in query_params.items())
        return f"{endpoint}?{params_str}"


class RequestBuilderAction(BaseAction):
    """Build requests."""
    action_type = "request_builder"
    display_name = "请求构建器"
    description = "构建HTTP请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            method = params.get("method", "GET")
            url = params.get("url", "")
            headers = params.get("headers", {})
            body = params.get("body", None)
            auth = params.get("auth", None)

            if auth:
                if auth.get("type") == "bearer":
                    headers["Authorization"] = f"Bearer {auth.get('token', '')}"
                elif auth.get("type") == "basic":
                    import base64
                    credentials = f"{auth.get('username')}:{auth.get('password')}"
                    headers["Authorization"] = f"Basic {base64.b64encode(credentials.encode()).decode()}"

            request = {
                "method": method,
                "url": url,
                "headers": headers,
                "body": body
            }

            return ActionResult(
                success=True,
                data={
                    "request": request,
                    "ready": True
                },
                message=f"Request built: {method} {url}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Request builder error: {str(e)}")


class ResponseBuilderAction(BaseAction):
    """Build responses."""
    action_type = "response_builder"
    display_name = "响应构建器"
    description = "构建HTTP响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            status_code = params.get("status_code", 200)
            headers = params.get("headers", {})
            body = params.get("body", None)
            status_text = params.get("status_text", "OK")

            response = {
                "status_code": status_code,
                "status_text": status_text,
                "headers": headers,
                "body": body
            }

            return ActionResult(
                success=status_code < 400,
                data={
                    "response": response,
                    "built_at": datetime.now().isoformat()
                },
                message=f"Response built: {status_code} {status_text}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response builder error: {str(e)}")
