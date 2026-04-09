"""API Request Builder Action Module.

Provides comprehensive API request building with parameter handling,
header management, authentication integration, and request templating.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """HTTP methods supported."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ContentType(Enum):
    """Content types for request body."""
    JSON = "application/json"
    FORM_URLENCODED = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    XML = "application/xml"
    TEXT = "text/plain"
    HTML = "text/html"


@dataclass
class QueryParam:
    """A query parameter definition."""
    name: str
    value: Any
    encoding: str = "url"  # url, raw, json


@dataclass
class RequestHeader:
    """A request header definition."""
    name: str
    value: Any
    condition: Optional[str] = None  # Only add if condition is true


@dataclass
class RequestTemplate:
    """A reusable request template."""
    name: str
    method: HTTPMethod
    url: str
    headers: List[RequestHeader] = field(default_factory=list)
    query_params: List[QueryParam] = field(default_factory=list)
    body: Optional[Any] = None
    content_type: Optional[ContentType] = None
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BuiltRequest:
    """A fully built request ready for execution."""
    method: str
    url: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[Union[bytes, str, Dict]]
    content_type: Optional[str]
    timeout: float


class APIRequestBuilderAction(BaseAction):
    """API Request Builder Action for constructing API requests.

    Provides flexible request building with templates, authentication,
    dynamic parameters, and header management.

    Examples:
        >>> action = APIRequestBuilderAction()
        >>> result = action.execute(ctx, {
        ...     "method": "POST",
        ...     "url": "https://api.example.com/users",
        ...     "body": {"name": "Alice", "email": "alice@example.com"},
        ...     "add_auth": True
        ... })
    """

    action_type = "api_request_builder"
    display_name = "API请求构建"
    description = "灵活的API请求构建器，支持模板、认证、动态参数"

    def __init__(self):
        super().__init__()
        self._templates: Dict[str, RequestTemplate] = {}

    def register_template(self, template: Union[RequestTemplate, Dict]) -> None:
        """Register a request template."""
        if isinstance(template, dict):
            if "method" in template and isinstance(template["method"], str):
                template["method"] = HTTPMethod(template["method"])
            template = RequestTemplate(**template)
        self._templates[template.name] = template

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Build an API request.

        Args:
            context: Execution context.
            params: Dict with keys:
                - method: HTTP method
                - url: Request URL
                - template_name: Use a registered template
                - headers: Additional headers
                - query_params: Query parameters
                - body: Request body
                - content_type: Content-Type override
                - add_auth: Add authentication headers
                - auth_credentials: Auth credentials dict
                - encode_params: Encode body as form

        Returns:
            ActionResult with built request details.
        """
        try:
            # Get template if specified
            template_name = params.get("template_name")
            if template_name and template_name in self._templates:
                template = self._templates[template_name]
                method = template.method.value
                url = template.url
                headers = {h.name: h.value for h in template.headers}
                query_params_list = [(p.name, p.value) for p in template.query_params]
                body = template.body
                content_type = template.content_type.value if template.content_type else None
                timeout = template.timeout
            else:
                method = params.get("method", "GET").upper()
                url = params.get("url", "")
                headers = {}
                query_params_list = []
                body = params.get("body")
                content_type = params.get("content_type")
                timeout = params.get("timeout", 30.0)

            # Resolve variables in URL
            url = self._resolve_variables(url, params)

            # Add dynamic headers
            custom_headers = params.get("headers", {})
            headers.update(custom_headers)

            # Add auth if requested
            if params.get("add_auth", False):
                auth_headers = self._build_auth_headers(params.get("auth_credentials"))
                headers.update(auth_headers)

            # Build query parameters
            query_params_dict: Dict[str, str] = {}
            for name, value in query_params_list:
                resolved_value = self._resolve_value(value, params)
                query_params_dict[name] = str(resolved_value) if resolved_value is not None else ""

            # Add custom query params
            custom_query = params.get("query_params", {})
            for name, value in custom_query.items():
                query_params_dict[name] = str(value) if value is not None else ""

            # Process body
            processed_body = body
            if body is not None:
                if isinstance(body, dict) and params.get("encode_params", False):
                    # Form encode
                    from urllib.parse import urlencode
                    processed_body = urlencode(body).encode()
                    if content_type is None:
                        content_type = ContentType.FORM_URLENCODED.value
                elif isinstance(body, dict) and content_type is None:
                    # Default to JSON
                    processed_body = json.dumps(body).encode()
                    content_type = ContentType.JSON.value
                elif isinstance(body, str):
                    processed_body = body.encode() if isinstance(body, str) else body
                    if not content_type:
                        content_type = ContentType.TEXT.value

            # Add content type header
            if content_type and "Content-Type" not in headers:
                headers["Content-Type"] = content_type

            # Add common headers
            if "User-Agent" not in headers:
                headers["User-Agent"] = "RabAi-AutoClick/1.0"
            if "X-Request-Time" not in headers:
                headers["X-Request-Time"] = str(int(time.time()))

            built = BuiltRequest(
                method=method,
                url=url,
                headers=headers,
                query_params=query_params_dict,
                body=processed_body,
                content_type=content_type,
                timeout=timeout,
            )

            return ActionResult(
                success=True,
                message=f"Built {method} request to {url}",
                data={
                    "method": built.method,
                    "url": built.url,
                    "headers": built.headers,
                    "query_params": built.query_params,
                    "body_preview": self._get_body_preview(built.body),
                    "content_type": built.content_type,
                    "timeout": built.timeout,
                    "ready_to_execute": True,
                }
            )

        except Exception as e:
            logger.exception("Request builder failed")
            return ActionResult(
                success=False,
                message=f"Request builder error: {str(e)}"
            )

    def execute_request(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Build and execute an API request in one step.

        Args:
            context: Execution context.
            params: Same as execute() plus:
                - verify_ssl: Verify SSL certificates (default: True)

        Returns:
            ActionResult with response data.
        """
        import urllib.request
        import urllib.error
        import urllib.parse

        # First build the request
        build_result = self.execute(context, params)
        if not build_result.success:
            return build_result

        data = build_result.data
        url = data["url"]
        headers = data["headers"]
        query_params = data.get("query_params", {})

        # Add query params to URL
        if query_params:
            separator = "&" if "?" in url else "?"
            query_str = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in query_params.items())
            url = url + separator + query_str

        try:
            method = data["method"]
            req = urllib.request.Request(url, headers=headers, method=method)

            body = data.get("body_preview")
            if body and method in ("POST", "PUT", "PATCH"):
                if isinstance(body, dict):
                    req.data = json.dumps(body).encode()
                else:
                    req.data = str(body).encode()

            verify_ssl = params.get("verify_ssl", True)

            timeout = data.get("timeout", 30.0)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                content = response.read()
                resp_headers = dict(response.headers)

                try:
                    response_data = json.loads(content)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    response_data = content.decode(errors="replace")

                return ActionResult(
                    success=True,
                    message=f"Request succeeded: {response.status}",
                    data={
                        "status_code": response.status,
                        "headers": resp_headers,
                        "data": response_data,
                        "url": url,
                    }
                )

        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP error {e.code}: {e.reason}",
                data={"status_code": e.code, "error": str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Request failed: {str(e)}",
                data={"error": str(e)}
            )

    def _resolve_variables(self, template: str, params: Dict[str, Any]) -> str:
        """Resolve ${variable} placeholders in template."""
        result = template
        import re
        for match in re.finditer(r'\$\{([^}]+)\}', template):
            var_path = match.group(1)
            value = self._get_nested_param(var_path, params)
            if value is not None:
                result = result.replace(match.group(0), str(value))
        return result

    def _get_nested_param(self, path: str, params: Dict[str, Any]) -> Any:
        """Get nested parameter using dot notation."""
        parts = path.split(".")
        value = params
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _resolve_value(self, value: Any, params: Dict[str, Any]) -> Any:
        """Resolve a value that may contain variables."""
        if isinstance(value, str):
            return self._resolve_variables(value, params)
        elif isinstance(value, dict):
            return {k: self._resolve_value(v, params) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_value(v, params) for v in value]
        return value

    def _build_auth_headers(self, credentials: Optional[Dict]) -> Dict[str, str]:
        """Build authentication headers from credentials."""
        headers = {}
        if not credentials:
            return headers

        auth_type = credentials.get("type", "bearer")
        if auth_type == "bearer":
            token = credentials.get("token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key":
            key_name = credentials.get("key_name", "X-API-Key")
            key_value = credentials.get("key_value", "")
            headers[key_name] = key_value
        elif auth_type == "basic":
            import base64
            username = credentials.get("username", "")
            password = credentials.get("password", "")
            auth_str = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {auth_str}"

        return headers

    def _get_body_preview(self, body: Any) -> Any:
        """Get a preview of the body for logging."""
        if body is None:
            return None
        if isinstance(body, bytes):
            try:
                decoded = body.decode()
                if len(decoded) > 200:
                    return decoded[:200] + "..."
                return decoded
            except UnicodeDecodeError:
                return f"<{len(body)} bytes binary data>"
        if isinstance(body, dict):
            preview = dict(body)
            for key in preview:
                if isinstance(preview[key], str) and len(preview[key]) > 100:
                    preview[key] = preview[key][:100] + "..."
            return preview
        return body

    def get_required_params(self) -> List[str]:
        return ["url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "method": "GET",
            "template_name": None,
            "headers": {},
            "query_params": {},
            "body": None,
            "content_type": None,
            "add_auth": False,
            "auth_credentials": None,
            "encode_params": False,
            "timeout": 30.0,
        }
