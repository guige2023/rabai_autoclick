"""API Mock Action Module.

Provides API mocking capabilities for testing and development,
including response templating and dynamic mocking.
"""

import json
import time
import hashlib
import re
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MockResponseType(Enum):
    """Type of mock response."""
    STATIC = "static"
    TEMPLATE = "template"
    DYNAMIC = "dynamic"
    FAULT = "fault"
    DELAYED = "delayed"


@dataclass
class MockEndpoint:
    """Defines a mocked API endpoint."""
    path: str
    method: str = "GET"
    response_status: int = 200
    response_body: Any = None
    response_headers: Dict[str, str] = field(default_factory=dict)
    response_type: MockResponseType = MockResponseType.STATIC
    delay_ms: int = 0
    fault_type: Optional[str] = None
    fault_percentage: float = 0.0
    template_vars: List[str] = field(default_factory=list)


@dataclass
class MockRequest:
    """Captured mock request."""
    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[Any]
    query_params: Dict[str, str]
    timestamp: float = field(default_factory=time.time)


@dataclass
class MockResponse:
    """Generated mock response."""
    status_code: int
    headers: Dict[str, str]
    body: Any
    delay_ms: int = 0


class APIMockHandler:
    """Handles API mocking operations."""

    def __init__(self):
        self._endpoints: Dict[str, MockEndpoint] = {}
        self._request_history: List[MockRequest] = []
        self._template_processors: Dict[str, Callable] = {}
        self._fault_injectors: Dict[str, Callable] = {}

    def register_endpoint(self, endpoint: MockEndpoint) -> None:
        """Register a mock endpoint."""
        key = f"{endpoint.method}:{endpoint.path}"
        self._endpoints[key] = endpoint

    def unregister_endpoint(self, path: str, method: str = "GET") -> bool:
        """Unregister a mock endpoint."""
        key = f"{method}:{path}"
        if key in self._endpoints:
            del self._endpoints[key]
            return True
        return False

    def get_endpoint(self, path: str, method: str = "GET") -> Optional[MockEndpoint]:
        """Get a registered endpoint."""
        key = f"{method}:{path}"
        return self._endpoints.get(key)

    def match_endpoint(self, path: str, method: str = "GET") -> Optional[MockEndpoint]:
        """Match endpoint using pattern matching."""
        key = f"{method}:{path}"
        if key in self._endpoints:
            return self._endpoints[key]

        for pattern, endpoint in self._endpoints.items():
            if pattern.startswith(f"{method}:"):
                pattern_path = pattern[len(f"{method}:"):]
                if self._match_pattern(pattern_path, path):
                    return endpoint
        return None

    def _match_pattern(self, pattern: str, path: str) -> bool:
        """Match URL pattern with path."""
        pattern_regex = re.sub(r'\{[^}]+\}', '[^/]+', pattern)
        pattern_regex = f"^{pattern_regex}$"
        return bool(re.match(pattern_regex, path))

    def record_request(self, request: MockRequest) -> None:
        """Record an incoming request."""
        self._request_history.append(request)
        if len(self._request_history) > 10000:
            self._request_history = self._request_history[-5000:]

    def get_request_history(
        self,
        path: Optional[str] = None,
        method: Optional[str] = None,
        limit: int = 100
    ) -> List[MockRequest]:
        """Get request history with optional filtering."""
        results = self._request_history

        if path:
            results = [r for r in results if r.path == path]
        if method:
            results = [r for r in results if r.method == method]

        return results[-limit:]

    def generate_response(
        self,
        endpoint: MockEndpoint,
        context: Optional[Dict[str, Any]] = None
    ) -> MockResponse:
        """Generate a mock response."""
        context = context or {}

        if endpoint.response_type == MockResponseType.DELAYED:
            delay = endpoint.delay_ms
        elif endpoint.response_type == MockResponseType.FAULT:
            delay = 0
        else:
            delay = endpoint.delay_ms

        body = self._process_response_body(endpoint, context)

        return MockResponse(
            status_code=endpoint.response_status,
            headers=endpoint.response_headers,
            body=body,
            delay_ms=delay
        )

    def _process_response_body(
        self,
        endpoint: MockEndpoint,
        context: Dict[str, Any]
    ) -> Any:
        """Process response body based on type."""
        if endpoint.response_type == MockResponseType.STATIC:
            return endpoint.response_body

        elif endpoint.response_type == MockResponseType.TEMPLATE:
            return self._process_template(
                endpoint.response_body,
                context,
                endpoint.template_vars
            )

        elif endpoint.response_type == MockResponseType.DYNAMIC:
            processor = self._template_processors.get(endpoint.path)
            if processor:
                return processor(context)
            return endpoint.response_body

        elif endpoint.response_type == MockResponseType.FAULT:
            return self._inject_fault(endpoint.fault_type)

        return endpoint.response_body

    def _process_template(
        self,
        template: Any,
        context: Dict[str, Any],
        vars_list: List[str]
    ) -> Any:
        """Process template with context variables."""
        if isinstance(template, str):
            for var in vars_list:
                value = context.get(var, f"{{{var}}}")
                template = template.replace(f"{{{var}}}", str(value))
            return template
        elif isinstance(template, dict):
            return {
                k: self._process_template(v, context, vars_list)
                for k, v in template.items()
            }
        elif isinstance(template, list):
            return [
                self._process_template(item, context, vars_list)
                for item in template
            ]
        return template

    def _inject_fault(self, fault_type: Optional[str]) -> Dict[str, Any]:
        """Inject fault response."""
        faults = {
            "500": {"error": "Internal Server Error", "code": "ERR_INTERNAL"},
            "502": {"error": "Bad Gateway", "code": "ERR_BAD_GATEWAY"},
            "503": {"error": "Service Unavailable", "code": "ERR_UNAVAILABLE"},
            "504": {"error": "Gateway Timeout", "code": "ERR_TIMEOUT"},
            "429": {"error": "Too Many Requests", "code": "ERR_RATE_LIMITED"},
            "401": {"error": "Unauthorized", "code": "ERR_UNAUTHORIZED"},
            "403": {"error": "Forbidden", "code": "ERR_FORBIDDEN"},
            "404": {"error": "Not Found", "code": "ERR_NOT_FOUND"},
        }
        return faults.get(fault_type, {"error": "Unknown Fault", "code": "ERR_UNKNOWN"})

    def register_template_processor(
        self,
        path: str,
        processor: Callable[[Dict[str, Any]], Any]
    ) -> None:
        """Register a dynamic template processor."""
        self._template_processors[path] = processor

    def register_fault_injector(
        self,
        name: str,
        injector: Callable[[], Dict[str, Any]]
    ) -> None:
        """Register a custom fault injector."""
        self._fault_injectors[name] = injector

    def clear_history(self) -> None:
        """Clear request history."""
        self._request_history.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get mock statistics."""
        return {
            "total_endpoints": len(self._endpoints),
            "total_requests": len(self._request_history),
            "endpoints": [
                {"path": e.path, "method": e.method, "type": e.response_type.value}
                for e in self._endpoints.values()
            ]
        }


class APIMockAction(BaseAction):
    """Action for API mocking operations."""

    def __init__(self):
        super().__init__("api_mock")
        self._handler = APIMockHandler()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute API mock action."""
        try:
            operation = params.get("operation", "match")

            if operation == "register":
                return self._register_endpoint(params)
            elif operation == "unregister":
                return self._unregister_endpoint(params)
            elif operation == "match":
                return self._match_request(params)
            elif operation == "generate":
                return self._generate_response(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Register a mock endpoint."""
        endpoint = MockEndpoint(
            path=params.get("path", "/"),
            method=params.get("method", "GET"),
            response_status=params.get("response_status", 200),
            response_body=params.get("response_body"),
            response_headers=params.get("response_headers", {}),
            response_type=MockResponseType(
                params.get("response_type", "static")
            ),
            delay_ms=params.get("delay_ms", 0),
            fault_type=params.get("fault_type"),
            fault_percentage=params.get("fault_percentage", 0.0),
            template_vars=params.get("template_vars", [])
        )

        self._handler.register_endpoint(endpoint)
        return ActionResult(
            success=True,
            message=f"Endpoint registered: {endpoint.method} {endpoint.path}"
        )

    def _unregister_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Unregister a mock endpoint."""
        path = params.get("path", "/")
        method = params.get("method", "GET")

        success = self._handler.unregister_endpoint(path, method)
        return ActionResult(
            success=success,
            message=f"Endpoint unregistered: {method} {path}"
        )

    def _match_request(self, params: Dict[str, Any]) -> ActionResult:
        """Match and return mock response."""
        path = params.get("path", "/")
        method = params.get("method", "GET")

        endpoint = self._handler.match_endpoint(path, method)
        if not endpoint:
            return ActionResult(
                success=False,
                message=f"No mock found for: {method} {path}"
            )

        context = params.get("context", {})
        response = self._handler.generate_response(endpoint, context)

        return ActionResult(
            success=True,
            data={
                "status_code": response.status_code,
                "headers": response.headers,
                "body": response.body,
                "delay_ms": response.delay_ms
            }
        )

    def _generate_response(self, params: Dict[str, Any]) -> ActionResult:
        """Generate a mock response directly."""
        endpoint = MockEndpoint(
            path=params.get("path", "/"),
            response_status=params.get("response_status", 200),
            response_body=params.get("response_body"),
            response_headers=params.get("response_headers", {}),
            response_type=MockResponseType(params.get("response_type", "static")),
            delay_ms=params.get("delay_ms", 0)
        )

        context = params.get("context", {})
        response = self._handler.generate_response(endpoint, context)

        return ActionResult(
            success=True,
            data={
                "status_code": response.status_code,
                "headers": response.headers,
                "body": response.body,
                "delay_ms": response.delay_ms
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get request history."""
        history = self._handler.get_request_history(
            path=params.get("path"),
            method=params.get("method"),
            limit=params.get("limit", 100)
        )

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "method": r.method,
                        "path": r.path,
                        "timestamp": r.timestamp
                    }
                    for r in history
                ]
            }
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get mock statistics."""
        return ActionResult(success=True, data=self._handler.get_stats())

    def _clear_history(self, params: Dict[str, Any]) -> ActionResult:
        """Clear request history."""
        self._handler.clear_history()
        return ActionResult(success=True, message="History cleared")
