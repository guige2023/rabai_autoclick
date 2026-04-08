"""API mocking action module for RabAI AutoClick.

Provides API mocking operations:
- MockServerAction: Create a mock HTTP server
- MockResponseAction: Generate mock HTTP responses
- MockScenarioAction: Define and run mock scenarios
- ApiContractVerifyAction: Verify API contracts
"""

import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urlparse, parse_qs

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MockResponse:
    """Mock HTTP response definition."""

    def __init__(self, status: int = 200, headers: Optional[Dict] = None, body: Any = None, delay_ms: int = 0):
        self.status = status
        self.headers = headers or {"Content-Type": "application/json"}
        self.body = body
        self.delay_ms = delay_ms

    def to_handler(self):
        return {"status": self.status, "headers": self.headers, "body": self.body, "delay_ms": self.delay_ms}


class MockRoute:
    """Mock route definition."""

    def __init__(self, method: str, path: str, response: MockResponse, params: Optional[Dict] = None):
        self.method = method.upper()
        self.path = path
        self.response = response
        self.params = params or {}

    def matches(self, method: str, path: str) -> bool:
        return self.method == method.upper() and self.path == path


class MockServerAction(BaseAction):
    """Create and manage a mock HTTP server."""
    action_type = "mock_server"
    display_name = "Mock服务器"
    description = "创建Mock HTTP服务器"

    def __init__(self):
        super().__init__()
        self._server = None
        self._thread = None
        self._routes: List[MockRoute] = []
        self._default_response = MockResponse(404, body={"error": "Not found"})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "add_route")
            port = params.get("port", 8080)
            host = params.get("host", "localhost")

            if action == "add_route":
                method = params.get("method", "GET")
                path = params.get("path", "/")
                status = params.get("status", 200)
                headers = params.get("headers", {"Content-Type": "application/json"})
                body = params.get("body", {})
                delay_ms = params.get("delay_ms", 0)

                route = MockRoute(method, path, MockResponse(status, headers, body, delay_ms))
                self._routes.append(route)
                return ActionResult(
                    success=True,
                    message=f"Added route {method} {path}",
                    data={"route_count": len(self._routes)},
                )

            elif action == "start":
                if self._server:
                    return ActionResult(success=False, message="Server already running")

                class MockHandler(BaseHTTPRequestHandler):
                    routes = self._routes
                    default_response = self._default_response

                    def do_GET(self):
                        self._handle_request("GET")

                    def do_POST(self):
                        self._handle_request("POST")

                    def do_PUT(self):
                        self._handle_request("PUT")

                    def do_DELETE(self):
                        self._handle_request("DELETE")

                    def do_PATCH(self):
                        self._handle_request("PATCH")

                    def _handle_request(self, method: str):
                        matched = None
                        for route in self.routes:
                            if route.matches(method, self.path):
                                matched = route
                                break
                        if not matched:
                            matched_route = self.default_response
                        else:
                            matched_route = matched.response

                        if matched_route.delay_ms > 0:
                            time.sleep(matched_route.delay_ms / 1000)

                        self.send_response(matched_route.status)
                        for key, value in matched_route.headers.items():
                            self.send_header(key, value)
                        self.end_headers()

                        body = matched_route.body
                        if isinstance(body, dict):
                            body = json.dumps(body).encode("utf-8")
                        elif isinstance(body, str):
                            body = body.encode("utf-8")
                        if body:
                            self.wfile.write(body)

                    def log_message(self, format, *args):
                        pass

                self._server = HTTPServer((host, port), MockHandler)
                self._thread = Thread(target=self._server.serve_forever, daemon=True)
                self._thread.start()
                return ActionResult(
                    success=True,
                    message=f"Mock server started on {host}:{port}",
                    data={"host": host, "port": port, "routes": len(self._routes)},
                )

            elif action == "stop":
                if self._server:
                    self._server.shutdown()
                    self._server = None
                    self._thread = None
                return ActionResult(success=True, message="Mock server stopped")

            elif action == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._routes)} routes configured",
                    data={"routes": [{"method": r.method, "path": r.path} for r in self._routes]},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"MockServer error: {e}")


class MockResponseAction(BaseAction):
    """Generate mock HTTP responses."""
    action_type = "mock_response"
    display_name = "Mock响应生成"
    description = "生成Mock HTTP响应数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", "default")
            status = params.get("status", 200)
            count = params.get("count", 1)
            fields = params.get("fields", {})

            if template == "user":
                responses = []
                for i in range(count):
                    responses.append({
                        "id": fields.get("id_start", 1) + i,
                        "name": f"User {fields.get('id_start', 1) + i}",
                        "email": f"user{fields.get('id_start', 1) + i}@example.com",
                        "active": True,
                    })
            elif template == "product":
                responses = []
                for i in range(count):
                    responses.append({
                        "id": fields.get("id_start", 1) + i,
                        "name": f"Product {fields.get('id_start', 1) + i}",
                        "price": round(10.0 + (fields.get('id_start', 1) + i) * 1.5, 2),
                        "in_stock": True,
                    })
            elif template == "order":
                responses = []
                for i in range(count):
                    responses.append({
                        "order_id": f"ORD-{fields.get('prefix', '1000')}{fields.get('id_start', 1) + i}",
                        "total": round(50.0 + (fields.get('id_start', 1) + i) * 10.0, 2),
                        "status": "pending",
                    })
            elif template == "paginated":
                page = fields.get("page", 1)
                page_size = fields.get("page_size", 10)
                total = fields.get("total", 100)
                start = (page - 1) * page_size
                items = [{"id": start + i, "data": f"item_{start + i}"} for i in range(min(page_size, total - start))]
                responses = {
                    "data": items,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total": total,
                        "total_pages": (total + page_size - 1) // page_size,
                    },
                }
            else:
                responses = [{"id": i + 1, "data": f"item_{i + 1}"} for i in range(count)]

            return ActionResult(
                success=True,
                message=f"Generated {count if count > 1 or template != 'paginated' else 1} mock response(s)",
                data={"response": responses, "status": status},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MockResponse error: {e}")


class MockScenarioAction(BaseAction):
    """Define and run mock API scenarios."""
    action_type = "mock_scenario"
    display_name = "Mock场景"
    description = "定义和运行Mock API场景"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            scenario = params.get("scenario", "happy_path")
            data = params.get("data", {})

            scenarios = {
                "happy_path": {
                    "name": "Happy Path",
                    "steps": [
                        {"action": "GET", "path": "/api/users", "response": {"users": [{"id": 1, "name": "Alice"}]}},
                        {"action": "POST", "path": "/api/users", "response": {"id": 2, "name": "Bob"}},
                        {"action": "GET", "path": "/api/users/2", "response": {"id": 2, "name": "Bob"}},
                    ],
                },
                "not_found": {
                    "name": "Not Found",
                    "steps": [
                        {"action": "GET", "path": "/api/users/999", "response": {"error": "Not found"}, "status": 404},
                    ],
                },
                "validation_error": {
                    "name": "Validation Error",
                    "steps": [
                        {"action": "POST", "path": "/api/users", "response": {"error": "Validation failed", "details": ["name required"]}, "status": 400},
                    ],
                },
                "unauthorized": {
                    "name": "Unauthorized",
                    "steps": [
                        {"action": "GET", "path": "/api/admin", "response": {"error": "Unauthorized"}, "status": 401},
                    ],
                },
            }

            if isinstance(scenario, str):
                if scenario not in scenarios:
                    return ActionResult(success=False, message=f"Unknown scenario: {scenario}")
                selected = scenarios[scenario]
            else:
                selected = scenario

            name = selected.get("name", "Custom")
            steps = selected.get("steps", [])

            results = []
            for step in steps:
                results.append({
                    "action": step.get("action"),
                    "path": step.get("path"),
                    "status": step.get("status", 200),
                    "response": step.get("response"),
                })

            return ActionResult(
                success=True,
                message=f"Scenario '{name}' executed: {len(steps)} steps",
                data={"scenario_name": name, "steps": results, "step_count": len(steps)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MockScenario error: {e}")


class ApiContractVerifyAction(BaseAction):
    """Verify API contracts against responses."""
    action_type = "api_contract_verify"
    display_name = "API契约验证"
    description = "验证API响应是否符合契约规范"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            contract = params.get("contract", {})
            strict = params.get("strict", False)

            if not response:
                return ActionResult(success=False, message="response is required")
            if not contract:
                return ActionResult(success=False, message="contract is required")

            violations = []

            required_fields = contract.get("required_fields", [])
            for field in required_fields:
                if field not in response:
                    violations.append({"type": "missing_required", "field": field})

            field_types = contract.get("field_types", {})
            for field, expected_type in field_types.items():
                if field in response:
                    value = response[field]
                    actual_type = type(value).__name__
                    if expected_type == "integer" and not isinstance(value, int):
                        violations.append({"type": "type_mismatch", "field": field, "expected": expected_type, "actual": actual_type})
                    elif expected_type == "string" and not isinstance(value, str):
                        violations.append({"type": "type_mismatch", "field": field, "expected": expected_type, "actual": actual_type})
                    elif expected_type == "boolean" and not isinstance(value, bool):
                        violations.append({"type": "type_mismatch", "field": field, "expected": expected_type, "actual": actual_type})
                    elif expected_type == "array" and not isinstance(value, list):
                        violations.append({"type": "type_mismatch", "field": field, "expected": expected_type, "actual": actual_type})
                    elif expected_type == "object" and not isinstance(value, dict):
                        violations.append({"type": "type_mismatch", "field": field, "expected": expected_type, "actual": actual_type})

            enum_fields = contract.get("enum_fields", {})
            for field, allowed_values in enum_fields.items():
                if field in response and response[field] not in allowed_values:
                    violations.append({"type": "enum_violation", "field": field, "allowed": allowed_values, "actual": response[field]})

            if strict:
                extra_fields = set(response.keys()) - set(required_fields) - set(field_types.keys())
                for field in extra_fields:
                    if field not in contract.get("optional_fields", []):
                        violations.append({"type": "extra_field", "field": field})

            return ActionResult(
                success=len(violations) == 0,
                message=f"Contract verification: {'PASSED' if not violations else f'FAILED ({len(violations)} violations)'}",
                data={"passed": len(violations) == 0, "violations": violations, "violation_count": len(violations)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ApiContractVerify error: {e}")
