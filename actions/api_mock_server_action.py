"""Mock API server action module for RabAI AutoClick.

Provides mock server operations:
- MockServerAction: Start/configure a mock API server
- MockEndpointAction: Define individual mock endpoints
- MockResponseAction: Configure mock responses with delay/variation
- MockScenarioAction: Define multi-request scenarios
- MockAuthProviderAction: Provide mock authentication
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MockResponseMode(Enum):
    """Mock response generation modes."""
    FIXED = auto()
    RANDOM = auto()
    SEQUENTIAL = auto()
    DYNAMIC = auto()


class MockEndpoint:
    """Represents a single mock endpoint."""

    def __init__(
        self,
        path: str,
        method: str,
        responses: List[Dict[str, Any]],
        mode: MockResponseMode = MockResponseMode.FIXED,
        delay_ms: int = 0,
    ) -> None:
        self.id = str(uuid.uuid4())
        self.path = path
        self.method = method.upper()
        self.responses = responses
        self.mode = mode
        self.delay_ms = delay_ms
        self._response_index = 0
        self._call_count = 0
        self._call_history: List[Dict[str, Any]] = []

    def get_response(self) -> Tuple[Dict[str, Any], int]:
        """Get the next response based on mode."""
        self._call_count += 1
        call_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "call_number": self._call_count,
        }
        self._call_history.append(call_record)

        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        if self.mode == MockResponseMode.SEQUENTIAL:
            response = self.responses[self._response_index % len(self.responses)]
            self._response_index += 1
        elif self.mode == MockResponseMode.RANDOM:
            import random
            response = random.choice(self.responses)
        else:
            response = self.responses[0] if self.responses else {"status": 500, "body": {"error": "No response configured"}}

        return response, self._call_count

    def matches(self, path: str, method: str) -> bool:
        """Check if this endpoint matches the request."""
        return self.path == path and self.method == method.upper()


class MockServerAction(BaseAction):
    """Mock API server management."""
    action_type = "mock_server"
    display_name = "Mock服务器"
    description = "配置和启动Mock API服务器"

    def __init__(self) -> None:
        super().__init__()
        self._endpoints: Dict[str, MockEndpoint] = {}
        self._scenarios: Dict[str, List[str]] = {}
        self._running = False
        self._server_config: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "status")
            if action == "start":
                return self._start_server(params)
            elif action == "stop":
                return self._stop_server()
            elif action == "add_endpoint":
                return self._add_endpoint(params)
            elif action == "remove_endpoint":
                return self._remove_endpoint(params)
            elif action == "status":
                return self._get_status()
            elif action == "scenario":
                return self._run_scenario(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Mock server failed: {e}")

    def _start_server(self, params: Dict[str, Any]) -> ActionResult:
        host = params.get("host", "127.0.0.1")
        port = params.get("port", 8080)
        self._server_config = {"host": host, "port": port, "started_at": datetime.now(timezone.utc).isoformat()}
        self._running = True
        return ActionResult(
            success=True,
            message=f"Mock server started on {host}:{port}",
            data={"host": host, "port": port, "endpoints": len(self._endpoints)},
        )

    def _stop_server(self) -> ActionResult:
        self._running = False
        return ActionResult(success=True, message="Mock server stopped")

    def _add_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        responses = params.get("responses", [{"status": 200, "body": {"message": "ok"}}])
        mode_str = params.get("mode", "FIXED")
        delay_ms = params.get("delay_ms", 0)

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            mode = MockResponseMode[mode_str.upper()]
        except KeyError:
            mode = MockResponseMode.FIXED

        endpoint = MockEndpoint(path, method, responses, mode, delay_ms)
        key = f"{method.upper()}:{path}"
        self._endpoints[key] = endpoint
        return ActionResult(
            success=True,
            message=f"Endpoint {method.upper()} {path} added",
            data={"endpoint_id": endpoint.id, "total_endpoints": len(self._endpoints)},
        )

    def _remove_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        key = f"{method.upper()}:{path}"
        if key in self._endpoints:
            del self._endpoints[key]
            return ActionResult(success=True, message=f"Endpoint removed: {key}")
        return ActionResult(success=False, message=f"Endpoint not found: {key}")

    def _get_status(self) -> ActionResult:
        return ActionResult(
            success=True,
            message=f"Mock server {'running' if self._running else 'stopped'}",
            data={
                "running": self._running,
                "config": self._server_config,
                "endpoint_count": len(self._endpoints),
                "scenario_count": len(self._scenarios),
            },
        )

    def _run_scenario(self, params: Dict[str, Any]) -> ActionResult:
        scenario_name = params.get("scenario_name", "")
        if not scenario_name:
            return ActionResult(success=False, message="scenario_name is required")
        if scenario_name not in self._scenarios:
            return ActionResult(success=False, message=f"Scenario not found: {scenario_name}")
        steps = self._scenarios[scenario_name]
        results = []
        for step in steps:
            results.append({"step": step, "executed": True, "timestamp": datetime.now(timezone.utc).isoformat()})
        return ActionResult(success=True, message=f"Scenario {scenario_name} executed", data={"results": results})

    def add_scenario(self, name: str, endpoint_keys: List[str]) -> None:
        """Register a multi-step scenario."""
        self._scenarios[name] = endpoint_keys


class MockEndpointAction(BaseAction):
    """Define individual mock endpoints with advanced matching."""
    action_type = "mock_endpoint"
    display_name = "Mock端点"
    description = "定义具有高级匹配条件的Mock端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            path_pattern = params.get("path_pattern", "")
            method = params.get("method", "GET")
            response = params.get("response", {"status": 200, "body": {"message": "ok"}})
            headers = params.get("headers", {})
            query_params = params.get("query_params", {})

            if not path_pattern:
                return ActionResult(success=False, message="path_pattern is required")

            endpoint = {
                "id": str(uuid.uuid4()),
                "path_pattern": path_pattern,
                "method": method.upper(),
                "response": response,
                "headers": headers,
                "query_params": query_params,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(success=True, message=f"Mock endpoint created: {method} {path_pattern}", data=endpoint)
        except Exception as e:
            return ActionResult(success=False, message=f"Mock endpoint creation failed: {e}")


class MockResponseAction(BaseAction):
    """Configure mock responses with dynamic variation."""
    action_type = "mock_response"
    display_name = "Mock响应"
    description = "配置带延迟和变体的Mock响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            responses = params.get("responses", [])
            mode = params.get("mode", "FIXED")
            delay_range = params.get("delay_range", [0, 0])
            variation = params.get("variation", {})

            if not responses:
                return ActionResult(success=False, message="responses are required")

            config = {
                "id": str(uuid.uuid4()),
                "responses": responses,
                "mode": mode,
                "delay_range": delay_range,
                "variation": variation,
            }

            return ActionResult(
                success=True,
                message=f"Mock response configured in {mode} mode",
                data=config,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Mock response configuration failed: {e}")


class MockScenarioAction(BaseAction):
    """Define multi-request scenarios for testing."""
    action_type = "mock_scenario"
    display_name = "Mock场景"
    description = "定义多请求测试场景"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            steps = params.get("steps", [])
            setup = params.get("setup", {})
            teardown = params.get("teardown", {})

            if not name:
                return ActionResult(success=False, message="name is required")
            if not steps:
                return ActionResult(success=False, message="steps are required")

            scenario = {
                "id": str(uuid.uuid4()),
                "name": name,
                "steps": steps,
                "setup": setup,
                "teardown": teardown,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(success=True, message=f"Scenario '{name}' created with {len(steps)} steps", data=scenario)
        except Exception as e:
            return ActionResult(success=False, message=f"Scenario creation failed: {e}")


class MockAuthProviderAction(BaseAction):
    """Provide mock authentication for testing."""
    action_type = "mock_auth_provider"
    display_name = "Mock认证提供者"
    description = "为测试提供Mock认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            auth_type = params.get("auth_type", "bearer")
            tokens = params.get("tokens", [])
            user_credentials = params.get("user_credentials", {})

            if auth_type == "bearer" and not tokens:
                return ActionResult(success=False, message="bearer auth requires tokens")

            provider = {
                "id": str(uuid.uuid4()),
                "auth_type": auth_type,
                "tokens": [{"token": t, "active": True} for t in tokens] if tokens else [],
                "user_credentials": user_credentials,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            def validate_token(token: str) -> bool:
                return any(t["token"] == token and t.get("active", False) for t in provider["tokens"])

            def issue_token(user_id: str) -> str:
                new_token = f"mock_token_{uuid.uuid4().hex[:16]}"
                provider["tokens"].append({"token": new_token, "active": True, "user_id": user_id})
                return new_token

            return ActionResult(
                success=True,
                message=f"Mock {auth_type} auth provider created",
                data={"provider": provider, "token_count": len(provider["tokens"])},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Mock auth provider failed: {e}")
