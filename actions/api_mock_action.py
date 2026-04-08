"""API Mock Action Module.

Creates mock API endpoints and responses for testing
and development environments.
"""

from __future__ import annotations

import sys
import os
import time
import json
import re
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MockEndpoint:
    """A mock endpoint definition."""
    path_pattern: str
    method: str
    response_status: int
    response_body: Any
    response_headers: Dict[str, str] = field(default_factory=dict)
    delay_ms: int = 0
    enabled: bool = True


class APIMockAction(BaseAction):
    """
    API mocking for testing and development.

    Creates mock endpoints with custom responses,
    delays, and dynamic response generation.

    Example:
        mock = APIMockAction()
        result = mock.execute(ctx, {"action": "add_endpoint", "path": "/api/users", "response_body": {"users": []}})
    """
    action_type = "api_mock"
    display_name = "API模拟"
    description = "API模拟：测试和开发环境"

    def __init__(self) -> None:
        super().__init__()
        self._endpoints: List[MockEndpoint] = []
        self._response_sequences: Dict[str, List[Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "add_endpoint":
                return self._add_endpoint(params)
            elif action == "remove_endpoint":
                return self._remove_endpoint(params)
            elif action == "handle_request":
                return self._handle_request(params)
            elif action == "list_endpoints":
                return self._list_endpoints(params)
            elif action == "reset_sequences":
                return self._reset_sequences(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Mock error: {str(e)}")

    def _add_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        status = params.get("response_status", 200)
        body = params.get("response_body", {})
        headers = params.get("response_headers", {})
        delay = params.get("delay_ms", 0)

        if not path:
            return ActionResult(success=False, message="path is required")

        endpoint = MockEndpoint(path_pattern=path, method=method.upper(), response_status=status, response_body=body, response_headers=headers, delay_ms=delay)
        self._endpoints.append(endpoint)

        return ActionResult(success=True, message=f"Mock endpoint added: {method} {path}")

    def _remove_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET").upper()

        self._endpoints = [e for e in self._endpoints if not (e.path_pattern == path and e.method == method)]

        return ActionResult(success=True, message=f"Mock endpoint removed: {method} {path}")

    def _handle_request(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "/")
        method = params.get("method", "GET").upper()

        endpoint = self._find_endpoint(path, method)

        if not endpoint:
            return ActionResult(success=False, message=f"No mock for {method} {path}")

        if not endpoint.enabled:
            return ActionResult(success=False, message=f"Mock endpoint disabled: {method} {path}")

        if endpoint.delay_ms > 0:
            time.sleep(endpoint.delay_ms / 1000.0)

        response_body = endpoint.response_body
        if isinstance(response_body, str) and response_body.startswith("$sequence:"):
            seq_name = response_body.split(":", 1)[1]
            response_body = self._get_sequence_item(seq_name)

        return ActionResult(success=True, message=f"Mock response: {endpoint.response_status}", data={"status": endpoint.response_status, "body": response_body, "headers": endpoint.response_headers})

    def _list_endpoints(self, params: Dict[str, Any]) -> ActionResult:
        return ActionResult(success=True, data={"endpoints": [{"path": e.path_pattern, "method": e.method, "status": e.response_status, "enabled": e.enabled} for e in self._endpoints], "count": len(self._endpoints)})

    def _reset_sequences(self, params: Dict[str, Any]) -> ActionResult:
        self._response_sequences.clear()
        return ActionResult(success=True, message="Sequences reset")

    def _find_endpoint(self, path: str, method: str) -> Optional[MockEndpoint]:
        for endpoint in self._endpoints:
            if endpoint.method != method:
                continue
            if self._path_matches(endpoint.path_pattern, path):
                return endpoint
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        if pattern == path:
            return True
        regex_pattern = pattern.replace("*", ".*").replace("{", "(?P<").replace("}", ">[^/]+)")
        try:
            return bool(re.match(f"^{regex_pattern}$", path))
        except re.error:
            return False

    def _get_sequence_item(self, seq_name: str) -> Any:
        if seq_name not in self._response_sequences:
            self._response_sequences[seq_name] = []
        seq = self._response_sequences[seq_name]
        if not seq:
            return {}
        return seq.pop(0)
