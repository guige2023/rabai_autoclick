"""API Mocking Action Module.

Provides API mocking and stubbing capabilities for testing including
response templating, dynamic responses, latency simulation, error
injection, and request/response recording and playback.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class MockResponseType(Enum):
    """Types of mock responses."""
    STATIC = "static"
    DYNAMIC = "dynamic"
    TEMPLATED = "templated"
    RANDOM = "random"
    SEQUENTIAL = "sequential"
    ERROR = "error"


@dataclass
class MockResponse:
    """A mock API response."""
    response_id: str
    response_type: MockResponseType = MockResponseType.STATIC
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    body: Any = None
    delay_ms: float = 0.0
    error_type: Optional[str] = None
    template: Optional[str] = None
    dynamic_func: Optional[Callable] = None


@dataclass
class MockEndpoint:
    """A mock API endpoint."""
    endpoint_id: str
    method: str
    path_pattern: str
    responses: List[MockResponse]
    current_response_index: int = 0
    hit_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecordedRequest:
    """A recorded API request."""
    timestamp: datetime
    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[Any]
    query_params: Dict[str, str]


@dataclass
class MockingConfig:
    """Configuration for API mocking."""
    default_status_code: int = 200
    default_delay_ms: float = 0.0
    simulate_latency: bool = False
    latency_range_ms: Tuple[float, float] = (10.0, 200.0)
    error_injection_rate: float = 0.0
    record_requests: bool = True
    passthrough_on_miss: bool = False


class ResponseGenerator:
    """Generate dynamic mock responses."""

    @staticmethod
    def generate_random(items: List[Any]) -> Any:
        """Return a random item from a list."""
        return random.choice(items) if items else None

    @staticmethod
    def generate_sequential(responses: List[MockResponse], index: int) -> Tuple[MockResponse, int]:
        """Return next sequential response."""
        next_index = (index + 1) % len(responses)
        return responses[index], next_index

    @staticmethod
    def generate_templated(
        template: str,
        context: Dict[str, Any]
    ) -> Any:
        """Generate response from template with context."""
        try:
            for key, value in context.items():
                template = template.replace(f"{{{key}}}", str(value))
            return json.loads(template)
        except json.JSONDecodeError:
            return template

    @staticmethod
    def generate_error(error_type: str, status_code: int) -> Tuple[int, Dict[str, Any]]:
        """Generate an error response."""
        error_body = {
            "error": error_type,
            "message": f"Mock error: {error_type}",
            "timestamp": datetime.now().isoformat()
        }
        return status_code, error_body


class ApiMockingAction(BaseAction):
    """Action for mocking API endpoints."""

    def __init__(self):
        super().__init__(name="api_mocking")
        self._config = MockingConfig()
        self._endpoints: Dict[str, MockEndpoint] = {}
        self._recordings: List[RecordedRequest] = []
        self._lock = threading.Lock()
        self._request_history: List[Dict[str, Any]] = []

    def configure(self, config: MockingConfig):
        """Configure mocking settings."""
        self._config = config

    def register_endpoint(
        self,
        endpoint_id: str,
        method: str,
        path_pattern: str,
        responses: List[MockResponse]
    ) -> ActionResult:
        """Register a mock endpoint."""
        try:
            with self._lock:
                if endpoint_id in self._endpoints:
                    return ActionResult(success=False, error=f"Endpoint {endpoint_id} already exists")

                endpoint = MockEndpoint(
                    endpoint_id=endpoint_id,
                    method=method.upper(),
                    path_pattern=path_pattern,
                    responses=responses
                )
                self._endpoints[endpoint_id] = endpoint
                return ActionResult(success=True, data={
                    "endpoint_id": endpoint_id,
                    "method": method,
                    "path": path_pattern
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_response(
        self,
        endpoint_id: str,
        response: MockResponse
    ) -> ActionResult:
        """Add a response variant to an endpoint."""
        try:
            with self._lock:
                if endpoint_id not in self._endpoints:
                    return ActionResult(success=False, error=f"Endpoint {endpoint_id} not found")

                self._endpoints[endpoint_id].responses.append(response)
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def mock_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        query_params: Optional[Dict[str, str]] = None
    ) -> ActionResult:
        """Execute a mock request and return mock response."""
        try:
            with self._lock:
                method = method.upper()
                matched_endpoint = None

                for endpoint in self._endpoints.values():
                    if endpoint.method == method and self._path_matches(path, endpoint.path_pattern):
                        matched_endpoint = endpoint
                        break

                if not matched_endpoint:
                    if self._config.passthrough_on_miss:
                        return ActionResult(
                            success=False,
                            error="No matching mock endpoint and passthrough is disabled"
                        )
                    return ActionResult(success=False, error="No matching mock endpoint found")

                matched_endpoint.hit_count += 1

                if self._config.record_requests:
                    recording = RecordedRequest(
                        timestamp=datetime.now(),
                        method=method,
                        path=path,
                        headers=headers or {},
                        body=body,
                        query_params=query_params or {}
                    )
                    self._recordings.append(recording)

                context = {
                    "method": method,
                    "path": path,
                    "headers": headers,
                    "body": body,
                    "timestamp": datetime.now().isoformat(),
                    "hit_count": matched_endpoint.hit_count
                }

                response = self._select_response(matched_endpoint, context)

                if response.delay_ms > 0 or self._config.simulate_latency:
                    delay = response.delay_ms
                    if self._config.simulate_latency:
                        delay = random.uniform(*self._config.latency_range_ms)
                    time.sleep(delay / 1000.0)

                if response.response_type == MockResponseType.ERROR:
                    status, error_body = ResponseGenerator.generate_error(
                        response.error_type or "unknown",
                        response.status_code
                    )
                    return ActionResult(
                        success=False,
                        data={
                            "status_code": status,
                            "headers": response.headers,
                            "body": error_body,
                            "error": True
                        }
                    )

                return ActionResult(
                    success=True,
                    data={
                        "status_code": response.status_code,
                        "headers": response.headers,
                        "body": response.body,
                        "endpoint_id": matched_endpoint.endpoint_id
                    }
                )
        except Exception as e:
            logger.exception("Mock request failed")
            return ActionResult(success=False, error=str(e))

    def _path_matches(self, path: str, pattern: str) -> bool:
        """Check if path matches the endpoint pattern."""
        import re
        pattern_regex = pattern.replace("{id}", r"[^/]+").replace("{*}", r".*")
        return bool(re.match(pattern_regex, path))

    def _select_response(
        self,
        endpoint: MockEndpoint,
        context: Dict[str, Any]
    ) -> MockResponse:
        """Select appropriate response based on response type."""
        if not endpoint.responses:
            return MockResponse(
                response_id="default",
                body={"message": "No response configured"}
            )

        response_type = endpoint.responses[0].response_type

        if response_type == MockResponseType.STATIC:
            return endpoint.responses[0]

        elif response_type == MockResponseType.RANDOM:
            return random.choice(endpoint.responses)

        elif response_type == MockResponseType.SEQUENTIAL:
            resp, next_idx = ResponseGenerator.generate_sequential(
                endpoint.responses,
                endpoint.current_response_index
            )
            endpoint.current_response_index = next_idx
            return resp

        elif response_type == MockResponseType.TEMPLATED:
            resp = endpoint.responses[0]
            if resp.template:
                body = ResponseGenerator.generate_templated(resp.template, context)
                resp.body = body
            return resp

        elif response_type == MockResponseType.DYNAMIC:
            resp = endpoint.responses[0]
            if resp.dynamic_func:
                resp.body = resp.dynamic_func(context)
            return resp

        return endpoint.responses[0]

    def inject_error(
        self,
        endpoint_id: str,
        error_type: str,
        probability: float = 1.0
    ) -> ActionResult:
        """Inject an error into an endpoint's responses."""
        try:
            with self._lock:
                if endpoint_id not in self._endpoints:
                    return ActionResult(success=False, error=f"Endpoint {endpoint_id} not found")

                error_response = MockResponse(
                    response_id=f"error_injection_{int(time.time())}",
                    response_type=MockResponseType.ERROR,
                    status_code=500,
                    error_type=error_type
                )
                self._endpoints[endpoint_id].responses.insert(0, error_response)
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_recordings(
        self,
        limit: Optional[int] = None
    ) -> ActionResult:
        """Get recorded requests."""
        try:
            recordings = self._recordings[-limit:] if limit else self._recordings
            return ActionResult(success=True, data={
                "count": len(recordings),
                "recordings": [
                    {
                        "timestamp": r.timestamp.isoformat(),
                        "method": r.method,
                        "path": r.path,
                        "headers": r.headers,
                        "body": r.body
                    }
                    for r in recordings
                ]
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def clear_recordings(self) -> ActionResult:
        """Clear recorded requests."""
        try:
            with self._lock:
                self._recordings.clear()
                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_stats(self) -> Dict[str, Any]:
        """Get mocking statistics."""
        with self._lock:
            return {
                "total_endpoints": len(self._endpoints),
                "total_recordings": len(self._recordings),
                "endpoints": {
                    ep_id: {
                        "hit_count": ep.hit_count,
                        "response_count": len(ep.responses),
                        "method": ep.method,
                        "path": ep.path_pattern
                    }
                    for ep_id, ep in self._endpoints.items()
                }
            }

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute mocking action."""
        try:
            action = params.get("action", "mock")

            if action == "mock":
                return self.mock_request(
                    params["method"],
                    params["path"],
                    params.get("headers"),
                    params.get("body"),
                    params.get("query_params")
                )
            elif action == "register":
                responses = [
                    MockResponse(
                        response_id=r.get("response_id", f"resp_{i}"),
                        response_type=MockResponseType(r.get("response_type", "static")),
                        status_code=r.get("status_code", 200),
                        body=r.get("body"),
                        delay_ms=r.get("delay_ms", 0.0)
                    )
                    for i, r in enumerate(params.get("responses", []))
                ]
                return self.register_endpoint(
                    params["endpoint_id"],
                    params["method"],
                    params["path_pattern"],
                    responses
                )
            elif action == "stats":
                return ActionResult(success=True, data=self.get_stats())
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
