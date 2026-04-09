"""
API Shadow Mode Action Module

Enables shadow traffic testing where API requests are duplicated
to a shadow endpoint without affecting production traffic.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import logging

logger = logging.getLogger(__name__)


class ShadowStrategy(Enum):
    """Shadow traffic routing strategies."""

    MIRROR = auto()
    DUPLICATE = auto()
    SPLIT = auto()
    CANARY = auto()


class ShadowResult:
    """Result of shadow API call execution."""

    def __init__(
        self,
        primary_result: Any,
        shadow_triggered: bool,
        shadow_result: Optional[Any] = None,
        shadow_error: Optional[str] = None,
        latency_diff_ms: Optional[float] = None,
    ) -> None:
        self.primary_result = primary_result
        self.shadow_triggered = shadow_triggered
        self.shadow_result = shadow_result
        self.shadow_error = shadow_error
        self.latency_diff_ms = latency_diff_ms


@dataclass
class ShadowConfig:
    """Configuration for shadow mode testing."""

    shadow_endpoint: str
    strategy: ShadowStrategy = ShadowStrategy.MIRROR
    shadow_probability: float = 1.0
    capture_response: bool = True
    capture_errors: bool = True
    header_prefix: str = "X-Shadow-"
    track_latency: bool = True
    sampling_rate: float = 1.0
    tags: List[str] = field(default_factory=list)


@dataclass
class ShadowRequest:
    """Shadow request metadata."""

    request_id: str
    trace_id: str
    shadow_endpoint: str
    original_endpoint: str
    method: str
    headers: Dict[str, str]
    body: Optional[Any]
    timestamp: float = field(default_factory=time.time)
    strategy: ShadowStrategy = ShadowStrategy.MIRROR


@dataclass
class ShadowResponse:
    """Shadow response metadata."""

    request_id: str
    status_code: Optional[int] = None
    body: Optional[Any] = None
    headers: Dict[str, str] = field(default_factory=dict)
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class ShadowTrafficRouter:
    """Routes requests to shadow endpoints based on strategy."""

    def __init__(self, config: ShadowConfig) -> None:
        self.config = config
        self._response_log: List[ShadowResponse] = []

    def build_shadow_headers(self, request: ShadowRequest) -> Dict[str, str]:
        """Build headers for shadow request."""
        headers = copy.deepcopy(request.headers)
        headers[f"{self.config.header_prefix}Request-ID"] = request.request_id
        headers[f"{self.config.header_prefix}Trace-ID"] = request.trace_id
        headers[f"{self.config.header_prefix}Original-Endpoint"] = request.original_endpoint
        headers[f"{self.config.header_prefix}Strategy"] = request.strategy.name
        headers[f"{self.config.header_prefix}Timestamp"] = str(request.timestamp)
        return headers

    def create_shadow_request(self, original: Dict[str, Any]) -> ShadowRequest:
        """Create a shadow request from original request."""
        return ShadowRequest(
            request_id=str(uuid.uuid4()),
            trace_id=original.get("headers", {}).get("X-Trace-ID", str(uuid.uuid4())),
            shadow_endpoint=self.config.shadow_endpoint,
            original_endpoint=original.get("url", ""),
            method=original.get("method", "GET"),
            headers=self.build_shadow_headers(
                ShadowRequest(
                    request_id=str(uuid.uuid4()),
                    trace_id=str(uuid.uuid4()),
                    shadow_endpoint=self.config.shadow_endpoint,
                    original_endpoint=original.get("url", ""),
                    method=original.get("method", "GET"),
                    headers=original.get("headers", {}),
                    body=None,
                )
            ),
            body=original.get("body"),
            strategy=self.config.strategy,
        )

    async def send_shadow_request(
        self,
        request: ShadowRequest,
        http_client: Any,
    ) -> ShadowResponse:
        """Send request to shadow endpoint and capture response."""
        start = time.time()
        response = ShadowResponse(request_id=request.request_id)

        try:
            resp = await http_client.request(
                method=request.method,
                url=request.shadow_endpoint,
                headers=request.headers,
                json=request.body,
                timeout=30.0,
            )
            response.status_code = resp.status_code
            if self.config.capture_response:
                response.body = resp.json() if resp.content else None
            response.headers = dict(resp.headers)
            response.latency_ms = (time.time() - start) * 1000
        except Exception as e:
            response.error = str(e)
            response.latency_ms = (time.time() - start) * 1000

        self._response_log.append(response)
        return response

    def get_shadow_responses(self, request_id: str) -> List[ShadowResponse]:
        """Get all shadow responses for a request."""
        return [r for r in self._response_log if r.request_id == request_id]


class ShadowModeAction:
    """Action class for shadow traffic testing."""

    def __init__(
        self,
        config: Optional[ShadowConfig] = None,
        http_client: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.router = ShadowTrafficRouter(config or ShadowConfig(shadow_endpoint=""))
        self.http_client = http_client
        self._request_log: List[ShadowRequest] = []

    async def execute_async(
        self,
        primary_coro: Awaitable[Any],
        original_request: Dict[str, Any],
    ) -> ShadowResult:
        """Execute primary request with optional shadow traffic."""
        primary_result = None
        shadow_triggered = False
        shadow_result = None
        shadow_error = None
        latency_diff_ms = None

        should_shadow = (
            self.http_client is not None
            and self.config is not None
            and self.config.shadow_endpoint
            and (
                self.config.sampling_rate >= 1.0
                or random.random() < self.config.sampling_rate
            )
        )

        if should_shadow:
            shadow_request = self.router.create_shadow_request(original_request)
            self._request_log.append(shadow_request)

            shadow_response = await self.router.send_shadow_request(
                shadow_request,
                self.http_client,
            )
            shadow_triggered = True
            if shadow_response.error:
                shadow_error = shadow_response.error
            else:
                shadow_result = shadow_response.body
            latency_diff_ms = shadow_response.latency_ms

        try:
            primary_result = await primary_coro
        except Exception as e:
            if not shadow_triggered:
                raise
            shadow_error = f"Primary error (shadow still ran): {str(e)}"

        return ShadowResult(
            primary_result=primary_result,
            shadow_triggered=shadow_triggered,
            shadow_result=shadow_result,
            shadow_error=shadow_error,
            latency_diff_ms=latency_diff_ms,
        )

    def get_request_log(self) -> List[ShadowRequest]:
        """Return all shadow requests made."""
        return self._request_log.copy()
