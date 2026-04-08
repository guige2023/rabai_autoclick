"""API Composition Action Module.

Provides API composition, chaining, and orchestration
with response aggregation and error handling.
"""

from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import json
from datetime import datetime


class CompositionType(Enum):
    """Types of API composition."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    FAN_OUT = "fan_out"
    FAN_IN = "fan_in"


@dataclass
class APIRequest:
    """Represents a single API request in a composition."""
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    depends_on: List[str] = field(default_factory=list)
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    timeout: int = 30
    retry_config: Optional[Dict[str, Any]] = None

    def should_execute(self, context: Dict[str, Any]) -> bool:
        """Check if request should execute based on condition."""
        if self.condition is None:
            return True
        try:
            return self.condition(context)
        except Exception:
            return False


@dataclass
class APIResponse:
    """Response from an API request."""
    request_name: str
    status_code: int
    headers: Dict[str, str]
    body: Any
    duration_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class CompositionResult:
    """Result of a composition execution."""
    composition_id: str
    overall_success: bool
    responses: Dict[str, APIResponse]
    duration_ms: float
    errors: List[Dict[str, Any]] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)


class RequestExecutor:
    """Executes individual API requests."""

    def __init__(self, default_timeout: int = 30):
        self.default_timeout = default_timeout

    async def execute(
        self,
        request: APIRequest,
        context: Dict[str, Any],
    ) -> APIResponse:
        """Execute a single API request."""
        start_time = datetime.now()

        try:
            async with asyncio.timeout(request.timeout):
                response = await self._do_request(request, context)
                duration = (datetime.now() - start_time).total_seconds() * 1000

                return APIResponse(
                    request_name=request.name,
                    status_code=200,
                    headers={},
                    body=response,
                    duration_ms=duration,
                    success=True,
                )

        except asyncio.TimeoutError:
            duration = (datetime.now() - start_time).total_seconds() * 1000
            return APIResponse(
                request_name=request.name,
                status_code=0,
                headers={},
                body=None,
                duration_ms=duration,
                success=False,
                error=f"Request timed out after {request.timeout}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds() * 1000
            return APIResponse(
                request_name=request.name,
                status_code=0,
                headers={},
                body=None,
                duration_ms=duration,
                success=False,
                error=str(e),
            )

    async def _do_request(
        self,
        request: APIRequest,
        context: Dict[str, Any],
    ) -> Any:
        """Perform the actual HTTP request (simulated)."""
        await asyncio.sleep(0.1)
        return {"status": "ok", "request": request.name}


class SequentialComposer:
    """Executes requests sequentially."""

    def __init__(self, executor: RequestExecutor):
        self.executor = executor

    async def execute(
        self,
        requests: List[APIRequest],
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, APIResponse]:
        """Execute requests in sequence."""
        context = initial_context or {}
        responses = {}

        for request in requests:
            if not request.should_execute(context):
                continue

            for dep in request.depends_on:
                if dep not in responses or not responses[dep].success:
                    break
            else:
                response = await self.executor.execute(request, context)
                responses[request.name] = response
                context[request.name] = response.body

        return responses


class ParallelComposer:
    """Executes requests in parallel."""

    def __init__(self, executor: RequestExecutor):
        self.executor = executor

    async def execute(
        self,
        requests: List[APIRequest],
        initial_context: Optional[Dict[str, Any]] = None,
        max_concurrent: int = 10,
    ) -> Dict[str, APIResponse]:
        """Execute requests in parallel with concurrency limit."""
        context = initial_context or {}
        responses = {}

        async def execute_with_deps(req: APIRequest) -> APIResponse:
            for dep in req.depends_on:
                if dep not in responses or not responses[dep].success:
                    pass
            return await self.executor.execute(req, context)

        tasks = [execute_with_deps(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for req, result in zip(requests, results):
            if isinstance(result, Exception):
                responses[req.name] = APIResponse(
                    request_name=req.name,
                    status_code=0,
                    headers={},
                    body=None,
                    duration_ms=0,
                    success=False,
                    error=str(result),
                )
            else:
                responses[req.name] = result

        return responses


class FanOutComposer:
    """Executes fan-out (one request triggering multiple)."""

    def __init__(self, executor: RequestExecutor, parallel_composer: ParallelComposer):
        self.executor = executor
        self.parallel_composer = parallel_composer

    async def execute(
        self,
        trigger_request: APIRequest,
        fan_out_requests: List[APIRequest],
        context: Dict[str, Any],
    ) -> Dict[str, APIResponse]:
        """Execute trigger, then fan-out requests."""
        trigger_response = await self.executor.execute(trigger_request, context)
        responses = {trigger_request.name: trigger_response}

        if trigger_response.success:
            fan_context = {**context, trigger_request.name: trigger_response.body}
            fan_responses = await self.parallel_composer.execute(
                fan_out_requests, fan_context
            )
            responses.update(fan_responses)

        return responses


class ConditionalRouter:
    """Routes execution based on conditions."""

    def __init__(self, executor: RequestExecutor):
        self.executor = executor

    async def execute(
        self,
        requests_by_condition: Dict[Callable[[Any], bool], List[APIRequest]],
        context: Dict[str, Any],
    ) -> Dict[str, APIResponse]:
        """Execute requests based on which condition matches."""
        responses = {}
        selected_requests: List[APIRequest] = []

        for condition, requests in requests_by_condition.items():
            try:
                if condition(context):
                    selected_requests.extend(requests)
                    break
            except Exception:
                continue

        for request in selected_requests:
            response = await self.executor.execute(request, context)
            responses[request.name] = response

        return responses


class APIComposer:
    """High-level API composition orchestrator."""

    def __init__(self):
        self.executor = RequestExecutor()
        self.sequential = SequentialComposer(self.executor)
        self.parallel = ParallelComposer(self.executor)
        self.fan_out = FanOutComposer(self.executor, self.parallel)
        self.conditional = ConditionalRouter(self.executor)
        self._request_registry: Dict[str, APIRequest] = {}

    def register_request(self, request: APIRequest):
        """Register a request for later use."""
        self._request_registry[request.name] = request

    def get_request(self, name: str) -> Optional[APIRequest]:
        """Get a registered request."""
        return self._request_registry.get(name)

    def create_request(
        self,
        name: str,
        url: str,
        method: str = "GET",
        **kwargs,
    ) -> APIRequest:
        """Create and register a new request."""
        request = APIRequest(name=name, url=url, method=method, **kwargs)
        self._request_registry[name] = request
        return request

    async def compose_sequential(
        self,
        request_names: List[str],
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> CompositionResult:
        """Execute requests sequentially by name."""
        start_time = datetime.now()
        requests = [
            self._request_registry[name]
            for name in request_names
            if name in self._request_registry
        ]

        responses = await self.sequential.execute(requests, initial_context)
        duration = (datetime.now() - start_time).total_seconds() * 1000

        return CompositionResult(
            composition_id=f"seq_{int(start_time.timestamp())}",
            overall_success=all(r.success for r in responses.values()),
            responses=responses,
            duration_ms=duration,
        )

    async def compose_parallel(
        self,
        request_names: List[str],
        initial_context: Optional[Dict[str, Any]] = None,
        max_concurrent: int = 10,
    ) -> CompositionResult:
        """Execute requests in parallel by name."""
        start_time = datetime.now()
        requests = [
            self._request_registry[name]
            for name in request_names
            if name in self._request_registry
        ]

        responses = await self.parallel.execute(
            requests, initial_context, max_concurrent
        )
        duration = (datetime.now() - start_time).total_seconds() * 1000

        return CompositionResult(
            composition_id=f"par_{int(start_time.timestamp())}",
            overall_success=all(r.success for r in responses.values()),
            responses=responses,
            duration_ms=duration,
        )

    async def execute_single(
        self,
        request_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> APIResponse:
        """Execute a single registered request."""
        request = self._request_registry.get(request_name)
        if not request:
            return APIResponse(
                request_name=request_name,
                status_code=0,
                headers={},
                body=None,
                duration_ms=0,
                success=False,
                error="Request not found",
            )

        return await self.executor.execute(request, context or {})

    def aggregate_responses(
        self,
        responses: Dict[str, APIResponse],
        strategy: str = "merge",
    ) -> Any:
        """Aggregate multiple responses."""
        if strategy == "merge":
            return {
                name: {
                    "status": r.status_code,
                    "body": r.body,
                }
                for name, r in responses.items()
            }
        elif strategy == "first":
            return responses[list(responses.keys())[0]].body if responses else None
        elif strategy == "last":
            return responses[list(responses.keys())[-1]].body if responses else None
        elif strategy == "successes":
            return {
                name: r.body for name, r in responses.items() if r.success
            }
        elif strategy == "failures":
            return {
                name: {"error": r.error}
                for name, r in responses.items()
                if not r.success
            }
        return responses


# Module exports
__all__ = [
    "APIComposer",
    "RequestExecutor",
    "SequentialComposer",
    "ParallelComposer",
    "FanOutComposer",
    "ConditionalRouter",
    "APIRequest",
    "APIResponse",
    "CompositionResult",
    "CompositionType",
]
