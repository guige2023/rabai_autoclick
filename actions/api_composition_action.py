"""
API Composition Action - Composes multiple API calls into workflows.

This module provides API composition capabilities for chaining,
branching, and merging API requests into complex workflows.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from collections import defaultdict


class CompositionType(Enum):
    """Type of API composition."""
    SEQUENCE = "sequence"
    PARALLEL = "parallel"
    BRANCH = "branch"
    MERGE = "merge"
    LOOP = "loop"
    MAP = "map"


@dataclass
class API CallSpec:
    """Specification for a single API call."""
    call_id: str
    name: str
    method: str = "GET"
    url: str | None = None
    url_template: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    timeout: float = 30.0
    depends_on: list[str] = field(default_factory=list)
    transform: Callable[[Any], Any] | None = None


@dataclass
class CompositionNode:
    """A node in the composition graph."""
    node_id: str
    name: str
    composition_type: CompositionType
    calls: list[APICallSpec] = field(default_factory=list)
    condition: Callable[[Any], bool] | None = None
    max_iterations: int = 1
    children: list[str] = field(default_factory=list)


@dataclass
class CompositionResult:
    """Result of a composition execution."""
    success: bool
    results: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    duration_ms: float = 0.0
    calls_made: int = 0


class APIComposer:
    """
    Composes multiple API calls into workflows.
    
    Example:
        composer = APIComposer()
        composer.add_call("get_user", "GET", "/users/{user_id}")
        composer.add_call("get_orders", "GET", "/users/{user_id}/orders")
        result = await composer.execute({"user_id": "123"})
    """
    
    def __init__(self) -> None:
        self._calls: dict[str, APICallSpec] = {}
        self._nodes: dict[str, CompositionNode] = {}
        self._execution_graph: dict[str, list[str]] = defaultdict(list)
    
    def add_call(
        self,
        call_id: str,
        method: str,
        url: str,
        depends_on: list[str] | None = None,
        transform: Callable[[Any], Any] | None = None,
        **kwargs,
    ) -> None:
        """Add an API call to the composition."""
        call = APICallSpec(
            call_id=call_id,
            name=call_id,
            method=method,
            url=url,
            depends_on=depends_on or [],
            transform=transform,
            **kwargs,
        )
        self._calls[call_id] = call
    
    def add_node(
        self,
        node_id: str,
        name: str,
        composition_type: CompositionType,
        calls: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Add a composition node."""
        call_specs = [self._calls[c] for c in (calls or []) if c in self._calls]
        node = CompositionNode(
            node_id=node_id,
            name=name,
            composition_type=composition_type,
            calls=call_specs,
            **kwargs,
        )
        self._nodes[node_id] = node
    
    async def execute(
        self,
        context: dict[str, Any],
        start_nodes: list[str] | None = None,
    ) -> CompositionResult:
        """Execute the composition."""
        start_time = time.time()
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        calls_made = 0
        
        pending_calls = set(self._calls.keys())
        executed: set[str] = set()
        
        while pending_calls:
            ready = [
                cid for cid in pending_calls
                if all(dep in executed for dep in self._calls[cid].depends_on)
            ]
            
            if not ready:
                break
            
            for call_id in ready:
                call = self._calls[call_id]
                resolved_url = self._resolve_url(call, context, results)
                
                try:
                    response = await self._execute_call(call, resolved_url, context)
                    if call.transform:
                        results[call_id] = call.transform(response)
                    else:
                        results[call_id] = response
                    executed.add(call_id)
                    calls_made += 1
                except Exception as e:
                    errors[call_id] = str(e)
                    executed.add(call_id)
                
                pending_calls.discard(call_id)
        
        return CompositionResult(
            success=len(errors) == 0,
            results=results,
            errors=errors,
            duration_ms=(time.time() - start_time) * 1000,
            calls_made=calls_made,
        )
    
    async def _execute_call(
        self,
        call: APICallSpec,
        url: str,
        context: dict[str, Any],
    ) -> Any:
        """Execute a single API call."""
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=call.timeout)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            resolved_params = self._resolve_params(call.params, context)
            resolved_headers = self._resolve_params(call.headers, context)
            resolved_body = self._resolve_params(call.body or {}, context)
            
            async with session.request(
                call.method,
                url,
                headers=resolved_headers,
                params=resolved_params,
                json=resolved_body if call.body else None,
            ) as response:
                if response.content_type and "json" in response.content_type:
                    return await response.json()
                return await response.text()
    
    def _resolve_url(self, call: APICallSpec, context: dict[str, Any], results: dict[str, Any]) -> str:
        """Resolve URL template with context."""
        url = call.url or call.url_template or ""
        
        for key, value in {**context, **results}.items():
            url = url.replace(f"{{{key}}}", str(value))
        
        return url
    
    def _resolve_params(
        self,
        params: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve parameters with context."""
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str):
                for k, v in {**context}.items():
                    value = value.replace(f"{{{k}}}", str(v))
            resolved[key] = value
        return resolved


class APISequenceComposer:
    """Composes API calls in sequence with context passing."""
    
    def __init__(self) -> None:
        self._steps: list[tuple[str, Callable[[dict[str, Any]], Any]]] = []
    
    def add_step(
        self,
        name: str,
        handler: Callable[[dict[str, Any]], Any],
    ) -> APISequenceComposer:
        """Add a sequential step."""
        self._steps.append((name, handler))
        return self
    
    async def execute(
        self,
        initial_context: dict[str, Any],
    ) -> CompositionResult:
        """Execute steps sequentially."""
        start_time = time.time()
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        context = initial_context.copy()
        
        for name, handler in self._steps:
            try:
                result = handler(context)
                if asyncio.iscoroutine(result):
                    result = await result
                results[name] = result
                if isinstance(result, dict):
                    context.update(result)
                elif isinstance(result, list):
                    context[f"{name}_result"] = result
            except Exception as e:
                errors[name] = str(e)
        
        return CompositionResult(
            success=len(errors) == 0,
            results=results,
            errors=errors,
            duration_ms=(time.time() - start_time) * 1000,
            calls_made=len(self._steps),
        )


class APICompositionAction:
    """
    API composition action for complex workflows.
    
    Example:
        action = APICompositionAction()
        
        action.add_call("step1", "GET", "/api/data")
        action.add_call("step2", "POST", "/api/process", depends_on=["step1"])
        
        result = await action.execute({"initial": "value"})
    """
    
    def __init__(self) -> None:
        self.composer = APIComposer()
        self.sequence_composer = APISequenceComposer()
    
    def add_call(
        self,
        call_id: str,
        method: str,
        url: str,
        depends_on: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Add an API call."""
        self.composer.add_call(call_id, method, url, depends_on, **kwargs)
    
    def add_sequence_step(
        self,
        name: str,
        handler: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Add a sequence step."""
        self.sequence_composer.add_step(name, handler)
    
    async def execute(
        self,
        context: dict[str, Any],
        use_sequence: bool = False,
    ) -> CompositionResult:
        """Execute composition."""
        if use_sequence:
            return await self.sequence_composer.execute(context)
        return await self.composer.execute(context)


# Export public API
__all__ = [
    "CompositionType",
    "APICallSpec",
    "CompositionNode",
    "CompositionResult",
    "APIComposer",
    "APISequenceComposer",
    "APICompositionAction",
]
