"""API Context Action Module.

Manages request/response context lifecycle including propagation,
isolation, and cleanup for nested API calls.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class APIContext:
    """Represents a single API call context."""
    request_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    parent_id: Optional[str] = None
    url: str = ""
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Optional[Dict[str, Any]] = field(default_factory=dict)
    body: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    tags: Set[str] = field(default_factory=set)

    def elapsed_ms(self) -> float:
        """Return elapsed time since context creation."""
        return (time.time() - self.created_at) * 1000

    def with_parent(self, parent_id: str) -> "APIContext":
        """Create a child context with this context as parent."""
        child = copy.deepcopy(self)
        child.request_id = str(uuid.uuid4())[:8]
        child.parent_id = self.request_id
        child.created_at = time.time()
        return child

    def add_tag(self, tag: str) -> None:
        """Add a tag to this context."""
        self.tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        """Remove a tag from this context."""
        self.tags.discard(tag)


class ContextStack:
    """Manages a stack of API contexts for nested calls."""

    def __init__(self):
        self._stack: List[APIContext] = []
        self._lock = asyncio.Lock()

    async def push(self, context: APIContext) -> None:
        """Push a context onto the stack."""
        async with self._lock:
            self._stack.append(context)

    async def pop(self) -> Optional[APIContext]:
        """Pop and return the top context."""
        async with self._lock:
            if self._stack:
                return self._stack.pop()
            return None

    async def peek(self) -> Optional[APIContext]:
        """Return the top context without removing it."""
        async with self._lock:
            if self._stack:
                return self._stack[-1]
            return None

    async def depth(self) -> int:
        """Return current stack depth."""
        async with self._lock:
            return len(self._stack)

    async def get_all(self) -> List[APIContext]:
        """Return all contexts in the stack (bottom to top)."""
        async with self._lock:
            return copy.deepcopy(self._stack)


class ContextRegistry:
    """Registry for tracking all active API contexts."""

    def __init__(self):
        self._contexts: Dict[str, APIContext] = {}
        self._lock = asyncio.Lock()
        self._history: List[APIContext] = []
        self._max_history = 1000

    async def register(self, context: APIContext) -> None:
        """Register a new context."""
        async with self._lock:
            self._contexts[context.request_id] = context

    async def unregister(self, request_id: str) -> Optional[APIContext]:
        """Unregister and return a context."""
        async with self._lock:
            ctx = self._contexts.pop(request_id, None)
            if ctx:
                self._history.append(ctx)
                if len(self._history) > self._max_history:
                    self._history.pop(0)
            return ctx

    async def get(self, request_id: str) -> Optional[APIContext]:
        """Get a context by ID."""
        async with self._lock:
            ctx = self._contexts.get(request_id)
            return copy.deepcopy(ctx) if ctx else None

    async def update(self, request_id: str, **kwargs: Any) -> bool:
        """Update context fields."""
        async with self._lock:
            ctx = self._contexts.get(request_id)
            if not ctx:
                return False
            for key, value in kwargs.items():
                if hasattr(ctx, key):
                    setattr(ctx, key, value)
            return True

    async def list_by_tag(self, tag: str) -> List[APIContext]:
        """List all contexts with a specific tag."""
        async with self._lock:
            return [copy.deepcopy(c) for c in self._contexts.values() if tag in c.tags]

    async def list_by_parent(self, parent_id: str) -> List[APIContext]:
        """List all child contexts of a parent."""
        async with self._lock:
            return [
                copy.deepcopy(c) for c in self._contexts.values()
                if c.parent_id == parent_id
            ]

    async def count(self) -> int:
        """Return number of registered contexts."""
        async with self._lock:
            return len(self._contexts)

    async def get_history(self, limit: int = 100) -> List[APIContext]:
        """Return recent context history."""
        async with self._lock:
            return copy.deepcopy(self._history[-limit:])


class ContextPropagator:
    """Propagates context headers and metadata to child requests."""

    CONTEXT_HEADER = "X-Request-ID"
    PARENT_HEADER = "X-Parent-ID"
    TRACE_HEADER = "X-Trace-ID"

    def __init__(self, registry: ContextRegistry):
        self._registry = registry
        self._propagation_enabled = True

    def enable(self) -> None:
        """Enable context propagation."""
        self._propagation_enabled = True

    def disable(self) -> None:
        """Disable context propagation."""
        self._propagation_enabled = False

    def inject_headers(self, context: APIContext) -> Dict[str, str]:
        """Inject context information into request headers."""
        if not self._propagation_enabled:
            return {}

        headers = {
            self.CONTEXT_HEADER: context.request_id,
            self.TRACE_HEADER: context.request_id
        }
        if context.parent_id:
            headers[self.PARENT_HEADER] = context.parent_id

        for key, value in context.metadata.items():
            if isinstance(value, str):
                headers[f"X-Context-{key.title()}"] = value

        return headers

    def extract_from_headers(self, headers: Dict[str, str]) -> Dict[str, Optional[str]]:
        """Extract context information from response headers."""
        return {
            "request_id": headers.get(self.CONTEXT_HEADER),
            "parent_id": headers.get(self.PARENT_HEADER),
            "trace_id": headers.get(self.TRACE_HEADER)
        }

    async def create_child_context(
        self,
        parent: APIContext,
        url: str,
        method: str = "GET"
    ) -> APIContext:
        """Create a child context inheriting from parent."""
        child = parent.with_parent(parent.request_id)
        child.url = url
        child.method = method
        child.headers.update(self.inject_headers(child))
        await self._registry.register(child)
        return child


class APIContextAction:
    """Main action class for API context management."""

    def __init__(self):
        self._registry = ContextRegistry()
        self._stack = ContextStack()
        self._propagator = ContextPropagator(self._registry)

    @property
    def registry(self) -> ContextRegistry:
        """Return the context registry."""
        return self._registry

    @property
    def stack(self) -> ContextStack:
        """Return the context stack."""
        return self._stack

    @property
    def propagator(self) -> ContextPropagator:
        """Return the context propagator."""
        return self._propagator

    async def create_context(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> APIContext:
        """Create and register a new API context."""
        context = APIContext(
            url=url,
            method=method,
            headers=headers or {},
            params=params,
            body=body,
            metadata=metadata or {}
        )
        if tags:
            for tag in tags:
                context.add_tag(tag)

        # Inject propagation headers
        injected = self._propagator.inject_headers(context)
        context.headers.update(injected)

        await self._registry.register(context)
        await self._stack.push(context)

        return context

    async def complete_context(self, request_id: str) -> None:
        """Mark a context as complete and clean up."""
        ctx = await self._registry.unregister(request_id)
        if ctx:
            logger.debug(f"Context {request_id} completed in {ctx.elapsed_ms():.2f}ms")

    async def get_active_count(self) -> int:
        """Return number of active contexts."""
        return await self._registry.count()

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API context action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform (create, complete, get, propagate)
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "create")

        if operation == "create":
            result = await self.create_context(
                url=context.get("url", ""),
                method=context.get("method", "GET"),
                headers=context.get("headers"),
                params=context.get("params"),
                body=context.get("body"),
                tags=context.get("tags"),
                metadata=context.get("metadata")
            )
            return {
                "success": True,
                "context": {
                    "request_id": result.request_id,
                    "parent_id": result.parent_id,
                    "url": result.url,
                    "method": result.method,
                    "tags": list(result.tags),
                    "elapsed_ms": result.elapsed_ms()
                }
            }

        elif operation == "complete":
            request_id = context.get("request_id", "")
            await self.complete_context(request_id)
            return {"success": True, "request_id": request_id}

        elif operation == "get":
            request_id = context.get("request_id", "")
            ctx = await self._registry.get(request_id)
            if ctx:
                return {
                    "success": True,
                    "context": {
                        "request_id": ctx.request_id,
                        "parent_id": ctx.parent_id,
                        "url": ctx.url,
                        "method": ctx.method,
                        "tags": list(ctx.tags)
                    }
                }
            return {"success": False, "error": "Context not found"}

        elif operation == "propagate":
            parent_id = context.get("parent_id", "")
            parent = await self._registry.get(parent_id)
            if not parent:
                return {"success": False, "error": "Parent context not found"}

            child = await self._propagator.create_child_context(
                parent,
                context.get("url", ""),
                context.get("method", "GET")
            )
            return {
                "success": True,
                "child_context": {
                    "request_id": child.request_id,
                    "parent_id": child.parent_id
                }
            }

        elif operation == "stats":
            count = await self._registry.count()
            history = await self._registry.get_history(limit=10)
            return {
                "success": True,
                "active_count": count,
                "recent_history": [
                    {"request_id": c.request_id, "url": c.url}
                    for c in history
                ]
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
