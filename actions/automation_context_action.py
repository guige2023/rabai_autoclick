"""Automation Context Action Module.

Provides shared context management for automation workflows including
variable storage, state propagation, and context isolation.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class ContextScope(Enum):
    """Context scope levels."""
    GLOBAL = "global"
    WORKFLOW = "workflow"
    STEP = "step"
    LOCAL = "local"


@dataclass
class AutomationVariable:
    """Represents a single variable in the automation context."""
    name: str
    value: Any
    scope: ContextScope = ContextScope.LOCAL
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    encrypted: bool = False

    def update(self, value: Any) -> None:
        """Update the variable value."""
        self.value = value
        self.updated_at = time.time()

    def age_ms(self) -> float:
        """Return age since creation in milliseconds."""
        return (time.time() - self.created_at) * 1000


@dataclass
class AutomationContext:
    """Represents a workflow or step execution context."""
    context_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = "unnamed"
    parent_id: Optional[str] = None
    variables: Dict[str, AutomationVariable] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    scope: ContextScope = ContextScope.WORKFLOW
    created_at: float = field(default_factory=time.time)
    tags: Set[str] = field(default_factory=set)

    def elapsed_ms(self) -> float:
        """Return elapsed time since context creation."""
        return (time.time() - self.created_at) * 1000

    def set_var(
        self,
        name: str,
        value: Any,
        scope: ContextScope = ContextScope.LOCAL,
        encrypted: bool = False
    ) -> AutomationVariable:
        """Set a variable in this context."""
        if name in self.variables:
            var = self.variables[name]
            var.update(value)
        else:
            var = AutomationVariable(
                name=name,
                value=value,
                scope=scope,
                encrypted=encrypted
            )
            self.variables[name] = var
        return var

    def get_var(self, name: str, default: Any = None) -> Any:
        """Get a variable value from this context."""
        var = self.variables.get(name)
        return var.value if var else default

    def delete_var(self, name: str) -> bool:
        """Delete a variable from this context."""
        return self.variables.pop(name, None) is not None

    def list_vars(self, scope: Optional[ContextScope] = None) -> Dict[str, Any]:
        """List variables, optionally filtered by scope."""
        if scope is None:
            return {k: v.value for k, v in self.variables.items()}
        return {
            k: v.value for k, v in self.variables.items()
            if v.scope == scope
        }


class ContextScopeManager:
    """Manages variable scoping and inheritance across contexts."""

    def __init__(self):
        self._contexts: Dict[str, AutomationContext] = {}
        self._lock = asyncio.Lock()
        self._global_context: Optional[AutomationContext] = None

    async def initialize(self) -> None:
        """Initialize the global context."""
        async with self._lock:
            if self._global_context is None:
                self._global_context = AutomationContext(
                    name="global",
                    scope=ContextScope.GLOBAL
                )
                self._contexts["global"] = self._global_context

    async def create_context(
        self,
        name: str,
        parent_id: Optional[str] = None,
        scope: ContextScope = ContextScope.WORKFLOW,
        metadata: Optional[Dict[str, Any]] = None
    ) -> AutomationContext:
        """Create a new context."""
        async with self._lock:
            context = AutomationContext(
                name=name,
                parent_id=parent_id,
                scope=scope,
                metadata=metadata or {}
            )
            self._contexts[context.context_id] = context
            return copy.deepcopy(context)

    async def get_context(self, context_id: str) -> Optional[AutomationContext]:
        """Get a context by ID."""
        async with self._lock:
            ctx = self._contexts.get(context_id)
            return copy.deepcopy(ctx) if ctx else None

    async def resolve_variable(
        self,
        context_id: str,
        var_name: str,
        inherit: bool = True
    ) -> Any:
        """Resolve a variable, optionally searching parent contexts."""
        if not inherit:
            ctx = await self.get_context(context_id)
            return ctx.get_var(var_name) if ctx else None

        # Search scope chain: local -> step -> workflow -> global
        search_order = [ContextScope.LOCAL, ContextScope.STEP, ContextScope.WORKFLOW]

        async with self._lock:
            ctx_ids = [context_id]
            ctx = self._contexts.get(context_id)
            if ctx and ctx.parent_id:
                ctx_ids.append(ctx.parent_id)
                parent = self._contexts.get(ctx.parent_id)
                if parent and parent.parent_id:
                    ctx_ids.append(parent.parent_id)

            # Add global
            if self._global_context:
                ctx_ids.append(self._global_context.context_id)

            for cid in ctx_ids:
                c = self._contexts.get(cid)
                if c and var_name in c.variables:
                    return c.variables[var_name].value

        return None

    async def set_variable(
        self,
        context_id: str,
        name: str,
        value: Any,
        scope: ContextScope = ContextScope.LOCAL
    ) -> bool:
        """Set a variable in a context."""
        async with self._lock:
            ctx = self._contexts.get(context_id)
            if not ctx:
                return False
            ctx.set_var(name, value, scope)
            return True

    async def delete_context(self, context_id: str) -> bool:
        """Delete a context."""
        async with self._lock:
            if context_id == "global":
                return False
            return self._contexts.pop(context_id, None) is not None

    async def list_contexts(self) -> List[Dict[str, Any]]:
        """List all contexts."""
        async with self._lock:
            return [
                {
                    "context_id": c.context_id,
                    "name": c.name,
                    "scope": c.scope.value,
                    "parent_id": c.parent_id,
                    "variable_count": len(c.variables)
                }
                for c in self._contexts.values()
            ]


class AutomationContextAction:
    """Main action class for automation context management."""

    def __init__(self):
        self._manager = ContextScopeManager()
        self._initialized = False

    async def ensure_initialized(self) -> None:
        """Ensure the context manager is initialized."""
        if not self._initialized:
            await self._manager.initialize()
            self._initialized = True

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation context action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        await self.ensure_initialized()

        operation = context.get("operation", "create")

        if operation == "create":
            result = await self._manager.create_context(
                name=context.get("name", "unnamed"),
                parent_id=context.get("parent_id"),
                scope=ContextScope(context.get("scope", "workflow")),
                metadata=context.get("metadata")
            )
            return {
                "success": True,
                "context": {
                    "context_id": result.context_id,
                    "name": result.name,
                    "scope": result.scope.value,
                    "parent_id": result.parent_id
                }
            }

        elif operation == "get":
            ctx = await self._manager.get_context(context.get("context_id", ""))
            if ctx:
                return {
                    "success": True,
                    "context": {
                        "context_id": ctx.context_id,
                        "name": ctx.name,
                        "variables": ctx.list_vars(),
                        "scope": ctx.scope.value
                    }
                }
            return {"success": False, "error": "Context not found"}

        elif operation == "set_var":
            success = await self._manager.set_variable(
                context_id=context.get("context_id", ""),
                name=context.get("name", ""),
                value=context.get("value"),
                scope=ContextScope(context.get("scope", "local"))
            )
            return {"success": success}

        elif operation == "get_var":
            value = await self._manager.resolve_variable(
                context_id=context.get("context_id", ""),
                var_name=context.get("name", ""),
                inherit=context.get("inherit", True)
            )
            return {"success": True, "value": value}

        elif operation == "delete":
            success = await self._manager.delete_context(context.get("context_id", ""))
            return {"success": success}

        elif operation == "list":
            contexts = await self._manager.list_contexts()
            return {"success": True, "contexts": contexts}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
