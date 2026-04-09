"""
Automation Execution Context Action.

Manages shared execution context for complex automation workflows,
supporting context propagation, scoping, and state isolation.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

logger = logging.getLogger(__name__)


class ContextScope(Enum):
    """Scope levels for context data."""
    GLOBAL = auto()    # Shared across all executions
    WORKFLOW = auto()  # Shared within a workflow
    STAGE = auto()     # Isolated to a single stage
    STEP = auto()      # Isolated to a single step


@dataclass
class ContextEntry:
    """A single entry in the execution context."""
    key: str
    value: Any
    scope: ContextScope
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = "unknown"
    modified_at: Optional[datetime] = None
    readonly: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ExecutionContextSnapshot:
    """Snapshot of context at a point in time."""
    snapshot_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    workflow_id: str
    stage_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    scope: ContextScope = ContextScope.WORKFLOW


class ContextManager:
    """
    Manages execution context with scoping and isolation.

    Example:
        ctx = ExecutionContext(workflow_id="wf-001")
        ctx.set("user_id", 123, scope=ContextScope.STAGE)
        ctx.set("cache_enabled", True, scope=ContextScope.WORKFLOW)
        user_id = ctx.get("user_id")
        snapshot = ctx.snapshot()
    """

    def __init__(self, workflow_id: str) -> None:
        self.workflow_id = workflow_id
        self._global: Dict[str, ContextEntry] = {}
        self._workflow: Dict[str, ContextEntry] = {}
        self._stage: Dict[str, ContextEntry] = {}
        self._step: Dict[str, ContextEntry] = {}
        self._snapshots: Dict[str, ExecutionContextSnapshot] = {}
        self._current_scope = ContextScope.WORKFLOW
        self._stage_stack: List[str] = []
        self._observers: List[Callable[[str, Any, ContextScope], None]] = []
        self._lock = asyncio.Lock()

    def set(
        self,
        key: str,
        value: Any,
        scope: Optional[ContextScope] = None,
        readonly: bool = False,
        tags: Optional[Dict[str, str]] = None,
        created_by: str = "unknown",
    ) -> None:
        """Set a value in the context at the specified scope."""
        effective_scope = scope or self._current_scope
        store = self._get_store(effective_scope)
        entry = ContextEntry(
            key=key,
            value=value,
            scope=effective_scope,
            created_by=created_by,
            readonly=readonly,
            tags=tags or {},
        )
        store[key] = entry
        self._notify_observers(key, value, effective_scope)

    def get(self, key: str, default: Any = None, max_scope: ContextScope = ContextScope.GLOBAL) -> Any:
        """Get a value, searching from step -> stage -> workflow -> global."""
        for scope in [ContextScope.STEP, ContextScope.STAGE, ContextScope.WORKFLOW, ContextScope.GLOBAL]:
            if scope.value > max_scope.value:
                continue
            store = self._get_store(scope)
            if key in store:
                return store[key].value
        return default

    def get_with_scope(self, key: str) -> Optional[ContextEntry]:
        """Get entry with its scope information."""
        for scope in [ContextScope.STEP, ContextScope.STAGE, ContextScope.WORKFLOW, ContextScope.GLOBAL]:
            store = self._get_store(scope)
            if key in store:
                return store[key]
        return None

    def delete(self, key: str, scope: Optional[ContextScope] = None) -> bool:
        """Delete a key from specified scope or all scopes."""
        if scope:
            store = self._get_store(scope)
            if key in store:
                del store[key]
                return True
            return False
        # Delete from all scopes
        found = False
        for store in [self._step, self._stage, self._workflow, self._global]:
            if key in store:
                del store[key]
                found = True
        return found

    def exists(self, key: str) -> bool:
        """Check if key exists in any scope."""
        return any(key in self._get_store(s) for s in ContextScope)

    def keys(self, scope: Optional[ContextScope] = None) -> List[str]:
        """Get all keys at specified scope or all scopes."""
        if scope:
            return list(self._get_store(scope).keys())
        all_keys: Set[str] = set()
        for store in [self._step, self._stage, self._workflow, self._global]:
            all_keys.update(store.keys())
        return list(all_keys)

    def push_stage(self, stage_id: str) -> None:
        """Push a new stage onto the stack."""
        self._stage_stack.append(stage_id)
        self._current_scope = ContextScope.STAGE
        logger.debug("Stage pushed: %s, stack: %s", stage_id, self._stage_stack)

    def pop_stage(self) -> Optional[str]:
        """Pop the current stage from the stack."""
        if self._stage_stack:
            stage_id = self._stage_stack.pop()
            self._current_scope = ContextScope.WORKFLOW if not self._stage_stack else ContextScope.STAGE
            logger.debug("Stage popped: %s, remaining: %s", stage_id, self._stage_stack)
            return stage_id
        return None

    def snapshot(self, stage_id: Optional[str] = None) -> ExecutionContextSnapshot:
        """Create a snapshot of current context state."""
        snapshot_id = str(uuid.uuid4())[:8]
        data = {k: v for store in [self._step, self._stage, self._workflow, self._global]
                for k, v in store.items()}
        # Deep copy values to freeze state
        snapshot_data = {k: copy.deepcopy(v.value) for k, v in data.items()}
        snapshot = ExecutionContextSnapshot(
            snapshot_id=snapshot_id,
            workflow_id=self.workflow_id,
            stage_id=stage_id or (self._stage_stack[-1] if self._stage_stack else None),
            data=snapshot_data,
        )
        self._snapshots[snapshot_id] = snapshot
        return snapshot

    def restore_snapshot(self, snapshot_id: str) -> None:
        """Restore context from a snapshot."""
        snapshot = self._snapshots.get(snapshot_id)
        if not snapshot:
            raise KeyError(f"Snapshot {snapshot_id} not found")
        # Clear current context
        for store in [self._step, self._stage, self._workflow, self._global]:
            store.clear()
        # Restore from snapshot
        for key, value in snapshot.data.items():
            self.set(key, copy.deepcopy(value))

    def observe(self, callback: Callable[[str, Any, ContextScope], None]) -> None:
        """Register an observer for context changes."""
        self._observers.append(callback)

    def _notify_observers(self, key: str, value: Any, scope: ContextScope) -> None:
        for observer in self._observers:
            try:
                observer(key, value, scope)
            except Exception as exc:
                logger.error("Context observer raised: %s", exc)

    def _get_store(self, scope: ContextScope) -> Dict[str, ContextEntry]:
        if scope == ContextScope.GLOBAL:
            return self._global
        elif scope == ContextScope.WORKFLOW:
            return self._workflow
        elif scope == ContextScope.STAGE:
            return self._stage
        elif scope == ContextScope.STEP:
            return self._step
        return self._workflow

    def isolate_step(self) -> Self:
        """Create a copy of this context for isolated step execution."""
        new_ctx = ExecutionContext(workflow_id=self.workflow_id)
        new_ctx._global = copy.copy(self._global)
        new_ctx._workflow = copy.copy(self._workflow)
        new_ctx._stage = copy.copy(self._stage)
        # Step context is fresh (isolated)
        return new_ctx

    def merge_from(self, other: ExecutionContext, scope: ContextScope = ContextScope.WORKFLOW) -> None:
        """Merge entries from another context."""
        source_store = other._get_store(scope)
        for key, entry in source_store.items():
            self.set(key, entry.value, scope=entry.scope, readonly=entry.readonly, tags=entry.tags)


# Alias for backward compatibility
ExecutionContext = ContextManager
