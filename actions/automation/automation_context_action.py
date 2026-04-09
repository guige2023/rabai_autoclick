"""Execution context management for automation workflows.

Provides structured context propagation, isolation, and sharing
across automation workflow steps.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import pprint


class ContextScope(Enum):
    """Scope determines context visibility and isolation."""
    GLOBAL = "global"
    WORKFLOW = "workflow"
    STEP = "step"
    ISOLATED = "isolated"


class ContextAccess(Enum):
    """Access control for context entries."""
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    PRIVATE = "private"


@dataclass
class ContextEntry:
    """A single entry in the execution context."""
    key: str
    value: Any
    scope: ContextScope = ContextScope.WORKFLOW
    access: ContextAccess = ContextAccess.READ_WRITE
    owner: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)


@dataclass
class ExecutionFrame:
    """A single frame in the execution stack."""
    frame_id: str
    step_name: str
    parent_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    context_snapshot: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ContextStore:
    """Thread-safe context storage with versioning and access control."""

    def __init__(self):
        self._store: Dict[str, ContextEntry] = {}
        self._lock = threading.RLock()
        self._version_history: Dict[str, List[Dict[str, Any]]] = {}
        self._max_history = 100

    def set(
        self,
        key: str,
        value: Any,
        scope: ContextScope = ContextScope.WORKFLOW,
        access: ContextAccess = ContextAccess.READ_WRITE,
        owner: Optional[str] = None,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ContextEntry:
        """Set a value in the context."""
        with self._lock:
            now = time.time()
            existing = self._store.get(key)

            if existing:
                self._version_history.setdefault(key, []).append({
                    "value": existing.value,
                    "version": existing.version,
                    "updated_at": existing.updated_at,
                    "updated_by": existing.owner,
                })
                if len(self._version_history[key]) > self._max_history:
                    self._version_history[key] = self._version_history[key][-self._max_history:]

                existing.value = value
                existing.updated_at = now
                existing.version += 1
                existing.metadata = metadata or existing.metadata
                if tags:
                    existing.tags.update(tags)
                return existing

            entry = ContextEntry(
                key=key,
                value=value,
                scope=scope,
                access=access,
                owner=owner,
                tags=tags or set(),
                metadata=metadata or {},
            )
            self._store[key] = entry
            return entry

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the context."""
        with self._lock:
            entry = self._store.get(key)
            return entry.value if entry else default

    def get_entry(self, key: str) -> Optional[ContextEntry]:
        """Get the full context entry."""
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key from the context."""
        with self._lock:
            return self._store.pop(key, None) is not None

    def get_by_tags(self, tags: Set[str]) -> Dict[str, Any]:
        """Get all entries matching any of the given tags."""
        with self._lock:
            return {
                k: v.value
                for k, v in self._store.items()
                if v.tags.intersection(tags)
            }

    def get_by_scope(self, scope: ContextScope) -> Dict[str, Any]:
        """Get all entries in a specific scope."""
        with self._lock:
            return {
                k: v.value
                for k, v in self._store.items()
                if v.scope == scope
            }

    def get_history(self, key: str) -> List[Dict[str, Any]]:
        """Get version history for a key."""
        with self._lock:
            return list(self._version_history.get(key, []))

    def snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of the entire context."""
        with self._lock:
            return {k: v.value for k, v in self._store.items()}

    def restore(self, snapshot: Dict[str, Any]) -> None:
        """Restore context from a snapshot."""
        with self._lock:
            self._store.clear()
            now = time.time()
            for key, value in snapshot.items():
                self._store[key] = ContextEntry(
                    key=key,
                    value=value,
                    updated_at=now,
                )

    def clear_scope(self, scope: ContextScope) -> int:
        """Clear all entries in a specific scope."""
        with self._lock:
            to_remove = [k for k, v in self._store.items() if v.scope == scope]
            for k in to_remove:
                del self._store[k]
            return len(to_remove)


class ExecutionStack:
    """Manages the call stack of execution frames."""

    def __init__(self):
        self._frames: List[ExecutionFrame] = []
        self._lock = threading.RLock()

    def push(
        self,
        step_name: str,
        context_snapshot: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Push a new frame onto the stack."""
        with self._lock:
            parent = self._frames[-1].frame_id if self._frames else None
            frame_id = str(uuid.uuid4())[:12]
            frame = ExecutionFrame(
                frame_id=frame_id,
                step_name=step_name,
                parent_id=parent,
                context_snapshot=copy.deepcopy(context_snapshot),
                metadata=metadata or {},
            )
            self._frames.append(frame)
            return frame_id

    def pop(self, frame_id: Optional[str] = None) -> Optional[ExecutionFrame]:
        """Pop a frame from the stack."""
        with self._lock:
            if not self._frames:
                return None
            if frame_id:
                idx = next(
                    (i for i, f in enumerate(self._frames) if f.frame_id == frame_id),
                    None
                )
                if idx is not None:
                    return self._frames.pop(idx)
                return None
            return self._frames.pop()

    def get_current(self) -> Optional[ExecutionFrame]:
        """Get the current frame without popping."""
        with self._lock:
            return self._frames[-1] if self._frames else None

    def get_ancestors(self, frame_id: str) -> List[ExecutionFrame]:
        """Get all ancestor frames up to root."""
        with self._lock:
            frame_map = {f.frame_id: f for f in self._frames}
            ancestors = []
            current = frame_map.get(frame_id)
            while current and current.parent_id:
                current = frame_map.get(current.parent_id)
                if current:
                    ancestors.append(current)
            return ancestors

    def depth(self) -> int:
        """Get the current stack depth."""
        with self._lock:
            return len(self._frames)


class AutomationContextAction:
    """Action providing execution context management."""

    def __init__(self):
        self._store = ContextStore()
        self._stack = ExecutionStack()
        self._globals: Dict[str, Any] = {}

    def set_global(self, key: str, value: Any) -> None:
        """Set a global context value."""
        self._globals[key] = value
        self._store.set(key, value, scope=ContextScope.GLOBAL)

    def get_global(self, key: str, default: Any = None) -> Any:
        """Get a global context value."""
        return self._globals.get(key, default)

    def set(
        self,
        key: str,
        value: Any,
        scope: str = "workflow",
        access: str = "read_write",
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Set a context value."""
        try:
            scope_enum = ContextScope(scope)
        except ValueError:
            scope_enum = ContextScope.WORKFLOW
        try:
            access_enum = ContextAccess(access)
        except ValueError:
            access_enum = ContextAccess.READ_WRITE

        entry = self._store.set(
            key=key,
            value=value,
            scope=scope_enum,
            access=access_enum,
            owner=owner,
            tags=set(tags) if tags else None,
        )
        return {
            "key": entry.key,
            "version": entry.version,
            "updated_at": datetime.fromtimestamp(entry.updated_at).isoformat(),
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get a context value."""
        return self._store.get(key, default)

    def get_all(self, scope: Optional[str] = None) -> Dict[str, Any]:
        """Get all context values, optionally filtered by scope."""
        if scope:
            try:
                scope_enum = ContextScope(scope)
                return self._store.get_by_scope(scope_enum)
            except ValueError:
                return self._store.snapshot()
        return self._store.snapshot()

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an action with context management.

        Required params:
            operation: callable - The operation to execute

        Optional params:
            step_name: str - Name of the step for stack tracking
            inherit_context: bool - Whether to merge context into store (default True)
            isolate: bool - Whether to use isolated context (default False)
            metadata: dict - Additional frame metadata
        """
        operation = params.get("operation")
        step_name = params.get("step_name", "unnamed_step")
        inherit_context = params.get("inherit_context", True)
        isolate = params.get("isolate", False)
        frame_metadata = params.get("metadata", {})

        if not callable(operation):
            raise ValueError("operation must be a callable")

        if inherit_context:
            for key, value in context.items():
                if isolate:
                    self._store.set(
                        key, copy.deepcopy(value),
                        scope=ContextScope.ISOLATED,
                        access=ContextAccess.PRIVATE,
                    )
                else:
                    self._store.set(
                        key, value,
                        scope=ContextScope.STEP,
                        access=ContextAccess.READ_WRITE,
                    )

        snapshot = self._store.snapshot()
        frame_id = self._stack.push(step_name, snapshot, frame_metadata)

        try:
            result = operation(context=self._store.snapshot(), params=params)
            return {
                "frame_id": frame_id,
                "step_name": step_name,
                "result": result,
                "stack_depth": self._stack.depth(),
            }
        finally:
            self._stack.pop(frame_id)

    def push_scope(
        self,
        step_name: str,
        context_snapshot: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Manually push a new execution scope."""
        snapshot = context_snapshot or self._store.snapshot()
        return self._stack.push(step_name, snapshot)

    def pop_scope(self, frame_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Pop the current or a specific execution scope."""
        frame = self._stack.pop(frame_id)
        if frame:
            return {
                "frame_id": frame.frame_id,
                "step_name": frame.step_name,
                "metadata": frame.metadata,
            }
        return None

    def get_scope_tree(self) -> List[Dict[str, Any]]:
        """Get the full execution scope tree."""
        current = self._stack.get_current()
        if not current:
            return []
        ancestors = self._stack.get_ancestors(current.frame_id)
        return [
            {
                "frame_id": f.frame_id,
                "step_name": f.step_name,
                "parent_id": f.parent_id,
                "created_at": datetime.fromtimestamp(f.created_at).isoformat(),
                "metadata": f.metadata,
            }
            for f in reversed(ancestors) + [current]
        ]

    def get_history(self, key: str) -> List[Dict[str, Any]]:
        """Get version history for a context key."""
        return self._store.get_history(key)

    def clear_scope(self, scope: str) -> int:
        """Clear context entries for a specific scope."""
        try:
            scope_enum = ContextScope(scope)
            return self._store.clear_scope(scope_enum)
        except ValueError:
            return 0
