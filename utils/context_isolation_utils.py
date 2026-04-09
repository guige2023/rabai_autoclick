"""
Context Isolation Utilities for Test Automation.

This module provides utilities for context isolation in UI automation,
including session management, state isolation, and test independence.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Callable, List, TypeVar, Generic
from contextvars import ContextVar
import threading
import uuid


T = TypeVar("T")


@dataclass
class IsolationContext:
    """Context for isolated execution."""
    id: str
    name: str
    state: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ContextStore:
    """
    Thread-safe context storage for isolated execution.
    """

    def __init__(self):
        """Initialize context store."""
        self._contexts: Dict[str, IsolationContext] = {}
        self._current: ContextVar[Optional[str]] = ContextVar("current_context", default=None)
        self._lock = threading.RLock()

    def create(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> IsolationContext:
        """
        Create a new isolation context.

        Args:
            name: Context name
            metadata: Optional metadata

        Returns:
            Created IsolationContext
        """
        context = IsolationContext(
            id=str(uuid.uuid4()),
            name=name,
            metadata=metadata or {}
        )
        with self._lock:
            self._contexts[context.id] = context
        return context

    def enter(self, context_id: str) -> bool:
        """
        Enter a context (set as current).

        Args:
            context_id: Context ID

        Returns:
            True if successful
        """
        with self._lock:
            if context_id in self._contexts:
                self._current.set(context_id)
                return True
        return False

    def exit(self) -> None:
        """Exit current context."""
        self._current.set(None)

    def get_current(self) -> Optional[IsolationContext]:
        """Get current context."""
        context_id = self._current.get()
        if context_id:
            with self._lock:
                return self._contexts.get(context_id)
        return None

    def get(self, context_id: str) -> Optional[IsolationContext]:
        """Get context by ID."""
        with self._lock:
            return self._contexts.get(context_id)

    def delete(self, context_id: str) -> bool:
        """
        Delete a context.

        Args:
            context_id: Context ID

        Returns:
            True if deleted
        """
        with self._lock:
            if context_id in self._contexts:
                del self._contexts[context_id]
                return True
        return False

    def set_state(self, key: str, value: Any) -> None:
        """Set state value in current context."""
        context = self.get_current()
        if context:
            context.state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        """Get state value from current context."""
        context = self.get_current()
        if context:
            return context.state.get(key, default)
        return default


_global_store = ContextStore()


def get_global_store() -> ContextStore:
    """Get the global context store."""
    return _global_store


class IsolatedExecution(Generic[T]):
    """
    Context manager for isolated execution.
    """

    def __init__(
        self,
        name: str,
        initial_state: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize isolated execution.

        Args:
            name: Context name
            initial_state: Initial state values
            metadata: Context metadata
        """
        self.name = name
        self.initial_state = initial_state or {}
        self.metadata = metadata or {}
        self._context: Optional[IsolationContext] = None
        self._result: Optional[T] = None

    def __enter__(self) -> 'IsolatedExecution[T]':
        """Enter isolated context."""
        self._context = _global_store.create(self.name, self.metadata)
        self._context.state = copy.deepcopy(self.initial_state)
        _global_store.enter(self._context.id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit isolated context."""
        _global_store.exit()
        if self._context:
            _global_store.delete(self._context.id)
        self._context = None

    def set_result(self, result: T) -> None:
        """Set execution result."""
        self._result = result

    def get_result(self) -> Optional[T]:
        """Get execution result."""
        return self._result


def isolate_state(
    state: Dict[str, Any],
    deep: bool = True
) -> Dict[str, Any]:
    """
    Create an isolated copy of state.

    Args:
        state: State to isolate
        deep: Deep copy flag

    Returns:
        Isolated state copy
    """
    if deep:
        return copy.deepcopy(state)
    return state.copy()


def merge_isolated_states(
    base: Dict[str, Any],
    override: Dict[str, Any],
    conflict_strategy: str = "override"
) -> Dict[str, Any]:
    """
    Merge two isolated states.

    Args:
        base: Base state
        override: Override state
        conflict_strategy: "override" or "keep_base"

    Returns:
        Merged state
    """
    result = copy.deepcopy(base)

    for key, value in override.items():
        if key not in result or conflict_strategy == "override":
            result[key] = copy.deepcopy(value)

    return result
