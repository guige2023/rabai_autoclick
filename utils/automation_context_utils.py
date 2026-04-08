"""Automation context utilities for managing automation workflow state.

This module provides utilities for managing context during automation
workflows, including context isolation, state preservation, and cleanup.
"""

from __future__ import annotations

import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# Context variables for automation state
automation_context: ContextVar[Optional["AutomationContext"]] = ContextVar(
    "automation_context", default=None
)


@dataclass
class AutomationContext:
    """Context for an automation workflow execution.
    
    Provides isolation between concurrent automation runs and
    maintains state throughout a workflow's lifetime.
    """
    id: str
    name: str
    start_time: float = field(default_factory=time.monotonic)
    data: dict = field(default_factory=dict)
    _running: bool = False
    
    def __post_init__(self):
        self.id = self.id or str(uuid.uuid4())
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time since context creation."""
        return time.monotonic() - self.start_time
    
    @property
    def is_running(self) -> bool:
        """Check if the context is still running."""
        return self._running
    
    def set(self, key: str, value: Any) -> None:
        """Store a value in the context."""
        self.data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the context."""
        return self.data.get(key, default)
    
    def clear(self) -> None:
        """Clear all stored data."""
        self.data.clear()
    
    def enter(self) -> "AutomationContext":
        """Enter this context (marks as running)."""
        self._running = True
        automation_context.set(self)
        return self
    
    def exit(self) -> None:
        """Exit this context (marks as stopped)."""
        self._running = False
        automation_context.set(None)


class ContextManager:
    """Manages automation contexts and provides context-sensitive operations."""
    
    def __init__(self):
        self._contexts: dict[str, AutomationContext] = {}
        self._cleanup_callbacks: list[Callable[[AutomationContext], None]] = []
    
    def create_context(self, name: str) -> AutomationContext:
        """Create a new automation context.
        
        Args:
            name: Name/identifier for the context.
        
        Returns:
            New AutomationContext.
        """
        ctx = AutomationContext(
            id=str(uuid.uuid4()),
            name=name,
        )
        self._contexts[ctx.id] = ctx
        return ctx
    
    def get_context(self, context_id: str) -> Optional[AutomationContext]:
        """Get a context by ID."""
        return self._contexts.get(context_id)
    
    def get_current_context(self) -> Optional[AutomationContext]:
        """Get the current active context."""
        return automation_context.get()
    
    def destroy_context(self, context_id: str) -> bool:
        """Destroy a context and run cleanup callbacks.
        
        Args:
            context_id: ID of the context to destroy.
        
        Returns:
            True if context was destroyed.
        """
        ctx = self._contexts.pop(context_id, None)
        if ctx is not None:
            ctx.exit()
            for callback in self._cleanup_callbacks:
                try:
                    callback(ctx)
                except Exception:
                    pass
            return True
        return False
    
    def on_cleanup(self, callback: Callable[[AutomationContext], None]) -> None:
        """Register a cleanup callback.
        
        Args:
            callback: Called when a context is destroyed.
        """
        self._cleanup_callbacks.append(callback)
    
    def get_all_contexts(self) -> list[AutomationContext]:
        """Get all active contexts."""
        return list(self._contexts.values())


# Global context manager
_global_context_manager: Optional[ContextManager] = None


def get_context_manager() -> ContextManager:
    """Get the global context manager."""
    global _global_context_manager
    if _global_context_manager is None:
        _global_context_manager = ContextManager()
    return _global_context_manager


def create_automation_context(name: str) -> AutomationContext:
    """Create and enter a new automation context.
    
    Args:
        name: Name for the context.
    
    Returns:
        The new AutomationContext, entered as current.
    """
    ctx = get_context_manager().create_context(name)
    ctx.enter()
    return ctx


def get_current_context() -> Optional[AutomationContext]:
    """Get the current automation context."""
    return get_context_manager().get_current_context()


def destroy_current_context() -> bool:
    """Destroy the current context if one exists."""
    ctx = get_current_context()
    if ctx:
        return get_context_manager().destroy_context(ctx.id)
    return False


class context:
    """Context manager for automation context.
    
    Usage:
        with automation_context("my_workflow") as ctx:
            ctx.set("step", 1)
            # ... automation code ...
    """
    
    def __init__(self, name: str):
        self.name = name
        self._ctx: Optional[AutomationContext] = None
    
    def __enter__(self) -> AutomationContext:
        self._ctx = create_automation_context(self.name)
        return self._ctx
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        destroy_current_context()
