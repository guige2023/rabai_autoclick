"""
UI Focus Context Utilities - Focus context management for UI automation.

This module provides utilities for managing focus context during UI
automation. It tracks the current focus state, manages focus transitions,
and helps maintain proper focus hierarchy during complex automation flows.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class FocusState:
    """Represents a focus state at a point in time.
    
    Attributes:
        id: Unique identifier for this focus state.
        element_id: ID of the focused element.
        element_type: Type of the focused element.
        timestamp: Time when focus was acquired.
        context: Optional context data.
        parent_focus: ID of the parent focus context.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    element_id: Optional[str] = None
    element_type: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    context: dict = field(default_factory=dict)
    parent_focus: Optional[str] = None


@dataclass
class FocusTransition:
    """Represents a transition between focus states.
    
    Attributes:
        id: Unique identifier for this transition.
        from_state: ID of the source focus state.
        to_state: ID of the destination focus state.
        transition_type: Type of transition (direct, nested, etc.).
        trigger: What triggered this transition.
        duration: Time taken for the transition.
        success: Whether the transition completed successfully.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    from_state: Optional[str] = None
    to_state: Optional[str] = None
    transition_type: str = "direct"
    trigger: Optional[str] = None
    duration: float = 0.0
    success: bool = True


class FocusContext:
    """Manages the current focus context and history.
    
    Provides methods for acquiring focus, releasing focus, tracking
    focus transitions, and querying focus history.
    
    Example:
        >>> context = FocusContext()
        >>> context.acquire("btn_submit", element_type="button")
        >>> state = context.get_current_state()
        >>> context.release()
    """
    
    def __init__(self) -> None:
        """Initialize an empty focus context."""
        self._states: dict[str, FocusState] = {}
        self._transitions: list[FocusTransition] = []
        self._current_state_id: Optional[str] = None
        self._focus_stack: list[str] = []
    
    def acquire(
        self,
        element_id: str,
        element_type: Optional[str] = None,
        context: Optional[dict] = None,
        push: bool = True
    ) -> FocusState:
        """Acquire focus for an element.
        
        Args:
            element_id: ID of the element gaining focus.
            element_type: Optional type of the element.
            context: Optional context data.
            push: Whether to push to focus stack (True) or replace (False).
            
        Returns:
            The new FocusState.
        """
        previous_state_id = self._current_state_id
        
        state = FocusState(
            element_id=element_id,
            element_type=element_type,
            context=context or {},
            parent_focus=previous_state_id
        )
        self._states[state.id] = state
        self._current_state_id = state.id
        
        if push:
            self._focus_stack.append(state.id)
        else:
            self._focus_stack = [state.id]
        
        transition = FocusTransition(
            from_state=previous_state_id,
            to_state=state.id,
            transition_type="push" if push else "replace",
            trigger="acquire"
        )
        self._transitions.append(transition)
        
        return state
    
    def release(self) -> Optional[FocusState]:
        """Release focus, returning to previous state if any.
        
        Returns:
            The new current FocusState, or None if stack is empty.
        """
        if len(self._focus_stack) <= 1:
            return self._current_state_id
    
    def get_current_state(self) -> Optional[FocusState]:
        """Get the current focus state.
        
        Returns:
            Current FocusState, or None if no focus.
        """
        if self._current_state_id:
            return self._states.get(self._current_state_id)
        return None
    
    def get_state(self, state_id: str) -> Optional[FocusState]:
        """Get a focus state by ID.
        
        Args:
            state_id: State identifier.
            
        Returns:
            FocusState if found.
        """
        return self._states.get(state_id)
    
    def get_focus_stack(self) -> list[FocusState]:
        """Get the current focus stack.
        
        Returns:
            List of FocusStates from bottom to top.
        """
        return [
            self._states[sid]
            for sid in self._focus_stack
            if sid in self._states
        ]
    
    def get_transitions(
        self,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None
    ) -> list[FocusTransition]:
        """Get focus transitions, optionally filtered.
        
        Args:
            from_state: Optional source state filter.
            to_state: Optional destination state filter.
            
        Returns:
            List of matching transitions.
        """
        transitions = self._transitions
        
        if from_state is not None:
            transitions = [t for t in transitions if t.from_state == from_state]
        if to_state is not None:
            transitions = [t for t in transitions if t.to_state == to_state]
        
        return transitions
    
    def find_state_by_element(
        self,
        element_id: str,
        max_age: Optional[float] = None
    ) -> Optional[FocusState]:
        """Find a focus state by element ID.
        
        Args:
            element_id: Element ID to search for.
            max_age: Maximum age of state in seconds.
            
        Returns:
            Most recent FocusState for the element.
        """
        current_time = time.time()
        states = [
            s for s in self._states.values()
            if s.element_id == element_id
        ]
        
        if max_age is not None:
            states = [
                s for s in states
                if current_time - s.timestamp <= max_age
            ]
        
        if not states:
            return None
        
        return max(states, key=lambda s: s.timestamp)
    
    def get_focused_element_id(self) -> Optional[str]:
        """Get the ID of the currently focused element.
        
        Returns:
            Element ID, or None if no focus.
        """
        state = self.get_current_state()
        return state.element_id if state else None
    
    def is_focused(self, element_id: str) -> bool:
        """Check if a specific element is currently focused.
        
        Args:
            element_id: Element ID to check.
            
        Returns:
            True if the element has focus.
        """
        return self.get_focused_element_id() == element_id
    
    def is_focused_in_stack(self, element_id: str) -> bool:
        """Check if an element is anywhere in the focus stack.
        
        Args:
            element_id: Element ID to check.
            
        Returns:
            True if element is in the focus stack.
        """
        return any(
            self._states.get(sid, FocusState()).element_id == element_id
            for sid in self._focus_stack
        )
    
    def clear(self) -> None:
        """Clear all focus state and transitions."""
        self._states.clear()
        self._transitions.clear()
        self._current_state_id = None
        self._focus_stack.clear()
    
    def iterate_states(self, reverse: bool = False) -> Iterator[FocusState]:
        """Iterate over focus states.
        
        Args:
            reverse: If True, iterate newest first.
            
        Yields:
            FocusStates in order.
        """
        states = sorted(self._states.values(), key=lambda s: s.timestamp)
        if reverse:
            states = list(reversed(states))
        yield from states


class FocusContextManager:
    """Manages multiple focus contexts.
    
    Provides context isolation and switching between different
    focus contexts for complex automation scenarios.
    
    Example:
        >>> manager = FocusContextManager()
        >>> context = manager.create_context("dialog_flow")
        >>> manager.set_active("dialog_flow")
    """
    
    def __init__(self) -> None:
        """Initialize an empty context manager."""
        self._contexts: dict[str, FocusContext] = {}
        self._active_context_id: Optional[str] = None
    
    def create_context(self, context_id: str) -> FocusContext:
        """Create a new focus context.
        
        Args:
            context_id: Unique identifier for the context.
            
        Returns:
            The created FocusContext.
        """
        context = FocusContext()
        self._contexts[context_id] = context
        
        if self._active_context_id is None:
            self._active_context_id = context_id
        
        return context
    
    def get_context(self, context_id: str) -> Optional[FocusContext]:
        """Get a context by ID.
        
        Args:
            context_id: Context identifier.
            
        Returns:
            FocusContext if found.
        """
        return self._contexts.get(context_id)
    
    def set_active(self, context_id: str) -> bool:
        """Set the active context.
        
        Args:
            context_id: Context to make active.
            
        Returns:
            True if context was activated.
        """
        if context_id in self._contexts:
            self._active_context_id = context_id
            return True
        return False
    
    def get_active_context(self) -> Optional[FocusContext]:
        """Get the currently active context.
        
        Returns:
            Active FocusContext, or None.
        """
        if self._active_context_id:
            return self._contexts.get(self._active_context_id)
        return None
    
    def get_active_context_id(self) -> Optional[str]:
        """Get the ID of the active context.
        
        Returns:
            Active context ID, or None.
        """
        return self._active_context_id
    
    def remove_context(self, context_id: str) -> bool:
        """Remove a focus context.
        
        Args:
            context_id: Context to remove.
            
        Returns:
            True if context was removed.
        """
        if context_id in self._contexts:
            del self._contexts[context_id]
            
            if self._active_context_id == context_id:
                self._active_context_id = (
                    next(iter(self._contexts), None)
                )
            return True
        return False
    
    def iterate_contexts(self) -> Iterator[tuple[str, FocusContext]]:
        """Iterate over all contexts.
        
        Yields:
            Tuples of (context_id, FocusContext).
        """
        yield from self._contexts.items()


@dataclass
class FocusGuard:
    """Guard for automatic focus management.
    
    Ensures focus is properly restored when exiting a scope,
    even if an exception occurs.
    
    Example:
        >>> context = FocusContext()
        >>> with FocusGuard(context, "btn_submit"):
        ...     # perform actions
        ...     pass
    """
    context: FocusContext
    element_id: str
    element_type: Optional[str] = None
    context_data: Optional[dict] = None
    _entered: bool = field(default=False, repr=False)
    
    def __enter__(self) -> FocusGuard:
        """Enter the focus guard scope."""
        self.context.acquire(
            self.element_id,
            self.element_type,
            self.context_data
        )
        self._entered = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the focus guard scope."""
        if self._entered:
            self.context.release()


def create_focus_guard(
    context: FocusContext,
    element_id: str,
    element_type: Optional[str] = None,
    context_data: Optional[dict] = None
) -> FocusGuard:
    """Create a focus guard for automatic focus management.
    
    Args:
        context: Focus context to use.
        element_id: Element to acquire focus on.
        element_type: Optional element type.
        context_data: Optional context data.
        
    Returns:
        Configured FocusGuard.
    """
    return FocusGuard(
        context=context,
        element_id=element_id,
        element_type=element_type,
        context_data=context_data
    )
