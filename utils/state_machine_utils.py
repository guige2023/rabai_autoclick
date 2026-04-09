"""
State Machine Utilities for UI Automation.

This module provides utilities for implementing state machines to manage
complex automation workflows with defined states and transitions.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class StateTransition:
    """Represents a transition between states."""
    
    def __init__(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Any]] = None
    ):
        self.from_state = from_state
        self.to_state = to_state
        self.event = event
        self.guard = guard
        self.action = action
    
    def can_execute(self) -> bool:
        """Check if transition can execute."""
        if self.guard is None:
            return True
        return self.guard()
    
    def execute(self) -> Any:
        """Execute the transition action."""
        if self.action:
            return self.action()
        return None


@dataclass
class StateMachineConfig:
    """Configuration for a state machine."""
    initial_state: str
    states: list[str]
    final_states: list[str] = field(default_factory=list)
    on_entry: Optional[Callable[[str], None]] = None
    on_exit: Optional[Callable[[str], None]] = None


class StateMachine:
    """
    A simple state machine implementation.
    
    Example:
        sm = StateMachine(initial_state="idle")
        sm.add_state("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")
        
        sm.fire("start")  # Transitions from idle to running
    """
    
    def __init__(self, config: Optional[StateMachineConfig] = None):
        self._config = config
        self._current_state: str = config.initial_state if config else ""
        self._states: set[str] = set(config.states) if config else set()
        self._transitions: dict[tuple[str, str], StateTransition] = {}
        self._transition_map: dict[tuple[str, str], str] = {}  # (from, event) -> to
        self._history: list[str] = []
        
        if config:
            for state in config.states:
                self._states.add(state)
    
    @property
    def current_state(self) -> str:
        """Get the current state."""
        return self._current_state
    
    @property
    def history(self) -> list[str]:
        """Get state history."""
        return list(self._history)
    
    def add_state(self, state: str) -> None:
        """Add a state to the machine."""
        self._states.add(state)
    
    def add_states(self, states: list[str]) -> None:
        """Add multiple states."""
        for state in states:
            self._states.add(state)
    
    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Any]] = None
    ) -> None:
        """
        Add a transition.
        
        Args:
            from_state: Source state
            to_state: Target state
            event: Event that triggers transition
            guard: Optional guard condition
            action: Optional action to execute on transition
        """
        transition = StateTransition(from_state, to_state, event, guard, action)
        self._transitions[(from_state, to_state)] = transition
        self._transition_map[(from_state, event)] = to_state
    
    def can_fire(self, event: str) -> bool:
        """Check if an event can fire in current state."""
        return (self._current_state, event) in self._transition_map
    
    def fire(self, event: str) -> tuple[bool, Optional[str]]:
        """
        Fire an event to transition.
        
        Args:
            event: Event to fire
            
        Returns:
            Tuple of (transition_succeeded, error_message)
        """
        key = (self._current_state, event)
        
        if key not in self._transition_map:
            return False, f"No transition for event '{event}' from state '{self._current_state}'"
        
        to_state = self._transition_map[key]
        transition = self._transitions.get((self._current_state, to_state))
        
        if transition and not transition.can_execute():
            return False, "Guard condition failed"
        
        # Execute on_exit if configured
        if self._config and self._config.on_exit:
            self._config.on_exit(self._current_state)
        
        # Execute transition action
        result = None
        if transition and transition.action:
            result = transition.execute()
        
        # Record history
        self._history.append(self._current_state)
        
        # Change state
        self._current_state = to_state
        
        # Execute on_entry if configured
        if self._config and self._config.on_entry:
            self._config.on_entry(self._current_state)
        
        return True, None
    
    def get_available_events(self) -> list[str]:
        """Get events that can be fired from current state."""
        return [
            event for (state, event) in self._transition_map.keys()
            if state == self._current_state
        ]
    
    def is_in_state(self, state: str) -> bool:
        """Check if currently in a specific state."""
        return self._current_state == state
    
    def is_final_state(self) -> bool:
        """Check if current state is a final state."""
        if not self._config:
            return False
        return self._current_state in self._config.final_states
    
    def reset(self) -> None:
        """Reset to initial state."""
        if self._config:
            self._current_state = self._config.initial_state
        self._history.clear()


class HierarchicalStateMachine(StateMachine):
    """
    Hierarchical/nested state machine.
    
    Supports states within states for complex workflows.
    """
    
    def __init__(self, config: Optional[StateMachineConfig] = None):
        super().__init__(config)
        self._parent_map: dict[str, str] = {}  # child -> parent
        self._substates: dict[str, set[str]] = {}  # parent -> children
    
    def add_substate(self, parent: str, child: str) -> None:
        """Add a substate to a parent state."""
        self._parent_map[child] = parent
        self._substates.setdefault(parent, set()).add(child)
    
    def get_parent_state(self, state: str) -> Optional[str]:
        """Get the parent of a state."""
        return self._parent_map.get(state)
    
    def get_substates(self, state: str) -> set[str]:
        """Get substates of a state."""
        return self._substates.get(state, set())
    
    def is_substate_of(self, state: str, potential_parent: str) -> bool:
        """Check if state is a substate of potential parent."""
        current = self._parent_map.get(state)
        
        while current:
            if current == potential_parent:
                return True
            current = self._parent_map.get(current)
        
        return False


class StateHistory:
    """
    Maintains history of state machine states.
    
    Useful for debugging and reporting.
    """
    
    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self._entries: list[dict[str, Any]] = []
    
    def record(
        self,
        from_state: str,
        to_state: str,
        event: str,
        result: Any = None
    ) -> None:
        """Record a state transition."""
        import time
        import uuid
        
        entry = {
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "from_state": from_state,
            "to_state": to_state,
            "event": event,
            "result": result
        }
        
        self._entries.append(entry)
        
        if len(self._entries) > self.max_history:
            self._entries.pop(0)
    
    def get_last(self, count: int = 1) -> list[dict[str, Any]]:
        """Get the last n transitions."""
        return self._entries[-count:] if self._entries else []
    
    def get_by_state(self, state: str) -> list[dict[str, Any]]:
        """Get all transitions involving a state."""
        return [
            e for e in self._entries
            if e["from_state"] == state or e["to_state"] == state
        ]
    
    def clear(self) -> None:
        """Clear history."""
        self._entries.clear()
