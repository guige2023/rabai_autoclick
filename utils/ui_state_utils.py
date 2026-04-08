"""
UI state machine and state transition utilities.

Provides utilities for building and managing UI state machines,
tracking state transitions, and handling state-based automation.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Callable, Any, Set, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import time


T = TypeVar('T')


class TransitionType(Enum):
    """Types of state transitions."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class StateTransition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    trigger: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class StateConfig:
    """Configuration for a state."""
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    do_action: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False


class StateMachine(Generic[T]):
    """Generic state machine implementation."""
    
    def __init__(self, initial_state: Optional[str] = None):
        """Initialize state machine.
        
        Args:
            initial_state: Name of initial state
        """
        self._states: Dict[str, StateConfig] = {}
        self._transitions: Dict[str, Dict[str, List[StateTransition]]] = {}
        self._current_state: Optional[str] = initial_state
        self._history: List[StateTransition] = []
        self._listeners: List[Callable[[str, str], None]] = []
        self._active = True
    
    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        is_initial: bool = False,
        is_final: bool = False
    ) -> "StateMachine":
        """Add a state to the machine.
        
        Args:
            name: State name
            entry_action: Action on entering state
            exit_action: Action on exiting state
            do_action: Action while in state
            is_initial: Set as initial state
            is_final: Set as final state
            
        Returns:
            Self for chaining
        """
        config = StateConfig(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action,
            is_initial=is_initial,
            is_final=is_final
        )
        
        self._states[name] = config
        
        if is_initial:
            self._current_state = name
        
        if name not in self._transitions:
            self._transitions[name] = {}
        
        return self
    
    def add_transition(
        self,
        from_state: str,
        to_state: str,
        trigger: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None
    ) -> "StateMachine":
        """Add a transition between states.
        
        Args:
            from_state: Source state
            to_state: Target state
            trigger: Event that triggers transition
            guard: Optional condition function
            action: Optional action on transition
            
        Returns:
            Self for chaining
        """
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            trigger=trigger,
            guard=guard,
            action=action
        )
        
        if from_state not in self._transitions:
            self._transitions[from_state] = {}
        
        if trigger not in self._transitions[from_state]:
            self._transitions[from_state][trigger] = []
        
        self._transitions[from_state][trigger].append(transition)
        
        return self
    
    def trigger(self, event: str, data: Any = None) -> bool:
        """Trigger an event.
        
        Args:
            event: Event name
            data: Optional event data
            
        Returns:
            True if transition was made
        """
        if not self._active or not self._current_state:
            return False
        
        transitions = self._transitions.get(self._current_state, {}).get(event, [])
        
        for transition in transitions:
            if transition.guard and not transition.guard(data):
                continue
            
            # Execute transition
            self._execute_transition(transition, data)
            return True
        
        return False
    
    def _execute_transition(self, transition: StateTransition, data: Any) -> None:
        """Execute a state transition.
        
        Args:
            transition: Transition to execute
            data: Event data
        """
        old_state = self._current_state
        
        # Exit current state
        if old_state and self._states[old_state].exit_action:
            self._states[old_state].exit_action(data)
        
        # Execute transition action
        if transition.action:
            transition.action(data)
        
        # Enter new state
        self._current_state = transition.to_state
        
        if transition.to_state in self._states:
            entry_action = self._states[transition.to_state].entry_action
            if entry_action:
                entry_action(data)
        
        # Record history
        transition.timestamp = time.time()
        self._history.append(transition)
        
        # Notify listeners
        for listener in self._listeners:
            try:
                listener(old_state, transition.to_state)
            except Exception:
                pass
    
    def get_current_state(self) -> Optional[str]:
        """Get current state name."""
        return self._current_state
    
    def is_in_state(self, state: str) -> bool:
        """Check if in specific state."""
        return self._current_state == state
    
    def can_trigger(self, event: str) -> bool:
        """Check if event can be triggered."""
        if not self._current_state:
            return False
        transitions = self._transitions.get(self._current_state, {}).get(event, [])
        return len(transitions) > 0
    
    def get_available_events(self) -> List[str]:
        """Get list of events that can be triggered."""
        if not self._current_state:
            return []
        return list(self._transitions.get(self._current_state, {}).keys())
    
    def add_listener(self, listener: Callable[[str, str], None]) -> None:
        """Add a state change listener.
        
        Args:
            listener: Function called on state change (from_state, to_state)
        """
        self._listeners.append(listener)
    
    def get_history(self) -> List[StateTransition]:
        """Get transition history."""
        return list(self._history)
    
    def start(self) -> None:
        """Start the state machine."""
        self._active = True
        if self._current_state and self._states[self._current_state].entry_action:
            self._states[self._current_state].entry_action(None)
    
    def stop(self) -> None:
        """Stop the state machine."""
        self._active = False
    
    def reset(self) -> None:
        """Reset to initial state."""
        self._history.clear()
        for name, config in self._states.items():
            if config.is_initial:
                self._current_state = name
                break


class HierarchicalStateMachine(StateMachine):
    """Hierarchical/nested state machine."""
    
    def __init__(self, initial_state: Optional[str] = None):
        """Initialize hierarchical state machine."""
        super().__init__(initial_state)
        self._parent_states: Dict[str, str] = {}
        self._child_machines: Dict[str, StateMachine] = {}
    
    def add_parent_state(
        self,
        name: str,
        parent: Optional[str] = None,
        **kwargs
    ) -> "HierarchicalStateMachine":
        """Add a parent (container) state.
        
        Args:
            name: State name
            parent: Parent state name (for nesting)
            **kwargs: Same as add_state
        """
        self.add_state(name, **kwargs)
        if parent:
            self._parent_states[name] = parent
        return self
    
    def get_parent_state(self, state: str) -> Optional[str]:
        """Get parent of a state."""
        return self._parent_states.get(state)
    
    def get_root_state(self, state: str) -> str:
        """Get root (top-level) state."""
        while True:
            parent = self.get_parent_state(state)
            if parent is None:
                return state
            state = parent


class UIAutomationState:
    """Pre-built state machine for UI automation."""
    
    def __init__(self):
        """Initialize UI automation state machine."""
        self.machine = StateMachine("idle")
        self._setup_states()
    
    def _setup_states(self) -> None:
        """Setup standard UI states."""
        self.machine.add_state("idle", is_initial=True)
        self.machine.add_state("waiting")
        self.machine.add_state("running")
        self.machine.add_state("paused")
        self.machine.add_state("completed")
        self.machine.add_state("error")
        
        self.machine.add_transition("idle", "running", "start")
        self.machine.add_transition("waiting", "running", "resume")
        self.machine.add_transition("running", "waiting", "pause", guard=lambda x: x)
        self.machine.add_transition("running", "completed", "complete")
        self.machine.add_transition("running", "error", "error")
        self.machine.add_transition("waiting", "idle", "cancel")
        self.machine.add_transition("error", "idle", "reset")
    
    def start(self) -> bool:
        """Start automation."""
        return self.machine.trigger("start", True)
    
    def pause(self) -> bool:
        """Pause automation."""
        return self.machine.trigger("pause", True)
    
    def resume(self) -> bool:
        """Resume automation."""
        return self.machine.trigger("resume")
    
    def stop(self) -> bool:
        """Stop automation."""
        return self.machine.trigger("cancel")
    
    def complete(self) -> bool:
        """Mark as completed."""
        return self.machine.trigger("complete")
    
    def error(self) -> bool:
        """Mark as error."""
        return self.machine.trigger("error")
    
    def reset(self) -> bool:
        """Reset to idle."""
        return self.machine.trigger("reset")
    
    @property
    def is_running(self) -> bool:
        """Check if running."""
        return self.machine.is_in_state("running")
    
    @property
    def is_paused(self) -> bool:
        """Check if paused."""
        return self.machine.is_in_state("paused")
    
    @property
    def is_idle(self) -> bool:
        """Check if idle."""
        return self.machine.is_in_state("idle")
    
    @property
    def state(self) -> str:
        """Get current state."""
        return self.machine.get_current_state() or "unknown"


def create_ui_state_machine() -> UIAutomationState:
    """Create a UI automation state machine."""
    return UIAutomationState()


def create_two_state_machine(
    state_a: str,
    state_b: str,
    initial: Optional[str] = None
) -> StateMachine:
    """Create a simple two-state machine.
    
    Args:
        state_a: First state name
        state_b: Second state name
        initial: Initial state
        
    Returns:
        StateMachine instance
    """
    machine = StateMachine(initial or state_a)
    machine.add_state(state_a, is_initial=(state_a == (initial or state_a)))
    machine.add_state(state_b, is_initial=(state_b == initial))
    machine.add_transition(state_a, state_b, "toggle")
    machine.add_transition(state_b, state_a, "toggle")
    return machine
