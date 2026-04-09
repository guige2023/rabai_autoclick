"""
UI state machine utilities for managing UI automation states.

Provides state machine implementation for complex UI workflows
with transitions, guards, and actions.

Author: Auto-generated
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class StateTransitionResult(Enum):
    """Result of a state transition."""
    SUCCESS = auto()
    REJECTED = auto()
    INVALID = auto()


@dataclass
class StateTransition:
    """A transition between states."""
    from_state: str
    to_state: str
    event: str
    guard: Callable[[], bool] | None = None
    action: Callable[[], None] | None = None
    
    def can_execute(self) -> bool:
        """Check if transition can execute (guard passes)."""
        if self.guard is None:
            return True
        return self.guard()


@dataclass
class StateMachineEvent:
    """An event to trigger state transition."""
    name: str
    data: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class StateInfo:
    """Information about a state."""
    name: str
    enter_action: Callable[[], None] | None = None
    exit_action: Callable[[], None] | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class TransitionResult:
    """Result of a transition attempt."""
    success: bool
    result: StateTransitionResult
    from_state: str
    to_state: str
    event: str
    message: str = ""


class UIStateMachine:
    """
    State machine for UI automation.
    
    Example:
        sm = UIStateMachine("dialog_flow")
        sm.add_state("closed")
        sm.add_state("open")
        sm.add_transition("closed", "open", "open_dialog")
        sm.add_transition("open", "closed", "close_dialog")
        
        sm.handle_event(StateMachineEvent("open_dialog"))
        assert sm.current_state == "open"
    """
    
    def __init__(self, name: str, initial_state: str = ""):
        self._name = name
        self._initial_state = initial_state
        self._current_state = initial_state
        self._states: dict[str, StateInfo] = {}
        self._transitions: dict[tuple[str, str], list[StateTransition]] = defaultdict(list)
        self._transition_history: list[TransitionResult] = []
        self._listeners: dict[str, list[Callable[[str, str], None]]] = defaultdict(list)
        self._active = False
    
    @property
    def name(self) -> str:
        """State machine name."""
        return self._name
    
    @property
    def current_state(self) -> str:
        """Current state."""
        return self._current_state
    
    @property
    def initial_state(self) -> str:
        """Initial state."""
        return self._initial_state
    
    def add_state(
        self,
        name: str,
        enter_action: Callable[[], None] | None = None,
        exit_action: Callable[[], None] | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Add a state to the state machine."""
        self._states[name] = StateInfo(
            name=name,
            enter_action=enter_action,
            exit_action=exit_action,
            metadata=metadata or {},
        )
    
    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Callable[[], bool] | None = None,
        action: Callable[[], None] | None = None,
    ) -> StateTransition:
        """
        Add a transition between states.
        
        Args:
            from_state: Source state
            to_state: Target state
            event: Event name to trigger transition
            guard: Optional guard condition
            action: Optional action to execute on transition
            
        Returns:
            The created StateTransition
        """
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
        )
        self._transitions[(from_state, to_state)].append(transition)
        return transition
    
    def handle_event(self, event: StateMachineEvent) -> TransitionResult:
        """
        Handle an event and attempt state transition.
        
        Args:
            event: The event to process
            
        Returns:
            TransitionResult with outcome
        """
        if not self._active:
            return TransitionResult(
                success=False,
                result=StateTransitionResult.INVALID,
                from_state=self._current_state,
                to_state=self._current_state,
                event=event.name,
                message="State machine is not active",
            )
        
        # Find matching transitions
        matching_transitions = self._find_transitions(
            self._current_state, event.name
        )
        
        if not matching_transitions:
            return TransitionResult(
                success=False,
                result=StateTransitionResult.INVALID,
                from_state=self._current_state,
                to_state=self._current_state,
                event=event.name,
                message=f"No transition found for event '{event.name}' from state '{self._current_state}'",
            )
        
        # Find first transition whose guard passes
        for transition in matching_transitions:
            if transition.can_execute():
                return self._execute_transition(transition, event)
        
        return TransitionResult(
            success=False,
            result=StateTransitionResult.REJECTED,
            from_state=self._current_state,
            to_state=self._current_state,
            event=event.name,
            message="All transition guards rejected",
        )
    
    def _find_transitions(
        self, from_state: str, event: str
    ) -> list[StateTransition]:
        """Find all transitions matching state and event."""
        transitions = []
        for (fs, _), trans in self._transitions.items():
            if fs == from_state:
                for t in trans:
                    if t.event == event:
                        transitions.append(t)
        return transitions
    
    def _execute_transition(
        self, transition: StateTransition, event: StateMachineEvent
    ) -> TransitionResult:
        """Execute a state transition."""
        old_state = self._current_state
        
        # Execute exit action
        if old_state in self._states:
            exit_action = self._states[old_state].exit_action
            if exit_action is not None:
                exit_action()
        
        # Execute transition action
        if transition.action is not None:
            transition.action()
        
        # Update state
        self._current_state = transition.to_state
        
        # Execute enter action
        if transition.to_state in self._states:
            enter_action = self._states[transition.to_state].enter_action
            if enter_action is not None:
                enter_action()
        
        # Notify listeners
        self._notify_listeners(old_state, transition.to_state)
        
        result = TransitionResult(
            success=True,
            result=StateTransitionResult.SUCCESS,
            from_state=old_state,
            to_state=transition.to_state,
            event=event.name,
        )
        
        self._transition_history.append(result)
        return result
    
    def _notify_listeners(
        self, from_state: str, to_state: str
    ) -> None:
        """Notify listeners of state change."""
        key = f"{from_state}_to_{to_state}"
        for listener in self._listeners.get(key, []):
            listener(from_state, to_state)
        for listener in self._listeners.get("*", []):
            listener(from_state, to_state)
    
    def add_listener(
        self,
        from_state: str,
        to_state: str,
        callback: Callable[[str, str], None],
    ) -> None:
        """Add a listener for state transitions."""
        key = f"{from_state}_to_{to_state}"
        self._listeners[key].append(callback)
    
    def can_transition(self, event: str) -> bool:
        """Check if an event can trigger a transition from current state."""
        transitions = self._find_transitions(self._current_state, event)
        return any(t.can_execute() for t in transitions)
    
    def get_available_events(self) -> list[str]:
        """Get list of events that can be handled in current state."""
        transitions = self._find_transitions(self._current_state, "")
        return list(set(t.event for t in transitions))
    
    def reset(self) -> None:
        """Reset to initial state."""
        self._current_state = self._initial_state
        self._transition_history.clear()
    
    def start(self) -> None:
        """Start the state machine."""
        self._active = True
    
    def stop(self) -> None:
        """Stop the state machine."""
        self._active = False
    
    def get_history(self) -> list[TransitionResult]:
        """Get transition history."""
        return list(self._transition_history)


def create_dialog_flow_sm() -> UIStateMachine:
    """
    Create a typical dialog flow state machine.
    
    States: closed -> opening -> open -> closing -> closed
    """
    sm = UIStateMachine("dialog_flow", initial_state="closed")
    
    def on_open():
        print("Dialog opening")
    
    def on_close():
        print("Dialog closing")
    
    sm.add_state("closed", metadata={"modal": False})
    sm.add_state("opening", enter_action=on_open)
    sm.add_state("open")
    sm.add_state("closing", enter_action=on_close)
    
    sm.add_transition("closed", "opening", "show")
    sm.add_transition("opening", "open", "animation_complete")
    sm.add_transition("open", "closing", "hide")
    sm.add_transition("closing", "closed", "animation_complete")
    
    return sm
