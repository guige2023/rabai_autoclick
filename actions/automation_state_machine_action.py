"""
Automation State Machine Action Module

Implements a state machine for managing automation workflow states.
Supports hierarchical states, guards, and transition actions.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
from datetime import datetime

T = TypeVar('T')


class StateType(Enum):
    """Type of state."""
    INITIAL = "initial"
    NORMAL = "normal"
    FINAL = "final"
    CHOICE = "choice"
    HISTORY = "history"
    PARALLEL = "parallel"


@dataclass
class State:
    """A state in the state machine."""
    id: str
    name: str
    state_type: StateType = StateType.NORMAL
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    do_action: Optional[Callable] = None
    parent: Optional[str] = None
    children: list[str] = field(default_factory=list)


@dataclass
class Transition:
    """A state transition."""
    source: str
    target: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None
    conditions: list[str] = field(default_factory=list)


@dataclass
class TransitionResult:
    """Result of a transition attempt."""
    success: bool
    from_state: str
    to_state: Optional[str]
    event: str
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class StateMachineHistory:
    """History of state machine execution."""
    transitions: list[TransitionResult] = field(default_factory=list)
    state_visits: dict[str, int] = field(default_factory=dict)
    total_transitions: int = 0


class AutomationStateMachine:
    """
    State machine for automation workflows.
    
    Example:
        sm = AutomationStateMachine()
        
        sm.add_state("idle", StateType.INITIAL)
        sm.add_state("running")
        sm.add_state("completed", StateType.FINAL)
        
        sm.add_transition("idle", "running", "start")
        sm.add_transition("running", "completed", "finish")
        
        sm.start()
        
        result = sm.trigger_event("start")
    """
    
    def __init__(self, name: str = "state_machine"):
        self.name = name
        self._states: dict[str, State] = {}
        self._transitions: dict[str, list[Transition]] = {}
        self._current_state: Optional[str] = None
        self._history = StateMachineHistory()
        self._running = False
        self._context: dict[str, Any] = {}
        self._state_handlers: dict[str, Callable] = {}
        self._lock = asyncio.Lock()
    
    def add_state(
        self,
        state_id: str,
        state_type: StateType = StateType.NORMAL,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        parent: Optional[str] = None
    ) -> None:
        """
        Add a state to the state machine.
        
        Args:
            state_id: Unique state identifier
            state_type: Type of state
            entry_action: Action to execute on entry
            exit_action: Action to execute on exit
            do_action: Action to execute while in state
            parent: Parent state ID for hierarchical states
        """
        state = State(
            id=state_id,
            name=state_id,
            state_type=state_type,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action,
            parent=parent
        )
        
        self._states[state_id] = state
        self._transitions[state_id] = []
        
        if parent and parent in self._states:
            self._states[parent].children.append(state_id)
        
        if state_type == StateType.INITIAL and self._current_state is None:
            self._current_state = state_id
    
    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None
    ) -> None:
        """
        Add a transition between states.
        
        Args:
            source: Source state ID
            target: Target state ID
            event: Event that triggers this transition
            guard: Optional guard condition
            action: Optional action to execute on transition
        """
        if source not in self._transitions:
            self._transitions[source] = []
        
        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action
        )
        
        self._transitions[source].append(transition)
    
    def add_choice_state(
        self,
        state_id: str,
        choice_fn: Callable[[], str]
    ) -> None:
        """
        Add a choice state with a function that returns the next state.
        
        Args:
            state_id: Choice state ID
            choice_fn: Function that returns target state ID
        """
        self.add_state(state_id, StateType.CHOICE)
        self._state_handlers[state_id] = choice_fn
    
    def start(self, initial_state: Optional[str] = None) -> None:
        """Start the state machine."""
        self._running = True
        
        if initial_state:
            self._current_state = initial_state
        elif self._current_state is None:
            initial_states = [
                s.id for s in self._states.values()
                if s.state_type == StateType.INITIAL
            ]
            if initial_states:
                self._current_state = initial_states[0]
        
        if self._current_state:
            self._execute_entry_action(self._current_state)
    
    def stop(self) -> None:
        """Stop the state machine."""
        self._running = False
    
    def _execute_entry_action(self, state_id: str) -> None:
        """Execute entry action for a state."""
        state = self._states.get(state_id)
        if state and state.entry_action:
            try:
                result = state.entry_action(self._context)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                pass
    
    def _execute_exit_action(self, state_id: str) -> None:
        """Execute exit action for a state."""
        state = self._states.get(state_id)
        if state and state.exit_action:
            try:
                result = state.exit_action(self._context)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                pass
    
    def _execute_transition_action(self, transition: Transition) -> None:
        """Execute transition action."""
        if transition.action:
            try:
                result = transition.action(self._context)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                pass
    
    async def trigger_event(self, event: str, **context) -> TransitionResult:
        """
        Trigger an event and attempt to transition.
        
        Args:
            event: Event name
            **context: Additional context data
            
        Returns:
            TransitionResult indicating success/failure
        """
        if not self._running:
            return TransitionResult(
                success=False,
                from_state=self._current_state or "none",
                to_state=None,
                event=event,
                error="State machine is not running"
            )
        
        self._context.update(context)
        
        from_state = self._current_state
        
        if from_state not in self._transitions:
            return TransitionResult(
                success=False,
                from_state=from_state,
                to_state=None,
                event=event,
                error=f"No transitions defined from state: {from_state}"
            )
        
        for transition in self._transitions[from_state]:
            if transition.event != event:
                continue
            
            if transition.guard:
                try:
                    guard_result = transition.guard()
                    if asyncio.iscoroutine(guard_result):
                        guard_result = await guard_result
                    if not guard_result:
                        continue
                except Exception as e:
                    return TransitionResult(
                        success=False,
                        from_state=from_state,
                        to_state=None,
                        event=event,
                        error=f"Guard evaluation failed: {str(e)}"
                    )
            
            self._execute_exit_action(from_state)
            self._execute_transition_action(transition)
            
            target = transition.target
            
            if target in self._state_handlers:
                target = self._state_handlers[target]()
            
            self._current_state = target
            self._execute_entry_action(target)
            
            transition_result = TransitionResult(
                success=True,
                from_state=from_state,
                to_state=target,
                event=event
            )
            
            self._history.transitions.append(transition_result)
            self._history.state_visits[target] = self._history.state_visits.get(target, 0) + 1
            self._history.total_transitions += 1
            
            return transition_result
        
        return TransitionResult(
            success=False,
            from_state=from_state,
            to_state=None,
            event=event,
            error=f"No matching transition for event '{event}' from state '{from_state}'"
        )
    
    def get_current_state(self) -> Optional[str]:
        """Get the current state ID."""
        return self._current_state
    
    def get_state_info(self, state_id: str) -> Optional[State]:
        """Get information about a state."""
        return self._states.get(state_id)
    
    def get_available_events(self) -> list[str]:
        """Get events available from the current state."""
        if self._current_state not in self._transitions:
            return []
        return [t.event for t in self._transitions[self._current_state]]
    
    def get_history(self) -> StateMachineHistory:
        """Get state machine history."""
        return self._history
    
    def get_stats(self) -> dict[str, Any]:
        """Get state machine statistics."""
        return {
            "name": self.name,
            "current_state": self._current_state,
            "total_states": len(self._states),
            "total_transitions": self._history.total_transitions,
            "state_visits": dict(self._history.state_visits),
            "running": self._running
        }
