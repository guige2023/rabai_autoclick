"""Automation State Machine v2 Action.

Hierarchical state machine with guards and actions.
"""
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import time


class StateMachineEvent(Enum):
    ENTER = "enter"
    EXIT = "exit"
    TRANSITION = "transition"


@dataclass
class State:
    state_id: str
    name: str
    on_enter: Optional[Callable] = None
    on_exit: Optional[Callable] = None
    on_update: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    from_state: str
    to_state: str
    event_name: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None


class AutomationStateMachineV2Action:
    """Hierarchical state machine with guards."""

    def __init__(self, name: str, initial_state: str) -> None:
        self.name = name
        self.initial_state = initial_state
        self.current_state = initial_state
        self.states: Dict[str, State] = {}
        self.transitions: List[Transition] = []
        self.history: List[Dict[str, Any]] = []

    def add_state(self, state: State) -> None:
        self.states[state.state_id] = state

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event_name: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None,
    ) -> None:
        self.transitions.append(Transition(
            from_state=from_state,
            to_state=to_state,
            event_name=event_name,
            guard=guard,
            action=action,
        ))

    def trigger(self, event_name: str) -> bool:
        applicable = [
            t for t in self.transitions
            if t.from_state == self.current_state and t.event_name == event_name
        ]
        for t in applicable:
            if t.guard and not t.guard():
                continue
            old_state = self.current_state
            old = self.states.get(old_state)
            if old and old.on_exit:
                old.on_exit()
            if t.action:
                t.action()
            new = self.states.get(t.to_state)
            if new and new.on_enter:
                new.on_enter()
            self.current_state = t.to_state
            self.history.append({
                "timestamp": time.time(),
                "event": event_name,
                "from": old_state,
                "to": t.to_state,
            })
            return True
        return False

    def update(self) -> None:
        state = self.states.get(self.current_state)
        if state and state.on_update:
            state.on_update()

    def get_state(self) -> str:
        return self.current_state

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.history[-limit:]
