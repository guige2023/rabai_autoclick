"""Workflow State Machine Action Module.

State machine workflow with guards, actions, and transitions.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .state_machine_action import State, StateMachine, StateTransition, StateMachineConfig


class WorkflowEvent(Enum):
    """Workflow events."""
    START = "start"
    ADVANCE = "advance"
    REVERT = "revert"
    COMPLETE = "complete"
    CANCEL = "cancel"
    PAUSE = "pause"
    RESUME = "resume"


@dataclass
class WorkflowState:
    """Workflow state with metadata."""
    state: State
    entered_at: float
    context: dict = field(default_factory=dict)
    history: list = field(default_factory=list)


class StateMachineWorkflow:
    """Workflow implemented as a state machine."""

    def __init__(self, name: str, initial_state: str) -> None:
        self.name = name
        self._sm = StateMachine(name, initial_state)
        self._current_state_data: WorkflowState | None = None
        self._on_enter_handlers: dict[str, Callable] = {}
        self._on_exit_handlers: dict[str, Callable] = {}

    def add_state(
        self,
        state_name: str,
        on_enter: Callable | None = None,
        on_exit: Callable | None = None
    ) -> StateMachineWorkflow:
        """Add a state to the workflow."""
        self._sm.add_state(state_name)
        if on_enter:
            self._on_enter_handlers[state_name] = on_enter
        if on_exit:
            self._on_exit_handlers[state_name] = on_exit
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Callable[[dict], bool] | None = None,
        action: Callable | None = None
    ) -> StateMachineWorkflow:
        """Add a transition."""
        self._sm.add_transition(from_state, to_state, event, guard, action)
        return self

    async def start(self, context: dict | None = None) -> str:
        """Start the workflow."""
        initial = self._sm.current_state
        self._current_state_data = WorkflowState(
            state=State(initial, {}),
            entered_at=time.time(),
            context=dict(context or {})
        )
        handler = self._on_enter_handlers.get(initial)
        if handler:
            result = handler(self._current_state_data.context)
            if asyncio.iscoroutine(result):
                await result
        return initial

    async def send_event(self, event: WorkflowEvent, **context) -> tuple[bool, str]:
        """Send an event to the workflow."""
        if not self._current_state_data:
            return False, "Workflow not started"
        old_state = self._sm.current_state
        handler = self._on_exit_handlers.get(old_state)
        if handler:
            result = handler(self._current_state_data.context)
            if asyncio.iscoroutine(result):
                await result
        try:
            await self._sm.send(event.value, **context)
        except Exception:
            return False, "Transition not allowed"
        new_state = self._sm.current_state
        self._current_state_data.state = State(new_state, {})
        self._current_state_data.entered_at = time.time()
        self._current_state_data.history.append({
            "from": old_state,
            "to": new_state,
            "event": event.value,
            "timestamp": time.time()
        })
        handler = self._on_enter_handlers.get(new_state)
        if handler:
            result = handler(self._current_state_data.context)
            if asyncio.iscoroutine(result):
                await result
        return True, new_state

    def get_current_state(self) -> str:
        """Get current state name."""
        return self._sm.current_state if self._sm else None

    def get_context(self) -> dict:
        """Get workflow context."""
        return self._current_state_data.context if self._current_state_data else {}

    def get_history(self) -> list:
        """Get state transition history."""
        return self._current_state_data.history if self._current_state_data else []
