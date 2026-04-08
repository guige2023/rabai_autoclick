"""State Machine Action Module.

Provides state machine implementation for
workflow state management.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Transition type."""
    EXTERNAL = "external"
    INTERNAL = "internal"


@dataclass
class State:
    """State definition."""
    state_id: str
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None


@dataclass
class Transition:
    """State transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None


class StateMachine:
    """State machine implementation."""

    def __init__(self, machine_id: str, initial_state: str):
        self.machine_id = machine_id
        self.current_state = initial_state
        self._states: Dict[str, State] = {}
        self._transitions: List[Transition] = []
        self._history: List[Dict] = []

    def add_state(self, state: State) -> None:
        """Add a state."""
        self._states[state.state_id] = state

    def add_transition(self, transition: Transition) -> None:
        """Add a transition."""
        self._transitions.append(transition)

    def trigger(self, event: str, data: Optional[Dict] = None) -> bool:
        """Trigger an event."""
        for transition in self._transitions:
            if transition.from_state != self.current_state:
                continue
            if transition.event != event:
                continue

            if transition.guard and not transition.guard(data):
                continue

            state = self._states.get(self.current_state)
            if state and state.exit_action:
                state.exit_action()

            self._history.append({
                "from": self.current_state,
                "to": transition.to_state,
                "event": event,
                "timestamp": time.time()
            })

            self.current_state = transition.to_state

            state = self._states.get(self.current_state)
            if state and state.entry_action:
                state.entry_action()

            if transition.action:
                transition.action(data)

            return True

        return False

    def get_state(self) -> str:
        """Get current state."""
        return self.current_state

    def get_history(self) -> List[Dict]:
        """Get state history."""
        return self._history


class StateMachineAction(BaseAction):
    """Action for state machine operations."""

    def __init__(self):
        super().__init__("state_machine")
        self._machines: Dict[str, StateMachine] = {}

    def execute(self, params: Dict) -> ActionResult:
        """Execute state machine action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_state":
                return self._add_state(params)
            elif operation == "trigger":
                return self._trigger(params)
            elif operation == "get_state":
                return self._get_state(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create state machine."""
        machine_id = params.get("machine_id", "")
        initial = params.get("initial_state", "initial")

        machine = StateMachine(machine_id, initial)
        self._machines[machine_id] = machine

        return ActionResult(success=True, data={"machine_id": machine_id})

    def _add_state(self, params: Dict) -> ActionResult:
        """Add state to machine."""
        machine_id = params.get("machine_id", "")
        machine = self._machines.get(machine_id)

        if not machine:
            return ActionResult(success=False, message="Machine not found")

        state = State(
            state_id=params.get("state_id", ""),
            name=params.get("name", "")
        )

        machine.add_state(state)

        return ActionResult(success=True)

    def _trigger(self, params: Dict) -> ActionResult:
        """Trigger event."""
        machine_id = params.get("machine_id", "")
        machine = self._machines.get(machine_id)

        if not machine:
            return ActionResult(success=False, message="Machine not found")

        success = machine.trigger(
            params.get("event", ""),
            params.get("data")
        )

        return ActionResult(success=success, data={
            "success": success,
            "current_state": machine.get_state()
        })

    def _get_state(self, params: Dict) -> ActionResult:
        """Get current state."""
        machine_id = params.get("machine_id", "")
        machine = self._machines.get(machine_id)

        if not machine:
            return ActionResult(success=False, message="Machine not found")

        return ActionResult(success=True, data={
            "machine_id": machine_id,
            "current_state": machine.get_state()
        })
