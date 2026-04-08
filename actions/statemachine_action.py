"""State machine action module for RabAI AutoClick.

Provides state machine utilities:
- StateMachine: Simple state machine
- Transition: State transition
- StateMachineBuilder: Build state machines
"""

from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StateMachine:
    """Simple state machine."""

    def __init__(self, machine_id: str = ""):
        self.machine_id = machine_id or str(uuid.uuid4())
        self._states: Set[str] = set()
        self._transitions: Dict[str, Dict[str, str]] = {}
        self._current_state: Optional[str] = None
        self._initial_state: Optional[str] = None
        self._final_states: Set[str] = set()

    def add_state(self, state: str, is_initial: bool = False, is_final: bool = False) -> None:
        """Add a state."""
        self._states.add(state)
        if is_initial:
            self._initial_state = state
            self._current_state = state
        if is_final:
            self._final_states.add(state)

    def add_transition(self, from_state: str, event: str, to_state: str) -> None:
        """Add a transition."""
        if from_state not in self._transitions:
            self._transitions[from_state] = {}
        self._transitions[from_state][event] = to_state

    def set_initial_state(self, state: str) -> None:
        """Set initial state."""
        if state in self._states:
            self._initial_state = state
            self._current_state = state

    def trigger(self, event: str) -> Dict[str, Any]:
        """Trigger an event."""
        if self._current_state is None:
            return {"success": False, "error": "State machine not initialized"}

        if self._current_state in self._final_states:
            return {"success": False, "error": "State machine in final state"}

        transitions = self._transitions.get(self._current_state, {})

        if event not in transitions:
            return {"success": False, "error": f"No transition for event '{event}' from state '{self._current_state}'"}

        old_state = self._current_state
        self._current_state = transitions[event]

        return {
            "success": True,
            "from_state": old_state,
            "to_state": self._current_state,
            "event": event,
        }

    def get_current_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    def is_final(self) -> bool:
        """Check if in final state."""
        return self._current_state in self._final_states

    def reset(self) -> None:
        """Reset to initial state."""
        self._current_state = self._initial_state


class StateMachineBuilder:
    """Build state machines from config."""

    @staticmethod
    def build(config: Dict[str, Any]) -> StateMachine:
        """Build state machine from config."""
        machine_id = config.get("id", str(uuid.uuid4()))
        sm = StateMachine(machine_id)

        for state_config in config.get("states", []):
            state_name = state_config["name"]
            is_initial = state_config.get("initial", False)
            is_final = state_config.get("final", False)
            sm.add_state(state_name, is_initial=is_initial, is_final=is_final)

        for trans_config in config.get("transitions", []):
            sm.add_transition(
                trans_config["from"],
                trans_config["event"],
                trans_config["to"],
            )

        return sm


class StateMachineAction(BaseAction):
    """State machine action."""
    action_type = "statemachine"
    display_name = "状态机"
    description = "简单状态机"

    def __init__(self):
        super().__init__()
        self._machines: Dict[str, StateMachine] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_state":
                return self._add_state(params)
            elif operation == "add_transition":
                return self._add_transition(params)
            elif operation == "trigger":
                return self._trigger(params)
            elif operation == "current":
                return self._current(params)
            elif operation == "reset":
                return self._reset(params)
            elif operation == "build":
                return self._build(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"StateMachine error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create state machine."""
        machine_id = params.get("machine_id", str(uuid.uuid4()))

        sm = StateMachine(machine_id)
        self._machines[machine_id] = sm

        return ActionResult(success=True, message=f"State machine created: {machine_id}", data={"machine_id": machine_id})

    def _add_state(self, params: Dict[str, Any]) -> ActionResult:
        """Add a state."""
        machine_id = params.get("machine_id")
        state = params.get("state")
        is_initial = params.get("initial", False)
        is_final = params.get("final", False)

        if not machine_id or not state:
            return ActionResult(success=False, message="machine_id and state are required")

        sm = self._machines.get(machine_id)
        if not sm:
            return ActionResult(success=False, message=f"State machine not found: {machine_id}")

        sm.add_state(state, is_initial=is_initial, is_final=is_final)

        return ActionResult(success=True, message=f"State added: {state}")

    def _add_transition(self, params: Dict[str, Any]) -> ActionResult:
        """Add a transition."""
        machine_id = params.get("machine_id")
        from_state = params.get("from_state")
        event = params.get("event")
        to_state = params.get("to_state")

        if not machine_id or not from_state or not event or not to_state:
            return ActionResult(success=False, message="machine_id, from_state, event, and to_state are required")

        sm = self._machines.get(machine_id)
        if not sm:
            return ActionResult(success=False, message=f"State machine not found: {machine_id}")

        sm.add_transition(from_state, event, to_state)

        return ActionResult(success=True, message=f"Transition added: {from_state} --[{event}]--> {to_state}")

    def _trigger(self, params: Dict[str, Any]) -> ActionResult:
        """Trigger an event."""
        machine_id = params.get("machine_id")
        event = params.get("event")

        if not machine_id or not event:
            return ActionResult(success=False, message="machine_id and event are required")

        sm = self._machines.get(machine_id)
        if not sm:
            return ActionResult(success=False, message=f"State machine not found: {machine_id}")

        result = sm.trigger(event)

        return ActionResult(success=result["success"], message=result.get("error", f"Transitioned to {result.get('to_state')}"), data=result)

    def _current(self, params: Dict[str, Any]) -> ActionResult:
        """Get current state."""
        machine_id = params.get("machine_id")

        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")

        sm = self._machines.get(machine_id)
        if not sm:
            return ActionResult(success=False, message=f"State machine not found: {machine_id}")

        current = sm.get_current_state()
        is_final = sm.is_final()

        return ActionResult(success=True, message=f"Current state: {current}", data={"current": current, "is_final": is_final})

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset state machine."""
        machine_id = params.get("machine_id")

        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")

        sm = self._machines.get(machine_id)
        if not sm:
            return ActionResult(success=False, message=f"State machine not found: {machine_id}")

        sm.reset()

        return ActionResult(success=True, message="State machine reset")

    def _build(self, params: Dict[str, Any]) -> ActionResult:
        """Build state machine from config."""
        config = params.get("config")

        if not config:
            return ActionResult(success=False, message="config is required")

        try:
            sm = StateMachineBuilder.build(config)
            self._machines[sm.machine_id] = sm
            return ActionResult(success=True, message=f"State machine built: {sm.machine_id}", data={"machine_id": sm.machine_id})
        except Exception as e:
            return ActionResult(success=False, message=f"Build failed: {e}")
