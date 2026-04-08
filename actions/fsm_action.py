"""FSM action module for RabAI AutoClick.

Provides finite state machine implementation:
- StateMachine: Finite state machine
- State: State representation
- Transition: Transition between states
- FSMBuilder: Build FSM from config
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StateType(Enum):
    """State type."""
    INITIAL = "initial"
    NORMAL = "normal"
    FINAL = "final"


@dataclass
class State:
    """State representation."""
    name: str
    state_type: StateType = StateType.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """Transition between states."""
    from_state: str
    to_state: str
    event: str
    condition: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], Any]] = None


class FiniteStateMachine:
    """Finite state machine."""

    def __init__(self, machine_id: str = ""):
        self.machine_id = machine_id or str(uuid.uuid4())
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = {}
        self._current_state: Optional[str] = None
        self._initial_state: Optional[str] = None
        self._final_states: Set[str] = set()
        self._history: List[str] = []

    def add_state(self, state: State) -> None:
        """Add a state."""
        self._states[state.name] = state
        if state.state_type == StateType.INITIAL:
            self._initial_state = state.name
        if state.state_type == StateType.FINAL:
            self._final_states.add(state.name)

    def add_transition(self, transition: Transition) -> None:
        """Add a transition."""
        key = f"{transition.from_state}:{transition.event}"
        if key not in self._transitions:
            self._transitions[key] = []
        self._transitions[key].append(transition)

    def set_initial_state(self, state_name: str) -> None:
        """Set initial state."""
        if state_name in self._states:
            self._initial_state = state_name
            self._current_state = state_name

    def get_current_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    def get_states(self) -> List[str]:
        """Get all state names."""
        return list(self._states.keys())

    def is_final_state(self) -> bool:
        """Check if in final state."""
        return self._current_state in self._final_states

    def trigger(self, event: str) -> Dict[str, Any]:
        """Trigger an event."""
        if self._current_state is None:
            return {"success": False, "error": "Machine not initialized"}

        if self.is_final_state():
            return {"success": False, "error": "Machine in final state"}

        key = f"{self._current_state}:{event}"
        transitions = self._transitions.get(key, [])

        for transition in transitions:
            if transition.condition is None or transition.condition():
                if transition.action:
                    transition.action()

                old_state = self._current_state
                self._current_state = transition.to_state
                self._history.append(transition.to_state)

                return {
                    "success": True,
                    "from_state": old_state,
                    "to_state": transition.to_state,
                    "event": event,
                }

        return {"success": False, "error": f"No valid transition for event '{event}' from state '{self._current_state}'"}

    def get_history(self) -> List[str]:
        """Get state history."""
        return self._history.copy()

    def reset(self) -> None:
        """Reset the machine."""
        self._current_state = self._initial_state
        self._history.clear()


class FSMBuilder:
    """Build FSM from config."""

    @staticmethod
    def build(config: Dict[str, Any]) -> FiniteStateMachine:
        """Build FSM from config."""
        machine_id = config.get("id", str(uuid.uuid4()))
        fsm = FiniteStateMachine(machine_id)

        for state_config in config.get("states", []):
            state_type_str = state_config.get("type", "normal")
            state_type = StateType.NORMAL
            if state_type_str == "initial":
                state_type = StateType.INITIAL
            elif state_type_str == "final":
                state_type = StateType.FINAL

            state = State(
                name=state_config["name"],
                state_type=state_type,
                metadata=state_config.get("metadata", {}),
            )
            fsm.add_state(state)

        for trans_config in config.get("transitions", []):
            transition = Transition(
                from_state=trans_config["from"],
                to_state=trans_config["to"],
                event=trans_config["event"],
            )
            fsm.add_transition(transition)

        initial = config.get("initial")
        if initial:
            fsm.set_initial_state(initial)

        return fsm


class FSMAction(BaseAction):
    """Finite state machine action."""
    action_type = "fsm"
    display_name = "有限状态机"
    description = "状态机和工作流"

    def __init__(self):
        super().__init__()
        self._machines: Dict[str, FiniteStateMachine] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "trigger":
                return self._trigger(params)
            elif operation == "state":
                return self._get_state(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "reset":
                return self._reset(params)
            elif operation == "build":
                return self._build(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"FSM error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a FSM."""
        machine_id = params.get("machine_id", str(uuid.uuid4()))

        fsm = FiniteStateMachine(machine_id)
        self._machines[machine_id] = fsm

        return ActionResult(success=True, message=f"FSM created: {machine_id}", data={"machine_id": machine_id})

    def _trigger(self, params: Dict[str, Any]) -> ActionResult:
        """Trigger an event."""
        machine_id = params.get("machine_id")
        event = params.get("event")

        if not machine_id or not event:
            return ActionResult(success=False, message="machine_id and event are required")

        fsm = self._machines.get(machine_id)
        if not fsm:
            return ActionResult(success=False, message=f"FSM not found: {machine_id}")

        result = fsm.trigger(event)

        return ActionResult(success=result["success"], message=result.get("error", "Event triggered"), data=result)

    def _get_state(self, params: Dict[str, Any]) -> ActionResult:
        """Get current state."""
        machine_id = params.get("machine_id")

        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")

        fsm = self._machines.get(machine_id)
        if not fsm:
            return ActionResult(success=False, message=f"FSM not found: {machine_id}")

        current = fsm.get_current_state()
        states = fsm.get_states()
        is_final = fsm.is_final_state()

        return ActionResult(success=True, message=f"Current state: {current}", data={"current": current, "states": states, "is_final": is_final})

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get state history."""
        machine_id = params.get("machine_id")

        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")

        fsm = self._machines.get(machine_id)
        if not fsm:
            return ActionResult(success=False, message=f"FSM not found: {machine_id}")

        history = fsm.get_history()

        return ActionResult(success=True, message=f"{len(history)} transitions", data={"history": history})

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset a FSM."""
        machine_id = params.get("machine_id")

        if not machine_id:
            return ActionResult(success=False, message="machine_id is required")

        fsm = self._machines.get(machine_id)
        if not fsm:
            return ActionResult(success=False, message=f"FSM not found: {machine_id}")

        fsm.reset()

        return ActionResult(success=True, message="FSM reset")

    def _build(self, params: Dict[str, Any]) -> ActionResult:
        """Build FSM from config."""
        config = params.get("config")

        if not config:
            return ActionResult(success=False, message="config is required")

        try:
            fsm = FSMBuilder.build(config)
            self._machines[fsm.machine_id] = fsm
            return ActionResult(success=True, message=f"FSM built: {fsm.machine_id}", data={"machine_id": fsm.machine_id})
        except Exception as e:
            return ActionResult(success=False, message=f"Build failed: {e}")
