"""State machine action module for RabAI AutoClick.

Provides state machine operations:
- StateMachineCreateAction: Create a state machine
- StateMachineTransitionAction: Transition between states
- StateMachineGuardAction: Guard conditions for transitions
- StateMachineHistoryAction: Track state machine history
"""

from typing import Any, Dict, List, Optional, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransitionType(Enum):
    """Types of state transitions."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL


@dataclass
class StateHistoryEntry:
    """Represents a state history entry."""
    state: str
    timestamp: datetime
    event: str
    transition: Optional[str] = None


class StateMachine:
    """Simple state machine implementation."""

    def __init__(self, initial_state: str, name: str = "unnamed"):
        self.name = name
        self.initial_state = initial_state
        self.current_state = initial_state
        self.states: Set[str] = {initial_state}
        self.transitions: Dict[str, List[Transition]] = {}
        self.history: List[StateHistoryEntry] = [
            StateHistoryEntry(state=initial_state, timestamp=datetime.utcnow(), event="init")
        ]

    def add_state(self, state: str) -> None:
        """Add a state to the state machine."""
        self.states.add(state)

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None
    ) -> None:
        """Add a transition to the state machine."""
        self.states.add(from_state)
        self.states.add(to_state)
        transition = Transition(from_state, to_state, event, guard, action)
        if event not in self.transitions:
            self.transitions[event] = []
        self.transitions[event].append(transition)

    def can_transition(self, event: str, context: Any = None) -> bool:
        """Check if a transition is possible for the given event."""
        if event not in self.transitions:
            return False
        for transition in self.transitions[event]:
            if transition.from_state != self.current_state:
                continue
            if transition.guard and not transition.guard(context):
                continue
            return True
        return False

    def trigger(self, event: str, context: Any = None) -> Optional[str]:
        """Trigger an event and transition if possible."""
        if event not in self.transitions:
            return None

        for transition in self.transitions[event]:
            if transition.from_state != self.current_state:
                continue
            if transition.guard and not transition.guard(context):
                continue

            if transition.action:
                transition.action(context)

            prev_state = self.current_state
            self.current_state = transition.to_state
            self.history.append(StateHistoryEntry(
                state=self.current_state,
                timestamp=datetime.utcnow(),
                event=event,
                transition=f"{prev_state} -> {self.current_state}"
            ))
            return self.current_state

        return None

    def get_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get state history."""
        entries = self.history[-limit:]
        return [
            {
                "state": e.state,
                "timestamp": e.timestamp.isoformat(),
                "event": e.event,
                "transition": e.transition
            }
            for e in entries
        ]


_machines: Dict[str, StateMachine] = {}


class StateMachineCreateAction(BaseAction):
    """Create a new state machine."""
    action_type = "state_machine_create"
    display_name = "创建状态机"
    description = "创建新的状态机"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            initial_state = params.get("initial_state", "initial")
            states = params.get("states", [])
            transitions = params.get("transitions", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            machine = StateMachine(initial_state=initial_state, name=name)

            for state in states:
                machine.add_state(state)

            for trans in transitions:
                machine.add_transition(
                    from_state=trans.get("from_state", ""),
                    to_state=trans.get("to_state", ""),
                    event=trans.get("event", ""),
                    guard=None,
                    action=None
                )

            _machines[name] = machine

            return ActionResult(
                success=True,
                message=f"State machine '{name}' created",
                data={
                    "name": name,
                    "initial_state": initial_state,
                    "states": list(machine.states),
                    "current_state": machine.current_state
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"State machine creation failed: {str(e)}")


class StateMachineTransitionAction(BaseAction):
    """Trigger a state transition."""
    action_type = "state_machine_transition"
    display_name = "状态机转换"
    description = "触发状态机转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            event = params.get("event", "")

            if not name:
                return ActionResult(success=False, message="name is required")
            if not event:
                return ActionResult(success=False, message="event is required")

            if name not in _machines:
                return ActionResult(success=False, message=f"State machine '{name}' not found")

            machine = _machines[name]
            prev_state = machine.current_state
            new_state = machine.trigger(event)

            if new_state:
                return ActionResult(
                    success=True,
                    message=f"Transitioned: {prev_state} -> {new_state} on event '{event}'",
                    data={
                        "previous_state": prev_state,
                        "current_state": new_state,
                        "event": event
                    }
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"No transition available for event '{event}' from state '{prev_state}'",
                    data={"current_state": prev_state, "event": event}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"State machine transition failed: {str(e)}")


class StateMachineGuardAction(BaseAction):
    """Define and evaluate guard conditions for transitions."""
    action_type = "state_machine_guard"
    display_name = "状态机守卫"
    description = "评估状态机转换的守卫条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            event = params.get("event", "")
            guard_expression = params.get("guard_expression", "")
            guard_params = params.get("guard_params", {})

            if not name:
                return ActionResult(success=False, message="name is required")
            if not event:
                return ActionResult(success=False, message="event is required")

            if name not in _machines:
                return ActionResult(success=False, message=f"State machine '{name}' not found")

            machine = _machines[name]
            result = machine.can_transition(event, guard_params)

            return ActionResult(
                success=True,
                message=f"Guard condition for event '{event}': {'allowed' if result else 'denied'}",
                data={
                    "allowed": result,
                    "current_state": machine.current_state,
                    "event": event,
                    "guard_expression": guard_expression
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Guard evaluation failed: {str(e)}")


class StateMachineHistoryAction(BaseAction):
    """Get state machine transition history."""
    action_type = "state_machine_history"
    display_name = "状态机历史"
    description = "获取状态机转换历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            limit = params.get("limit", 100)

            if not name:
                return ActionResult(success=False, message="name is required")

            if name not in _machines:
                return ActionResult(success=False, message=f"State machine '{name}' not found")

            machine = _machines[name]
            history = machine.get_history(limit)

            return ActionResult(
                success=True,
                message=f"Retrieved {len(history)} history entries for '{name}'",
                data={
                    "machine_name": name,
                    "current_state": machine.current_state,
                    "history": history
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"History retrieval failed: {str(e)}")
