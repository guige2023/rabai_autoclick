"""
Automation State Machine Action Module.

Implements a state machine for managing complex automation workflows
 with transitions, guards, and side effects.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class StateMachineEvent(Enum):
    """Events that can trigger transitions."""
    START = "start"
    STOP = "stop"
    PAUSE = "pause"
    RESUME = "resume"
    RESET = "reset"
    COMPLETE = "complete"
    ERROR = "error"
    CUSTOM = "custom"


@dataclass
class StateTransition:
    """A transition between states."""
    from_state: str
    to_state: str
    event: StateMachineEvent
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None
    description: str = ""


@dataclass
class StateDefinition:
    """Definition of a state."""
    name: str
    on_enter: Optional[Callable[[], None]] = None
    on_exit: Optional[Callable[[], None]] = None
    on_update: Optional[Callable[[float], None]] = None
    is_final: bool = False


@dataclass
class StateMachineContext:
    """Context data for the state machine."""
    current_state: str
    previous_state: Optional[str] = None
    history: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationStateMachineAction:
    """
    State machine for workflow automation.

    Manages complex automation workflows as state machines with
    transitions, guards, actions, and comprehensive history tracking.

    Example:
        sm = AutomationStateMachineAction("idle")
        sm.add_state("running", on_enter=start_process)
        sm.add_state("completed", is_final=True)
        sm.add_transition("idle", "running", StateMachineEvent.START)
        sm.add_transition("running", "completed", StateMachineEvent.COMPLETE)
        sm.handle_event(StateMachineEvent.START)
    """

    def __init__(
        self,
        initial_state: str,
        name: str = "state_machine",
    ) -> None:
        self.name = name
        self._initial_state = initial_state
        self._states: Dict[str, StateDefinition] = {}
        self._transitions: List[StateTransition] = []
        self._transition_map: Dict[tuple[str, str], StateTransition] = {}
        self._context = StateMachineContext(current_state=initial_state)
        self._listeners: Dict[str, List[Callable]] = {
            "enter": [],
            "exit": [],
            "transition": [],
            "error": [],
        }

    def add_state(
        self,
        name: str,
        on_enter: Optional[Callable[[], None]] = None,
        on_exit: Optional[Callable[[], None]] = None,
        on_update: Optional[Callable[[float], None]] = None,
        is_final: bool = False,
    ) -> "AutomationStateMachineAction":
        """Add a state to the state machine."""
        state = StateDefinition(
            name=name,
            on_enter=on_enter,
            on_exit=on_exit,
            on_update=on_update,
            is_final=is_final,
        )
        self._states[name] = state
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: StateMachineEvent,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None,
        description: str = "",
    ) -> "AutomationStateMachineAction":
        """Add a transition between states."""
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
            description=description,
        )
        self._transitions.append(transition)
        self._transition_map[(from_state, to_state)] = transition
        return self

    def handle_event(
        self,
        event: StateMachineEvent,
        event_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Handle an event and attempt to transition states."""
        current = self._context.current_state
        transition = self._find_transition(current, event)

        if transition is None:
            logger.debug(f"No transition found for {event.value} from state {current}")
            return False

        if transition.guard and not transition.guard():
            logger.debug(f"Transition guard failed for {event.value}")
            self._trigger_listener("error", {
                "event": event.value,
                "state": current,
                "reason": "guard_failed",
            })
            return False

        try:
            if transition.action:
                transition.action()

            self._change_state(transition.to_state)

            self._trigger_listener("transition", {
                "from": current,
                "to": transition.to_state,
                "event": event.value,
            })

            return True

        except Exception as e:
            logger.error(f"Transition error: {e}")
            self._trigger_listener("error", {
                "event": event.value,
                "state": current,
                "error": str(e),
            })
            return False

    def _find_transition(
        self,
        from_state: str,
        event: StateMachineEvent,
    ) -> Optional[StateTransition]:
        """Find a valid transition for the given state and event."""
        for transition in self._transitions:
            if transition.from_state == from_state and transition.event == event:
                return transition
        return None

    def _change_state(self, new_state: str) -> None:
        """Change to a new state, triggering callbacks."""
        old_state = self._context.current_state

        if old_state in self._states:
            on_exit = self._states[old_state].on_exit
            if on_exit:
                try:
                    on_exit()
                except Exception as e:
                    logger.error(f"on_exit error: {e}")

        self._context.previous_state = old_state
        self._context.history.append(old_state)
        self._context.current_state = new_state

        self._trigger_listener("exit", {"state": old_state})
        self._trigger_listener("enter", {"state": new_state})

    def can_handle_event(self, event: StateMachineEvent) -> bool:
        """Check if the current state can handle an event."""
        current = self._context.current_state
        return self._find_transition(current, event) is not None

    def get_available_events(self) -> List[StateMachineEvent]:
        """Get list of events available from current state."""
        current = self._context.current_state
        return [
            t.event for t in self._transitions
            if t.from_state == current
        ]

    def get_current_state(self) -> str:
        """Get the current state name."""
        return self._context.current_state

    def get_history(self) -> List[str]:
        """Get state transition history."""
        return self._context.history.copy()

    def reset(self) -> None:
        """Reset the state machine to initial state."""
        self._context = StateMachineContext(current_state=self._initial_state)

    def is_final_state(self) -> bool:
        """Check if current state is a final state."""
        current = self._context.current_state
        state_def = self._states.get(current)
        return state_def.is_final if state_def else False

    def on(
        self,
        listener_type: str,
        handler: Callable,
    ) -> "AutomationStateMachineAction":
        """Register an event listener."""
        if listener_type in self._listeners:
            self._listeners[listener_type].append(handler)
        return self

    def _trigger_listener(
        self,
        listener_type: str,
        data: Dict[str, Any],
    ) -> None:
        """Trigger registered listeners."""
        for handler in self._listeners.get(listener_type, []):
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Listener error for {listener_type}: {e}")

    def get_allowed_transitions(self) -> List[str]:
        """Get list of state names that can be transitioned to."""
        current = self._context.current_state
        return [
            t.to_state for t in self._transitions
            if t.from_state == current
        ]

    def visualize(self) -> str:
        """Generate a simple text visualization of the state machine."""
        lines = [f"State Machine: {self.name}", "=" * 40]
        lines.append(f"Initial State: {self._initial_state}")
        lines.append(f"Current State: {self._context.current_state}")
        lines.append("")

        lines.append("States:")
        for name, state in self._states.items():
            marker = " [FINAL]" if state.is_final else ""
            lines.append(f"  - {name}{marker}")

        lines.append("")
        lines.append("Transitions:")
        for t in self._transitions:
            lines.append(f"  {t.from_state} --[{t.event.value}]--> {t.to_state}")
            if t.description:
                lines.append(f"    {t.description}")

        return "\n".join(lines)
