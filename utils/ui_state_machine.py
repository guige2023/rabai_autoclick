"""UI state machine for managing UI automation workflows.

Provides a state machine model for complex UI automation scenarios,
supporting hierarchical states, guards, and transitions.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class StateMachineEvent(Enum):
    """Events that can trigger transitions."""
    ENTER = auto()
    EXIT = auto()
    UPDATE = auto()
    TIMEOUT = auto()
    CUSTOM = auto()


@dataclass
class StateTransition:
    """A transition between two states.

    Attributes:
        source: Source state ID.
        target: Target state ID.
        event: Event that triggers this transition.
        guard: Optional guard condition function.
        action: Optional action to execute during transition.
        description: Human-readable description.
    """
    source: str
    target: str
    event: StateMachineEvent = StateMachineEvent.CUSTOM
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], Any]] = None
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def can_transition(self) -> bool:
        """Return True if guard allows this transition."""
        if self.guard is None:
            return True
        try:
            return self.guard()
        except Exception:
            return False


@dataclass
class UIState:
    """A state in the UI state machine.

    Attributes:
        state_id: Unique identifier for this state.
        name: Human-readable name.
        is_active: Whether this state is currently active.
        is_final: Whether this is a final/terminal state.
        parent: Parent state ID for hierarchical states.
        entry_action: Action to run when entering this state.
        exit_action: Action to run when exiting this state.
        update_action: Action to run while in this state.
        timeout: Optional timeout in seconds (fires TIMEOUT event).
        metadata: Additional state metadata.
    """
    state_id: str
    name: str
    is_active: bool = False
    is_final: bool = False
    parent: Optional[str] = None
    entry_action: Optional[Callable[[], Any]] = None
    exit_action: Optional[Callable[[], Any]] = None
    update_action: Optional[Callable[[], Any]] = None
    timeout: Optional[float] = None
    metadata: dict = field(default_factory=dict)

    def enter(self) -> Any:
        """Execute entry action."""
        if self.entry_action:
            return self.entry_action()
        return None

    def exit(self) -> Any:
        """Execute exit action."""
        if self.exit_action:
            return self.exit_action()
        return None

    def update(self) -> Any:
        """Execute update action."""
        if self.update_action:
            return self.update_action()
        return None


@dataclass
class StateMachineContext:
    """Shared context for state machine execution."""
    current_state_id: str = ""
    previous_state_id: str = ""
    event_history: list[tuple[str, StateMachineEvent]] = field(default_factory=list)
    state_data: dict[str, Any] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    def get_state_data(self, key: str, default: Any = None) -> Any:
        """Get data for current state."""
        return self.state_data.get(self.current_state_id, default)

    def set_state_data(self, key: str, value: Any) -> None:
        """Set data for current state."""
        self.state_data[self.current_state_id] = value


class UIStateMachine:
    """A state machine for managing UI automation workflows.

    Supports:
    - Hierarchical states
    - Guard conditions on transitions
    - Entry/exit/update actions per state
    - Event-driven transitions
    - Timeout transitions
    """

    def __init__(self, initial_state: str = "") -> None:
        """Initialize state machine with optional initial state."""
        self._states: dict[str, UIState] = {}
        self._transitions: dict[str, list[StateTransition]] = {}
        self._initial_state = initial_state
        self._current_state: Optional[UIState] = None
        self._context = StateMachineContext()
        self._is_running = False
        self._transition_callbacks: list[Callable[[StateTransition], None]] = []

    def add_state(
        self,
        state_id: str,
        name: str = "",
        is_final: bool = False,
        parent: Optional[str] = None,
        entry_action: Optional[Callable[[], Any]] = None,
        exit_action: Optional[Callable[[], Any]] = None,
        update_action: Optional[Callable[[], Any]] = None,
        timeout: Optional[float] = None,
    ) -> UIState:
        """Register a state in the machine."""
        state = UIState(
            state_id=state_id,
            name=name or state_id,
            is_final=is_final,
            parent=parent,
            entry_action=entry_action,
            exit_action=exit_action,
            update_action=update_action,
            timeout=timeout,
        )
        self._states[state_id] = state
        self._transitions.setdefault(state_id, [])
        return state

    def add_transition(
        self,
        source: str,
        target: str,
        event: StateMachineEvent = StateMachineEvent.CUSTOM,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Any]] = None,
        description: str = "",
    ) -> Optional[StateTransition]:
        """Add a transition between states.

        Returns the created transition, or None if states don't exist.
        """
        if source not in self._states or target not in self._states:
            return None
        transition = StateTransition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
            description=description,
        )
        self._transitions[source].append(transition)
        return transition

    def start(self, state_id: Optional[str] = None) -> None:
        """Start the state machine in the initial or specified state."""
        target_id = state_id or self._initial_state
        if target_id not in self._states:
            raise ValueError(f"State '{target_id}' does not exist")
        self._is_running = True
        self._context.current_state_id = target_id
        self._context.previous_state_id = ""
        self._enter_state(target_id)

    def stop(self) -> None:
        """Stop the state machine."""
        if self._current_state:
            self._current_state.exit()
        self._is_running = False
        if self._current_state:
            self._current_state.is_active = False
        self._current_state = None

    def send_event(self, event: StateMachineEvent, event_data: Any = None) -> bool:
        """Send an event to the state machine.

        Returns True if a transition occurred.
        """
        if not self._current_state:
            return False

        self._context.event_history.append(
            (self._current_state.state_id, event)
        )

        transitions = self._transitions.get(
            self._current_state.state_id, []
        )

        for transition in transitions:
            if transition.event != event:
                continue
            if not transition.can_transition():
                continue

            prev = self._current_state
            next_state = self._states.get(transition.target)
            if not next_state:
                continue

            prev.exit()
            if transition.action:
                transition.action()
            self._context.previous_state_id = prev.state_id
            self._enter_state(transition.target)
            self._notify_transition(transition)
            return True

        return False

    def update(self) -> Any:
        """Run the update action for the current state.

        Returns the result of the update action, if any.
        """
        if not self._current_state:
            return None
        return self._current_state.update()

    def _enter_state(self, state_id: str) -> None:
        """Enter a state and run its entry action."""
        state = self._states[state_id]
        state.is_active = True
        self._current_state = state
        self._context.current_state_id = state_id
        state.enter()

    def get_current_state(self) -> Optional[UIState]:
        """Return the currently active state."""
        return self._current_state

    def get_state(self, state_id: str) -> Optional[UIState]:
        """Return a state by ID."""
        return self._states.get(state_id)

    def is_in_state(self, state_id: str) -> bool:
        """Return True if currently in the given state."""
        return (
            self._current_state is not None
            and self._current_state.state_id == state_id
        )

    def get_active_states(self) -> list[UIState]:
        """Return list of currently active states (for hierarchy)."""
        active = []
        current = self._current_state
        while current:
            active.append(current)
            if current.parent:
                current = self._states.get(current.parent)
            else:
                break
        return active

    def on_transition(
        self, callback: Callable[[StateTransition], None]
    ) -> None:
        """Register a callback for state transitions."""
        self._transition_callbacks.append(callback)

    def _notify_transition(self, transition: StateTransition) -> None:
        """Notify all transition callbacks."""
        for cb in self._transition_callbacks:
            try:
                cb(transition)
            except Exception:
                pass

    @property
    def context(self) -> StateMachineContext:
        """Return the state machine context."""
        return self._context

    @property
    def is_running(self) -> bool:
        """Return True if state machine is running."""
        return self._is_running

    @property
    def all_states(self) -> list[UIState]:
        """Return all registered states."""
        return list(self._states.values())


# Helper to create a simple linear workflow state machine
def create_linear_workflow(
    states: list[str],
    transitions: Optional[list[tuple[str, str]]] = None,
) -> UIStateMachine:
    """Create a linear workflow state machine.

    Args:
        states: Ordered list of state IDs.
        transitions: Optional list of (from, to) transition pairs.
                    If None, transitions are sequential (states[i] -> states[i+1]).
    """
    sm = UIStateMachine(initial_state=states[0] if states else "")

    for state_id in states:
        sm.add_state(state_id)

    if transitions:
        for source, target in transitions:
            sm.add_transition(source, target)
    else:
        for i in range(len(states) - 1):
            sm.add_transition(states[i], states[i + 1])

    return sm
