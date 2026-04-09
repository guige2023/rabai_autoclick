"""
Input state machine for managing complex input sequences.

State machine for handling multi-step input gestures and validation.
"""

from __future__ import annotations

from typing import Callable, Optional, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class InputState(Enum):
    """States in the input state machine."""
    IDLE = auto()
    TOUCHING = auto()
    DRAGGING = auto()
    PINCHING = auto()
    WAITING = auto()
    COMPLETED = auto()
    CANCELLED = auto()
    ERROR = auto()


class InputEvent(Enum):
    """Events that drive state transitions."""
    TOUCH_DOWN = auto()
    TOUCH_MOVE = auto()
    TOUCH_UP = auto()
    TIMEOUT = auto()
    VALIDATE = auto()
    RESET = auto()
    ERROR = auto()


@dataclass
class StateTransition:
    """Represents a state transition."""
    from_state: InputState
    to_state: InputState
    event: InputEvent
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


@dataclass
class StateContext:
    """Context maintained during state machine execution."""
    start_time: float = 0
    current_position: tuple[float, float] = (0, 0)
    start_position: tuple[float, float] = (0, 0)
    end_position: tuple[float, float] = (0, 0)
    velocity: tuple[float, float] = (0, 0)
    metadata: dict = field(default_factory=dict)


class InputStateMachine:
    """State machine for managing input sequences."""
    
    def __init__(self):
        self._current_state = InputState.IDLE
        self._transitions: list[StateTransition] = []
        self._context = StateContext()
        self._handlers: dict[tuple[InputState, InputEvent], Callable[[], None]] = {}
        self._timeout_ms: float = 0
        self._last_event_time: float = 0
    
    def add_transition(
        self,
        from_state: InputState,
        to_state: InputState,
        event: InputEvent,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None
    ) -> "InputStateMachine":
        """Add a state transition rule."""
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action
        )
        self._transitions.append(transition)
        return self
    
    def on_transition(
        self,
        from_state: InputState,
        event: InputEvent,
        handler: Callable[[], None]
    ) -> "InputStateMachine":
        """Set handler for specific state/event combination."""
        self._handlers[(from_state, event)] = handler
        return self
    
    def set_timeout(self, timeout_ms: float) -> "InputStateMachine":
        """Set timeout for waiting states."""
        self._timeout_ms = timeout_ms
        return self
    
    def handle_event(self, event: InputEvent, context: Optional[dict] = None) -> bool:
        """Handle an input event and potentially transition state."""
        for transition in self._transitions:
            if transition.from_state != self._current_state:
                continue
            if transition.event != event:
                continue
            if transition.guard and not transition.guard():
                continue
            
            if transition.action:
                transition.action()
            
            handler_key = (self._current_state, event)
            if handler_key in self._handlers:
                self._handlers[handler_key]()
            
            self._current_state = transition.to_state
            return True
        
        return False
    
    def get_state(self) -> InputState:
        """Get current state."""
        return self._current_state
    
    def reset(self) -> None:
        """Reset state machine to idle."""
        self._current_state = InputState.IDLE
        self._context = StateContext()
    
    def is_terminal(self) -> bool:
        """Check if state machine is in a terminal state."""
        return self._current_state in (InputState.COMPLETED, InputState.CANCELLED, InputState.ERROR)


class GestureStateMachine(InputStateMachine):
    """Pre-configured state machine for gesture recognition."""
    
    def __init__(self):
        super().__init__()
        self._setup_gesture_transitions()
    
    def _setup_gesture_transitions(self) -> None:
        """Set up standard gesture recognition transitions."""
        self.add_transition(InputState.IDLE, InputState.TOUCHING, InputEvent.TOUCH_DOWN)
        self.add_transition(InputState.TOUCHING, InputState.DRAGGING, InputEvent.TOUCH_MOVE)
        self.add_transition(InputState.TOUCHING, InputState.COMPLETED, InputEvent.TOUCH_UP)
        self.add_transition(InputState.DRAGGING, InputState.WAITING, InputEvent.TOUCH_UP)
        self.add_transition(InputState.WAITING, InputState.COMPLETED, InputEvent.TIMEOUT)
        self.add_transition(InputState.IDLE, InputState.CANCELLED, InputEvent.RESET)
        self.add_transition(InputState.TOUCHING, InputState.CANCELLED, InputEvent.RESET)
        self.add_transition(InputState.DRAGGING, InputState.CANCELLED, InputEvent.RESET)
