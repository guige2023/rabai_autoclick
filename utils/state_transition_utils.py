"""UI state transition utilities for tracking and managing UI states.

This module provides utilities for tracking and validating UI state
transitions during automation workflows.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class TransitionType(Enum):
    """Type of state transition."""
    EXPECTED = auto()
    UNEXPECTED = auto()
    TIMEOUT = auto()
    ERROR = auto()


@dataclass
class StateTransition:
    """Represents a state transition event."""
    from_state: str
    to_state: str
    transition_type: TransitionType
    timestamp: float
    metadata: dict = field(default_factory=dict)


@dataclass 
class StateMachine:
    """Simple state machine for tracking UI states."""
    states: set[str]
    initial_state: str
    transitions: dict[str, set[str]]
    _current_state: str = field(init=False)
    _history: list[StateTransition] = field(default_factory=list)
    
    def __post_init__(self):
        self._current_state = self.initial_state
    
    @property
    def current_state(self) -> str:
        """Get the current state."""
        return self._current_state
    
    def add_transition(
        self,
        from_state: str,
        to_state: str,
        check: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Add a valid transition between states.
        
        Args:
            from_state: Source state.
            to_state: Target state.
            check: Optional callable to validate the transition.
        """
        if from_state not in self.states:
            self.states.add(from_state)
        if to_state not in self.states:
            self.states.add(to_state)
        
        key = f"{from_state}->{to_state}"
        if key not in self.transitions:
            self.transitions[key] = set()
        self.transitions[key].add(to_state)
    
    def transition(
        self,
        to_state: str,
        check: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Attempt to transition to a new state.
        
        Args:
            to_state: Target state.
            check: Optional callable to validate the transition.
        
        Returns:
            True if transition was successful.
        
        Raises:
            ValueError: If the transition is not valid.
        """
        if to_state not in self.states:
            raise ValueError(f"Unknown state: {to_state}")
        
        key = f"{self._current_state}->{to_state}"
        if key not in self.transitions or to_state not in self.transitions[key]:
            # Still allow transition but mark as unexpected
            trans_type = TransitionType.UNEXPECTED
        else:
            trans_type = TransitionType.EXPECTED
        
        # Run validation check if provided
        if check is not None and not check():
            trans_type = TransitionType.ERROR
        
        # Record transition
        transition = StateTransition(
            from_state=self._current_state,
            to_state=to_state,
            transition_type=trans_type,
            timestamp=time.monotonic(),
        )
        self._history.append(transition)
        
        self._current_state = to_state
        return trans_type == TransitionType.EXPECTED
    
    def get_history(self) -> list[StateTransition]:
        """Get the transition history."""
        return list(self._history)
    
    def is_valid_transition(self, to_state: str) -> bool:
        """Check if a transition to the given state is valid."""
        key = f"{self._current_state}->{to_state}"
        return key in self.transitions and to_state in self.transitions[key]


class UIStateWatcher:
    """Watches for UI state transitions based on conditions."""
    
    def __init__(
        self,
        state_checker: Callable[[], str],
        timeout: float = 30.0,
        poll_interval: float = 0.1,
    ):
        """Initialize the watcher.
        
        Args:
            state_checker: Callable that returns current state name.
            timeout: Maximum time to wait for a state.
            poll_interval: Time between state checks.
        """
        self.state_checker = state_checker
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._previous_state: Optional[str] = None
        self._transition_callbacks: list[Callable[[str, str], None]] = []
    
    def on_transition(
        self,
        callback: Callable[[str, str], None],
    ) -> None:
        """Register a callback for state transitions.
        
        Args:
            callback: Called with (from_state, to_state) on transition.
        """
        self._transition_callbacks.append(callback)
    
    def wait_for_state(
        self,
        target_state: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait for the UI to reach a target state.
        
        Args:
            target_state: State to wait for.
            timeout: Maximum time to wait (uses default if None).
        
        Returns:
            True if target state was reached.
        """
        timeout = timeout or self.timeout
        start_time = time.monotonic()
        self._previous_state = self.state_checker()
        
        while time.monotonic() - start_time < timeout:
            current = self.state_checker()
            
            if current != self._previous_state:
                for cb in self._transition_callbacks:
                    cb(self._previous_state, current)
                self._previous_state = current
            
            if current == target_state:
                return True
            
            time.sleep(self.poll_interval)
        
        return False
    
    def wait_for_transition(
        self,
        from_state: str,
        to_state: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait for a specific transition to occur.
        
        Args:
            from_state: State to transition from.
            to_state: State to transition to.
            timeout: Maximum time to wait.
        
        Returns:
            True if the transition occurred.
        """
        timeout = timeout or self.timeout
        start_time = time.monotonic()
        
        while time.monotonic() - start_time < timeout:
            current = self.state_checker()
            
            if (self._previous_state == from_state and 
                current == to_state):
                self._previous_state = current
                return True
            
            self._previous_state = current
            time.sleep(self.poll_interval)
        
        return False
