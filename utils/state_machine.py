"""
State Machine Utility

Implements a state machine for managing UI automation workflows.
Supports hierarchical states, transitions, and guards.

Example:
    >>> sm = AutomationStateMachine()
    >>> sm.add_state("idle")
    >>> sm.add_state("running")
    >>> sm.add_transition("idle", "running", trigger="start")
    >>> sm.start()
    >>> print(sm.current_state)
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class State:
    """A state in the state machine."""
    name: str
    on_enter: Optional[Callable[[], None]] = None
    on_exit: Optional[Callable[[], None]] = None
    on_tick: Optional[Callable[[], None]] = None
    timeout: Optional[float] = None  # Auto-transition after seconds
    timeout_target: Optional[str] = None


@dataclass
class Transition:
    """A transition between states."""
    from_state: str
    to_state: str
    trigger: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None


@dataclass
class TransitionEvent:
    """A completed transition event."""
    from_state: str
    to_state: str
    trigger: str
    timestamp: float


class AutomationStateMachine:
    """
    State machine for orchestrating automation workflows.

    Supports:
        - Named states with enter/exit handlers
        - Triggered transitions with guards
        - Timeout-based auto-transitions
        - State history
    """

    def __init__(self, initial_state: str = "idle") -> None:
        self.initial_state = initial_state
        self._states: dict[str, State] = {}
        self._transitions: list[Transition] = []
        self._current_state: str = initial_state
        self._history: list[TransitionEvent] = []
        self._lock = threading.RLock()
        self._running = False
        self._tick_thread: Optional[threading.Thread] = None
        self._state_entered_time: float = field(default_factory=time.time)
        self._callbacks: list[Callable[[TransitionEvent], None]] = []

    def add_state(
        self,
        name: str,
        on_enter: Optional[Callable[[], None]] = None,
        on_exit: Optional[Callable[[], None]] = None,
        on_tick: Optional[Callable[[], None]] = None,
        timeout: Optional[float] = None,
        timeout_target: Optional[str] = None,
    ) -> None:
        """
        Add a state to the machine.

        Args:
            name: State identifier.
            on_enter: Callback when entering state.
            on_exit: Callback when exiting state.
            on_tick: Callback called each tick while in state.
            timeout: Seconds before auto-transition.
            timeout_target: State to transition to on timeout.
        """
        with self._lock:
            self._states[name] = State(
                name=name,
                on_enter=on_enter,
                on_exit=on_exit,
                on_tick=on_tick,
                timeout=timeout,
                timeout_target=timeout_target,
            )

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        trigger: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None,
    ) -> None:
        """
        Add a transition.

        Args:
            from_state: Source state name.
            to_state: Destination state name.
            trigger: Trigger name that causes this transition.
            guard: Optional function that must return True for transition.
            action: Optional callback to execute during transition.
        """
        with self._lock:
            self._transitions.append(Transition(
                from_state=from_state,
                to_state=to_state,
                trigger=trigger,
                guard=guard,
                action=action,
            ))

    def trigger(self, trigger_name: str) -> bool:
        """
        Fire a trigger to cause a transition.

        Args:
            trigger_name: Name of the trigger.

        Returns:
            True if a transition occurred.
        """
        with self._lock:
            current = self._current_state

            for trans in self._transitions:
                if trans.from_state != current:
                    continue
                if trans.trigger != trigger_name:
                    continue
                if trans.guard and not trans.guard():
                    continue

                # Execute transition
                if trans.action:
                    try:
                        trans.action()
                    except Exception:
                        pass

                self._do_transition(trans)
                return True

        return False

    def _do_transition(self, trans: Transition) -> None:
        """Execute a transition."""
        old_state = trans.from_state
        new_state = trans.to_state

        # Call exit handler
        old_state_obj = self._states.get(old_state)
        if old_state_obj and old_state_obj.on_exit:
            try:
                old_state_obj.on_exit()
            except Exception:
                pass

        # Change state
        self._current_state = new_state
        self._state_entered_time = time.time()

        # Record history
        event = TransitionEvent(
            from_state=old_state,
            to_state=new_state,
            trigger=trans.trigger,
            timestamp=time.time(),
        )
        self._history.append(event)

        # Call enter handler
        new_state_obj = self._states.get(new_state)
        if new_state_obj and new_state_obj.on_enter:
            try:
                new_state_obj.on_enter()
            except Exception:
                pass

        # Notify callbacks
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def start(self, tick_interval: float = 0.1) -> None:
        """Start the state machine tick loop."""
        if self._running:
            return
        self._running = True
        self._tick_thread = threading.Thread(
            target=self._tick_loop,
            args=(tick_interval,),
            daemon=True,
        )
        self._tick_thread.start()

    def stop(self) -> None:
        """Stop the state machine."""
        self._running = False
        if self._tick_thread:
            self._tick_thread.join(timeout=2.0)
            self._tick_thread = None

    def _tick_loop(self, interval: float) -> None:
        """Background tick loop for timeout and on_tick handling."""
        import time as time_module

        while self._running:
            try:
                with self._lock:
                    state_obj = self._states.get(self._current_state)
                    if state_obj:
                        # on_tick
                        if state_obj.on_tick:
                            try:
                                state_obj.on_tick()
                            except Exception:
                                pass

                        # timeout
                        if state_obj.timeout and state_obj.timeout_target:
                            elapsed = time.time() - self._state_entered_time
                            if elapsed >= state_obj.timeout:
                                # Find transition for this timeout
                                for t in self._transitions:
                                    if t.from_state == self._current_state:
                                        self._do_transition(t)
                                        break
            except Exception:
                pass

            time_module.sleep(interval)

    @property
    def current_state(self) -> str:
        """Get current state name."""
        with self._lock:
            return self._current_state

    def is_state(self, state_name: str) -> bool:
        """Check if currently in a given state."""
        with self._lock:
            return self._current_state == state_name

    def get_history(self, limit: Optional[int] = None) -> list[TransitionEvent]:
        """Get transition history."""
        with self._lock:
            if limit:
                return list(self._history[-limit:])
            return list(self._history)

    def add_event_callback(
        self,
        callback: Callable[[TransitionEvent], None],
    ) -> None:
        """Register a callback for transition events."""
        self._callbacks.append(callback)

    def wait_for_state(
        self,
        state_name: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Block until state machine enters a state.

        Args:
            state_name: State to wait for.
            timeout: Max seconds to wait.

        Returns:
            True if state reached, False on timeout.
        """
        start = time.time()
        while True:
            if self.is_state(state_name):
                return True
            if timeout and (time.time() - start) >= timeout:
                return False
            time.sleep(0.05)
