"""
State Machine Action Module.

Provides finite state machine with transitions, guards,
and async event handling.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class StateMachineStatus(Enum):
    """State machine status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class Transition:
    """State transition definition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable] = None
    timeout: Optional[float] = None


@dataclass
class State:
    """State definition."""
    name: str
    on_enter: Optional[Callable] = None
    on_exit: Optional[Callable] = None
    on_update: Optional[Callable] = None


@dataclass
class StateContext:
    """Current state context."""
    current_state: str
    previous_state: Optional[str] = None
    event: Optional[str] = None
    metadata: dict = field(default_factory=dict)


class StateMachineError(Exception):
    """State machine error."""
    pass


class InvalidTransitionError(StateMachineError):
    """Invalid transition error."""
    pass


class GuardViolationError(StateMachineError):
    """Guard condition not met."""
    pass


class StateMachine:
    """Finite state machine."""

    def __init__(self, initial_state: str, name: str = "state_machine"):
        self.name = name
        self._initial_state = initial_state
        self._current_state = initial_state
        self._states: dict[str, State] = {}
        self._transitions: list[Transition] = []
        self._transition_map: dict[tuple[str, str], Transition] = {}
        self._status = StateMachineStatus.IDLE
        self._lock = asyncio.Lock()
        self._context = StateContext(current_state=initial_state)

    def add_state(
        self,
        name: str,
        on_enter: Optional[Callable] = None,
        on_exit: Optional[Callable] = None,
        on_update: Optional[Callable] = None
    ) -> "StateMachine":
        """Add a state."""
        state = State(
            name=name,
            on_enter=on_enter,
            on_exit=on_exit,
            on_update=on_update
        )
        self._states[name] = state
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable[[], bool]] = None,
        action: Optional[Callable] = None,
        timeout: Optional[float] = None
    ) -> "StateMachine":
        """Add a transition."""
        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
            timeout=timeout
        )
        self._transitions.append(transition)
        self._transition_map[(from_state, to_state)] = transition
        return self

    def _find_transition(self, from_state: str, event: str) -> Optional[Transition]:
        """Find transition for state and event."""
        for t in self._transitions:
            if t.from_state == from_state and t.event == event:
                return t
        return None

    async def _execute_callback(self, callback: Optional[Callable], *args: Any) -> None:
        """Execute callback."""
        if callback is None:
            return
        if asyncio.iscoroutinefunction(callback):
            await callback(*args)
        else:
            await asyncio.to_thread(callback, *args)

    async def send(self, event: str, data: Optional[Any] = None) -> bool:
        """Send event to state machine."""
        async with self._lock:
            if self._status == StateMachineStatus.STOPPED:
                raise StateMachineError("State machine is stopped")

            transition = self._find_transition(self._current_state, event)
            if transition is None:
                return False

            if transition.guard:
                try:
                    guard_result = transition.guard()
                    if asyncio.iscoroutinefunction(transition.guard):
                        guard_result = await guard_result
                    if not guard_result:
                        raise GuardViolationError(
                            f"Guard failed for transition {self._current_state} -> {transition.to_state}"
                        )
                except GuardViolationError:
                    raise
                except Exception as e:
                    raise GuardViolationError(f"Guard error: {e}")

            old_state = self._current_state

            if old_state in self._states:
                await self._execute_callback(
                    self._states[old_state].on_exit,
                    self._context
                )

            if transition.action:
                await self._execute_callback(
                    transition.action,
                    self._context
                )

            self._current_state = transition.to_state
            self._context = StateContext(
                current_state=transition.to_state,
                previous_state=old_state,
                event=event,
                metadata=data or {}
            )

            if transition.to_state in self._states:
                await self._execute_callback(
                    self._states[transition.to_state].on_enter,
                    self._context
                )

            return True

    async def start(self) -> None:
        """Start state machine."""
        async with self._lock:
            if self._current_state in self._states:
                await self._execute_callback(
                    self._states[self._current_state].on_enter,
                    self._context
                )
            self._status = StateMachineStatus.RUNNING

    async def stop(self) -> None:
        """Stop state machine."""
        async with self._lock:
            self._status = StateMachineStatus.STOPPED

    async def pause(self) -> None:
        """Pause state machine."""
        self._status = StateMachineStatus.PAUSED

    async def resume(self) -> None:
        """Resume state machine."""
        self._status = StateMachineStatus.RUNNING

    @property
    def current_state(self) -> str:
        """Get current state."""
        return self._current_state

    @property
    def status(self) -> StateMachineStatus:
        """Get status."""
        return self._status

    @property
    def context(self) -> StateContext:
        """Get context."""
        return self._context


class StateMachineAction:
    """
    State machine for workflow automation.

    Example:
        sm = StateMachineAction(initial_state="idle", name="order")

        sm.add_state("idle")
        sm.add_state("pending")
        sm.add_state("processing")
        sm.add_state("completed")

        sm.add_transition("idle", "pending", "submit")
        sm.add_transition("pending", "processing", "approve")
        sm.add_transition("processing", "completed", "finish")

        await sm.start()
        await sm.send("submit", order_data)
    """

    def __init__(self, initial_state: str, name: str = "state_machine"):
        self._sm = StateMachine(initial_state, name)

    def add_state(self, name: str, **kwargs: Any) -> "StateMachineAction":
        """Add state."""
        self._sm.add_state(name, **kwargs)
        return self

    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        **kwargs: Any
    ) -> "StateMachineAction":
        """Add transition."""
        self._sm.add_transition(from_state, to_state, event, **kwargs)
        return self

    async def send(self, event: str, data: Optional[Any] = None) -> bool:
        """Send event."""
        return await self._sm.send(event, data)

    async def start(self) -> None:
        """Start."""
        await self._sm.start()

    async def stop(self) -> None:
        """Stop."""
        await self._sm.stop()

    @property
    def current_state(self) -> str:
        """Current state."""
        return self._sm.current_state

    @property
    def context(self) -> StateContext:
        """Context."""
        return self._sm.context
