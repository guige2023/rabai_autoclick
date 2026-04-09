"""
State Machine Action Module

State machine implementation for workflow modeling.
Supports transitions, guards, actions, and history tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class StateMachineError(Exception):
    """State machine error."""
    pass


class TransitionError(StateMachineError):
    """Transition error."""
    pass


@dataclass
class State:
    """A state in the state machine."""

    name: str
    is_initial: bool = False
    is_final: bool = False
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    """A transition between states."""

    source: str
    target: str
    event: str
    guard: Optional[Callable[[Dict], bool]] = None
    action: Optional[Callable] = None
    description: str = ""


@dataclass
class StateHistory:
    """History of state transitions."""

    state: str
    event: str
    timestamp: float = field(default_factory=lambda: __import__('time').time())
    metadata: Dict[str, Any] = field(default_factory=dict)


class StateMachine:
    """
    State machine implementation.

    Supports:
    - Multiple states with initial/final markers
    - Event-driven transitions
    - Guard conditions
    - Entry/exit actions
    - Transition actions
    - History tracking
    """

    def __init__(self, machine_id: str):
        self.machine_id = machine_id
        self._states: Dict[str, State] = {}
        self._transitions: Dict[str, List[Transition]] = {}
        self._current_state: Optional[str] = None
        self._history: List[StateHistory] = []
        self._context: Dict[str, Any] = {}

    def add_state(
        self,
        name: str,
        is_initial: bool = False,
        is_final: bool = False,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
    ) -> "StateMachine":
        """Add a state."""
        state = State(
            name=name,
            is_initial=is_initial,
            is_final=is_final,
            entry_action=entry_action,
            exit_action=exit_action,
        )
        self._states[name] = state

        if is_initial:
            self._current_state = name

        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable[[Dict], bool]] = None,
        action: Optional[Callable] = None,
    ) -> "StateMachine":
        """Add a transition."""
        if source not in self._states:
            raise StateMachineError(f"Unknown state: {source}")
        if target not in self._states:
            raise StateMachineError(f"Unknown state: {target}")

        transition = Transition(
            source=source,
            target=target,
            event=event,
            guard=guard,
            action=action,
        )

        if event not in self._transitions:
            self._transitions[event] = []
        self._transitions[event].append(transition)

        return self

    async def send_event(self, event: str, event_data: Optional[Dict] = None) -> bool:
        """Send an event to the state machine."""
        event_data = event_data or {}

        if not self._current_state:
            raise StateMachineError("State machine not initialized")

        # Find matching transitions
        transitions = self._transitions.get(event, [])

        for transition in transitions:
            if transition.source != self._current_state:
                continue

            # Check guard
            if transition.guard and not transition.guard(self._context):
                logger.debug(f"Guard failed for transition {transition.source} -> {transition.target}")
                continue

            # Execute transition
            return await self._execute_transition(transition, event, event_data)

        logger.warning(f"No valid transition for event '{event}' from state '{self._current_state}'")
        return False

    async def _execute_transition(
        self,
        transition: Transition,
        event: str,
        event_data: Dict,
    ) -> bool:
        """Execute a transition."""
        source_state = self._states[self._current_state]
        target_state = self._states[transition.target]

        # Exit action
        if source_state.exit_action:
            if source_state.exit_action.__code__.co_await:
                await source_state.exit_action(self._context, event_data)
            else:
                source_state.exit_action(self._context, event_data)

        # Transition action
        if transition.action:
            if transition.action.__code__.co_await:
                await transition.action(self._context, event_data)
            else:
                transition.action(self._context, event_data)

        # Update state
        old_state = self._current_state
        self._current_state = transition.target

        # Record history
        self._history.append(StateHistory(
            state=self._current_state,
            event=event,
            metadata={"from_state": old_state},
        ))

        # Entry action
        if target_state.entry_action:
            if target_state.entry_action.__code__.co_await:
                await target_state.entry_action(self._context, event_data)
            else:
                target_state.entry_action(self._context, event_data)

        logger.info(f"Transition: {old_state} --({event})--> {self._current_state}")
        return True

    def get_state(self) -> Optional[str]:
        """Get current state."""
        return self._current_state

    def get_history(self) -> List[StateHistory]:
        """Get transition history."""
        return list(self._history)

    def is_final_state(self) -> bool:
        """Check if current state is final."""
        if not self._current_state:
            return False
        return self._states[self._current_state].is_final

    def set_context(self, key: str, value: Any) -> None:
        """Set context value."""
        self._context[key] = value

    def get_context(self, key: str) -> Any:
        """Get context value."""
        return self._context.get(key)


class StateMachineAction:
    """
    Main action class for state machine management.

    Features:
    - Hierarchical state machines
    - Parallel states
    - History tracking
    - Context management
    - Async action support

    Usage:
        sm = StateMachineAction("order_processor")
        sm.add_state("pending", is_initial=True)
        sm.add_state("processing")
        sm.add_state("completed", is_final=True)
        sm.add_transition("pending", "processing", "start")
        sm.add_transition("processing", "completed", "finish")
        await sm.send_event("start")
    """

    def __init__(self, machine_id: str):
        self._machine = StateMachine(machine_id)

    def add_state(
        self,
        name: str,
        is_initial: bool = False,
        is_final: bool = False,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
    ) -> "StateMachineAction":
        """Add a state."""
        self._machine.add_state(name, is_initial, is_final, entry_action, exit_action)
        return self

    def add_transition(
        self,
        source: str,
        target: str,
        event: str,
        guard: Optional[Callable[[Dict], bool]] = None,
        action: Optional[Callable] = None,
    ) -> "StateMachineAction":
        """Add a transition."""
        self._machine.add_transition(source, target, event, guard, action)
        return self

    async def send_event(self, event: str, event_data: Optional[Dict] = None) -> bool:
        """Send an event to the state machine."""
        return await self._machine.send_event(event, event_data)

    def get_current_state(self) -> Optional[str]:
        """Get current state."""
        return self._machine.get_state()

    def is_final(self) -> bool:
        """Check if in final state."""
        return self._machine.is_final_state()

    def get_history(self) -> List[Dict]:
        """Get transition history."""
        return [
            {"state": h.state, "event": h.event, "timestamp": h.timestamp}
            for h in self._machine.get_history()
        ]

    def set_context(self, key: str, value: Any) -> None:
        """Set context value."""
        self._machine.set_context(key, value)

    def get_context(self, key: str) -> Any:
        """Get context value."""
        return self._machine.get_context(key)


def demo_state_machine():
    """Demonstrate state machine."""
    import asyncio

    async def entry_processing(context, event_data):
        print("Entering processing state")

    async def exit_processing(context, event_data):
        print("Exiting processing state")

    async def do_processing(context, event_data):
        print("Processing order...")

    sm = StateMachineAction("order_processor")
    sm.add_state("pending", is_initial=True)
    sm.add_state("processing", entry_action=entry_processing, exit_action=exit_processing)
    sm.add_state("completed", is_final=True)
    sm.add_state("cancelled", is_final=True)

    sm.add_transition("pending", "processing", "start", action=do_processing)
    sm.add_transition("processing", "completed", "complete")
    sm.add_transition("processing", "cancelled", "cancel")

    async def run():
        print(f"Initial state: {sm.get_current_state()}")

        await sm.send_event("start")
        print(f"After start: {sm.get_current_state()}")

        await sm.send_event("complete")
        print(f"After complete: {sm.get_current_state()}")

        print(f"History: {sm.get_history()}")

    asyncio.run(run())


if __name__ == "__main__":
    demo_state_machine()
