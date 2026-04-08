"""
Workflow State Machine Action Module.

Implements a state machine for workflow orchestration with transitions,
guards, actions, and history tracking. Supports hierarchical states.
"""
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from actions.base_action import BaseAction


class TransitionResult:
    """Result of a state transition."""
    def __init__(self, success: bool, from_state: str, to_state: str, error: Optional[str] = None):
        self.success = success
        self.from_state = from_state
        self.to_state = to_state
        self.error = error


class WorkflowStateAction(BaseAction):
    """State machine for workflow orchestration."""

    def __init__(self) -> None:
        super().__init__("workflow_state")
        self._states: set[str] = {"init", "running", "success", "failure", "cancelled"}
        self._transitions: dict[str, dict[str, Callable]] = {}
        self._current_state = "init"
        self._history: list[dict[str, Any]] = []
        self._state_data: dict[str, Any] = {}

    def execute(self, context: dict, params: dict) -> TransitionResult:
        """
        Execute a state transition.

        Args:
            context: Execution context
            params: Parameters:
                - action: Transition action name
                - guard: Optional guard function name
                - data: Optional data to store with state

        Returns:
            TransitionResult with transition outcome
        """
        action = params.get("action", "")
        guard_name = params.get("guard")
        data = params.get("data")

        if not action:
            return TransitionResult(False, self._current_state, self._current_state, "Action is required")

        if action not in self._transitions.get(self._current_state, {}):
            return TransitionResult(False, self._current_state, self._current_state, f"No transition '{action}' from state '{self._current_state}'")

        guard_func = self._transitions[self._current_state].get(f"{action}_guard")
        if guard_name and guard_func:
            try:
                if not guard_func(self._state_data):
                    return TransitionResult(False, self._current_state, self._current_state, "Guard condition failed")
            except Exception as e:
                return TransitionResult(False, self._current_state, self._current_state, f"Guard error: {str(e)}")

        transition_func = self._transitions[self._current_state].get(action)
        target_state = f"{self._current_state}_{action}" if transition_func else action

        if target_state not in self._states:
            return TransitionResult(False, self._current_state, self._current_state, f"Unknown state: {target_state}")

        from_state = self._current_state
        self._current_state = target_state
        if data is not None:
            self._state_data[target_state] = data

        self._history.append({
            "from": from_state,
            "to": target_state,
            "action": action,
            "timestamp": self._get_timestamp()
        })

        if transition_func:
            try:
                transition_func(self._state_data)
            except Exception as e:
                return TransitionResult(False, from_state, target_state, f"Transition action error: {str(e)}")

        return TransitionResult(True, from_state, target_state)

    def add_state(self, state: str) -> None:
        """Add a new state."""
        self._states.add(state)

    def add_transition(self, from_state: str, action: str, to_state: str, guard: Optional[Callable] = None) -> None:
        """Add a state transition."""
        if from_state not in self._transitions:
            self._transitions[from_state] = {}
        self._transitions[from_state][action] = to_state
        if guard:
            self._transitions[from_state][f"{action}_guard"] = guard

    def get_state(self) -> str:
        """Get current state."""
        return self._current_state

    def get_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get state transition history."""
        return self._history[-limit:]

    def _get_timestamp(self) -> float:
        """Get current timestamp."""
        import time
        return time.time()
