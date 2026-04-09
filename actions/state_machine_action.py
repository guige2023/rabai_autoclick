"""
State machine action for workflow state management.

Provides configurable state transitions with guards and actions.
"""

from typing import Any, Callable, Optional
import time


class StateMachineAction:
    """Configurable state machine for workflow management."""

    def __init__(self, initial_state: str = "idle") -> None:
        """
        Initialize state machine.

        Args:
            initial_state: Starting state
        """
        self.initial_state = initial_state
        self._current_state = initial_state
        self._states: dict[str, dict[str, Any]] = {}
        self._transitions: dict[tuple[str, str], dict[str, Any]] = {}
        self._history: list[dict[str, Any]] = []
        self._guards: dict[str, Callable] = {}
        self._entry_actions: dict[str, Callable] = {}
        self._exit_actions: dict[str, Callable] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute state machine operation.

        Args:
            params: Dictionary containing:
                - operation: 'add_state', 'add_transition', 'trigger', 'status'
                - state: State name
                - event: Event to trigger
                - context: Transition context

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "trigger")

        if operation == "add_state":
            return self._add_state(params)
        elif operation == "add_transition":
            return self._add_transition(params)
        elif operation == "trigger":
            return self._trigger_event(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "reset":
            return self._reset(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _add_state(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add state to state machine."""
        state = params.get("state", "")
        state_type = params.get("type", "normal")
        metadata = params.get("metadata", {})

        if not state:
            return {"success": False, "error": "State name is required"}

        self._states[state] = {
            "type": state_type,
            "metadata": metadata,
            "added_at": time.time(),
        }

        return {"success": True, "state": state}

    def _add_transition(
        self, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Add transition between states."""
        from_state = params.get("from_state", "")
        to_state = params.get("to_state", "")
        event = params.get("event", "")
        guard = params.get("guard")
        action = params.get("action")

        if not from_state or not to_state:
            return {"success": False, "error": "from_state and to_state are required"}

        self._transitions[(from_state, to_state)] = {
            "event": event,
            "guard": guard,
            "action": action,
            "added_at": time.time(),
        }

        return {"success": True, "from_state": from_state, "to_state": to_state}

    def _trigger_event(self, params: dict[str, Any]) -> dict[str, Any]:
        """Trigger event to cause state transition."""
        event = params.get("event", "")
        context = params.get("context", {})

        transition = self._find_transition(event, context)

        if not transition:
            return {
                "success": False,
                "error": f"No valid transition for event '{event}' from state '{self._current_state}'",
            }

        from_state = self._current_state
        to_state = transition["to_state"]

        self._execute_exit_action(from_state)
        self._current_state = to_state
        self._execute_entry_action(to_state)

        self._history.append(
            {
                "from_state": from_state,
                "to_state": to_state,
                "event": event,
                "context": context,
                "timestamp": time.time(),
            }
        )

        return {
            "success": True,
            "from_state": from_state,
            "to_state": to_state,
            "event": event,
        }

    def _find_transition(
        self, event: str, context: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """Find valid transition for event."""
        for (from_state, to_state), trans in self._transitions.items():
            if from_state != self._current_state:
                continue
            if trans["event"] != event:
                continue
            if trans["guard"] and not self._evaluate_guard(trans["guard"], context):
                continue
            return {"from_state": from_state, "to_state": to_state, **trans}
        return None

    def _evaluate_guard(self, guard: Any, context: dict[str, Any]) -> bool:
        """Evaluate guard condition."""
        if callable(guard):
            return guard(context)
        return True

    def _execute_entry_action(self, state: str) -> None:
        """Execute entry action for state."""
        if state in self._entry_actions:
            action = self._entry_actions[state]
            if callable(action):
                action()

    def _execute_exit_action(self, state: str) -> None:
        """Execute exit action for state."""
        if state in self._exit_actions:
            action = self._exit_actions[state]
            if callable(action):
                action()

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get current state machine status."""
        return {
            "success": True,
            "current_state": self._current_state,
            "initial_state": self.initial_state,
            "total_states": len(self._states),
            "total_transitions": len(self._transitions),
            "history_length": len(self._history),
        }

    def _reset(self, params: dict[str, Any]) -> dict[str, Any]:
        """Reset state machine to initial state."""
        self._current_state = self.initial_state
        self._history.append(
            {
                "event": "reset",
                "to_state": self.initial_state,
                "timestamp": time.time(),
            }
        )
        return {"success": True, "current_state": self._current_state}

    def get_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get state transition history."""
        return self._history[-limit:]

    def register_entry_action(self, state: str, action: Callable) -> None:
        """Register entry action for state."""
        self._entry_actions[state] = action

    def register_exit_action(self, state: str, action: Callable) -> None:
        """Register exit action for state."""
        self._exit_actions[state] = action
