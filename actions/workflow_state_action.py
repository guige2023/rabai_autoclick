"""
Workflow State Action Module.

Manages workflow execution state with persistence and recovery.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class WorkflowTransition:
    """Represents a state transition."""

    def __init__(
        self,
        from_state: WorkflowStatus,
        to_state: WorkflowStatus,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.condition = condition
        self.action = action

    def can_transition(self) -> bool:
        """Check if transition is allowed."""
        if self.condition is None:
            return True
        return self.condition()

    def execute(self) -> Any:
        """Execute transition action."""
        if self.action:
            return self.action()
        return None


@dataclass
class WorkflowState:
    """Current workflow state snapshot."""
    workflow_id: str
    status: WorkflowStatus
    current_step: int
    step_states: Dict[str, Any]
    context: Dict[str, Any]
    created_at: float
    updated_at: float
    error: Optional[str] = None


class WorkflowStateAction:
    """
    Manages workflow execution state with transitions.

    Supports pause/resume, persistence, and recovery.
    """

    def __init__(
        self,
        workflow_id: str,
        persist_path: Optional[Path] = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.persist_path = persist_path
        self._status = WorkflowStatus.PENDING
        self._current_step = 0
        self._step_states: Dict[str, Any] = {}
        self._context: Dict[str, Any] = {}
        self._transitions: Dict[WorkflowStatus, List[WorkflowTransition]] = {}
        self._listeners: Dict[WorkflowStatus, List[Callable[[WorkflowStatus], None]]] = {}
        self._created_at = time.time()
        self._updated_at = time.time()
        self._error: Optional[str] = None

        self._setup_default_transitions()

    def _setup_default_transitions(self) -> None:
        """Setup default state transitions."""
        self._transitions[WorkflowStatus.PENDING] = [
            WorkflowTransition(WorkflowStatus.PENDING, WorkflowStatus.RUNNING),
        ]
        self._transitions[WorkflowStatus.RUNNING] = [
            WorkflowTransition(WorkflowStatus.RUNNING, WorkflowStatus.COMPLETED),
            WorkflowTransition(WorkflowStatus.RUNNING, WorkflowStatus.PAUSED),
            WorkflowTransition(WorkflowStatus.RUNNING, WorkflowStatus.FAILED),
            WorkflowTransition(WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED),
        ]
        self._transitions[WorkflowStatus.PAUSED] = [
            WorkflowTransition(WorkflowStatus.PAUSED, WorkflowStatus.RUNNING),
            WorkflowTransition(WorkflowStatus.PAUSED, WorkflowStatus.CANCELLED),
        ]

    def transition(self, to_status: WorkflowStatus) -> bool:
        """
        Transition to a new status.

        Args:
            to_status: Target status

        Returns:
            True if transition successful
        """
        valid_transitions = self._transitions.get(self._status, [])

        for trans in valid_transitions:
            if trans.to_state == to_status:
                if not trans.can_transition():
                    return False

                trans.execute()
                self._status = to_status
                self._updated_at = time.time()
                self._notify_listeners(to_status)

                if self.persist_path:
                    self.persist()

                return True

        return False

    def _notify_listeners(self, status: WorkflowStatus) -> None:
        """Notify status change listeners."""
        for listener in self._listeners.get(status, []):
            listener(status)

    def on_status_change(
        self,
        status: WorkflowStatus,
        listener: Callable[[WorkflowStatus], None],
    ) -> None:
        """Register a status change listener."""
        if status not in self._listeners:
            self._listeners[status] = []
        self._listeners[status].append(listener)

    def set_step(self, step: int, state: Any = None) -> None:
        """Set current workflow step."""
        self._current_step = step
        if state is not None:
            self._step_states[f"step_{step}"] = state
        self._updated_at = time.time()

    def get_step_state(self, step: int) -> Any:
        """Get state for a specific step."""
        return self._step_states.get(f"step_{step}")

    def update_context(self, key: str, value: Any) -> None:
        """Update workflow context."""
        self._context[key] = value
        self._updated_at = time.time()

    def get_context(self, key: str, default: Any = None) -> Any:
        """Get value from context."""
        return self._context.get(key, default)

    def set_error(self, error: str) -> None:
        """Set workflow error."""
        self._error = error
        self._updated_at = time.time()

    def persist(self) -> None:
        """Persist state to disk."""
        if not self.persist_path:
            return

        state = self.get_snapshot()
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.persist_path, "w") as f:
            json.dump(state, f, indent=2)

    def restore(self) -> bool:
        """Restore state from disk."""
        if not self.persist_path or not self.persist_path.exists():
            return False

        try:
            with open(self.persist_path) as f:
                state = json.load(f)

            self._status = WorkflowStatus(state["status"])
            self._current_step = state["current_step"]
            self._step_states = state.get("step_states", {})
            self._context = state.get("context", {})
            self._error = state.get("error")
            self._updated_at = state.get("updated_at", time.time())

            return True
        except Exception:
            return False

    def get_snapshot(self) -> Dict[str, Any]:
        """Get current state snapshot."""
        return {
            "workflow_id": self.workflow_id,
            "status": self._status.name,
            "current_step": self._current_step,
            "step_states": self._step_states,
            "context": self._context,
            "created_at": self._created_at,
            "updated_at": self._updated_at,
            "error": self._error,
        }

    @property
    def status(self) -> WorkflowStatus:
        """Get current status."""
        return self._status

    @property
    def current_step(self) -> int:
        """Get current step."""
        return self._current_step

    @property
    def is_terminal(self) -> bool:
        """Check if in terminal state."""
        return self._status in {
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        }
