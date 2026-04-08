"""Action Batch and Macro Utilities.

Batches multiple UI actions into atomic or transactional operations.
Supports undo/redo, action recording, and macro playback.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class BatchState(Enum):
    """State of a batch operation."""

    IDLE = auto()
    RECORDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    ROLLED_BACK = auto()


@dataclass
class Action:
    """A single action in a batch.

    Attributes:
        action_type: Type identifier for the action.
        target_id: Target element/window identifier.
        parameters: Action parameters.
        undo_action: Action to reverse this action.
        execute_func: Function to execute.
        undo_func: Function to undo.
    """

    action_type: str
    target_id: str = ""
    parameters: dict = field(default_factory=dict)
    execute_func: Optional[Callable[[], Any]] = None
    undo_func: Optional[Callable[[], Any]] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class BatchResult:
    """Result of batch execution.

    Attributes:
        success: Whether batch completed successfully.
        state: Final batch state.
        completed_actions: Number of actions completed.
        failed_action_index: Index of failed action, if any.
        error_message: Error message if failed.
        execution_time_ms: Total execution time.
    """

    success: bool
    state: BatchState
    completed_actions: int = 0
    failed_action_index: int = -1
    error_message: str = ""
    execution_time_ms: float = 0.0


class ActionBatch:
    """A batch of actions that can be executed atomically.

    Example:
        batch = ActionBatch()
        batch.add_action(Action("click", target_id="btn1"))
        batch.add_action(Action("type", target_id="input1", parameters={"text": "hello"}))
        result = batch.execute()
    """

    def __init__(
        self,
        name: str = "",
        atomic: bool = True,
        continue_on_failure: bool = False,
    ):
        """Initialize the action batch.

        Args:
            name: Batch name.
            atomic: Whether to execute atomically (all or nothing).
            continue_on_failure: Whether to continue on action failure.
        """
        self.name = name
        self.atomic = atomic
        self.continue_on_failure = continue_on_failure
        self._actions: list[Action] = []
        self._executed_actions: list[Action] = []
        self._state = BatchState.IDLE
        self._start_time: Optional[float] = None

    def add_action(
        self,
        action_type: str,
        target_id: str = "",
        parameters: Optional[dict] = None,
        execute_func: Optional[Callable[[], Any]] = None,
        undo_func: Optional[Callable[[], Any]] = None,
    ) -> "ActionBatch":
        """Add an action to the batch.

        Args:
            action_type: Type of action.
            target_id: Target element/window ID.
            parameters: Action parameters.
            execute_func: Execution function.
            undo_func: Undo function.

        Returns:
            Self for chaining.
        """
        action = Action(
            action_type=action_type,
            target_id=target_id,
            parameters=parameters or {},
            execute_func=execute_func,
            undo_func=undo_func,
        )
        self._actions.append(action)
        return self

    def execute(self) -> BatchResult:
        """Execute all actions in the batch.

        Returns:
            BatchResult with execution details.
        """
        self._state = BatchState.EXECUTING
        self._start_time = time.time()
        self._executed_actions = []

        failed_index = -1
        error_msg = ""

        for i, action in enumerate(self._actions):
            try:
                if action.execute_func:
                    action.execute_func()
                self._executed_actions.append(action)
            except Exception as e:
                error_msg = str(e)
                failed_index = i
                if self.atomic:
                    break
                elif not self.continue_on_failure:
                    break

        elapsed_ms = (time.time() - self._start_time) * 1000

        if failed_index >= 0:
            self._state = BatchState.FAILED
            return BatchResult(
                success=False,
                state=self._state,
                completed_actions=len(self._executed_actions),
                failed_action_index=failed_index,
                error_message=error_msg,
                execution_time_ms=elapsed_ms,
            )

        self._state = BatchState.COMPLETED
        return BatchResult(
            success=True,
            state=self._state,
            completed_actions=len(self._actions),
            execution_time_ms=elapsed_ms,
        )

    def undo(self) -> BatchResult:
        """Undo the batch execution.

        Returns:
            BatchResult with undo details.
        """
        self._start_time = time.time()
        undo_count = 0

        for action in reversed(self._executed_actions):
            try:
                if action.undo_func:
                    action.undo_func()
                undo_count += 1
            except Exception:
                break

        elapsed_ms = (time.time() - self._start_time) * 1000

        if undo_count < len(self._executed_actions):
            self._state = BatchState.FAILED
        else:
            self._state = BatchState.ROLLED_BACK

        return BatchResult(
            success=undo_count == len(self._executed_actions),
            state=self._state,
            completed_actions=undo_count,
            execution_time_ms=elapsed_ms,
        )

    def get_actions(self) -> list[Action]:
        """Get all actions in the batch.

        Returns:
            List of actions.
        """
        return list(self._actions)

    def clear(self) -> None:
        """Clear all actions from the batch."""
        self._actions.clear()
        self._executed_actions.clear()
        self._state = BatchState.IDLE


class ActionMacro:
    """Records and plays back action sequences.

    Example:
        macro = ActionMacro()
        macro.start_recording()
        # ... perform actions ...
        actions = macro.stop_recording()
        macro.playback(actions)
    """

    def __init__(self, name: str = ""):
        """Initialize the action macro.

        Args:
            name: Macro name.
        """
        self.name = name
        self._actions: list[Action] = []
        self._is_recording = False
        self._recording_start: Optional[float] = None

    def start_recording(self) -> None:
        """Start recording actions."""
        self._actions.clear()
        self._is_recording = True
        self._recording_start = time.time()

    def stop_recording(self) -> list[Action]:
        """Stop recording and return recorded actions.

        Returns:
            List of recorded actions.
        """
        self._is_recording = False
        return list(self._actions)

    def record_action(
        self,
        action_type: str,
        target_id: str = "",
        parameters: Optional[dict] = None,
    ) -> None:
        """Record an action.

        Args:
            action_type: Type of action.
            target_id: Target element/window ID.
            parameters: Action parameters.
        """
        if not self._is_recording:
            return

        action = Action(
            action_type=action_type,
            target_id=target_id,
            parameters=parameters or {},
        )
        self._actions.append(action)

    def playback(
        self,
        actions: Optional[list[Action]] = None,
        speed: float = 1.0,
        executor: Optional[Callable[[Action], Any]] = None,
    ) -> BatchResult:
        """Playback recorded actions.

        Args:
            actions: Actions to play (uses recorded if None).
            speed: Playback speed multiplier.
            executor: Function to execute each action.

        Returns:
            BatchResult of playback.
        """
        actions_to_play = actions if actions is not None else self._actions
        batch = ActionBatch(name=self.name, atomic=False)

        for action in actions_to_play:
            batch.add_action(
                action_type=action.action_type,
                target_id=action.target_id,
                parameters=action.parameters,
                execute_func=lambda a=action, e=executor: e(a) if e else None,
            )

        return batch.execute()

    def get_actions(self) -> list[Action]:
        """Get recorded actions.

        Returns:
            List of recorded actions.
        """
        return list(self._actions)

    def clear(self) -> None:
        """Clear recorded actions."""
        self._actions.clear()
        self._is_recording = False


class BatchManager:
    """Manages multiple action batches.

    Example:
        manager = BatchManager()
        manager.create_batch("login_flow")
        manager.add_to_batch("login_flow", click_action)
        manager.execute_batch("login_flow")
    """

    def __init__(self):
        """Initialize the batch manager."""
        self._batches: dict[str, ActionBatch] = {}

    def create_batch(
        self,
        name: str,
        atomic: bool = True,
    ) -> ActionBatch:
        """Create a named batch.

        Args:
            name: Batch name.
            atomic: Whether batch is atomic.

        Returns:
            Created ActionBatch.
        """
        batch = ActionBatch(name=name, atomic=atomic)
        self._batches[name] = batch
        return batch

    def get_batch(self, name: str) -> Optional[ActionBatch]:
        """Get a batch by name.

        Args:
            name: Batch name.

        Returns:
            ActionBatch or None.
        """
        return self._batches.get(name)

    def delete_batch(self, name: str) -> bool:
        """Delete a batch.

        Args:
            name: Batch name.

        Returns:
            True if batch was deleted.
        """
        if name in self._batches:
            del self._batches[name]
            return True
        return False

    def list_batches(self) -> list[str]:
        """List all batch names.

        Returns:
            List of batch names.
        """
        return list(self._batches.keys())

    def execute_batch(self, name: str) -> BatchResult:
        """Execute a named batch.

        Args:
            name: Batch name.

        Returns:
            BatchResult.
        """
        batch = self._batches.get(name)
        if batch:
            return batch.execute()
        return BatchResult(
            success=False,
            state=BatchState.FAILED,
            error_message=f"Batch '{name}' not found",
        )
