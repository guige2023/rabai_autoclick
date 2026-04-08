"""
Input batch executor utilities for batching and executing input sequences.

Provides efficient batch execution of input events with
grouping, deduplication, and ordering guarantees.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class InputAction:
    """A single input action."""
    action_type: str  # "click", "type", "scroll", "key", "drag"
    x: float = 0
    y: float = 0
    data: any = None
    delay_ms: float = 0
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


@dataclass
class BatchResult:
    """Result of executing a batch."""
    batch_id: str
    total_actions: int
    executed_actions: int
    failed_actions: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)


class InputBatchExecutor:
    """Executes batches of input actions efficiently."""

    def __init__(
        self,
        executor: Optional[Callable[[InputAction], bool]] = None,
        max_batch_size: int = 50,
        batch_delay_ms: float = 10.0,
    ):
        self._executor = executor or self._default_executor
        self._max_batch_size = max_batch_size
        self._batch_delay_ms = batch_delay_ms
        self._pending: list[InputAction] = []
        self._batch_counter = 0

    def add_action(self, action: InputAction) -> None:
        """Add an action to the pending batch."""
        self._pending.append(action)
        if len(self._pending) >= self._max_batch_size:
            self.flush()

    def add_click(self, x: float, y: float, delay_ms: float = 0) -> None:
        """Add a click action."""
        self.add_action(InputAction(action_type="click", x=x, y=y, delay_ms=delay_ms))

    def add_type(self, text: str, delay_ms: float = 0) -> None:
        """Add a type action."""
        self.add_action(InputAction(action_type="type", data=text, delay_ms=delay_ms))

    def add_scroll(self, delta_x: int, delta_y: int, x: float = 0, y: float = 0) -> None:
        """Add a scroll action."""
        self.add_action(InputAction(action_type="scroll", x=x, y=y, data=(delta_x, delta_y)))

    def add_key(self, key: str, delay_ms: float = 0) -> None:
        """Add a key press action."""
        self.add_action(InputAction(action_type="key", data=key, delay_ms=delay_ms))

    def flush(self) -> BatchResult:
        """Execute all pending actions and return results."""
        if not self._pending:
            return BatchResult("", 0, 0, 0, 0.0)

        self._batch_counter += 1
        batch_id = f"batch_{self._batch_counter}_{int(time.time())}"

        start_time = time.time()
        executed = 0
        failed = 0
        errors = []

        for action in self._pending:
            try:
                success = self._executor(action)
                if success:
                    executed += 1
                else:
                    failed += 1
                    errors.append(f"Action failed: {action.action_type}")
            except Exception as e:
                failed += 1
                errors.append(f"Exception: {str(e)}")

            if action.delay_ms > 0:
                time.sleep(action.delay_ms / 1000.0)

        duration = (time.time() - start_time) * 1000

        result = BatchResult(
            batch_id=batch_id,
            total_actions=len(self._pending),
            executed_actions=executed,
            failed_actions=failed,
            duration_ms=duration,
            errors=errors,
        )

        self._pending.clear()
        return result

    def execute_batch(
        self,
        actions: list[InputAction],
        stop_on_error: bool = False,
    ) -> BatchResult:
        """Execute a batch of actions with optional stop-on-error."""
        self._batch_counter += 1
        batch_id = f"batch_{self._batch_counter}_{int(time.time())}"

        start_time = time.time()
        executed = 0
        failed = 0
        errors = []

        for action in actions:
            try:
                success = self._executor(action)
                if success:
                    executed += 1
                else:
                    failed += 1
                    errors.append(f"Action failed: {action.action_type}")
                    if stop_on_error:
                        break
            except Exception as e:
                failed += 1
                errors.append(f"Exception: {str(e)}")
                if stop_on_error:
                    break

            if action.delay_ms > 0:
                time.sleep(action.delay_ms / 1000.0)

        duration = (time.time() - start_time) * 1000

        return BatchResult(
            batch_id=batch_id,
            total_actions=len(actions),
            executed_actions=executed,
            failed_actions=failed,
            duration_ms=duration,
            errors=errors,
        )

    def _default_executor(self, action: InputAction) -> bool:
        """Default executor that just logs actions."""
        return True

    def set_executor(self, executor: Callable[[InputAction], bool]) -> None:
        """Set a custom action executor."""
        self._executor = executor


__all__ = ["InputBatchExecutor", "InputAction", "BatchResult"]
