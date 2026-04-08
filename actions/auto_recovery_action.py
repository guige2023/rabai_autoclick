"""Auto-recovery automation action module.

Provides automatic recovery from failures with checkpoint/restore,
state snapshots, and fallback chains.
"""

from __future__ import annotations

import time
import json
import logging
import hashlib
from typing import Optional, Dict, Any, Callable, List, TypeVar
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RecoveryStrategy(Enum):
    """Recovery strategies when a task fails."""
    RETRY = "retry"
    ROLLBACK = "rollback"
    FALLBACK = "fallback"
    CHECKPOINT_RESTORE = "checkpoint_restore"
    SKIP = "skip"
    ABORT = "abort"


@dataclass
class RecoveryConfig:
    """Configuration for recovery behavior."""
    max_retries: int = 3
    retry_delay: float = 1.0
    strategy: RecoveryStrategy = RecoveryStrategy.RETRY
    checkpoint_interval: int = 10
    state_file: Optional[str] = None


@dataclass
class Checkpoint:
    """A point-in-time snapshot of execution state."""
    task_name: str
    step_index: int
    state: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    checksum: str = ""


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    recovered: bool
    strategy_used: RecoveryStrategy
    recovered_at_step: int = 0
    state: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class AutoRecoveryAction:
    """Automatic recovery handler for automation tasks.

    Supports retry, rollback, fallback chains, and checkpoint/restore.

    Example:
        recovery = AutoRecoveryAction(strategy=RecoveryStrategy.ROLLBACK)
        recovery.register_task("process", main_task, rollback=rollback_task)
        result = recovery.execute()
    """

    def __init__(
        self,
        strategy: RecoveryStrategy = RecoveryStrategy.RETRY,
        max_retries: int = 3,
        checkpoint_file: Optional[str] = None,
    ) -> None:
        """Initialize auto-recovery action.

        Args:
            strategy: Default recovery strategy.
            max_retries: Maximum retry attempts.
            checkpoint_file: Path to store checkpoints.
        """
        self.strategy = strategy
        self.max_retries = max_retries
        self.checkpoint_file = checkpoint_file
        self._tasks: Dict[str, Callable[[], Any]] = {}
        self._fallbacks: Dict[str, Callable[[], Any]] = {}
        self._checkpoints: List[Checkpoint] = []

    def register_task(
        self,
        name: str,
        task: Callable[[], Any],
        fallback: Optional[Callable[[], Any]] = None,
    ) -> "AutoRecoveryAction":
        """Register a task with optional fallback.

        Args:
            name: Task name.
            task: Main task callable.
            fallback: Fallback callable if main fails.

        Returns:
            Self for chaining.
        """
        self._tasks[name] = task
        if fallback:
            self._fallbacks[name] = fallback
        return self

    def execute(self, initial_state: Optional[Dict[str, Any]] = None) -> RecoveryResult:
        """Execute all registered tasks with recovery.

        Args:
            initial_state: Initial execution state.

        Returns:
            RecoveryResult with outcome details.
        """
        state = initial_state or {}
        step_index = 0

        for name, task in self._tasks.items():
            self._save_checkpoint(name, step_index, state)

            try:
                state[name] = task()
                step_index += 1

            except Exception as e:
                logger.warning("Task '%s' failed: %s", name, e)
                result = self._recover(name, task, state, step_index, e)
                if result.recovered:
                    state[name] = result.state
                    step_index = result.recovered_at_step + 1
                else:
                    return RecoveryResult(
                        recovered=False,
                        strategy_used=self.strategy,
                        recovered_at_step=step_index,
                        state=state,
                        error=str(e),
                    )

        return RecoveryResult(
            recovered=True,
            strategy_used=self.strategy,
            recovered_at_step=step_index,
            state=state,
        )

    def execute_single(
        self,
        func: Callable[..., T],
        *args,
        **kwargs,
    ) -> T:
        """Execute a single function with auto-recovery.

        Args:
            func: Function to execute.
            *args: Positional args.
            **kwargs: Keyword args.

        Returns:
            Function result.

        Raises:
            Exception if all recovery attempts fail.
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                logger.debug("Attempt %d failed: %s", attempt + 1, e)
                if attempt < self.max_retries:
                    delay = self.max_retries * 0.5
                    time.sleep(delay)

        raise last_error

    def _recover(
        self,
        name: str,
        task: Callable[[], Any],
        state: Dict[str, Any],
        step_index: int,
        error: Exception,
    ) -> RecoveryResult:
        """Attempt recovery using configured strategy."""
        strategy = self.strategy

        if strategy == RecoveryStrategy.FALLBACK and name in self._fallbacks:
            logger.info("Attempting fallback for '%s'", name)
            try:
                result = self._fallbacks[name]()
                return RecoveryResult(
                    recovered=True,
                    strategy_used=RecoveryStrategy.FALLBACK,
                    recovered_at_step=step_index,
                    state=result,
                )
            except Exception as fallback_error:
                logger.error("Fallback also failed for '%s': %s", name, fallback_error)

        elif strategy == RecoveryStrategy.CHECKPOINT_RESTORE:
            checkpoint = self._load_checkpoint(name)
            if checkpoint:
                logger.info("Restoring checkpoint for '%s'", name)
                return RecoveryResult(
                    recovered=True,
                    strategy_used=RecoveryStrategy.CHECKPOINT_RESTORE,
                    recovered_at_step=checkpoint.step_index,
                    state=checkpoint.state,
                )

        elif strategy == RecoveryStrategy.RETRY:
            for attempt in range(self.max_retries):
                try:
                    result = task()
                    return RecoveryResult(
                        recovered=True,
                        strategy_used=RecoveryStrategy.RETRY,
                        recovered_at_step=step_index,
                        state=result,
                    )
                except Exception as retry_error:
                    logger.debug("Retry attempt %d failed: %s", attempt + 1, retry_error)
                    time.sleep(self.max_retries * 0.5)

        elif strategy == RecoveryStrategy.SKIP:
            logger.info("Skipping failed task '%s'", name)
            return RecoveryResult(
                recovered=True,
                strategy_used=RecoveryStrategy.SKIP,
                recovered_at_step=step_index,
                state=None,
            )

        return RecoveryResult(
            recovered=False,
            strategy_used=strategy,
            recovered_at_step=step_index,
            error=str(error),
        )

    def _save_checkpoint(self, task_name: str, step_index: int, state: Dict[str, Any]) -> None:
        """Save a checkpoint to disk."""
        if not self.checkpoint_file:
            return

        checkpoint = Checkpoint(
            task_name=task_name,
            step_index=step_index,
            state=dict(state),
            checksum=self._compute_checksum(state),
        )
        self._checkpoints.append(checkpoint)

        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump([c.__dict__ for c in self._checkpoints], f, indent=2)
        except IOError as e:
            logger.error("Failed to save checkpoint: %s", e)

    def _load_checkpoint(self, task_name: str) -> Optional[Checkpoint]:
        """Load the latest checkpoint for a task."""
        if not self.checkpoint_file:
            return None

        try:
            with open(self.checkpoint_file, "r") as f:
                data = json.load(f)

            checkpoints = [
                Checkpoint(**item) for item in data
                if item["task_name"] == task_name
            ]
            return checkpoints[-1] if checkpoints else None
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _compute_checksum(self, state: Dict[str, Any]) -> str:
        """Compute a checksum of the state."""
        state_str = json.dumps(state, sort_keys=True, default=str)
        return hashlib.sha256(state_str.encode()).hexdigest()[:16]
