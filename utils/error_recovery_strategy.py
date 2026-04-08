"""Error recovery strategy framework for UI automation.

Provides a framework for defining and executing error recovery
strategies when UI automation actions fail.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class RecoveryStrategy(Enum):
    """Predefined error recovery strategies."""
    RETRY = auto()
    RETRY_WITH_BACKOFF = auto()
    SKIP = auto()
    FALLBACK = auto()
    CLEANUP = auto()
    ESCALATE = auto()
    IGNORE = auto()


@dataclass
class RecoveryStep:
    """A single step in a recovery sequence.

    Attributes:
        strategy: The recovery strategy to use.
        action: Optional callable action to execute.
        max_attempts: Maximum attempts for this step.
        delay: Delay in seconds between attempts.
        description: Human-readable description.
    """
    strategy: RecoveryStrategy
    action: Optional[Callable[[], Any]] = None
    max_attempts: int = 3
    delay: float = 1.0
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class ErrorRecoveryEngine:
    """Engine for executing error recovery strategies.

    Manages a sequence of recovery steps and executes them
    when automation actions fail.
    """

    def __init__(self) -> None:
        """Initialize with empty recovery strategies."""
        self._strategies: dict[str, list[RecoveryStep]] = {}
        self._on_recovery_callbacks: list[
            Callable[[str, RecoveryStep, bool], None]
        ] = []

    def register(
        self,
        error_type: str,
        steps: list[RecoveryStep],
    ) -> None:
        """Register a recovery sequence for an error type."""
        self._strategies[error_type] = steps

    def register_simple(
        self,
        error_type: str,
        strategy: RecoveryStrategy,
        action: Optional[Callable[[], Any]] = None,
        max_attempts: int = 3,
        delay: float = 1.0,
    ) -> None:
        """Register a simple single-step recovery."""
        step = RecoveryStep(
            strategy=strategy,
            action=action,
            max_attempts=max_attempts,
            delay=delay,
        )
        self._strategies[error_type] = [step]

    def recover(
        self,
        error_type: str,
        error_data: Any = None,
    ) -> bool:
        """Execute recovery for an error type.

        Returns True if recovery succeeded.
        """
        steps = self._strategies.get(error_type, [])
        for step in steps:
            success = self._execute_step(step)
            self._notify_recovery(error_type, step, success)
            if success:
                return True
        return False

    def _execute_step(self, step: RecoveryStep) -> bool:
        """Execute a single recovery step."""
        for attempt in range(step.max_attempts):
            try:
                if step.action:
                    result = step.action()
                    if result:
                        return True
            except Exception:
                pass
            if attempt < step.max_attempts - 1:
                import time
                time.sleep(step.delay)
        return False

    def on_recovery(
        self,
        callback: Callable[[str, RecoveryStep, bool], None],
    ) -> None:
        """Register a callback for recovery attempts."""
        self._on_recovery_callbacks.append(callback)

    def _notify_recovery(
        self,
        error_type: str,
        step: RecoveryStep,
        success: bool,
    ) -> None:
        """Notify recovery callbacks."""
        for cb in self._on_recovery_callbacks:
            try:
                cb(error_type, step, success)
            except Exception:
                pass


# Convenience decorators
def recovery_step(
    strategy: RecoveryStrategy,
    max_attempts: int = 3,
    delay: float = 1.0,
) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Decorator to mark a function as a recovery step."""
    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        func._recovery_step = True
        func._strategy = strategy
        func._max_attempts = max_attempts
        func._delay = delay
        return func
    return decorator
