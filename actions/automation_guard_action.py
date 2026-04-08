"""
Automation Guard Action Module.

Guard rails and circuit breakers for automation,
prevents runaway processes and enforces limits.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class GuardState(Enum):
    """Guard state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class GuardConfig:
    """Guard configuration."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    max_execution_time: float = 300.0


class AutomationGuardAction:
    """
    Circuit breaker and guard rail for automation.

    Prevents runaway automation by enforcing
    failure thresholds and execution time limits.

    Example:
        guard = AutomationGuardAction(failure_threshold=3)
        with guard.protect():
            run_automation()
    """

    def __init__(
        self,
        config: Optional[GuardConfig] = None,
        name: str = "default",
    ) -> None:
        self.config = config or GuardConfig()
        self.name = name
        self._state = GuardState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0

    def protect(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute function with guard protection."""
        self._total_calls += 1

        if not self._can_execute():
            raise RuntimeError(
                f"Guard '{self.name}' is OPEN. Circuit breaker tripped."
            )

        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise

        finally:
            elapsed = time.time() - start_time
            if elapsed > self.config.max_execution_time:
                logger.warning(
                    "Guard '%s': Execution took %.2fs (max: %.2fs)",
                    self.name, elapsed, self.config.max_execution_time
                )

    def _can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self._state == GuardState.CLOSED:
            return True

        if self._state == GuardState.OPEN:
            if self._should_attempt_recovery():
                self._state = GuardState.HALF_OPEN
                self._half_open_calls = 0
                logger.info("Guard '%s' entering HALF_OPEN state", self.name)
                return True
            return False

        if self._state == GuardState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

        return False

    def _on_success(self) -> None:
        """Handle successful execution."""
        self._failure_count = 0
        self._total_successes += 1

        if self._state == GuardState.HALF_OPEN:
            self._state = GuardState.CLOSED
            logger.info("Guard '%s' recovered to CLOSED state", self.name)

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self._failure_count += 1
        self._total_failures += 1
        self._last_failure_time = time.time()

        if self._state == GuardState.HALF_OPEN:
            self._state = GuardState.OPEN
            logger.warning("Guard '%s' failing in HALF_OPEN, reopening", self.name)

        elif self._failure_count >= self.config.failure_threshold:
            self._state = GuardState.OPEN
            logger.warning(
                "Guard '%s' OPENED after %d failures",
                self.name, self._failure_count
            )

    def _should_attempt_recovery(self) -> bool:
        """Check if recovery should be attempted."""
        if self._last_failure_time is None:
            return True

        return (
            time.time() - self._last_failure_time
        ) >= self.config.recovery_timeout

    def get_state(self) -> GuardState:
        """Get current guard state."""
        return self._state

    def reset(self) -> None:
        """Reset guard to closed state."""
        self._state = GuardState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0
        self._last_failure_time = None

    def get_stats(self) -> dict[str, Any]:
        """Get guard statistics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "total_calls": self._total_calls,
            "total_successes": self._total_successes,
            "total_failures": self._total_failures,
            "success_rate": (
                self._total_successes / self._total_calls * 100
                if self._total_calls > 0 else 100.0
            ),
        }
