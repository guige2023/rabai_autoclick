"""
Action guard executor with precondition checking.

Executes actions only when guard conditions are met,
with timeout and retry support.

Author: AutoClick Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class GuardCondition:
    """
    A precondition that must be satisfied before action execution.

    Attributes:
        name: Human-readable name for debugging
        check: Callable that returns True when condition is met
        timeout: Max seconds to wait for condition
        poll_interval: Seconds between condition checks
    """

    name: str
    check: Callable[[], bool]
    timeout: float = 10.0
    poll_interval: float = 0.1


@dataclass
class GuardedAction:
    """
    An action protected by guard conditions.

    Attributes:
        name: Action identifier
        execute: The action to execute
        guards: List of conditions that must pass
        on_failure: Optional callback on execution failure
        metadata: Additional context
    """

    name: str
    execute: Callable[[], Any]
    guards: list[GuardCondition] = field(default_factory=list)
    on_failure: Callable[[Exception], None] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionOutcome:
    """Result of a guarded action execution."""

    success: bool
    action_name: str
    result: Any = None
    error: str | None = None
    guard_results: dict[str, bool] = field(default_factory=dict)
    duration_ms: float = 0.0
    waited_ms: float = 0.0


class ActionGuardExecutor:
    """
    Executes actions only when guard conditions are satisfied.

    Waits for conditions to become true, with timeout handling.

    Example:
        executor = ActionGuardExecutor()

        action = GuardedAction(
            name="click_submit",
            execute=lambda: page.click("#submit"),
            guards=[
                GuardCondition(
                    name="button_visible",
                    check=lambda: page.is_visible("#submit"),
                    timeout=5.0,
                )
            ],
        )

        result = executor.execute(action)
    """

    def __init__(
        self,
        default_timeout: float = 10.0,
        default_poll_interval: float = 0.1,
        raise_on_guard_timeout: bool = True,
    ) -> None:
        """
        Initialize guard executor.

        Args:
            default_timeout: Default guard wait timeout
            default_poll_interval: Default check interval
            raise_on_guard_timeout: Raise exception vs return failed outcome
        """
        self._default_timeout = default_timeout
        self._default_poll_interval = default_poll_interval
        self._raise_on_guard_timeout = raise_on_guard_timeout

    def execute(self, action: GuardedAction) -> ExecutionOutcome:
        """
        Execute a guarded action.

        Args:
            action: The guarded action to execute

        Returns:
            ExecutionOutcome with result or error details
        """
        start_time = time.time()
        waited = 0.0

        guard_results: dict[str, bool] = {}

        for guard in action.guards:
            timeout = guard.timeout or self._default_timeout
            poll_interval = guard.poll_interval or self._default_poll_interval

            satisfied, wait_time = self._wait_for_condition(
                guard.check, timeout, poll_interval
            )

            guard_results[guard.name] = satisfied
            waited += wait_time

            if not satisfied:
                duration = (time.time() - start_time) * 1000

                if self._raise_on_guard_timeout:
                    raise GuardTimeoutError(
                        f"Guard '{guard.name}' for action '{action.name}' "
                        f"was not satisfied within {timeout}s"
                    )

                return ExecutionOutcome(
                    success=False,
                    action_name=action.name,
                    guard_results=guard_results,
                    duration_ms=duration,
                    waited_ms=waited,
                    error=f"Guard '{guard.name}' timed out",
                )

        try:
            result = action.execute()
            duration = (time.time() - start_time) * 1000

            return ExecutionOutcome(
                success=True,
                action_name=action.name,
                result=result,
                guard_results=guard_results,
                duration_ms=duration,
                waited_ms=waited,
            )

        except Exception as e:
            duration = (time.time() - start_time) * 1000

            if action.on_failure:
                try:
                    action.on_failure(e)
                except Exception:
                    pass

            return ExecutionOutcome(
                success=False,
                action_name=action.name,
                guard_results=guard_results,
                duration_ms=duration,
                waited_ms=waited,
                error=f"{type(e).__name__}: {str(e)}",
            )

    def _wait_for_condition(
        self,
        check: Callable[[], bool],
        timeout: float,
        poll_interval: float,
    ) -> tuple[bool, float]:
        """Wait for a condition to become true."""
        start = time.time()

        while time.time() - start < timeout:
            if check():
                return True, (time.time() - start) * 1000
            time.sleep(poll_interval)

        return False, timeout * 1000

    def execute_batch(
        self,
        actions: list[GuardedAction],
        stop_on_failure: bool = True,
    ) -> list[ExecutionOutcome]:
        """
        Execute multiple guarded actions in sequence.

        Args:
            actions: List of actions to execute
            stop_on_failure: Halt batch on first failure

        Returns:
            List of outcomes for each action
        """
        results: list[ExecutionOutcome] = []

        for action in actions:
            result = self.execute(action)
            results.append(result)

            if not result.success and stop_on_failure:
                break

        return results


class GuardTimeoutError(Exception):
    """Raised when a guard condition times out."""

    pass
