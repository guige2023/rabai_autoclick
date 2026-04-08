"""
Automation Guard Action Module.

Provides pre/post condition guards for automation actions
 with automatic rollback on failure.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class GuardType(Enum):
    """Type of guard."""
    PRECONDITION = "precondition"
    POSTCONDITION = "postcondition"
    INVARIANT = "invariant"


@dataclass
class GuardResult:
    """Result of guard evaluation."""
    passed: bool
    guard_name: str
    message: str = ""
    details: Optional[dict[str, Any]] = None


@dataclass
class GuardAction:
    """A guard condition with optional remediation."""
    name: str
    guard_type: GuardType
    condition: Callable[[], bool]
    error_message: str = "Guard condition failed"
    remediation: Optional[Callable[[], Any]] = None


class AutomationGuardAction:
    """
    Pre/post condition guards for automation workflows.

    Evaluates conditions before and after actions with
    automatic rollback and remediation support.

    Example:
        guard = AutomationGuardAction()
        guard.add_precondition("app_running", check_app_running)
        guard.add_postcondition("data_saved", check_data_saved)
        with guard.guard("my_action"):
            perform_action()
    """

    def __init__(self) -> None:
        self._preconditions: list[GuardAction] = []
        self._postconditions: list[GuardAction] = []
        self._invariants: list[GuardAction] = []

    def add_precondition(
        self,
        name: str,
        condition: Callable[[], bool],
        error_message: str = "Precondition failed",
        remediation: Optional[Callable[[], Any]] = None,
    ) -> "AutomationGuardAction":
        """Add a precondition guard."""
        guard = GuardAction(
            name=name,
            guard_type=GuardType.PRECONDITION,
            condition=condition,
            error_message=error_message,
            remediation=remediation,
        )
        self._preconditions.append(guard)
        return self

    def add_postcondition(
        self,
        name: str,
        condition: Callable[[], bool],
        error_message: str = "Postcondition failed",
        remediation: Optional[Callable[[], Any]] = None,
    ) -> "AutomationGuardAction":
        """Add a postcondition guard."""
        guard = GuardAction(
            name=name,
            guard_type=GuardType.POSTCONDITION,
            condition=condition,
            error_message=error_message,
            remediation=remediation,
        )
        self._postconditions.append(guard)
        return self

    def add_invariant(
        self,
        name: str,
        condition: Callable[[], bool],
        error_message: str = "Invariant violated",
    ) -> "AutomationGuardAction":
        """Add an invariant guard."""
        guard = GuardAction(
            name=name,
            guard_type=GuardType.INVARIANT,
            condition=condition,
            error_message=error_message,
        )
        self._invariants.append(guard)
        return self

    def evaluate_preconditions(self) -> list[GuardResult]:
        """Evaluate all preconditions."""
        return self._evaluate_guards(self._preconditions)

    def evaluate_postconditions(self) -> list[GuardResult]:
        """Evaluate all postconditions."""
        return self._evaluate_guards(self._postconditions)

    def evaluate_invariants(self) -> list[GuardResult]:
        """Evaluate all invariants."""
        return self._evaluate_guards(self._invariants)

    def _evaluate_guards(self, guards: list[GuardAction]) -> list[GuardResult]:
        """Evaluate a list of guards."""
        results: list[GuardResult] = []

        for guard in guards:
            try:
                passed = guard.condition()
                results.append(GuardResult(
                    passed=passed,
                    guard_name=guard.name,
                    message=guard.error_message if not passed else "",
                ))

                if not passed and guard.remediation:
                    logger.info(f"Running remediation for {guard.name}")
                    guard.remediation()

            except Exception as e:
                results.append(GuardResult(
                    passed=False,
                    guard_name=guard.name,
                    message=f"Guard evaluation error: {e}",
                ))

        return results

    def all_passed(self, results: list[GuardResult]) -> bool:
        """Check if all guards passed."""
        return all(r.passed for r in results)

    def guard(
        self,
        action_name: str,
        action_func: Callable,
        rollback: Optional[Callable[[], None]] = None,
    ) -> Any:
        """Execute an action with guard protection."""
        pre_results = self.evaluate_preconditions()
        if not self.all_passed(pre_results):
            failed = [r for r in pre_results if not r.passed]
            logger.error(f"Preconditions failed for {action_name}: {failed}")
            if rollback:
                rollback()
            raise RuntimeError(f"Preconditions failed for {action_name}")

        try:
            result = action_func()
        except Exception as e:
            logger.error(f"Action {action_name} failed: {e}")
            if rollback:
                rollback()
            raise

        post_results = self.evaluate_postconditions()
        if not self.all_passed(post_results):
            failed = [r for r in post_results if not r.passed]
            logger.error(f"Postconditions failed for {action_name}: {failed}")
            if rollback:
                rollback()
            raise RuntimeError(f"Postconditions failed for {action_name}")

        return result

    async def guard_async(
        self,
        action_name: str,
        action_func: Callable,
        rollback: Optional[Callable[[], None]] = None,
    ) -> Any:
        """Execute an async action with guard protection."""
        pre_results = self.evaluate_preconditions()
        if not self.all_passed(pre_results):
            raise RuntimeError(f"Preconditions failed for {action_name}")

        try:
            result = await action_func()
        except Exception as e:
            logger.error(f"Action {action_name} failed: {e}")
            if rollback:
                rollback()
            raise

        post_results = self.evaluate_postconditions()
        if not self.all_passed(post_results):
            if rollback:
                rollback()
            raise RuntimeError(f"Postconditions failed for {action_name}")

        return result
