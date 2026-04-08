"""Automation Guard Action Module.

Provides pre/post condition checking, validation chains,
and guard-based workflow enforcement.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class GuardCondition:
    """Guard condition definition."""
    name: str
    check_fn: Callable[[Any], bool]
    error_message: str
    severity: str = "error"


@dataclass
class GuardResult:
    """Guard check result."""
    passed: bool
    failed_conditions: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


class AutomationGuardAction:
    """Guard-based condition checker.

    Example:
        guard = AutomationGuardAction()

        guard.add_precondition("user_authenticated", is_authenticated)
        guard.add_postcondition("result_valid", validate_result)

        if guard.check_preconditions(context):
            result = execute_action()
            guard.check_postconditions(result)
    """

    def __init__(self) -> None:
        self._preconditions: List[GuardCondition] = []
        self._postconditions: List[GuardCondition] = []
        self._invariants: List[GuardCondition] = []

    def add_precondition(
        self,
        name: str,
        check_fn: Callable[[Any], bool],
        error_message: str = "",
        severity: str = "error",
    ) -> "AutomationGuardAction":
        """Add precondition.

        Returns self for chaining.
        """
        self._preconditions.append(GuardCondition(
            name=name,
            check_fn=check_fn,
            error_message=error_message or f"Precondition '{name}' failed",
            severity=severity,
        ))
        return self

    def add_postcondition(
        self,
        name: str,
        check_fn: Callable[[Any], bool],
        error_message: str = "",
        severity: str = "error",
    ) -> "AutomationGuardAction":
        """Add postcondition.

        Returns self for chaining.
        """
        self._postconditions.append(GuardCondition(
            name=name,
            check_fn=check_fn,
            error_message=error_message or f"Postcondition '{name}' failed",
            severity=severity,
        ))
        return self

    def add_invariant(
        self,
        name: str,
        check_fn: Callable[[Any], bool],
        error_message: str = "",
        severity: str = "error",
    ) -> "AutomationGuardAction":
        """Add invariant (checked before and after).

        Returns self for chaining.
        """
        self._invariants.append(GuardCondition(
            name=name,
            check_fn=check_fn,
            error_message=error_message or f"Invariant '{name}' violated",
            severity=severity,
        ))
        return self

    def check_preconditions(
        self,
        context: Any,
    ) -> GuardResult:
        """Check all preconditions.

        Args:
            context: Context object to check

        Returns:
            GuardResult
        """
        return self._check_conditions(
            self._preconditions + self._invariants,
            context,
            "preconditions"
        )

    def check_postconditions(
        self,
        result: Any,
        context: Optional[Any] = None,
    ) -> GuardResult:
        """Check all postconditions.

        Args:
            result: Result to check
            context: Optional context

        Returns:
            GuardResult
        """
        conditions = self._postconditions + self._invariants
        if context is not None:
            conditions = self._preconditions + conditions

        return self._check_conditions(conditions, result, "postconditions")

    def _check_conditions(
        self,
        conditions: List[GuardCondition],
        context: Any,
        condition_type: str,
    ) -> GuardResult:
        """Check conditions and return result."""
        failed: List[str] = []
        details: Dict[str, Any] = {}

        for condition in conditions:
            try:
                passed = condition.check_fn(context)
                details[condition.name] = passed

                if not passed:
                    failed.append(condition.error_message)
                    logger.warning(
                        f"{condition_type}: {condition.name} failed - "
                        f"{condition.error_message}"
                    )

            except Exception as e:
                details[condition.name] = False
                failed.append(f"{condition.name}: {str(e)}")
                logger.error(f"Guard check error for {condition.name}: {e}")

        return GuardResult(
            passed=len(failed) == 0,
            failed_conditions=failed,
            details=details,
        )

    def guard_execute(
        self,
        func: Callable[..., T],
        context: Any,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with guard checking.

        Args:
            func: Function to execute
            context: Context for precondition checks
            *args: Positional args for func
            **kwargs: Keyword args for func

        Returns:
            Function result

        Raises:
            GuardViolationError: If any guard fails
        """
        pre_result = self.check_preconditions(context)
        if not pre_result.passed:
            raise GuardViolationError(
                "Preconditions not met",
                pre_result.failed_conditions
            )

        try:
            if callable(func):
                result = func(*args, **kwargs)
            else:
                raise ValueError("func is not callable")

            post_result = self.check_postconditions(result, context)
            if not post_result.passed:
                raise GuardViolationError(
                    "Postconditions not met",
                    post_result.failed_conditions
                )

            return result

        except Exception as e:
            raise GuardViolationError(
                f"Execution failed: {str(e)}",
                []
            )

    def clear(self) -> None:
        """Clear all guards."""
        self._preconditions.clear()
        self._postconditions.clear()
        self._invariants.clear()


class GuardViolationError(Exception):
    """Raised when guard conditions are not met."""
    def __init__(self, message: str, failed_conditions: List[str]) -> None:
        super().__init__(message)
        self.failed_conditions = failed_conditions
