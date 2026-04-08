"""
Operation Guard Utilities

Provides utilities for guarding operations with
pre/post condition checks in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class GuardCheck:
    """Represents a guard condition check."""
    name: str
    passed: bool
    message: str = ""


class OperationGuard:
    """
    Guards operations with pre/post condition checks.
    
    Validates that conditions are met before and after
    operations execute.
    """

    def __init__(self) -> None:
        self._pre_checks: list[Callable[[], GuardCheck]] = []
        self._post_checks: list[Callable[[], GuardCheck]] = []

    def add_pre_check(
        self,
        name: str,
        check: Callable[[], bool],
        message: str = "",
    ) -> None:
        """Add a pre-operation check."""
        def check_wrapper() -> GuardCheck:
            try:
                passed = check()
                return GuardCheck(name=name, passed=passed, message=message)
            except Exception as e:
                return GuardCheck(name=name, passed=False, message=str(e))
        self._pre_checks.append(check_wrapper)

    def add_post_check(
        self,
        name: str,
        check: Callable[[], bool],
        message: str = "",
    ) -> None:
        """Add a post-operation check."""
        def check_wrapper() -> GuardCheck:
            try:
                passed = check()
                return GuardCheck(name=name, passed=passed, message=message)
            except Exception as e:
                return GuardCheck(name=name, passed=False, message=str(e))
        self._post_checks.append(check_wrapper)

    def run_pre_checks(self) -> list[GuardCheck]:
        """Run all pre-checks."""
        return [check() for check in self._pre_checks]

    def run_post_checks(self) -> list[GuardCheck]:
        """Run all post-checks."""
        return [check() for check in self._post_checks]

    def execute(
        self,
        operation: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Any, list[GuardCheck], list[GuardCheck]]:
        """
        Execute operation with guard checks.
        
        Returns:
            Tuple of (result, pre_checks, post_checks).
        """
        pre_results = self.run_pre_checks()
        any_failed = any(not c.passed for c in pre_results)
        if any_failed:
            return None, pre_results, []

        try:
            result = operation(*args, **kwargs)
        except Exception as e:
            post_results = self.run_post_checks()
            return None, pre_results, post_results

        post_results = self.run_post_checks()
        return result, pre_results, post_results
