"""
Action Guard Utility

Protects automation actions with pre/post condition checks.
Ensures actions execute only when appropriate conditions are met.

Example:
    >>> guard = ActionGuard()
    >>> guard.add_precondition(lambda ctx: ctx.get("app_focused"))
    >>> guard.add_postcondition(lambda ctx, result: result is not None)
    >>> result = guard.execute(lambda: click_button(), context)
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class GuardResult:
    """Result of a guarded action execution."""
    success: bool
    preconditions_passed: bool
    postconditions_passed: bool
    error: Optional[str] = None
    duration: float = 0.0


@dataclass
class Condition:
    """A condition with name and predicate."""
    name: str
    predicate: Callable[[dict], bool]
    description: str = ""
    enabled: bool = True


class ActionGuard:
    """
    Pre/post condition guard for automation actions.

    Args:
        strict: If True, raises on condition failure.
    """

    def __init__(self, strict: bool = False) -> None:
        self.strict = strict
        self._preconditions: list[Condition] = []
        self._postconditions: list[Condition] = []
        self._lock = threading.Lock()

    def add_precondition(
        self,
        predicate: Callable[[dict], bool],
        name: Optional[str] = None,
        description: str = "",
    ) -> None:
        """Add a precondition that must pass before action execution."""
        cond_name = name or f"pre_{len(self._preconditions)}"
        self._preconditions.append(Condition(
            name=cond_name,
            predicate=predicate,
            description=description,
        ))

    def add_postcondition(
        self,
        predicate: Callable[[dict, Any], bool],
        name: Optional[str] = None,
        description: str = "",
    ) -> None:
        """Add a postcondition that must pass after action execution."""
        cond_name = name or f"post_{len(self._postconditions)}"
        self._postconditions.append(Condition(
            name=cond_name,
            predicate=predicate,
            description=description,
        ))

    def remove_precondition(self, name: str) -> bool:
        """Remove a precondition by name."""
        with self._lock:
            for i, c in enumerate(self._preconditions):
                if c.name == name:
                    self._preconditions.pop(i)
                    return True
        return False

    def remove_postcondition(self, name: str) -> bool:
        """Remove a postcondition by name."""
        with self._lock:
            for i, c in enumerate(self._postconditions):
                if c.name == name:
                    self._postconditions.pop(i)
                    return True
        return False

    def execute(
        self,
        action: Callable[[], Any],
        context: Optional[dict] = None,
    ) -> tuple[Any, GuardResult]:
        """
        Execute an action with guard protection.

        Args:
            action: The action function to execute.
            context: Context dictionary passed to conditions.

        Returns:
            Tuple of (action result, GuardResult).
        """
        start_time = time.time()
        ctx = context or {}

        # Check preconditions
        pre_passed = True
        failed_pre: Optional[str] = None

        for cond in self._preconditions:
            if not cond.enabled:
                continue
            try:
                if not cond.predicate(ctx):
                    pre_passed = False
                    failed_pre = cond.name
                    break
            except Exception as e:
                pre_passed = False
                failed_pre = f"{cond.name}: {e}"
                break

        if not pre_passed:
            if self.strict:
                raise RuntimeError(f"Precondition failed: {failed_pre}")
            return None, GuardResult(
                success=False,
                preconditions_passed=False,
                postconditions_passed=True,
                error=f"Precondition failed: {failed_pre}",
                duration=time.time() - start_time,
            )

        # Execute action
        result = None
        error: Optional[str] = None
        try:
            result = action()
        except Exception as e:
            error = str(e)
            if self.strict:
                raise

        duration = time.time() - start_time

        # Check postconditions
        post_passed = True
        failed_post: Optional[str] = None

        if error is None:
            for cond in self._postconditions:
                if not cond.enabled:
                    continue
                try:
                    if not cond.predicate(ctx, result):
                        post_passed = False
                        failed_post = cond.name
                        break
                except Exception as e:
                    post_passed = False
                    failed_post = f"{cond.name}: {e}"
                    break

        if not post_passed:
            if self.strict:
                raise RuntimeError(f"Postcondition failed: {failed_post}")

        return result, GuardResult(
            success=error is None,
            preconditions_passed=pre_passed,
            postconditions_passed=post_passed,
            error=error or (f"Postcondition failed: {failed_post}" if not post_passed else None),
            duration=duration,
        )

    def check_preconditions(self, context: dict) -> tuple[bool, list[str]]:
        """
        Check all preconditions without executing action.

        Returns:
            Tuple of (all_passed, list of failed condition names).
        """
        failed = []
        for cond in self._preconditions:
            if not cond.enabled:
                continue
            try:
                if not cond.predicate(context):
                    failed.append(cond.name)
            except Exception as e:
                failed.append(f"{cond.name}: {e}")

        return (len(failed) == 0, failed)

    def enable_condition(self, name: str, enabled: bool) -> bool:
        """Enable or disable a condition by name."""
        for cond in self._preconditions:
            if cond.name == name:
                cond.enabled = enabled
                return True
        for cond in self._postconditions:
            if cond.name == name:
                cond.enabled = enabled
                return True
        return False

    def list_conditions(self) -> dict[str, list[str]]:
        """List all registered conditions."""
        return {
            "preconditions": [c.name for c in self._preconditions],
            "postconditions": [c.name for c in self._postconditions],
        }
