"""
Automation Guard Action Module.

Provides pre/post action guards with
validation and cleanup hooks.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from .api_retry_action import RetryConfig


class GuardType(Enum):
    """Guard types."""
    PRE = "pre"
    POST = "post"
    CLEANUP = "cleanup"
    VALIDATION = "validation"


@dataclass
class Guard:
    """Action guard."""
    name: str
    guard_type: GuardType
    func: Callable[[Any], bool]
    required: bool = False
    timeout: float = 5.0


@dataclass
class GuardResult:
    """Guard execution result."""
    success: bool
    guard_name: str
    message: str = ""
    error: Optional[Exception] = None


class AutomationGuardAction:
    """
    Pre/post action guards.

    Example:
        guard = AutomationGuardAction()

        guard.add_pre_guard("validate_input", validate_func)
        guard.add_post_guard("log_result", log_func)

        result = await guard.execute_with_guards(action_func, *args)
    """

    def __init__(self):
        self._pre_guards: list[Guard] = []
        self._post_guards: list[Guard] = []
        self._cleanup_guards: list[Guard] = []
        self._validation_guards: list[Guard] = []

    def add_pre_guard(
        self,
        name: str,
        func: Callable[[Any], bool],
        required: bool = True
    ) -> "AutomationGuardAction":
        """Add pre-execution guard."""
        guard = Guard(
            name=name,
            guard_type=GuardType.PRE,
            func=func,
            required=required
        )
        self._pre_guards.append(guard)
        return self

    def add_post_guard(
        self,
        name: str,
        func: Callable[[Any], bool],
        required: bool = False
    ) -> "AutomationGuardAction":
        """Add post-execution guard."""
        guard = Guard(
            name=name,
            guard_type=GuardType.POST,
            func=func,
            required=required
        )
        self._post_guards.append(guard)
        return self

    def add_cleanup_guard(
        self,
        name: str,
        func: Callable
    ) -> "AutomationGuardAction":
        """Add cleanup guard."""
        guard = Guard(
            name=name,
            guard_type=GuardType.CLEANUP,
            func=func,
            required=False
        )
        self._cleanup_guards.append(guard)
        return self

    def add_validation_guard(
        self,
        name: str,
        func: Callable[[Any], bool],
        required: bool = True
    ) -> "AutomationGuardAction":
        """Add validation guard."""
        guard = Guard(
            name=name,
            guard_type=GuardType.VALIDATION,
            func=func,
            required=required
        )
        self._validation_guards.append(guard)
        return self

    async def _run_guards(
        self,
        guards: list[Guard],
        context: Any
    ) -> list[GuardResult]:
        """Run guards and collect results."""
        results = []

        for guard in guards:
            try:
                if asyncio.iscoroutinefunction(guard.func):
                    result = await asyncio.wait_for(
                        guard.func(context),
                        timeout=guard.timeout
                    )
                else:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(guard.func, context),
                        timeout=guard.timeout
                    )

                guard_result = GuardResult(
                    success=bool(result),
                    guard_name=guard.name,
                    message="Guard passed" if result else "Guard returned False"
                )

            except asyncio.TimeoutError:
                guard_result = GuardResult(
                    success=not guard.required,
                    guard_name=guard.name,
                    message="Guard timed out",
                    error=TimeoutError(f"Guard {guard.name} timed out")
                )

            except Exception as e:
                guard_result = GuardResult(
                    success=not guard.required,
                    guard_name=guard.name,
                    message=f"Guard failed: {str(e)}",
                    error=e
                )

            results.append(guard_result)

            if not guard_result.success and guard.required:
                break

        return results

    async def execute_with_guards(
        self,
        action_func: Callable,
        context: Any = None,
        *args: Any,
        **kwargs: Any
    ) -> tuple[Any, list[GuardResult]]:
        """Execute action with guards."""
        pre_results = await self._run_guards(self._pre_guards, context)

        for result in pre_results:
            if not result.success and result.guard_name in [g.name for g in self._pre_guards if g.required]:
                return None, pre_results

        action_result = None
        action_error = None

        try:
            if asyncio.iscoroutinefunction(action_func):
                action_result = await action_func(context, *args, **kwargs)
            else:
                action_result = await asyncio.to_thread(
                    action_func, context, *args, **kwargs
                )
        except Exception as e:
            action_error = e

        post_results = await self._run_guards(self._post_guards, action_result)

        if action_error:
            raise action_error

        return action_result, pre_results + post_results

    async def run_cleanup(self, context: Any = None) -> list[GuardResult]:
        """Run cleanup guards."""
        return await self._run_guards(self._cleanup_guards, context)

    def get_guard_names(self) -> dict[str, int]:
        """Get guard counts by type."""
        return {
            "pre": len(self._pre_guards),
            "post": len(self._post_guards),
            "cleanup": len(self._cleanup_guards),
            "validation": len(self._validation_guards)
        }
