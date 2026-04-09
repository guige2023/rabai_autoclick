"""Automation Guard Action Module.

Provides guardrails and validation for automation tasks including
pre-condition checks, post-condition verification, and constraint enforcement.

Example:
    >>> from actions.automation.automation_guard_action import AutomationGuardAction
    >>> action = AutomationGuardAction()
    >>> result = await action.execute_with_guard(task, preconditions, postconditions)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading
import time


class GuardType(Enum):
    """Type of guard check."""
    PRECONDITION = "precondition"
    POSTCONDITION = "postcondition"
    INVARIANT = "invariant"
    CONSTRAINT = "constraint"


class GuardStatus(Enum):
    """Status of guard check."""
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"
    ERROR = "error"


@dataclass
class Guard:
    """Guard definition.
    
    Attributes:
        name: Guard name
        guard_type: Type of guard
        check_func: Validation function
        description: Guard description
        severity: Severity level if fail
    """
    name: str
    guard_type: GuardType
    check_func: Callable[[Dict[str, Any]], bool]
    description: str = ""
    severity: str = "error"


@dataclass
class GuardResult:
    """Result of guard evaluation.
    
    Attributes:
        guard_name: Name of evaluated guard
        status: Guard status
        message: Result message
        duration: Evaluation duration
    """
    guard_name: str
    status: GuardStatus
    message: str = ""
    duration: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GuardConfig:
    """Configuration for guard enforcement.
    
    Attributes:
        fail_fast: Stop on first failure
        log_failures: Log guard failures
        collect_metrics: Collect guard metrics
        enforce_invariants: Enforce invariant checks
    """
    fail_fast: bool = True
    log_failures: bool = True
    collect_metrics: bool = True
    enforce_invariants: bool = True


@dataclass
class GuardStats:
    """Guard statistics.
    
    Attributes:
        total_checks: Total guard checks performed
        passed: Number passed
        failed: Number failed
        avg_duration: Average check duration
    """
    total_checks: int
    passed: int
    failed: int
    skipped: int
    avg_duration: float


class AutomationGuardAction:
    """Guardrail handler for automation tasks.
    
    Provides pre/post condition checking, invariant enforcement,
    and constraint validation for automation workflows.
    
    Attributes:
        config: Guard configuration
        _guards: Registered guards
        _metrics: Guard metrics
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[GuardConfig] = None,
    ) -> None:
        """Initialize guard action.
        
        Args:
            config: Guard configuration
        """
        self.config = config or GuardConfig()
        self._guards: Dict[str, Guard] = {}
        self._metrics: List[GuardResult] = []
        self._lock = threading.RLock()
    
    def register_guard(
        self,
        name: str,
        guard_type: GuardType,
        check_func: Callable[[Dict[str, Any]], bool],
        description: str = "",
        severity: str = "error",
    ) -> None:
        """Register a guard.
        
        Args:
            name: Guard name
            guard_type: Type of guard
            check_func: Validation function
            description: Guard description
            severity: Severity level
        """
        with self._lock:
            self._guards[name] = Guard(
                name=name,
                guard_type=guard_type,
                check_func=check_func,
                description=description,
                severity=severity,
            )
    
    def unregister_guard(self, name: str) -> bool:
        """Unregister a guard.
        
        Args:
            name: Guard name
        
        Returns:
            True if unregistered
        """
        with self._lock:
            if name in self._guards:
                del self._guards[name]
                return True
            return False
    
    async def check_preconditions(
        self,
        context: Dict[str, Any],
    ) -> List[GuardResult]:
        """Check all preconditions.
        
        Args:
            context: Execution context
        
        Returns:
            List of guard results
        """
        return await self._check_guards(
            GuardType.PRECONDITION,
            context,
        )
    
    async def check_postconditions(
        self,
        context: Dict[str, Any],
    ) -> List[GuardResult]:
        """Check all postconditions.
        
        Args:
            context: Execution context
        
        Returns:
            List of guard results
        """
        return await self._check_guards(
            GuardType.POSTCONDITION,
            context,
        )
    
    async def check_invariants(
        self,
        context: Dict[str, Any],
    ) -> List[GuardResult]:
        """Check all invariants.
        
        Args:
            context: Execution context
        
        Returns:
            List of guard results
        """
        if not self.config.enforce_invariants:
            return []
        
        return await self._check_guards(
            GuardType.INVARIANT,
            context,
        )
    
    async def _check_guards(
        self,
        guard_type: GuardType,
        context: Dict[str, Any],
    ) -> List[GuardResult]:
        """Check guards of specific type.
        
        Args:
            guard_type: Type of guards to check
            context: Execution context
        
        Returns:
            List of guard results
        """
        results: List[GuardResult] = []
        
        with self._lock:
            guards = [
                g for g in self._guards.values()
                if g.guard_type == guard_type
            ]
        
        for guard in guards:
            result = await self._evaluate_guard(guard, context)
            results.append(result)
            
            if self.config.collect_metrics:
                with self._lock:
                    self._metrics.append(result)
            
            if self.config.fail_fast and result.status == GuardStatus.FAIL:
                break
        
        return results
    
    async def _evaluate_guard(
        self,
        guard: Guard,
        context: Dict[str, Any],
    ) -> GuardResult:
        """Evaluate single guard.
        
        Args:
            guard: Guard to evaluate
            context: Execution context
        
        Returns:
            GuardResult
        """
        start_time = time.time()
        
        try:
            passed = guard.check_func(context)
            
            duration = time.time() - start_time
            
            return GuardResult(
                guard_name=guard.name,
                status=GuardStatus.PASS if passed else GuardStatus.FAIL,
                message="Guard passed" if passed else f"Guard failed: {guard.description}",
                duration=duration,
            )
        
        except Exception as e:
            duration = time.time() - start_time
            
            return GuardResult(
                guard_name=guard.name,
                status=GuardStatus.ERROR,
                message=f"Guard error: {str(e)}",
                duration=duration,
            )
    
    async def execute_with_guard(
        self,
        task: Callable[..., Any],
        context: Dict[str, Any],
        preconditions: Optional[List[str]] = None,
        postconditions: Optional[List[str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Tuple[Any, List[GuardResult], List[GuardResult]]:
        """Execute task with guardrails.
        
        Args:
            task: Task to execute
            context: Execution context
            preconditions: List of precondition guard names
            postconditions: List of postcondition guard names
            *args: Task positional arguments
            **kwargs: Task keyword arguments
        
        Returns:
            Tuple of (result, precondition_results, postcondition_results)
        """
        pre_results = await self.check_preconditions(context)
        
        if any(r.status == GuardStatus.FAIL for r in pre_results):
            raise RuntimeError(
                f"Preconditions failed: {[r.guard_name for r in pre_results if r.status == GuardStatus.FAIL]}"
            )
        
        result = None
        try:
            if asyncio.iscoroutinefunction(task):
                result = await task(*args, **kwargs)
            else:
                result = task(*args, **kwargs)
        finally:
            context["result"] = result
        
        post_results = await self.check_postconditions(context)
        
        return result, pre_results, post_results
    
    def get_stats(self) -> GuardStats:
        """Get guard statistics.
        
        Returns:
            GuardStats
        """
        with self._lock:
            if not self._metrics:
                return GuardStats(0, 0, 0, 0, 0.0)
            
            durations = [m.duration for m in self._metrics]
            
            return GuardStats(
                total_checks=len(self._metrics),
                passed=sum(1 for m in self._metrics if m.status == GuardStatus.PASS),
                failed=sum(1 for m in self._metrics if m.status == GuardStatus.FAIL),
                skipped=sum(1 for m in self._metrics if m.status == GuardStatus.SKIP),
                avg_duration=sum(durations) / len(durations) if durations else 0.0,
            )
    
    def clear_metrics(self) -> None:
        """Clear collected metrics."""
        with self._lock:
            self._metrics.clear()
