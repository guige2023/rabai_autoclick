"""
API Fault Injection Action Module.

Fault injection framework for testing API resilience by introducing
controlled failures, delays, and errors into API call paths.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FaultType(Enum):
    """Types of faults that can be injected."""
    DELAY = "delay"
    ERROR = "error"
    TIMEOUT = "timeout"
    ABORT = "abort"
    DUPLICATE = "duplicate"
    CORRUPT = "corrupt"
    REJECT = "reject"


@dataclass
class FaultRule:
    """A rule defining when and how to inject a fault."""
    name: str
    fault_type: FaultType
    probability: float = 1.0  # 0.0 to 1.0
    delay_ms: int = 0
    error_message: str = "Injected fault"
    error_code: int = 500
    rate_limit_count: int = 0  # inject after N calls
    rate_limit_period: float = 60.0  # seconds
    target_operations: List[str] = field(default_factory=list)  # empty = all


@dataclass
class FaultResult:
    """Result of a fault injection action."""
    fault_injected: bool
    fault_type: Optional[FaultType] = None
    delay_ms: float = 0.0
    error_message: Optional[str] = None
    original_result: Any = None


@dataclass
class InjectionStats:
    """Statistics for fault injection campaigns."""
    total_calls: int = 0
    faults_injected: int = 0
    faults_by_type: Dict[FaultType, int] = field(default_factory=dict)
    average_delay_ms: float = 0.0


class FaultInjector:
    """
    Injects faults into API calls based on configured rules.

    Supports delay, error, timeout, abort, and other fault types
    with configurable probability and targeting.
    """

    def __init__(self) -> None:
        self._rules: List[FaultRule] = []
        self._call_counts: Dict[str, int] = {}
        self._enabled = True
        self._stats = InjectionStats()

    def add_rule(self, rule: FaultRule) -> None:
        """Add a fault injection rule."""
        self._rules.append(rule)
        logger.info(f"Added fault rule: {rule.name} ({rule.fault_type.value})")

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name."""
        for i, rule in enumerate(self._rules):
            if rule.name == name:
                del self._rules[i]
                logger.info(f"Removed fault rule: {name}")
                return True
        return False

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable fault injection."""
        self._enabled = enabled

    def get_stats(self) -> InjectionStats:
        """Get injection statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset injection statistics."""
        self._stats = InjectionStats()
        self._call_counts = {}

    def _should_inject(self, rule: FaultRule, operation_name: str) -> bool:
        """Determine if a fault should be injected."""
        if not self._enabled:
            return False

        if rule.target_operations and operation_name not in rule.target_operations:
            return False

        if rule.probability < 1.0 and random.random() > rule.probability:
            return False

        # Rate limiting check
        if rule.rate_limit_count > 0:
            key = f"{rule.name}:{operation_name}"
            count = self._call_counts.get(key, 0) + 1
            self._call_counts[key] = count
            if count < rule.rate_limit_count:
                return False

        return True

    def inject(
        self,
        operation_name: str,
    ) -> Optional[FaultResult]:
        """Attempt to inject a fault for the given operation."""
        if not self._enabled:
            return None

        self._stats.total_calls += 1

        applicable_rules = [r for r in self._rules if not r.target_operations or operation_name in r.target_operations]

        for rule in applicable_rules:
            if not self._should_inject(rule, operation_name):
                continue

            result = FaultResult(fault_injected=True)

            if rule.fault_type == FaultType.DELAY:
                result.fault_type = FaultType.DELAY
                result.delay_ms = rule.delay_ms
                time.sleep(rule.delay_ms / 1000.0)

            elif rule.fault_type == FaultType.ERROR:
                result.fault_type = FaultType.ERROR
                result.error_message = rule.error_message

            elif rule.fault_type == FaultType.TIMEOUT:
                result.fault_type = FaultType.TIMEOUT
                result.delay_ms = rule.delay_ms
                time.sleep(rule.delay_ms / 1000.0)
                result.error_message = "Request timed out (injected)"

            elif rule.fault_type == FaultType.ABORT:
                result.fault_type = FaultType.ABORT
                result.error_message = "Connection aborted (injected)"

            elif rule.fault_type == FaultType.REJECT:
                result.fault_type = FaultType.REJECT
                result.error_code = rule.error_code
                result.error_message = f"Request rejected with code {rule.error_code} (injected)"

            # Update stats
            self._stats.faults_injected += 1
            if result.fault_type:
                type_count = self._stats.faults_by_type.get(result.fault_type, 0)
                self._stats.faults_by_type[result.fault_type] = type_count + 1

            self._stats.average_delay_ms = (
                (self._stats.average_delay_ms * (self._stats.faults_injected - 1) + result.delay_ms)
                / self._stats.faults_injected
            )

            logger.debug(f"Fault injected: {rule.name} for {operation_name}")
            return result

        return None

    async def inject_async(
        self,
        operation_name: str,
    ) -> Optional[FaultResult]:
        """Async version of inject."""
        if not self._enabled:
            return None

        self._stats.total_calls += 1

        applicable_rules = [r for r in self._rules if not r.target_operations or operation_name in r.target_operations]

        for rule in applicable_rules:
            if not self._should_inject(rule, operation_name):
                continue

            result = FaultResult(fault_injected=True)
            result.fault_type = rule.fault_type

            if rule.fault_type == FaultType.DELAY:
                result.delay_ms = rule.delay_ms
                await asyncio.sleep(rule.delay_ms / 1000.0)

            elif rule.fault_type == FaultType.ERROR:
                result.error_message = rule.error_message

            elif rule.fault_type == FaultType.TIMEOUT:
                result.delay_ms = rule.delay_ms
                await asyncio.sleep(rule.delay_ms / 1000.0)
                result.error_message = "Request timed out (injected)"

            elif rule.fault_type == FaultType.ABORT:
                result.error_message = "Connection aborted (injected)"

            elif rule.fault_type == FaultType.REJECT:
                result.error_code = rule.error_code
                result.error_message = f"Rejected with {rule.error_code} (injected)"

            # Update stats
            self._stats.faults_injected += 1
            type_count = self._stats.faults_by_type.get(result.fault_type, 0)
            self._stats.faults_by_type[result.fault_type] = type_count + 1

            if result.delay_ms > 0:
                total_delay = self._stats.average_delay_ms * (self._stats.faults_injected - 1)
                self._stats.average_delay_ms = (total_delay + result.delay_ms) / self._stats.faults_injected

            return result

        return None


class APIFaultInjectionAction:
    """
    High-level API fault injection wrapper.

    Wraps API calls with fault injection rules and provides
    a declarative interface for chaos engineering.

    Example:
        fault_injector = APIFaultInjectionAction()
        fault_injector.add_rule(FaultRule(
            name="slow-db",
            fault_type=FaultType.DELAY,
            probability=0.1,
            delay_ms=500,
            target_operations=["db_query"],
        ))

        result = await fault_injector.execute(api.query_data, "users")
    """

    def __init__(self) -> None:
        self.injector = FaultInjector()
        self._operation_hooks: Dict[str, Callable[[], None]] = {}

    def add_rule(
        self,
        name: str,
        fault_type: FaultType,
        probability: float = 1.0,
        delay_ms: int = 0,
        error_message: str = "Injected fault",
        error_code: int = 500,
        target_operations: Optional[List[str]] = None,
    ) -> None:
        """Add a fault injection rule."""
        rule = FaultRule(
            name=name,
            fault_type=fault_type,
            probability=probability,
            delay_ms=delay_ms,
            error_message=error_message,
            error_code=error_code,
            target_operations=target_operations or [],
        )
        self.injector.add_rule(rule)

    def remove_rule(self, name: str) -> bool:
        """Remove a fault rule."""
        return self.injector.remove_rule(name)

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable all fault injection."""
        self.injector.set_enabled(enabled)

    async def execute(
        self,
        func: Callable[..., T],
        operation_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute a function with fault injection."""
        fault_result = await self.injector.inject_async(operation_name)

        if fault_result and fault_result.fault_type in (FaultType.ERROR, FaultType.TIMEOUT, FaultType.ABORT, FaultType.REJECT):
            raise RuntimeError(fault_result.error_message or "Injected fault")

        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    def execute_sync(
        self,
        func: Callable[..., T],
        operation_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Synchronous execute with fault injection."""
        fault_result = self.injector.inject(operation_name)

        if fault_result and fault_result.fault_type in (FaultType.ERROR, FaultType.TIMEOUT, FaultType.ABORT, FaultType.REJECT):
            raise RuntimeError(fault_result.error_message or "Injected fault")

        return func(*args, **kwargs)

    def get_stats(self) -> InjectionStats:
        """Get injection statistics."""
        return self.injector.get_stats()

    def reset_stats(self) -> None:
        """Reset statistics."""
        self.injector.reset_stats()


def create_chaos_rules() -> List[FaultRule]:
    """Create a set of common chaos engineering rules."""
    return [
        FaultRule(
            name="random-delay",
            fault_type=FaultType.DELAY,
            probability=0.05,
            delay_ms=200,
            error_message="Simulated network latency",
        ),
        FaultRule(
            name="random-error",
            fault_type=FaultType.ERROR,
            probability=0.01,
            error_message="Simulated internal server error",
            error_code=500,
        ),
        FaultRule(
            name="random-timeout",
            fault_type=FaultType.TIMEOUT,
            probability=0.02,
            delay_ms=30000,
            error_message="Request timeout",
        ),
    ]
