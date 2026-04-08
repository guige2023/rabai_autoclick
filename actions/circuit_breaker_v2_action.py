"""Circuit Breaker V2 Action Module.

Provides circuit breaker pattern with
half-open state testing.
"""

import time
from typing import Any, Callable, Dict
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation."""
    name: str
    failure_threshold: int
    recovery_timeout: float
    success_threshold: int
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0


class CircuitBreakerV2Manager:
    """Manages circuit breakers."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}

    def create_breaker(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 3
    ) -> str:
        """Create circuit breaker."""
        self._breakers[name] = CircuitBreaker(
            name=name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            success_threshold=success_threshold
        )
        return name

    def get_breaker(self, name: str) -> CircuitBreaker:
        """Get breaker."""
        return self._breakers.get(name)

    def is_callable(self, name: str) -> bool:
        """Check if call is allowed."""
        breaker = self._breakers.get(name)
        if not breaker:
            return True

        if breaker.state == CircuitState.CLOSED:
            return True

        if breaker.state == CircuitState.OPEN:
            if time.time() - breaker.last_failure_time >= breaker.recovery_timeout:
                breaker.state = CircuitState.HALF_OPEN
                breaker.success_count = 0
                return True
            return False

        if breaker.state == CircuitState.HALF_OPEN:
            return True

        return False

    def record_success(self, name: str) -> None:
        """Record successful call."""
        breaker = self._breakers.get(name)
        if not breaker:
            return

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.success_count += 1
            if breaker.success_count >= breaker.success_threshold:
                breaker.state = CircuitState.CLOSED
                breaker.failure_count = 0

        elif breaker.state == CircuitState.CLOSED:
            breaker.failure_count = 0

    def record_failure(self, name: str) -> None:
        """Record failed call."""
        breaker = self._breakers.get(name)
        if not breaker:
            return

        breaker.failure_count += 1
        breaker.last_failure_time = time.time()

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.state = CircuitState.OPEN

        elif breaker.state == CircuitState.CLOSED:
            if breaker.failure_count >= breaker.failure_threshold:
                breaker.state = CircuitState.OPEN


class CircuitBreakerV2Action(BaseAction):
    """Action for circuit breaker operations."""

    def __init__(self):
        super().__init__("circuit_breaker_v2")
        self._manager = CircuitBreakerV2Manager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute circuit breaker action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "is_callable":
                return self._is_callable(params)
            elif operation == "record_success":
                return self._record_success(params)
            elif operation == "record_failure":
                return self._record_failure(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create breaker."""
        name = self._manager.create_breaker(
            name=params.get("name", ""),
            failure_threshold=params.get("failure_threshold", 5),
            recovery_timeout=params.get("recovery_timeout", 60),
            success_threshold=params.get("success_threshold", 3)
        )
        return ActionResult(success=True, data={"name": name})

    def _is_callable(self, params: Dict) -> ActionResult:
        """Check if callable."""
        callable = self._manager.is_callable(params.get("name", ""))
        return ActionResult(success=True, data={"callable": callable})

    def _record_success(self, params: Dict) -> ActionResult:
        """Record success."""
        self._manager.record_success(params.get("name", ""))
        return ActionResult(success=True)

    def _record_failure(self, params: Dict) -> ActionResult:
        """Record failure."""
        self._manager.record_failure(params.get("name", ""))
        return ActionResult(success=True)
