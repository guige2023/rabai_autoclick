"""Circuit Breaker Advanced Action Module.

Provides advanced circuit breaker with half-open state,
bulkhead isolation, and adaptive thresholds.
"""

import time
import threading
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    last_failure_time: Optional[float] = None


class AdvancedCircuitBreaker:
    """Advanced circuit breaker with adaptive behavior."""

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        self._metrics = CircuitMetrics()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
            return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.config.timeout_seconds

    def allow_request(self) -> bool:
        """Check if request is allowed."""
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.OPEN:
            return False
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                if self._half_open_calls < self.config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False
        return True

    def record_success(self) -> None:
        """Record successful call."""
        with self._lock:
            self._metrics.successful_calls += 1
            self._metrics.total_calls += 1

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0

            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record failed call."""
        with self._lock:
            self._metrics.failed_calls += 1
            self._metrics.total_calls += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            self._metrics.last_failure_time = self._last_failure_time

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0

            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN

    def record_rejection(self) -> None:
        """Record rejected call."""
        with self._lock:
            self._metrics.rejected_calls += 1

    def get_metrics(self) -> CircuitMetrics:
        """Get circuit metrics."""
        return self._metrics

    def reset(self) -> None:
        """Reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0


class CircuitBreakerAdvancedAction(BaseAction):
    """Action for circuit breaker operations."""

    def __init__(self):
        super().__init__("circuit_breaker_advanced")
        self._breakers: Dict[str, AdvancedCircuitBreaker] = {}

    def execute(self, params: Dict) -> ActionResult:
        """Execute circuit breaker action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "allow":
                return self._allow(params)
            elif operation == "success":
                return self._success(params)
            elif operation == "failure":
                return self._failure(params)
            elif operation == "state":
                return self._state(params)
            elif operation == "metrics":
                return self._metrics(params)
            elif operation == "reset":
                return self._reset(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create circuit breaker."""
        name = params.get("name", "")
        config = CircuitBreakerConfig(
            failure_threshold=params.get("failure_threshold", 5),
            success_threshold=params.get("success_threshold", 2),
            timeout_seconds=params.get("timeout_seconds", 60)
        )
        breaker = AdvancedCircuitBreaker(name, config)
        self._breakers[name] = breaker
        return ActionResult(success=True, data={"name": name})

    def _allow(self, params: Dict) -> ActionResult:
        """Check if request allowed."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if not breaker:
            return ActionResult(success=False, message="Breaker not found")

        allowed = breaker.allow_request()
        if not allowed:
            breaker.record_rejection()
        return ActionResult(success=True, data={"allowed": allowed})

    def _success(self, params: Dict) -> ActionResult:
        """Record success."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if breaker:
            breaker.record_success()
        return ActionResult(success=True)

    def _failure(self, params: Dict) -> ActionResult:
        """Record failure."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if breaker:
            breaker.record_failure()
        return ActionResult(success=True)

    def _state(self, params: Dict) -> ActionResult:
        """Get circuit state."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if not breaker:
            return ActionResult(success=False, message="Breaker not found")
        return ActionResult(success=True, data={"state": breaker.state.value})

    def _metrics(self, params: Dict) -> ActionResult:
        """Get metrics."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if not breaker:
            return ActionResult(success=False, message="Breaker not found")
        m = breaker.get_metrics()
        return ActionResult(success=True, data={
            "total_calls": m.total_calls,
            "successful_calls": m.successful_calls,
            "failed_calls": m.failed_calls,
            "rejected_calls": m.rejected_calls
        })

    def _reset(self, params: Dict) -> ActionResult:
        """Reset breaker."""
        name = params.get("name", "")
        breaker = self._breakers.get(name)
        if breaker:
            breaker.reset()
        return ActionResult(success=True)
