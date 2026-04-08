"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker utilities:
- CircuitBreaker: Circuit breaker pattern
- CircuitBreakerRegistry: Manage breakers
"""

from typing import Any, Callable, Dict, List, Optional
from enum import Enum
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
            return self._state

    def allow_request(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            state = self.state

            if state == CircuitState.CLOSED:
                return True

            if state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False

            return False

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            else:
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker stats."""
        with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
            }


class CircuitBreakerRegistry:
    """Registry for circuit breakers."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(**kwargs)
            return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker."""
        with self._lock:
            return self._breakers.get(name)

    def list_breakers(self) -> List[str]:
        """List all breakers."""
        with self._lock:
            return list(self._breakers.keys())


class CircuitBreakerAction(BaseAction):
    """Circuit breaker management action."""
    action_type = "circuit_breaker"
    display_name = "断路器"
    description = "熔断保护"

    def __init__(self):
        super().__init__()
        self._registry = CircuitBreakerRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "record_success":
                return self._record_success(params)
            elif operation == "record_failure":
                return self._record_failure(params)
            elif operation == "stats":
                return self._stats(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"CircuitBreaker error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a circuit breaker."""
        name = params.get("name", str(uuid.uuid4()))
        failure_threshold = params.get("failure_threshold", 5)
        recovery_timeout = params.get("recovery_timeout", 60.0)
        half_open_max_calls = params.get("half_open_max_calls", 3)

        breaker = self._registry.get_or_create(
            name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
        )

        return ActionResult(success=True, message=f"Circuit breaker created: {name}", data={"name": name})

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get circuit breaker state."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker not found: {name}")

        state = breaker.state

        return ActionResult(success=True, message=f"State: {state.value}", data={"name": name, "state": state.value})

    def _record_success(self, params: Dict[str, Any]) -> ActionResult:
        """Record a success."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker not found: {name}")

        breaker.record_success()

        return ActionResult(success=True, message="Success recorded")

    def _record_failure(self, params: Dict[str, Any]) -> ActionResult:
        """Record a failure."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker not found: {name}")

        breaker.record_failure()

        return ActionResult(success=True, message="Failure recorded")

    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get circuit breaker stats."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker not found: {name}")

        stats = breaker.get_stats()

        return ActionResult(success=True, message="Stats retrieved", data={"name": name, "stats": stats})

    def _list(self) -> ActionResult:
        """List all circuit breakers."""
        breakers = self._registry.list_breakers()

        return ActionResult(success=True, message=f"{len(breakers)} breakers", data={"breakers": breakers})
