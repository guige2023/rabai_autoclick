"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern:
- CircuitBreaker: Circuit breaker with states
- CircuitBreakerRegistry: Manage multiple breakers
- StateTransitions: Handle state transitions
- HealthMonitor: Monitor service health
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_state_change: float = 0.0
    last_failure: Optional[float] = None
    last_success: Optional[float] = None


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(self, name: str, config: Optional[CircuitConfig] = None):
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = threading.RLock()
        self.metrics = CircuitMetrics()

    @property
    def state(self) -> CircuitState:
        """Get current state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.config.timeout:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state

    def is_call_allowed(self) -> bool:
        """Check if call is allowed."""
        with self._lock:
            current_state = self.state
            if current_state == CircuitState.CLOSED:
                return True
            elif current_state == CircuitState.HALF_OPEN:
                return self._half_open_calls < self.config.half_open_max_calls
            return False

    def record_success(self):
        """Record successful call."""
        with self._lock:
            self.metrics.successful_calls += 1
            self.metrics.last_success = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    def record_failure(self):
        """Record failed call."""
        with self._lock:
            self.metrics.failed_calls += 1
            self.metrics.last_failure = time.time()
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def record_rejection(self):
        """Record rejected call."""
        with self._lock:
            self.metrics.rejected_calls += 1

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker."""
        if not self.is_call_allowed():
            self.record_rejection()
            raise Exception(f"Circuit breaker '{self.name}' is OPEN")

        with self._lock:
            self.metrics.total_calls += 1
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise

    def reset(self):
        """Reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            self.metrics.state_changes += 1

    def _transition_to(self, new_state: CircuitState):
        """Transition to new state."""
        if self._state == new_state:
            return

        self._state = new_state
        self.metrics.state_changes += 1
        self.metrics.last_state_change = time.time()

        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._success_count = 0


class CircuitBreakerRegistry:
    """Registry for circuit breakers."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()

    def register(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
        """Register a circuit breaker."""
        with self._lock:
            if name in self._breakers:
                return self._breakers[name]
            breaker = CircuitBreaker(name, config)
            self._breakers[name] = breaker
            return breaker

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker."""
        with self._lock:
            return self._breakers.get(name)

    def unregister(self, name: str) -> bool:
        """Unregister circuit breaker."""
        with self._lock:
            if name in self._breakers:
                del self._breakers[name]
                return True
            return False

    def list_breakers(self) -> List[str]:
        """List all circuit breakers."""
        with self._lock:
            return list(self._breakers.keys())


class CircuitBreakerAction(BaseAction):
    """Circuit breaker action."""
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "服务熔断保护机制"

    def __init__(self):
        super().__init__()
        self._registry = CircuitBreakerRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")

            if operation == "register":
                return self._register(params)
            elif operation == "call":
                return self._call(params)
            elif operation == "status":
                return self._get_status(params)
            elif operation == "list":
                return self._list_breakers()
            elif operation == "reset":
                return self._reset(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker error: {str(e)}")

    def _register(self, params: Dict) -> ActionResult:
        """Register circuit breaker."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        config = CircuitConfig(
            failure_threshold=params.get("failure_threshold", 5),
            success_threshold=params.get("success_threshold", 2),
            timeout=params.get("timeout", 60.0),
            half_open_max_calls=params.get("half_open_max_calls", 3),
        )

        breaker = self._registry.register(name, config)
        return ActionResult(success=True, message=f"Circuit breaker '{name}' registered")

    def _call(self, params: Dict) -> ActionResult:
        """Call with circuit breaker."""
        name = params.get("name", "default")
        func = params.get("func")

        breaker = self._registry.get(name)
        if not breaker:
            config = CircuitConfig()
            breaker = self._registry.register(name, config)

        if not func:
            return ActionResult(success=False, message="func is required")

        try:
            result = breaker.call(func)
            return ActionResult(
                success=True,
                message=f"Call succeeded, state: {breaker.state.value}",
                data={"state": breaker.state.value, "result": str(result)[:100]},
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Call failed: {str(e)}, state: {breaker.state.value}",
                data={"state": breaker.state.value, "rejected": breaker.metrics.rejected_calls > 0},
            )

    def _get_status(self, params: Dict) -> ActionResult:
        """Get circuit breaker status."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker '{name}' not found")

        m = breaker.metrics
        return ActionResult(
            success=True,
            message=f"State: {breaker.state.value}",
            data={
                "name": breaker.name,
                "state": breaker.state.value,
                "metrics": {
                    "total_calls": m.total_calls,
                    "successful_calls": m.successful_calls,
                    "failed_calls": m.failed_calls,
                    "rejected_calls": m.rejected_calls,
                    "state_changes": m.state_changes,
                    "last_failure": m.last_failure,
                    "last_success": m.last_success,
                },
            },
        )

    def _list_breakers(self) -> ActionResult:
        """List all breakers."""
        names = self._registry.list_breakers()
        breakers = []
        for name in names:
            breaker = self._registry.get(name)
            if breaker:
                breakers.append({"name": name, "state": breaker.state.value})
        return ActionResult(success=True, message=f"{len(breakers)} breakers", data={"breakers": breakers})

    def _reset(self, params: Dict) -> ActionResult:
        """Reset circuit breaker."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        breaker = self._registry.get(name)
        if not breaker:
            return ActionResult(success=False, message=f"Circuit breaker '{name}' not found")

        breaker.reset()
        return ActionResult(success=True, message=f"Circuit breaker '{name}' reset")
