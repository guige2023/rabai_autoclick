"""Circuit Breaker Action Module.

Provides circuit breaker pattern implementation for fault tolerance
with configurable thresholds and state transitions.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FailureReason(Enum):
    """Reason for circuit trip."""
    TIMEOUT = "timeout"
    ERROR = "error"
    SERVICE_UNAVAILABLE = "service_unavailable"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    name: str = ""


@dataclass
class CircuitBreaker:
    """Circuit breaker state."""
    name: str
    state: CircuitState
    config: CircuitBreakerConfig
    failures: int = 0
    successes: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: float = 0.0
    last_state_change: float = field(default_factory=time.time)
    half_open_calls: int = 0
    total_calls: int = 0
    total_failures: int = 0


class CircuitBreakerStore:
    """In-memory circuit breaker store."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}

    def get_or_create(self, name: str, config: CircuitBreakerConfig) -> CircuitBreaker:
        """Get or create circuit breaker."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                state=CircuitState.CLOSED,
                config=config
            )
        return self._breakers[name]


_global_store = CircuitBreakerStore()


class CircuitBreakerAction:
    """Circuit breaker action.

    Example:
        action = CircuitBreakerAction()

        action.configure("payment-service", failure_threshold=5, timeout_seconds=30)
        result = action.execute("payment-service", lambda: call_service())
    """

    def __init__(self, store: Optional[CircuitBreakerStore] = None):
        self._store = store or _global_store
        self._handlers: Dict[str, Callable] = {}

    def configure(self, name: str,
                  failure_threshold: int = 5,
                  success_threshold: int = 2,
                  timeout_seconds: float = 60.0,
                  half_open_max_calls: int = 3) -> Dict[str, Any]:
        """Configure circuit breaker."""
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout_seconds=timeout_seconds,
            half_open_max_calls=half_open_max_calls,
            name=name
        )
        self._store.get_or_create(name, config)

        return {
            "success": True,
            "name": name,
            "failure_threshold": failure_threshold,
            "success_threshold": success_threshold,
            "timeout_seconds": timeout_seconds,
            "message": f"Configured circuit breaker: {name}"
        }

    def record_success(self, name: str) -> Dict[str, Any]:
        """Record successful call."""
        breaker = self._store._breakers.get(name)
        if not breaker:
            return {"success": False, "message": "Circuit breaker not found"}

        breaker.total_calls += 1
        breaker.consecutive_failures = 0

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.consecutive_successes += 1
            breaker.half_open_calls -= 1
            if breaker.consecutive_successes >= breaker.config.success_threshold:
                breaker.state = CircuitState.CLOSED
                breaker.consecutive_successes = 0
                breaker.consecutive_failures = 0
        else:
            breaker.successes += 1

        return {
            "success": True,
            "name": name,
            "state": breaker.state.value,
            "message": f"Success recorded for {name}"
        }

    def record_failure(self, name: str, reason: str = "error") -> Dict[str, Any]:
        """Record failed call."""
        breaker = self._store._breakers.get(name)
        if not breaker:
            return {"success": False, "message": "Circuit breaker not found"}

        breaker.total_calls += 1
        breaker.total_failures += 1
        breaker.consecutive_failures += 1
        breaker.consecutive_successes = 0
        breaker.last_failure_time = time.time()

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.half_open_calls -= 1
            breaker.state = CircuitState.OPEN
            breaker.last_state_change = time.time()

        elif (breaker.consecutive_failures >= breaker.config.failure_threshold and
              breaker.state == CircuitState.CLOSED):
            breaker.state = CircuitState.OPEN
            breaker.last_state_change = time.time()

        return {
            "success": True,
            "name": name,
            "state": breaker.state.value,
            "consecutive_failures": breaker.consecutive_failures,
            "message": f"Failure recorded for {name}"
        }

    def get_state(self, name: str) -> Dict[str, Any]:
        """Get circuit breaker state."""
        breaker = self._store._breakers.get(name)
        if not breaker:
            return {"success": False, "message": "Circuit breaker not found"}

        now = time.time()
        time_since_last_failure = now - breaker.last_failure_time

        if (breaker.state == CircuitState.OPEN and
            time_since_last_failure >= breaker.config.timeout_seconds):
            breaker.state = CircuitState.HALF_OPEN
            breaker.consecutive_successes = 0
            breaker.consecutive_failures = 0
            breaker.half_open_calls = 0
            breaker.last_state_change = now

        return {
            "success": True,
            "name": name,
            "state": breaker.state.value,
            "failures": breaker.failures,
            "successes": breaker.successes,
            "consecutive_failures": breaker.consecutive_failures,
            "total_calls": breaker.total_calls,
            "total_failures": breaker.total_failures,
            "last_failure_time": breaker.last_failure_time,
            "last_state_change": breaker.last_state_change,
            "time_until_half_open": max(0, breaker.config.timeout_seconds - time_since_last_failure)
                                 if breaker.state == CircuitState.OPEN else 0
        }

    def is_allowed(self, name: str) -> Dict[str, Any]:
        """Check if call is allowed."""
        breaker = self._store._breakers.get(name)
        if not breaker:
            return {"success": False, "message": "Circuit breaker not found", "allowed": True}

        if breaker.state == CircuitState.CLOSED:
            return {"success": True, "allowed": True, "state": "closed"}

        if breaker.state == CircuitState.OPEN:
            time_since_open = time.time() - breaker.last_state_change
            if time_since_open >= breaker.config.timeout_seconds:
                breaker.state = CircuitState.HALF_OPEN
                breaker.last_state_change = time.time()
                return {"success": True, "allowed": True, "state": "half_open"}

            return {
                "success": True,
                "allowed": False,
                "state": "open",
                "retry_after": breaker.config.timeout_seconds - time_since_open
            }

        if breaker.state == CircuitState.HALF_OPEN:
            if breaker.half_open_calls < breaker.config.half_open_max_calls:
                breaker.half_open_calls += 1
                return {"success": True, "allowed": True, "state": "half_open"}

            return {"success": True, "allowed": False, "state": "half_open"}

        return {"success": True, "allowed": True, "state": "unknown"}

    def reset(self, name: str) -> Dict[str, Any]:
        """Reset circuit breaker to closed state."""
        breaker = self._store._breakers.get(name)
        if not breaker:
            return {"success": False, "message": "Circuit breaker not found"}

        breaker.state = CircuitState.CLOSED
        breaker.failures = 0
        breaker.successes = 0
        breaker.consecutive_failures = 0
        breaker.consecutive_successes = 0
        breaker.half_open_calls = 0
        breaker.last_state_change = time.time()

        return {
            "success": True,
            "name": name,
            "state": breaker.state.value,
            "message": f"Reset circuit breaker: {name}"
        }

    def list_breakers(self) -> Dict[str, Any]:
        """List all circuit breakers."""
        breakers = list(self._store._breakers.values())
        return {
            "success": True,
            "breakers": [
                {
                    "name": b.name,
                    "state": b.state.value,
                    "total_calls": b.total_calls,
                    "total_failures": b.total_failures
                }
                for b in breakers
            ],
            "count": len(breakers)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute circuit breaker action."""
    operation = params.get("operation", "")
    action = CircuitBreakerAction()

    try:
        if operation == "configure":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.configure(
                name=name,
                failure_threshold=params.get("failure_threshold", 5),
                success_threshold=params.get("success_threshold", 2),
                timeout_seconds=params.get("timeout_seconds", 60.0),
                half_open_max_calls=params.get("half_open_max_calls", 3)
            )

        elif operation == "record_success":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.record_success(name)

        elif operation == "record_failure":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.record_failure(name, params.get("reason", "error"))

        elif operation == "get_state":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.get_state(name)

        elif operation == "is_allowed":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.is_allowed(name)

        elif operation == "reset":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.reset(name)

        elif operation == "list":
            return action.list_breakers()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Circuit breaker error: {str(e)}"}
