"""API circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern for fault tolerance:
CLOSED, OPEN, HALF_OPEN states with configurable thresholds.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"    # Normal operation
    OPEN = "open"        # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5      # Failures to trip circuit
    success_threshold: int = 2      # Successes to close circuit from half-open
    timeout_seconds: float = 30.0   # Time before trying half-open
    half_open_max_calls: int = 3    # Max test calls in half-open
    window_seconds: float = 60.0    # Rolling window for failure counting


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics snapshot."""
    state: str
    failure_count: int
    success_count: int
    total_calls: int
    rejected_calls: int
    last_failure_time: Optional[float]
    last_success_time: Optional[float]
    consecutive_failures: int


class CircuitBreakerAction(BaseAction):
    """Circuit breaker for protecting failing API calls.
    
    State machine:
    - CLOSED: normal operation, track failures
    - OPEN: reject all calls immediately
    - HALF_OPEN: allow limited test calls
    
    Args:
        name: Circuit breaker name (per-endpoint)
        config: CircuitConfig with thresholds
    """

    def __init__(self, name: str = "default", config: Optional[CircuitConfig] = None):
        super().__init__()
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._consecutive_failures = 0
        self._total_calls = 0
        self._rejected_calls = 0
        self._last_failure_time: Optional[float] = None
        self._last_success_time: Optional[float] = None
        self._opened_at: Optional[float] = None
        self._half_open_calls = 0
        self._failure_window: deque = deque()  # rolling window of timestamps

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        self._total_calls += 1
        current_time = time.time()
        self._clean_window(current_time)

        if self._state == CircuitState.OPEN:
            self._rejected_calls += 1
            if current_time - (self._opened_at or 0) >= self.config.timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                self._failure_count = 0
                self._success_count = 0
            else:
                raise CircuitOpenError(f"Circuit {self.name} is OPEN")

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1
            if self._half_open_calls > self.config.half_open_max_calls:
                self._rejected_calls += 1
                raise CircuitOpenError(f"Circuit {self.name} half-open limit reached")

        try:
            result = func(*args, **kwargs)
            self._on_success(current_time)
            return result
        except Exception as e:
            self._on_failure(current_time)
            raise

    def _on_success(self, current_time: float):
        self._last_success_time = current_time
        self._success_count += 1
        self._consecutive_failures = 0
        self._failure_window.append((current_time, False))

        if self._state == CircuitState.HALF_OPEN:
            if self._success_count >= self.config.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0

    def _on_failure(self, current_time: float):
        self._last_failure_time = current_time
        self._failure_count += 1
        self._consecutive_failures += 1
        self._failure_window.append((current_time, True))

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = current_time
        elif self._consecutive_failures >= self.config.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = current_time

    def _clean_window(self, current_time: float):
        cutoff = current_time - self.config.window_seconds
        while self._failure_window and self._failure_window[0][0] < cutoff:
            self._failure_window.popleft()

    def execute(self, action: str) -> ActionResult:
        try:
            if action == "status":
                metrics = CircuitMetrics(
                    state=self._state.value,
                    failure_count=self._failure_count,
                    success_count=self._success_count,
                    total_calls=self._total_calls,
                    rejected_calls=self._rejected_calls,
                    last_failure_time=self._last_failure_time,
                    last_success_time=self._last_success_time,
                    consecutive_failures=self._consecutive_failures
                )
                return ActionResult(success=True, data={
                    "circuit": self.name,
                    "state": metrics.state,
                    "failure_count": metrics.failure_count,
                    "success_count": metrics.success_count,
                    "total_calls": metrics.total_calls,
                    "rejected_calls": metrics.rejected_calls,
                    "consecutive_failures": metrics.consecutive_failures,
                    "reject_rate": round(metrics.rejected_calls / metrics.total_calls, 4) if metrics.total_calls > 0 else 0.0
                })

            elif action == "reset":
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                self._consecutive_failures = 0
                self._failure_window.clear()
                return ActionResult(success=True, data={"circuit": self.name, "reset": True, "state": "closed"})

            elif action == "trip":
                self._state = CircuitState.OPEN
                self._opened_at = time.time()
                return ActionResult(success=True, data={"circuit": self.name, "state": "open"})

            elif action == "is_callable":
                return ActionResult(success=True, data={
                    "circuit": self.name,
                    "callable": self._state != CircuitState.OPEN
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class CircuitOpenError(Exception):
    """Raised when circuit is open and call is rejected."""
    pass


class MultiCircuitBreakerAction(BaseAction):
    """Manage multiple named circuit breakers.
    
    Provides centralized management of circuit breakers
    for different services/endpoints.
    """

    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, CircuitBreakerAction] = {}

    def get_or_create(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreakerAction:
        if name not in self._circuits:
            self._circuits[name] = CircuitBreakerAction(name, config)
        return self._circuits[name]

    def execute(
        self,
        action: str,
        circuit_name: Optional[str] = None,
        config_data: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        try:
            if action == "create":
                if not circuit_name:
                    return ActionResult(success=False, error="circuit_name required")
                cfg = CircuitConfig(**config_data) if config_data else None
                cb = self.get_or_create(circuit_name, cfg)
                return ActionResult(success=True, data={
                    "circuit": circuit_name, "created": True
                })

            elif action == "status":
                if circuit_name and circuit_name in self._circuits:
                    cb = self._circuits[circuit_name]
                    return cb.execute("status")
                # Return all circuits
                return ActionResult(success=True, data={
                    "circuits": {
                        name: cb._state.value for name, cb in self._circuits.items()
                    }
                })

            elif action == "reset_all":
                for cb in self._circuits.values():
                    cb.execute("reset")
                return ActionResult(success=True, data={"reset": len(self._circuits)})

            elif action == "trip":
                if circuit_name and circuit_name in self._circuits:
                    return self._circuits[circuit_name].execute("trip")
                return ActionResult(success=False, error="circuit not found")

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
