"""
Automation Circuit Breaker Advanced Action Module

Provides advanced circuit breaker patterns with adaptive thresholds, health scoring,
state persistence, and integration with monitoring systems.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    RECOVERING = "recovering"


class CircuitHealth(Enum):
    """Circuit health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class CircuitMetrics:
    """Metrics for circuit breaker."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    timeout_calls: int = 0
    success_rate: float = 100.0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


@dataclass
class CircuitEvent:
    """An event in circuit history."""

    event_id: str
    circuit_id: str
    event_type: str
    state_from: Optional[str] = None
    state_to: Optional[str] = None
    reason: Optional[str] = None
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class CircuitConfig:
    """Configuration for advanced circuit breaker."""

    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    rejection_threshold: int = 100
    health_score_threshold: float = 70.0
    latency_threshold_ms: float = 1000.0
    sliding_window_size: int = 100
    enable_adaptive_threshold: bool = True
    adaptation_factor: float = 0.1


class HealthCalculator:
    """Calculates circuit health based on multiple signals."""

    def __init__(self, config: Optional[CircuitConfig] = None):
        self.config = config or CircuitConfig()

    def calculate_health_score(self, metrics: CircuitMetrics) -> float:
        """
        Calculate health score from 0-100.

        Factors:
        - Success rate (40%)
        - Latency score (30%)
        - Availability (30%)
        """
        success_score = metrics.success_rate * 0.4

        # Latency score (lower is better)
        if metrics.avg_latency_ms < self.config.latency_threshold_ms:
            latency_score = 100 * (1 - metrics.avg_latency_ms / self.config.latency_threshold_ms) * 0.3
        else:
            latency_score = 0

        # Availability score
        total_possible = metrics.total_calls + metrics.rejected_calls
        if total_possible > 0:
            availability = metrics.successful_calls / total_possible * 100
        else:
            availability = 100
        availability_score = availability * 0.3

        return success_score + latency_score + availability_score

    def get_health_status(self, score: float) -> CircuitHealth:
        """Get health status from score."""
        if score >= 80:
            return CircuitHealth.HEALTHY
        elif score >= self.config.health_score_threshold:
            return CircuitHealth.DEGRADED
        else:
            return CircuitHealth.UNHEALTHY


class AdvancedCircuitBreaker:
    """Advanced circuit breaker with health scoring and adaptive thresholds."""

    def __init__(self, circuit_id: str, config: Optional[CircuitConfig] = None):
        self.circuit_id = circuit_id
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._metrics = CircuitMetrics()
        self._health_calculator = HealthCalculator(self.config)
        self._health_score: float = 100.0
        self._half_open_calls: int = 0
        self._half_open_successes: int = 0
        self._half_open_failures: int = 0
        self._events: List[CircuitEvent] = []
        self._sliding_window: List[bool] = []
        self._latencies: List[float] = []
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset."""
        if self._metrics.last_failure_time is None:
            return True
        elapsed = time.time() - self._metrics.last_failure_time
        return elapsed >= self.config.timeout_seconds

    def _transition_to(self, new_state: CircuitState, reason: Optional[str] = None) -> None:
        """Transition circuit to a new state."""
        old_state = self._state
        self._state = new_state

        event = CircuitEvent(
            event_id=f"evt_{uuid.uuid4().hex[:8]}",
            circuit_id=self.circuit_id,
            event_type="state_change",
            state_from=old_state.value,
            state_to=new_state.value,
            reason=reason,
        )
        self._events.append(event)
        logger.info(f"Circuit {self.circuit_id}: {old_state.value} -> {new_state.value} ({reason})")

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._half_open_successes = 0
            self._half_open_failures = 0

    async def record_success(self, latency_ms: float = 0.0) -> None:
        """Record a successful call."""
        async with self._lock:
            self._metrics.total_calls += 1
            self._metrics.successful_calls += 1
            self._metrics.last_success_time = time.time()
            self._metrics.consecutive_failures = 0
            self._metrics.consecutive_successes += 1

            # Update sliding window
            self._sliding_window.append(True)
            if len(self._sliding_window) > self.config.sliding_window_size:
                self._sliding_window.pop(0)

            # Update latency
            self._latencies.append(latency_ms)
            if len(self._latencies) > self.config.sliding_window_size:
                self._latencies.pop(0)
            self._update_latency_metrics()

            self._update_metrics()

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                self._half_open_successes += 1

                if self._half_open_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED, "recovery succeeded")

            elif self._state == CircuitState.CLOSED:
                if self.config.enable_adaptive_threshold:
                    self._adapt_thresholds()

    async def record_failure(self, error_type: str = "generic") -> None:
        """Record a failed call."""
        async with self._lock:
            self._metrics.total_calls += 1
            self._metrics.failed_calls += 1
            self._metrics.last_failure_time = time.time()
            self._metrics.consecutive_successes = 0
            self._metrics.consecutive_failures += 1

            # Update sliding window
            self._sliding_window.append(False)
            if len(self._sliding_window) > self.config.sliding_window_size:
                self._sliding_window.pop(0)

            self._update_metrics()

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                self._half_open_failures += 1
                self._transition_to(CircuitState.OPEN, f"half-open failure ({error_type})")

            elif self._state == CircuitState.CLOSED:
                if self._metrics.consecutive_failures >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN, f"failure threshold exceeded ({error_type})")

    async def record_timeout(self) -> None:
        """Record a timeout."""
        async with self._lock:
            self._metrics.total_calls += 1
            self._metrics.timeout_calls += 1
            await self.record_failure("timeout")

    def can_execute(self) -> bool:
        """Check if a call can be executed."""
        state = self.state

        if state == CircuitState.CLOSED:
            return True
        elif state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls
        else:  # OPEN
            return False

    def _update_metrics(self) -> None:
        """Update computed metrics."""
        total = self._metrics.total_calls
        if total > 0:
            self._metrics.success_rate = (self._metrics.successful_calls / total) * 100

        self._health_score = self._health_calculator.calculate_health_score(self._metrics)

    def _update_latency_metrics(self) -> None:
        """Update latency percentiles."""
        if not self._latencies:
            return

        sorted_latencies = sorted(self._latencies)
        n = len(sorted_latencies)

        p95_idx = int(n * 0.95)
        p99_idx = int(n * 0.99)

        self._metrics.p95_latency_ms = sorted_latencies[min(p95_idx, n - 1)]
        self._metrics.p99_latency_ms = sorted_latencies[min(p99_idx, n - 1)]

    def _adapt_thresholds(self) -> None:
        """Adapt failure threshold based on recent health."""
        if not self.config.enable_adaptive_threshold:
            return

        # If health is good, be more lenient
        if self._health_score > 90:
            pass  # Keep current threshold
        elif self._health_score < 60:
            self.config.failure_threshold = int(self.config.failure_threshold * (1 - self.config.adaptation_factor))

    def get_health_score(self) -> float:
        """Get current health score."""
        return self._health_score

    def get_health_status(self) -> CircuitHealth:
        """Get current health status."""
        return self._health_calculator.get_health_status(self._health_score)

    def get_metrics(self) -> CircuitMetrics:
        """Get circuit metrics."""
        return self._metrics

    def get_events(self, limit: int = 100) -> List[CircuitEvent]:
        """Get recent circuit events."""
        return self._events[-limit:]


class AutomationCircuitBreakerAdvancedAction:
    """
    Advanced circuit breaker action for automation workflows.

    Features:
    - Multiple circuit states (closed, open, half-open, recovering)
    - Health scoring based on success rate, latency, and availability
    - Adaptive thresholds that adjust based on health
    - Sliding window for metrics aggregation
    - Circuit event history
    - Health-based state transitions
    - Configurable half-open behavior

    Usage:
        cb = AutomationCircuitBreakerAdvancedAction("my-circuit", config)
        
        if cb.can_execute():
            try:
                result = await operation()
                await cb.record_success(latency_ms=50)
            except Exception as e:
                await cb.record_failure()
        else:
            raise CircuitOpenError("Circuit is open")
    """

    def __init__(self, config: Optional[CircuitConfig] = None):
        self.config = config or CircuitConfig()
        self._circuits: Dict[str, AdvancedCircuitBreaker] = {}
        self._stats = {
            "circuits_created": 0,
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "rejected_calls": 0,
        }

    def create_circuit(
        self,
        circuit_id: str,
        config: Optional[CircuitConfig] = None,
    ) -> AdvancedCircuitBreaker:
        """Create a new circuit breaker."""
        cfg = config or self.config
        circuit = AdvancedCircuitBreaker(circuit_id, cfg)
        self._circuits[circuit_id] = circuit
        self._stats["circuits_created"] += 1
        return circuit

    def get_circuit(self, circuit_id: str) -> Optional[AdvancedCircuitBreaker]:
        """Get a circuit by ID."""
        return self._circuits.get(circuit_id)

    async def execute(
        self,
        circuit_id: str,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a function with circuit breaker protection.

        Args:
            circuit_id: Circuit to use
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitOpenError if circuit is open
        """
        circuit = self._circuits.get(circuit_id)
        if circuit is None:
            circuit = self.create_circuit(circuit_id)

        if not circuit.can_execute():
            self._stats["rejected_calls"] += 1
            raise CircuitOpenError(f"Circuit {circuit_id} is open")

        start_time = time.time()
        self._stats["total_calls"] += 1

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            latency_ms = (time.time() - start_time) * 1000
            await circuit.record_success(latency_ms)
            self._stats["successful_calls"] += 1
            return result

        except asyncio.TimeoutError:
            await circuit.record_timeout()
            self._stats["failed_calls"] += 1
            raise

        except Exception as e:
            await circuit.record_failure(type(e).__name__)
            self._stats["failed_calls"] += 1
            raise

    def get_all_circuit_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuits."""
        status = {}
        for circuit_id, circuit in self._circuits.items():
            status[circuit_id] = {
                "state": circuit.state.value,
                "health_score": circuit.get_health_score(),
                "health_status": circuit.get_health_status().value,
                "metrics": circuit.get_metrics().__dict__,
            }
        return status

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            **self._stats.copy(),
            "total_circuits": len(self._circuits),
        }


class CircuitOpenError(Exception):
    """Raised when circuit is open and call is rejected."""

    pass


async def demo_circuit_breaker():
    """Demonstrate advanced circuit breaker."""
    config = CircuitConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=5.0,
        enable_adaptive_threshold=True,
    )
    cb_action = AutomationCircuitBreakerAdvancedAction(config)

    circuit = cb_action.create_circuit("api-service", config)

    call_count = 0

    async def unreliable_api():
        nonlocal call_count
        call_count += 1
        if call_count < 4:
            raise ConnectionError("Connection refused")
        return {"status": "ok"}

    for i in range(7):
        try:
            if circuit.can_execute():
                result = await cb_action.execute("api-service", unreliable_api)
                print(f"Call {i}: SUCCESS")
            else:
                print(f"Call {i}: REJECTED (circuit open)")
        except CircuitOpenError as e:
            print(f"Call {i}: REJECTED - {e}")
        except Exception as e:
            print(f"Call {i}: FAILED - {e}")

    status = cb_action.get_all_circuit_status()
    print(f"\nCircuit status: {status['api-service']['state']}")
    print(f"Health score: {status['api-service']['health_score']:.2f}")
    print(f"Stats: {cb_action.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_circuit_breaker())
