"""
API Circuit Tracker Action Module.

Tracks API circuit breaker state and metrics.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class CircuitState(Enum):
    """Circuit states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitMetrics:
    """Metrics for a circuit."""
    name: str
    state: CircuitState
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    total_latency_ms: float = 0.0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state_changed_at: float = field(default_factory=time.time)


@dataclass
class LatencyPercentile:
    """Latency percentile data."""
    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0


class ApiCircuitTrackerAction:
    """
    Tracks circuit breaker state and metrics.

    Provides visibility into API health and failure patterns.
    """

    def __init__(
        self,
        max_history: int = 1000,
    ) -> None:
        self.max_history = max_history
        self._circuits: Dict[str, CircuitMetrics] = {}
        self._call_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))

    def register_circuit(
        self,
        name: str,
        initial_state: CircuitState = CircuitState.CLOSED,
    ) -> None:
        """Register a circuit for tracking."""
        if name not in self._circuits:
            self._circuits[name] = CircuitMetrics(
                name=name,
                state=initial_state,
            )

    def record_call(
        self,
        circuit_name: str,
        success: bool,
        latency_ms: float,
    ) -> None:
        """
        Record a call result.

        Args:
            circuit_name: Circuit name
            success: Whether call succeeded
            latency_ms: Call latency in milliseconds
        """
        if circuit_name not in self._circuits:
            self.register_circuit(circuit_name)

        metrics = self._circuits[circuit_name]
        metrics.total_calls += 1
        metrics.total_latency_ms += latency_ms

        if success:
            metrics.successful_calls += 1
            metrics.last_success_time = time.time()
        else:
            metrics.failed_calls += 1
            metrics.last_failure_time = time.time()

        self._call_history[circuit_name].append({
            "success": success,
            "latency_ms": latency_ms,
            "timestamp": time.time(),
        })

    def record_rejection(
        self,
        circuit_name: str,
    ) -> None:
        """Record a rejected call."""
        if circuit_name not in self._circuits:
            self.register_circuit(circuit_name)

        self._circuits[circuit_name].rejected_calls += 1

    def update_state(
        self,
        circuit_name: str,
        new_state: CircuitState,
    ) -> None:
        """Update circuit state."""
        if circuit_name not in self._circuits:
            self.register_circuit(circuit_name)

        circuit = self._circuits[circuit_name]
        if circuit.state != new_state:
            circuit.state = new_state
            circuit.state_changed_at = time.time()

    def get_metrics(
        self,
        circuit_name: str,
    ) -> Optional[CircuitMetrics]:
        """Get metrics for a circuit."""
        return self._circuits.get(circuit_name)

    def get_all_metrics(self) -> Dict[str, CircuitMetrics]:
        """Get metrics for all circuits."""
        return self._circuits.copy()

    def get_percentiles(
        self,
        circuit_name: str,
    ) -> LatencyPercentile:
        """Calculate latency percentiles for a circuit."""
        history = self._call_history.get(circuit_name, deque())

        if not history:
            return LatencyPercentile()

        latencies = sorted([c["latency_ms"] for c in history])
        n = len(latencies)

        def percentile(p: float) -> float:
            idx = int(n * p)
            if idx >= n:
                idx = n - 1
            return latencies[idx]

        return LatencyPercentile(
            p50=percentile(0.50),
            p90=percentile(0.90),
            p95=percentile(0.95),
            p99=percentile(0.99),
        )

    def get_success_rate(
        self,
        circuit_name: str,
        window_seconds: Optional[float] = None,
    ) -> float:
        """
        Get success rate for a circuit.

        Args:
            circuit_name: Circuit name
            window_seconds: Optional time window

        Returns:
            Success rate 0.0 to 1.0
        """
        history = self._call_history.get(circuit_name, deque())

        if not history:
            return 1.0

        if window_seconds:
            cutoff = time.time() - window_seconds
            history = deque([c for c in history if c["timestamp"] >= cutoff], maxlen=self.max_history)

        if not history:
            return 1.0

        successes = sum(1 for c in history if c["success"])
        return successes / len(history)

    def get_average_latency(
        self,
        circuit_name: str,
    ) -> float:
        """Get average latency for a circuit."""
        metrics = self._circuits.get(circuit_name)
        if not metrics or metrics.total_calls == 0:
            return 0.0

        return metrics.total_latency_ms / metrics.total_calls

    def get_unhealthy_circuits(
        self,
        success_rate_threshold: float = 0.5,
        latency_p99_threshold: float = 1000.0,
    ) -> List[str]:
        """Get circuits that are unhealthy."""
        unhealthy = []

        for name, metrics in self._circuits.items():
            if metrics.state == CircuitState.OPEN:
                unhealthy.append(name)
                continue

            rate = self.get_success_rate(name)
            if rate < success_rate_threshold:
                unhealthy.append(name)
                continue

            percentiles = self.get_percentiles(name)
            if percentiles.p99 > latency_p99_threshold:
                unhealthy.append(name)

        return unhealthy

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all circuits."""
        total_calls = sum(m.total_calls for m in self._circuits.values())
        total_failures = sum(m.failed_calls for m in self._circuits.values())

        return {
            "total_circuits": len(self._circuits),
            "total_calls": total_calls,
            "total_failures": total_failures,
            "overall_success_rate": (
                (total_calls - total_failures) / total_calls
                if total_calls > 0 else 1.0
            ),
            "circuits_by_state": {
                state.value: sum(
                    1 for m in self._circuits.values() if m.state == state
                )
                for state in CircuitState
            },
            "unhealthy_circuits": self.get_unhealthy_circuits(),
        }
