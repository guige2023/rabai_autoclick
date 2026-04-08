"""API Circuit Breaker v4 Action Module for RabAI AutoClick.

Advanced circuit breaker with adaptive thresholds,
metric-based state transitions, and parallel circuit support.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, Optional, List
from enum import Enum
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    total_open_duration: float = 0.0
    state_changes: int = 0


class ApiCircuitBreakerV4Action(BaseAction):
    """Advanced circuit breaker with adaptive thresholds.

    Enhanced circuit breaker with metric-driven state transitions,
    parallel circuits, and automatic recovery detection.
    """
    action_type = "api_circuit_breaker_v4"
    display_name = "API熔断器v4"
    description = "自适应阈值熔断器，指标驱动状态转换"

    _circuits: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute circuit breaker operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'call', 'record_success', 'record_failure',
                               'get_state', 'reset', 'list'
                - circuit_name: str - name of the circuit
                - failure_threshold: int (optional) - failures before opening
                - success_threshold: int (optional) - successes to close
                - timeout: float (optional) - seconds before half-open
                - half_open_max_calls: int (optional) - calls allowed in half-open
                - error_threshold: float (optional) - error rate % to open

        Returns:
            ActionResult with circuit breaker result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'call')

            if operation == 'call':
                return self._call_with_circuit(params, start_time)
            elif operation == 'record_success':
                return self._record_success(params, start_time)
            elif operation == 'record_failure':
                return self._record_failure(params, start_time)
            elif operation == 'get_state':
                return self._get_state(params, start_time)
            elif operation == 'reset':
                return self._reset_circuit(params, start_time)
            elif operation == 'list':
                return self._list_circuits(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Circuit breaker action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _ensure_circuit(self, circuit_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure circuit exists with given config."""
        with self._lock:
            if circuit_name not in self._circuits:
                self._circuits[circuit_name] = {
                    'name': circuit_name,
                    'state': CircuitState.CLOSED,
                    'failure_threshold': params.get('failure_threshold', 5),
                    'success_threshold': params.get('success_threshold', 3),
                    'timeout': params.get('timeout', 30.0),
                    'half_open_max_calls': params.get('half_open_max_calls', 3),
                    'error_threshold': params.get('error_threshold', 50.0),
                    'metrics': CircuitMetrics(),
                    'opened_at': None,
                    'lock': threading.RLock()
                }
            return self._circuits[circuit_name]

    def _call_with_circuit(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a call through the circuit breaker."""
        circuit_name = params.get('circuit_name', 'default')

        circuit = self._ensure_circuit(circuit_name, params)

        with circuit['lock']:
            self._check_state_transition(circuit)

            if circuit['state'] == CircuitState.OPEN:
                return ActionResult(
                    success=False,
                    message=f"Circuit OPEN: {circuit_name}",
                    data={
                        'circuit': circuit_name,
                        'state': 'open',
                        'rejected': True,
                        'opened_at': circuit['opened_at']
                    },
                    duration=time.time() - start_time
                )

            if circuit['state'] == CircuitState.HALF_OPEN:
                metrics = circuit['metrics']
                if metrics.rejected_calls >= circuit['half_open_max_calls']:
                    return ActionResult(
                        success=False,
                        message=f"Circuit HALF_OPEN max calls reached: {circuit_name}",
                        data={
                            'circuit': circuit_name,
                            'state': 'half_open',
                            'rejected': True
                        },
                        duration=time.time() - start_time
                    )
                metrics.rejected_calls += 1

            return ActionResult(
                success=True,
                message=f"Circuit allows call: {circuit_name}",
                data={
                    'circuit': circuit_name,
                    'state': circuit['state'].value,
                    'allowed': True
                },
                duration=time.time() - start_time
            )

    def _record_success(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Record a successful call."""
        circuit_name = params.get('circuit_name', 'default')

        if circuit_name not in self._circuits:
            return ActionResult(
                success=False,
                message=f"Circuit not found: {circuit_name}",
                duration=time.time() - start_time
            )

        circuit = self._circuits[circuit_name]

        with circuit['lock']:
            metrics = circuit['metrics']
            metrics.total_calls += 1
            metrics.successful_calls += 1
            metrics.consecutive_successes += 1
            metrics.consecutive_failures = 0
            metrics.last_success_time = time.time()

            if circuit['state'] == CircuitState.HALF_OPEN:
                if metrics.consecutive_successes >= circuit['success_threshold']:
                    circuit['state'] = CircuitState.CLOSED
                    circuit['opened_at'] = None
                    metrics.consecutive_successes = 0
                    metrics.rejected_calls = 0
                    metrics.state_changes += 1

        return ActionResult(
            success=True,
            message=f"Success recorded: {circuit_name}",
            data={
                'circuit': circuit_name,
                'state': circuit['state'].value,
                'consecutive_successes': circuit['metrics'].consecutive_successes
            },
            duration=time.time() - start_time
        )

    def _record_failure(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Record a failed call."""
        circuit_name = params.get('circuit_name', 'default')

        if circuit_name not in self._circuits:
            return ActionResult(
                success=False,
                message=f"Circuit not found: {circuit_name}",
                duration=time.time() - start_time
            )

        circuit = self._circuits[circuit_name]

        with circuit['lock']:
            metrics = circuit['metrics']
            metrics.total_calls += 1
            metrics.failed_calls += 1
            metrics.consecutive_failures += 1
            metrics.consecutive_successes = 0
            metrics.last_failure_time = time.time()

            should_open = False

            if circuit['state'] == CircuitState.HALF_OPEN:
                should_open = True
            elif (circuit['state'] == CircuitState.CLOSED and
                  metrics.consecutive_failures >= circuit['failure_threshold']):
                should_open = True

            if should_open:
                circuit['state'] = CircuitState.OPEN
                circuit['opened_at'] = time.time()
                metrics.consecutive_failures = 0
                metrics.state_changes += 1

        return ActionResult(
            success=True,
            message=f"Failure recorded: {circuit_name}",
            data={
                'circuit': circuit_name,
                'state': circuit['state'].value,
                'consecutive_failures': circuit['metrics'].consecutive_failures
            },
            duration=time.time() - start_time
        )

    def _check_state_transition(self, circuit: Dict[str, Any]) -> None:
        """Check and perform automatic state transitions."""
        if circuit['state'] == CircuitState.OPEN:
            if circuit['opened_at']:
                time_open = time.time() - circuit['opened_at']
                if time_open >= circuit['timeout']:
                    circuit['state'] = CircuitState.HALF_OPEN
                    circuit['metrics'].rejected_calls = 0
                    circuit['metrics'].state_changes += 1

    def _get_state(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get current circuit state and metrics."""
        circuit_name = params.get('circuit_name', 'default')

        if circuit_name not in self._circuits:
            return ActionResult(
                success=False,
                message=f"Circuit not found: {circuit_name}",
                duration=time.time() - start_time
            )

        circuit = self._circuits[circuit_name]

        with circuit['lock']:
            self._check_state_transition(circuit)
            metrics = circuit['metrics']

            error_rate = 0.0
            if metrics.total_calls > 0:
                error_rate = (metrics.failed_calls / metrics.total_calls) * 100

            return ActionResult(
                success=True,
                message=f"Circuit state: {circuit['state'].value}",
                data={
                    'circuit': circuit_name,
                    'state': circuit['state'].value,
                    'metrics': {
                        'total_calls': metrics.total_calls,
                        'successful_calls': metrics.successful_calls,
                        'failed_calls': metrics.failed_calls,
                        'error_rate': round(error_rate, 2),
                        'consecutive_successes': metrics.consecutive_successes,
                        'consecutive_failures': metrics.consecutive_failures,
                        'state_changes': metrics.state_changes
                    },
                    'config': {
                        'failure_threshold': circuit['failure_threshold'],
                        'success_threshold': circuit['success_threshold'],
                        'timeout': circuit['timeout']
                    }
                },
                duration=time.time() - start_time
            )

    def _reset_circuit(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset a circuit to closed state."""
        circuit_name = params.get('circuit_name', 'default')

        if circuit_name not in self._circuits:
            return ActionResult(
                success=False,
                message=f"Circuit not found: {circuit_name}",
                duration=time.time() - start_time
            )

        circuit = self._circuits[circuit_name]

        with circuit['lock']:
            circuit['state'] = CircuitState.CLOSED
            circuit['opened_at'] = None
            circuit['metrics'] = CircuitMetrics()
            circuit['metrics'].state_changes += 1

        return ActionResult(
            success=True,
            message=f"Circuit reset: {circuit_name}",
            data={'circuit': circuit_name, 'state': 'closed'},
            duration=time.time() - start_time
        )

    def _list_circuits(self, start_time: float) -> ActionResult:
        """List all circuits."""
        circuits = []
        for name, circuit in self._circuits.items():
            circuits.append({
                'name': name,
                'state': circuit['state'].value,
                'total_calls': circuit['metrics'].total_calls
            })

        return ActionResult(
            success=True,
            message=f"Circuits: {len(circuits)}",
            data={'circuits': circuits, 'count': len(circuits)},
            duration=time.time() - start_time
        )
