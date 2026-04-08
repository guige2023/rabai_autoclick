"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern implementation for protecting
services from cascading failures.
"""

import sys
import os
import time
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5      # Failures before opening
    success_threshold: int = 2     # Successes to close from half-open
    timeout_seconds: float = 60.0  # Time before trying half-open
    excluded_exceptions: tuple = ()


@dataclass
class CircuitStats:
    """Circuit breaker statistics."""
    failures: int = 0
    successes: int = 0
    state: CircuitState = CircuitState.CLOSED
    last_failure_time: Optional[float] = None
    last_state_change: float = field(default_factory=time.time)
    total_calls: int = 0
    rejected_calls: int = 0


class CircuitBreakerAction(BaseAction):
    """Implement circuit breaker pattern.
    
    Monitors service health and opens circuit when failures
    exceed threshold, preventing cascading failures.
    """
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "服务保护和故障隔离"
    
    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, CircuitStats] = {}
        self._configs: Dict[str, CircuitConfig] = {}
        self._handlers: Dict[str, Callable] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute circuit breaker operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'call', 'register', 'status', 'reset', 'record'
                - circuit: Circuit name
                - config: Circuit config (for register)
                - success: Mark call as success (for record)
                - failure: Mark call as failure (for record)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'call').lower()
        
        if operation == 'call':
            return self._call(params)
        elif operation == 'register':
            return self._register(params)
        elif operation == 'status':
            return self._status(params)
        elif operation == 'reset':
            return self._reset(params)
        elif operation == 'record':
            return self._record(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a new circuit."""
        circuit = params.get('circuit')
        config = params.get('config', {})
        
        if not circuit:
            return ActionResult(success=False, message="circuit is required")
        
        circuit_config = CircuitConfig(
            failure_threshold=config.get('failure_threshold', 5),
            success_threshold=config.get('success_threshold', 2),
            timeout_seconds=config.get('timeout_seconds', 60.0),
            excluded_exceptions=config.get('excluded_exceptions', ())
        )
        
        handler = params.get('handler')
        if handler:
            self._handlers[circuit] = handler
        
        with self._lock:
            self._circuits[circuit] = CircuitStats()
            self._configs[circuit] = circuit_config
        
        return ActionResult(
            success=True,
            message=f"Registered circuit '{circuit}'",
            data={'circuit': circuit}
        )
    
    def _call(self, params: Dict[str, Any]) -> ActionResult:
        """Call through circuit breaker."""
        circuit = params.get('circuit')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        
        if not circuit:
            return ActionResult(success=False, message="circuit is required")
        
        # Get or create circuit
        with self._lock:
            if circuit not in self._circuits:
                self._circuits[circuit] = CircuitStats()
            if circuit not in self._configs:
                self._configs[circuit] = CircuitConfig()
            
            stats = self._circuits[circuit]
            config = self._configs[circuit]
        
        # Check circuit state
        current_state = self._get_state(stats, config)
        
        if current_state == CircuitState.OPEN:
            stats.rejected_calls += 1
            return ActionResult(
                success=False,
                message=f"Circuit '{circuit}' is OPEN",
                data={
                    'circuit': circuit,
                    'state': 'open',
                    'rejected': True
                }
            )
        
        # Half-open: allow one test request
        if current_state == CircuitState.HALF_OPEN:
            pass  # Allow through
        
        # Execute handler
        handler = self._handlers.get(circuit)
        
        if not handler:
            return ActionResult(
                success=False,
                message=f"No handler for circuit '{circuit}'"
            )
        
        stats.total_calls += 1
        
        try:
            result = handler(*args, **kwargs)
            self._record_success(circuit)
            return ActionResult(
                success=True,
                message=f"Circuit '{circuit}' call succeeded",
                data={'result': result, 'circuit': circuit}
            )
        except Exception as e:
            self._record_failure(circuit, e)
            return ActionResult(
                success=False,
                message=f"Circuit '{circuit}' call failed: {e}",
                data={'error': str(e), 'circuit': circuit}
            )
    
    def _record_success(self, circuit: str) -> None:
        """Record successful call."""
        with self._lock:
            stats = self._circuits[circuit]
            config = self._configs[circuit]
            
            stats.successes += 1
            stats.failures = 0
            
            if stats.state == CircuitState.HALF_OPEN:
                if stats.successes >= config.success_threshold:
                    stats.state = CircuitState.CLOSED
                    stats.last_state_change = time.time()
                    stats.successes = 0
    
    def _record_failure(self, circuit: str, error: Exception) -> None:
        """Record failed call."""
        with self._lock:
            stats = self._circuits[circuit]
            config = self._configs[circuit]
            
            # Check if exception is excluded
            if isinstance(error, config.excluded_exceptions):
                return
            
            stats.failures += 1
            stats.successes = 0
            stats.last_failure_time = time.time()
            
            if stats.state == CircuitState.HALF_OPEN:
                # Failed during half-open, go back to open
                stats.state = CircuitState.OPEN
                stats.last_state_change = time.time()
            elif stats.failures >= config.failure_threshold:
                stats.state = CircuitState.OPEN
                stats.last_state_change = time.time()
    
    def _get_state(
        self,
        stats: CircuitStats,
        config: CircuitConfig
    ) -> CircuitState:
        """Get current circuit state."""
        if stats.state == CircuitState.OPEN:
            # Check if timeout has passed
            if stats.last_failure_time:
                elapsed = time.time() - stats.last_failure_time
                if elapsed >= config.timeout_seconds:
                    stats.state = CircuitState.HALF_OPEN
                    stats.successes = 0
        
        return stats.state
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get circuit status."""
        circuit = params.get('circuit')
        
        if not circuit:
            # Return all circuits
            with self._lock:
                circuits = {
                    name: {
                        'state': s.state.value,
                        'failures': s.failures,
                        'successes': s.successes,
                        'total_calls': s.total_calls,
                        'rejected_calls': s.rejected_calls,
                        'last_failure': s.last_failure_time
                    }
                    for name, s in self._circuits.items()
                }
            
            return ActionResult(
                success=True,
                message=f"{len(circuits)} circuits",
                data={'circuits': circuits}
            )
        
        with self._lock:
            if circuit not in self._circuits:
                return ActionResult(
                    success=False,
                    message=f"Circuit '{circuit}' not found"
                )
            
            stats = self._circuits[circuit]
            config = self._configs.get(circuit, CircuitConfig())
            
            return ActionResult(
                success=True,
                message=f"Circuit '{circuit}' is {stats.state.value}",
                data={
                    'circuit': circuit,
                    'state': stats.state.value,
                    'failures': stats.failures,
                    'successes': stats.successes,
                    'total_calls': stats.total_calls,
                    'rejected_calls': stats.rejected_calls,
                    'failure_threshold': config.failure_threshold,
                    'timeout_seconds': config.timeout_seconds,
                    'last_state_change': stats.last_state_change
                }
            )
    
    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset a circuit to closed state."""
        circuit = params.get('circuit')
        
        with self._lock:
            if circuit in self._circuits:
                self._circuits[circuit] = CircuitStats()
                return ActionResult(
                    success=True,
                    message=f"Circuit '{circuit}' reset"
                )
        
        return ActionResult(
            success=False,
            message=f"Circuit '{circuit}' not found"
        )
    
    def _record(self, params: Dict[str, Any]) -> ActionResult:
        """Record call result."""
        circuit = params.get('circuit')
        success = params.get('success', True)
        error = params.get('error')
        
        if success:
            self._record_success(circuit)
        elif error:
            self._record_failure(circuit, Exception(error))
        
        return ActionResult(
            success=True,
            message=f"Recorded {'success' if success else 'failure'} for '{circuit}'"
        )
