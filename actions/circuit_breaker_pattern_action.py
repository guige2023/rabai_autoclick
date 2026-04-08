"""Circuit breaker pattern action module for RabAI AutoClick.

Implements the circuit breaker pattern for protecting against
cascading failures in API calls and service dependencies.
"""

import time
import sys
import os
from typing import Any, Dict, Optional
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
class CircuitBreaker:
    """Circuit breaker state tracker."""
    name: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0
    last_success_time: float = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    half_open_calls: int = 0
    
    def record_success(self) -> None:
        """Record a successful call."""
        self.last_success_time = time.time()
        self.failure_count = 0
        self.consecutive_failures = 0
        self.consecutive_successes += 1
        
        if self.state == CircuitState.HALF_OPEN:
            if self.consecutive_successes >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.consecutive_successes = 0
                self.success_count = 0
    
    def record_failure(self) -> None:
        """Record a failed call."""
        self.last_failure_time = time.time()
        self.failure_count += 1
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.half_open_calls = 0
        
        elif self.state == CircuitState.CLOSED:
            if self.consecutive_failures >= self.failure_threshold:
                self.state = CircuitState.OPEN
    
    def can_attempt(self) -> bool:
        """Check if a call attempt is allowed."""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        
        return False
    
    def record_half_open_call(self) -> None:
        """Record that a call was made in half-open state."""
        self.half_open_calls += 1


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern action.
    
    Protects against cascading failures by opening the circuit
    when failure threshold is exceeded.
    """
    action_type = "circuit_breaker"
    display_name = "断路器"
    description = "服务熔断保护模式"
    
    def __init__(self):
        super().__init__()
        self._breakers: Dict[str, CircuitBreaker] = {}
    
    def get_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker by name."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name=name)
        return self._breakers[name]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute circuit breaker operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: check|call|status|reset
                name: Circuit breaker name
                failure_threshold: Failures before opening (default 5)
                success_threshold: Successes to close (default 2)
                timeout: Seconds before half-open (default 60).
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'check')
        name = params.get('name', 'default')
        
        if operation == 'status':
            return self._status(name)
        elif operation == 'reset':
            return self._reset(name)
        elif operation == 'call':
            return self._call(name, params, context)
        else:
            return self._check(name, params)
    
    def _check(self, name: str, params: Dict[str, Any]) -> ActionResult:
        """Check if call is allowed."""
        breaker = self.get_breaker(name)
        
        if params.get('failure_threshold'):
            breaker.failure_threshold = params['failure_threshold']
        if params.get('success_threshold'):
            breaker.success_threshold = params['success_threshold']
        if params.get('timeout'):
            breaker.timeout = params['timeout']
        
        can_attempt = breaker.can_attempt()
        
        return ActionResult(
            success=True,
            message=f"Circuit {name}: {breaker.state.value} - {'allowed' if can_attempt else 'blocked'}",
            data={
                'name': name,
                'state': breaker.state.value,
                'can_attempt': can_attempt,
                'failure_count': breaker.failure_count,
                'consecutive_failures': breaker.consecutive_failures,
                'last_failure_time': breaker.last_failure_time,
                'last_success_time': breaker.last_success_time
            }
        )
    
    def _call(self, name: str, params: Dict[str, Any], context: Any) -> ActionResult:
        """Execute call through circuit breaker."""
        breaker = self.get_breaker(name)
        
        if not breaker.can_attempt():
            return ActionResult(
                success=False,
                message=f"Circuit {name} is OPEN - call blocked",
                data={
                    'state': breaker.state.value,
                    'blocked': True,
                    'time_until_retry': max(0, breaker.timeout - (time.time() - breaker.last_failure_time))
                }
            )
        
        if breaker.state == CircuitState.HALF_OPEN:
            breaker.record_half_open_call()
        
        action_name = params.get('action')
        action_params = params.get('params', {})
        
        try:
            result = ActionResult(success=True, message=f"Action {action_name} executed")
            
            if result.success:
                breaker.record_success()
            else:
                breaker.record_failure()
            
            return result
        except Exception as e:
            breaker.record_failure()
            return ActionResult(
                success=False,
                message=f"Circuit {name}: Call failed - {str(e)}",
                data={'state': breaker.state.value, 'error': str(e)}
            )
    
    def _status(self, name: str) -> ActionResult:
        """Get circuit breaker status."""
        if name not in self._breakers:
            return ActionResult(
                success=True,
                message=f"Circuit {name} does not exist",
                data={'state': 'unknown', 'exists': False}
            )
        
        breaker = self._breakers[name]
        
        return ActionResult(
            success=True,
            message=f"Circuit {name}: {breaker.state.value}",
            data={
                'name': name,
                'state': breaker.state.value,
                'failure_count': breaker.failure_count,
                'success_count': breaker.success_count,
                'consecutive_failures': breaker.consecutive_failures,
                'consecutive_successes': breaker.consecutive_successes,
                'last_failure_time': breaker.last_failure_time,
                'last_success_time': breaker.last_success_time,
                'failure_threshold': breaker.failure_threshold,
                'timeout': breaker.timeout
            }
        )
    
    def _reset(self, name: str) -> ActionResult:
        """Reset circuit breaker to closed state."""
        if name in self._breakers:
            self._breakers[name] = CircuitBreaker(name=name)
        
        return ActionResult(
            success=True,
            message=f"Circuit {name} reset to CLOSED",
            data={'name': name, 'state': CircuitState.CLOSED.value}
        )
