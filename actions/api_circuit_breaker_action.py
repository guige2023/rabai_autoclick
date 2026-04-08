"""API circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern implementation for protecting
API calls from cascading failures.
"""

import time
import sys
import os
from typing import Any, Dict, Optional
from enum import Enum
from dataclasses import dataclass

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
    timeout: float = 60.0
    half_open_max_calls: int = 3


class ApiCircuitBreakerAction(BaseAction):
    """API circuit breaker action for fault tolerance.
    
    Implements circuit breaker pattern to prevent cascading
    failures in API call chains.
    """
    action_type = "api_circuit_breaker"
    display_name = "API断路器"
    description = "API熔断保护机制"
    
    def __init__(self):
        super().__init__()
        self._breakers: Dict[str, 'CircuitBreaker'] = {}
    
    def get_breaker(self, name: str) -> 'CircuitBreaker':
        """Get or create circuit breaker by name."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name)
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
                failure_threshold: Failures before opening
                success_threshold: Successes to close
                timeout: Seconds before half-open.
        
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
        """Check circuit breaker state."""
        breaker = self.get_breaker(name)
        
        if params.get('failure_threshold'):
            breaker.config.failure_threshold = params['failure_threshold']
        if params.get('success_threshold'):
            breaker.config.success_threshold = params['success_threshold']
        if params.get('timeout'):
            breaker.config.timeout = params['timeout']
        
        can_attempt = breaker.can_attempt()
        
        return ActionResult(
            success=True,
            message=f"Circuit {name}: {breaker.state.value}",
            data={
                'name': name,
                'state': breaker.state.value,
                'can_attempt': can_attempt,
                'failure_count': breaker.failure_count,
                'consecutive_failures': breaker.consecutive_failures
            }
        )
    
    def _call(self, name: str, params: Dict[str, Any], context: Any) -> ActionResult:
        """Execute call through circuit breaker."""
        breaker = self.get_breaker(name)
        
        if not breaker.can_attempt():
            return ActionResult(
                success=False,
                message=f"Circuit {name} is OPEN",
                data={
                    'state': breaker.state.value,
                    'blocked': True,
                    'time_until_retry': breaker.time_until_retry()
                }
            )
        
        if breaker.state == CircuitState.HALF_OPEN:
            breaker.half_open_calls += 1
        
        return ActionResult(
            success=True,
            message=f"Call through {name} allowed",
            data={
                'state': breaker.state.value,
                'allowed': True
            }
        )
    
    def _status(self, name: str) -> ActionResult:
        """Get circuit breaker status."""
        if name not in self._breakers:
            return ActionResult(
                success=True,
                message=f"Circuit {name} unknown",
                data={'exists': False}
            )
        
        breaker = self._breakers[name]
        
        return ActionResult(
            success=True,
            message=f"Circuit {name}: {breaker.state.value}",
            data={
                'name': name,
                'state': breaker.state.value,
                'failure_count': breaker.failure_count,
                'success_count': breaker.success_count
            }
        )
    
    def _reset(self, name: str) -> ActionResult:
        """Reset circuit breaker."""
        if name in self._breakers:
            self._breakers[name] = CircuitBreaker(name)
        
        return ActionResult(
            success=True,
            message=f"Circuit {name} reset",
            data={'name': name, 'state': CircuitState.CLOSED.value}
        )


class CircuitBreaker:
    """Circuit breaker implementation."""
    
    def __init__(self, name: str):
        self.name = name
        self.state = CircuitState.CLOSED
        self.config = CircuitBreakerConfig()
        self.failure_count = 0
        self.success_count = 0
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.last_failure_time = 0
        self.last_success_time = 0
        self.half_open_calls = 0
    
    def can_attempt(self) -> bool:
        """Check if attempt is allowed."""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.config.timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            return self.half_open_calls < self.config.half_open_max_calls
        
        return False
    
    def time_until_retry(self) -> float:
        """Get seconds until retry is allowed."""
        if self.state != CircuitState.OPEN:
            return 0
        elapsed = time.time() - self.last_failure_time
        return max(0, self.config.timeout - elapsed)
