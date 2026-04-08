"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern implementation for fault tolerance
with half-open state and configurable thresholds.
"""

import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Union, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker implementation."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        expected_exceptions: tuple = (Exception,)
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self.expected_exceptions = expected_exceptions
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._lock = threading.RLock()
        self._callbacks: Dict[str, Callable] = {}
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
            return self._state
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset."""
        if self._last_failure_time is None:
            return False
        return time.time() - self._last_failure_time >= self.timeout
    
    def record_success(self):
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._trigger_callback('close')
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)
    
    def record_failure(self):
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0
                self._trigger_callback('open')
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._trigger_callback('open')
    
    def allow_request(self) -> bool:
        """Check if request is allowed."""
        return self.state != CircuitState.OPEN
    
    def get_wait_time(self) -> float:
        """Get time to wait before attempting reset."""
        with self._lock:
            if self._state != CircuitState.OPEN:
                return 0
            if self._last_failure_time is None:
                return 0
            elapsed = time.time() - self._last_failure_time
            return max(0, self.timeout - elapsed)
    
    def register_callback(self, event: str, callback: Callable):
        """Register a callback for circuit events."""
        self._callbacks[event] = callback
    
    def _trigger_callback(self, event: str):
        """Trigger a registered callback."""
        if event in self._callbacks:
            try:
                self._callbacks[event](self)
            except Exception:
                pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        with self._lock:
            return {
                'state': self.state.value,
                'failure_count': self._failure_count,
                'success_count': self._success_count,
                'last_failure_time': self._last_failure_time,
                'failure_threshold': self.failure_threshold,
                'success_threshold': self.success_threshold,
                'timeout': self.timeout
            }


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern for fault tolerance.
    
    Provides circuit breaker with closed, open, and half-open states.
    Automatically trips after configurable failure threshold.
    """
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式，保护系统免受级联故障"
    
    _breakers: Dict[str, CircuitBreaker] = {}
    _lock = threading.Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute circuit breaker operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create, call, state,
                   reset, info), breaker_config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'call')
        
        if action == 'create':
            return self._create_breaker(params)
        elif action == 'call':
            return self._call_with_breaker(params)
        elif action == 'state':
            return self._get_state(params)
        elif action == 'reset':
            return self._reset_breaker(params)
        elif action == 'info':
            return self._get_info(params)
        elif action == 'list':
            return self._list_breakers()
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_breaker(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new circuit breaker."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        failure_threshold = params.get('failure_threshold', 5)
        success_threshold = params.get('success_threshold', 2)
        timeout = params.get('timeout', 60.0)
        
        with self._lock:
            if name in self._breakers:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker '{name}' already exists"
                )
            
            breaker = CircuitBreaker(
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                timeout=timeout
            )
            
            self._breakers[name] = breaker
        
        return ActionResult(
            success=True,
            message=f"Circuit breaker '{name}' created",
            data=breaker.to_dict()
        )
    
    def _call_with_breaker(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a call through a circuit breaker."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        with self._lock:
            if name not in self._breakers:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker '{name}' not found"
                )
            breaker = self._breakers[name]
        
        if not breaker.allow_request():
            wait_time = breaker.get_wait_time()
            return ActionResult(
                success=False,
                message=f"Circuit breaker is OPEN. Wait {wait_time:.1f}s",
                data={
                    'state': CircuitState.OPEN.value,
                    'wait_time': wait_time
                }
            )
        
        func = params.get('func')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        
        try:
            if func:
                result = func(*args, **kwargs)
            else:
                result = params.get('result', True)
            
            breaker.record_success()
            
            return ActionResult(
                success=True,
                message="Call succeeded",
                data={
                    'state': breaker.state.value,
                    'result': result
                }
            )
            
        except Exception as e:
            breaker.record_failure()
            
            return ActionResult(
                success=False,
                message=f"Call failed: {e}",
                data={
                    'state': breaker.state.value,
                    'error': str(e)
                }
            )
    
    def _get_state(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get current state of a circuit breaker."""
        name = params.get('name')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        with self._lock:
            if name not in self._breakers:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker '{name}' not found"
                )
            breaker = self._breakers[name]
        
        return ActionResult(
            success=True,
            message=f"Circuit breaker '{name}' is {breaker.state.value}",
            data={
                'state': breaker.state.value,
                'wait_time': breaker.get_wait_time()
            }
        )
    
    def _reset_breaker(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reset a circuit breaker to closed state."""
        name = params.get('name')
        
        with self._lock:
            if name:
                if name not in self._breakers:
                    return ActionResult(
                        success=False,
                        message=f"Circuit breaker '{name}' not found"
                    )
                breaker = self._breakers[name]
                breaker._state = CircuitState.CLOSED
                breaker._failure_count = 0
                breaker._success_count = 0
                
                return ActionResult(
                    success=True,
                    message=f"Circuit breaker '{name}' reset to CLOSED",
                    data=breaker.to_dict()
                )
            else:
                for breaker in self._breakers.values():
                    breaker._state = CircuitState.CLOSED
                    breaker._failure_count = 0
                    breaker._success_count = 0
                
                return ActionResult(
                    success=True,
                    message="All circuit breakers reset to CLOSED"
                )
    
    def _get_info(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get detailed info about a circuit breaker."""
        name = params.get('name')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        with self._lock:
            if name not in self._breakers:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker '{name}' not found"
                )
            breaker = self._breakers[name]
        
        return ActionResult(
            success=True,
            message=f"Circuit breaker '{name}' info",
            data=breaker.to_dict()
        )
    
    def _list_breakers(self) -> ActionResult:
        """List all circuit breakers."""
        with self._lock:
            breakers_info = {
                name: breaker.to_dict()
                for name, breaker in self._breakers.items()
            }
        
        return ActionResult(
            success=True,
            message=f"Found {len(self._breakers)} circuit breakers",
            data={'breakers': breakers_info}
        )
