"""Resilience action module for RabAI AutoClick.

Provides circuit breaker, bulkhead, and retry pattern
implementations for fault-tolerant operations.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker.
    
    Attributes:
        failure_threshold: Failures before opening circuit.
        success_threshold: Successes to close circuit.
        timeout: Seconds before attempting half-open.
        expected_exceptions: Exceptions that count as failures.
    """
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    expected_exceptions: tuple = (Exception,)


class CircuitBreaker:
    """Circuit breaker implementation.
    
    Prevents cascading failures by short-circuiting
    calls when a service is experiencing issues.
    """
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        """Initialize circuit breaker.
        
        Args:
            name: Circuit breaker name.
            config: Circuit configuration.
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker.
        
        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        
        Returns:
            Function result.
        
        Raises:
            Exception: If circuit is open or function fails.
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.config.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.config.expected_exceptions as e:
            self._on_failure()
            raise
    
    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._failure_count = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._success_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
            elif self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
    
    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state
    
    def reset(self) -> None:
        """Reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None


class Bulkhead:
    """Bulkhead pattern implementation.
    
    Limits concurrent executions to prevent resource exhaustion.
    """
    
    def __init__(self, name: str, max_concurrent: int = 10):
        """Initialize bulkhead.
        
        Args:
            name: Bulkhead name.
            max_concurrent: Maximum concurrent executions.
        """
        self.name = name
        self.max_concurrent = max_concurrent
        self._semaphore: Optional[threading.Semaphore] = None
        self._lock = threading.Lock()
        self._active_count = 0
        self._waiting_count = 0
    
    def _get_semaphore(self) -> threading.Semaphore:
        """Get or create semaphore."""
        if self._semaphore is None:
            with self._lock:
                if self._semaphore is None:
                    self._semaphore = threading.Semaphore(self.max_concurrent)
        return self._semaphore
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through bulkhead.
        
        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        
        Returns:
            Function result.
        
        Raises:
            Exception: If max concurrent reached.
        """
        semaphore = self._get_semaphore()
        
        acquired = semaphore.acquire(blocking=False)
        
        if not acquired:
            raise Exception(f"Bulkhead {self.name} at capacity ({self.max_concurrent})")
        
        try:
            with self._lock:
                self._active_count += 1
            
            result = func(*args, **kwargs)
            return result
        finally:
            with self._lock:
                self._active_count -= 1
            semaphore.release()
    
    def get_stats(self) -> Dict[str, int]:
        """Get bulkhead statistics."""
        with self._lock:
            return {
                "name": self.name,
                "max_concurrent": self.max_concurrent,
                "active": self._active_count,
                "available": self.max_concurrent - self._active_count
            }


# Global resilience storage
_breakers: Dict[str, CircuitBreaker] = {}
_bulkheads: Dict[str, Bulkhead] = {}
_resilience_lock = threading.Lock()


class CircuitBreakerCallAction(BaseAction):
    """Execute function through circuit breaker."""
    action_type = "circuit_breaker_call"
    display_name = "断路器调用"
    description = "通过断路器执行函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute through circuit breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys: breaker_name, func_body, args, kwargs,
                   failure_threshold, success_threshold, timeout.
        
        Returns:
            ActionResult with execution result.
        """
        breaker_name = params.get('breaker_name', 'default')
        func_body = params.get('func_body', '')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        failure_threshold = params.get('failure_threshold', 5)
        success_threshold = params.get('success_threshold', 2)
        timeout = params.get('timeout', 30.0)
        
        if not func_body:
            return ActionResult(success=False, message="func_body is required")
        
        with _resilience_lock:
            if breaker_name not in _breakers:
                config = CircuitBreakerConfig(
                    failure_threshold=failure_threshold,
                    success_threshold=success_threshold,
                    timeout=timeout
                )
                _breakers[breaker_name] = CircuitBreaker(breaker_name, config)
            breaker = _breakers[breaker_name]
        
        try:
            func = eval(f"lambda: {func_body}") if isinstance(func_body, str) else func_body
            
            result = breaker.call(func, *args, **kwargs)
            
            return ActionResult(
                success=True,
                message=f"Call succeeded through {breaker_name}",
                data={
                    "circuit_state": breaker.get_state().value,
                    "result": str(result)[:200] if result else None
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Call failed: {str(e)}",
                data={
                    "circuit_state": breaker.get_state().value,
                    "error": str(e)
                }
            )


class CircuitBreakerStatusAction(BaseAction):
    """Get circuit breaker status."""
    action_type = "circuit_breaker_status"
    display_name = "断路器状态"
    description = "查看断路器状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get breaker status.
        
        Args:
            context: Execution context.
            params: Dict with keys: breaker_name.
        
        Returns:
            ActionResult with status.
        """
        breaker_name = params.get('breaker_name', 'default')
        
        with _resilience_lock:
            if breaker_name not in _breakers:
                return ActionResult(success=True, message=f"Breaker {breaker_name} not found", data={"exists": False})
            breaker = _breakers[breaker_name]
        
        state = breaker.get_state()
        
        return ActionResult(
            success=True,
            message=f"Circuit {breaker_name}: {state.value}",
            data={
                "name": breaker.name,
                "state": state.value,
                "failure_count": breaker._failure_count,
                "success_count": breaker._success_count,
                "last_failure_time": breaker._last_failure_time
            }
        )


class CircuitBreakerResetAction(BaseAction):
    """Reset a circuit breaker."""
    action_type = "circuit_breaker_reset"
    display_name = "断路器重置"
    description = "重置断路器状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reset breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys: breaker_name.
        
        Returns:
            ActionResult with reset status.
        """
        breaker_name = params.get('breaker_name', 'default')
        
        with _resilience_lock:
            if breaker_name not in _breakers:
                return ActionResult(success=True, message=f"Breaker {breaker_name} not found")
            breaker = _breakers[breaker_name]
        
        breaker.reset()
        
        return ActionResult(success=True, message=f"Circuit {breaker_name} reset to CLOSED")


class BulkheadExecuteAction(BaseAction):
    """Execute function through bulkhead."""
    action_type = "bulkhead_execute"
    display_name = "隔板执行"
    description = "通过隔板执行函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute through bulkhead.
        
        Args:
            context: Execution context.
            params: Dict with keys: bulkhead_name, func_body, args, kwargs,
                   max_concurrent.
        
        Returns:
            ActionResult with execution result.
        """
        bulkhead_name = params.get('bulkhead_name', 'default')
        func_body = params.get('func_body', '')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        max_concurrent = params.get('max_concurrent', 10)
        
        if not func_body:
            return ActionResult(success=False, message="func_body is required")
        
        with _resilience_lock:
            if bulkhead_name not in _bulkheads:
                _bulkheads[bulkhead_name] = Bulkhead(bulkhead_name, max_concurrent)
            bulkhead = _bulkheads[bulkhead_name]
        
        try:
            func = eval(f"lambda: {func_body}") if isinstance(func_body, str) else func_body
            
            result = bulkhead.execute(func, *args, **kwargs)
            stats = bulkhead.get_stats()
            
            return ActionResult(
                success=True,
                message=f"Bulkhead execution succeeded",
                data={
                    "result": str(result)[:200] if result else None,
                    "bulkhead_stats": stats
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Bulkhead execution failed: {str(e)}",
                data={"error": str(e)}
            )


class BulkheadStatusAction(BaseAction):
    """Get bulkhead status."""
    action_type = "bulkhead_status"
    display_name = "隔板状态"
    description = "查看隔板状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get bulkhead status.
        
        Args:
            context: Execution context.
            params: Dict with keys: bulkhead_name.
        
        Returns:
            ActionResult with status.
        """
        bulkhead_name = params.get('bulkhead_name', 'default')
        
        with _resilience_lock:
            if bulkhead_name not in _bulkheads:
                return ActionResult(success=True, message=f"Bulkhead {bulkhead_name} not found", data={"exists": False})
            bulkhead = _bulkheads[bulkhead_name]
        
        stats = bulkhead.get_stats()
        
        return ActionResult(success=True, message=f"Bulkhead {bulkhead_name} status", data=stats)
