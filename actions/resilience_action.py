"""Resilience action module for RabAI AutoClick.

Provides fault tolerance patterns: circuit breaker,
bulkhead isolation, retry with backoff, and fallback.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Blocking calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreaker:
    """Circuit breaker state holder."""
    name: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0
    last_success_time: float = 0
    lock: threading.Lock = None

    def __post_init__(self):
        if self.lock is None:
            self.lock = threading.Lock()


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern implementation.
    
    Trip circuit after threshold failures, allow
    recovery attempts after timeout.
    """
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式：故障超过阈值时断开，恢复后重连"

    _breakers: Dict[str, CircuitBreaker] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with circuit breaker protection.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - circuit_id: str
                - failure_threshold: int (failures to trip, default 5)
                - success_threshold: int (successes to close, default 3)
                - timeout: float (seconds before half-open, default 60)
                - action: str (call/success/failure/status)
                - save_to_var: str
        
        Returns:
            ActionResult with circuit breaker state.
        """
        circuit_id = params.get('circuit_id', 'default')
        failure_threshold = params.get('failure_threshold', 5)
        success_threshold = params.get('success_threshold', 3)
        timeout = params.get('timeout', 60.0)
        action = params.get('action', 'status')
        save_to_var = params.get('save_to_var', 'circuit_breaker')

        with self._lock:
            if circuit_id not in self._breakers:
                self._breakers[circuit_id] = CircuitBreaker(
                    name=circuit_id,
                    lock=threading.Lock(),
                )
            breaker = self._breakers[circuit_id]

        now = time.time()

        if action == 'status':
            return self._get_status(breaker, circuit_id, save_to_var, context)

        elif action == 'call':
            return self._try_call(breaker, circuit_id, timeout, save_to_var, context)

        elif action == 'success':
            return self._record_success(breaker, circuit_id, success_threshold, save_to_var, context)

        elif action == 'failure':
            return self._record_failure(breaker, circuit_id, failure_threshold, timeout, save_to_var, context)

        else:
            return ActionResult(success=False, message=f"Unknown action: {action}")

    def _get_status(self, breaker: CircuitBreaker, circuit_id: str,
                   save_to_var: str, context: Any) -> ActionResult:
        """Get current circuit status."""
        with breaker.lock:
            result = {
                'circuit_id': circuit_id,
                'state': breaker.state.value,
                'failure_count': breaker.failure_count,
                'success_count': breaker.success_count,
            }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Circuit {circuit_id}: {breaker.state.value}"
        )

    def _try_call(self, breaker: CircuitBreaker, circuit_id: str,
                  timeout: float, save_to_var: str, context: Any) -> ActionResult:
        """Attempt a call through the circuit."""
        with breaker.lock:
            now = time.time()

            if breaker.state == CircuitState.OPEN:
                # Check if timeout has passed
                if now - breaker.last_failure_time >= timeout:
                    breaker.state = CircuitState.HALF_OPEN
                    breaker.success_count = 0
                    result = {
                        'circuit_id': circuit_id,
                        'state': CircuitState.HALF_OPEN.value,
                        'allowed': True,
                        'message': 'Circuit half-open, testing recovery',
                    }
                else:
                    result = {
                        'circuit_id': circuit_id,
                        'state': CircuitState.OPEN.value,
                        'allowed': False,
                        'retry_after': timeout - (now - breaker.last_failure_time),
                    }
                    if context and save_to_var:
                        context.variables[save_to_var] = result
                    return ActionResult(
                        success=False,
                        data=result,
                        message=f"Circuit OPEN, retry after {result['retry_after']:.1f}s"
                    )
            else:
                result = {
                    'circuit_id': circuit_id,
                    'state': breaker.state.value,
                    'allowed': True,
                }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(success=True, data=result, message=f"Circuit {breaker.state.value}: call allowed")

    def _record_success(self, breaker: CircuitBreaker, circuit_id: str,
                        success_threshold: int, save_to_var: str,
                        context: Any) -> ActionResult:
        """Record successful call."""
        with breaker.lock:
            breaker.success_count += 1
            breaker.last_success_time = time.time()

            if breaker.state == CircuitState.HALF_OPEN:
                if breaker.success_count >= success_threshold:
                    breaker.state = CircuitState.CLOSED
                    breaker.failure_count = 0

            result = {
                'circuit_id': circuit_id,
                'state': breaker.state.value,
                'success_count': breaker.success_count,
            }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Circuit {breaker.state.value}: success recorded"
        )

    def _record_failure(self, breaker: CircuitBreaker, circuit_id: str,
                        failure_threshold: int, timeout: float,
                        save_to_var: str, context: Any) -> ActionResult:
        """Record failed call."""
        with breaker.lock:
            breaker.failure_count += 1
            breaker.last_failure_time = time.time()
            breaker.success_count = 0

            if breaker.state == CircuitState.HALF_OPEN:
                breaker.state = CircuitState.OPEN
            elif breaker.failure_count >= failure_threshold:
                breaker.state = CircuitState.OPEN

            result = {
                'circuit_id': circuit_id,
                'state': breaker.state.value,
                'failure_count': breaker.failure_count,
                'threshold': failure_threshold,
            }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=False,
            data=result,
            message=f"Circuit {breaker.state.value}: failure recorded"
        )


class BulkheadAction(BaseAction):
    """Bulkhead isolation pattern.
    
    Limit concurrent executions to prevent resource exhaustion.
    """
    action_type = "bulkhead"
    display_name = "隔板隔离"
    description = "隔板模式：限制并发执行数量防止资源耗尽"

    _semaphores: Dict[str, threading.Semaphore] = {}
    _locks: Dict[str, threading.Lock] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with bulkhead isolation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - bulkhead_id: str
                - max_concurrent: int (max concurrent executions)
                - action: str (acquire/release/status)
                - timeout: float (max wait for acquire)
                - save_to_var: str
        
        Returns:
            ActionResult with bulkhead state.
        """
        bulkhead_id = params.get('bulkhead_id', 'default')
        max_concurrent = params.get('max_concurrent', 5)
        action = params.get('action', 'status')
        timeout = params.get('timeout', 10.0)
        save_to_var = params.get('save_to_var', 'bulkhead')

        with threading.Lock():
            if bulkhead_id not in self._semaphores:
                self._semaphores[bulkhead_id] = threading.Semaphore(max_concurrent)
                self._locks[bulkhead_id] = 0
            sem = self._semaphores[bulkhead_id]

        if action == 'acquire':
            acquired = sem.acquire(timeout=timeout if timeout > 0 else None)
            if acquired:
                with threading.Lock():
                    self._locks[bulkhead_id] += 1

            result = {
                'bulkhead_id': bulkhead_id,
                'acquired': acquired,
                'max_concurrent': max_concurrent,
                'current': self._locks.get(bulkhead_id, 0),
            }

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(
                success=acquired,
                data=result,
                message=f"Bulkhead {'acquired' if acquired else 'timeout'}: {self._locks.get(bulkhead_id, 0)}/{max_concurrent}"
            )

        elif action == 'release':
            sem.release()
            with threading.Lock():
                self._locks[bulkhead_id] = max(0, self._locks.get(bulkhead_id, 0) - 1)

            result = {
                'bulkhead_id': bulkhead_id,
                'released': True,
                'current': self._locks.get(bulkhead_id, 0),
            }

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(success=True, data=result, message="Bulkhead released")

        else:  # status
            result = {
                'bulkhead_id': bulkhead_id,
                'max_concurrent': max_concurrent,
                'current': self._locks.get(bulkhead_id, 0),
                'available': sem._value if hasattr(sem, '_value') else 'unknown',
            }

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(success=True, data=result, message=f"Bulkhead: {self._locks.get(bulkhead_id, 0)}/{max_concurrent}")


class RetryWithBackoffAction(BaseAction):
    """Retry with exponential backoff.
    
    Retry failed operations with increasing delays.
    """
    action_type = "retry_backoff"
    display_name = "指数退避重试"
    description = "指数退避重试：失败后延迟递增重试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Retry with exponential backoff.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - max_attempts: int (default 3)
                - base_delay: float (initial delay seconds)
                - max_delay: float (cap delay at this)
                - backoff_multiplier: float (multiply delay each attempt)
                - jitter: bool (add random jitter)
                - save_to_var: str
        
        Returns:
            ActionResult with retry attempt record.
        """
        max_attempts = params.get('max_attempts', 3)
        base_delay = params.get('base_delay', 1.0)
        max_delay = params.get('max_delay', 60.0)
        backoff = params.get('backoff_multiplier', 2.0)
        jitter = params.get('jitter', True)
        save_to_var = params.get('save_to_var', 'retry_result')

        import random

        attempts = []
        for attempt in range(1, max_attempts + 1):
            delay = min(base_delay * (backoff ** (attempt - 1)), max_delay)
            if jitter:
                delay *= random.uniform(0.5, 1.5)

            attempts.append({
                'attempt': attempt,
                'delay': delay,
                'status': 'pending',
            })

        result = {
            'max_attempts': max_attempts,
            'base_delay': base_delay,
            'backoff_multiplier': backoff,
            'attempts': attempts,
            'would_retry': max_attempts > 1,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Retry policy: {max_attempts} attempts, backoff {backoff}x"
        )


class FallbackAction(BaseAction):
    """Provide fallback values on failure.
    
    Return fallback value or execute fallback
    function when primary action fails.
    """
    action_type = "fallback"
    display_name = "降级处理"
    description = "降级处理：主操作失败时返回备选值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with fallback.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - primary_value: any (primary result)
                - fallback_value: any (fallback if primary is None/error)
                - fallback_func: callable (optional fallback function)
                - fallback_params: dict
                - check_error: bool (treat errors as failure)
                - save_to_var: str
        
        Returns:
            ActionResult with fallback result.
        """
        primary_value = params.get('primary_value', None)
        fallback_value = params.get('fallback_value', None)
        fallback_func = params.get('fallback_func', None)
        fallback_params = params.get('fallback_params', {})
        check_error = params.get('check_error', False)
        save_to_var = params.get('save_to_var', 'fallback_result')

        used_fallback = False
        result_value = primary_value

        # Check if primary is valid
        primary_valid = primary_value is not None

        if not primary_valid:
            used_fallback = True
            if fallback_func and callable(fallback_func):
                try:
                    result_value = fallback_func(fallback_params)
                except Exception as e:
                    result_value = fallback_value
            else:
                result_value = fallback_value

        result = {
            'used_fallback': used_fallback,
            'primary_valid': primary_valid,
            'result': result_value,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Result: {'fallback' if used_fallback else 'primary'}"
        )
