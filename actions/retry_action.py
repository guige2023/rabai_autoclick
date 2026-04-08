"""Retry action module for RabAI AutoClick.

Provides retry logic with configurable backoff strategies,
circuit breakers, and error handling for resilient operations.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RetryAction(BaseAction):
    """Generic retry wrapper for operations.
    
    Supports exponential backoff and jitter.
    """
    action_type = "retry"
    display_name = "重试"
    description = "通用重试逻辑"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute operation with retry.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, operation_args, max_attempts,
                   backoff_base, backoff_factor, jitter, retry_on.
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '')
        operation_args = params.get('operation_args', {})
        max_attempts = params.get('max_attempts', 3)
        backoff_base = params.get('backoff_base', 2)
        backoff_factor = params.get('backoff_factor', 1)
        jitter = params.get('jitter', True)
        retry_on = params.get('retry_on', ['Exception'])

        if not operation:
            return ActionResult(success=False, message="operation is required")

        start_time = time.time()
        last_error = None

        for attempt in range(max_attempts):
            try:
                result = self._execute_operation(operation, operation_args, context)
                
                duration = time.time() - start_time
                
                return ActionResult(
                    success=True,
                    message=f"Operation succeeded on attempt {attempt + 1}",
                    data={'result': result, 'attempts': attempt + 1, 'duration': duration}
                )

            except Exception as e:
                last_error = str(e)
                error_type = type(e).__name__
                
                should_retry = any(
                    err in error_type or err in str(e) 
                    for err in retry_on
                )
                
                if not should_retry or attempt >= max_attempts - 1:
                    duration = time.time() - start_time
                    return ActionResult(
                        success=False,
                        message=f"Operation failed: {last_error}",
                        data={'error': last_error, 'attempts': attempt + 1, 'duration': duration}
                    )
                
                wait_time = backoff_factor * (backoff_base ** attempt)
                
                if jitter:
                    import random
                    wait_time *= (0.5 + random.random())
                
                time.sleep(wait_time)

        return ActionResult(
            success=False,
            message=f"Operation failed after {max_attempts} attempts",
            data={'error': last_error, 'attempts': max_attempts}
        )

    def _execute_operation(self, operation: str, args: Dict, context: Any) -> Any:
        """Execute named operation."""
        return {'status': 'executed', 'operation': operation}


class RetryBackoffAction(BaseAction):
    """Configure backoff strategies for retries.
    
    Supports linear, exponential, fibonacci, and constant backoff.
    """
    action_type = "retry_backoff"
    display_name = "退避重试"
    description = "多种退避策略的重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate backoff delay.
        
        Args:
            context: Execution context.
            params: Dict with keys: attempt, strategy, base, max_delay,
                   jitter_range.
        
        Returns:
            ActionResult with delay value.
        """
        attempt = params.get('attempt', 0)
        strategy = params.get('strategy', 'exponential')
        base = params.get('base', 2)
        max_delay = params.get('max_delay', 60)
        jitter_range = params.get('jitter_range', (0, 0.5))

        delay = 0

        if strategy == 'linear':
            delay = base * attempt

        elif strategy == 'exponential':
            delay = base ** attempt

        elif strategy == 'fibonacci':
            a, b = 1, 1
            for _ in range(attempt):
                a, b = b, a + b
            delay = a * base

        elif strategy == 'constant':
            delay = base

        elif strategy == 'polynomial':
            delay = base * (attempt ** 2)

        delay = min(delay, max_delay)

        import random
        jitter = random.uniform(*jitter_range)
        delay = delay * (1 + jitter)

        return ActionResult(
            success=True,
            message=f"Backoff delay for attempt {attempt}: {delay:.2f}s",
            data={
                'delay': delay,
                'strategy': strategy,
                'attempt': attempt
            }
        )


class RetryCircuitBreakerAction(BaseAction):
    """Circuit breaker pattern for preventing cascading failures.
    
    States: closed (normal), open (blocking), half-open (testing).
    """
    action_type = "retry_circuit_breaker"
    display_name = "断路器"
    description = "断路器模式防止级联失败"

    def __init__(self):
        super().__init__()
        self._state = 'closed'
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0
        self._opened_at = 0

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with circuit breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, operation_args,
                   failure_threshold, recovery_timeout, half_open_max.
        
        Returns:
            ActionResult with circuit state.
        """
        operation = params.get('operation', '')
        operation_args = params.get('operation_args', {})
        failure_threshold = params.get('failure_threshold', 5)
        recovery_timeout = params.get('recovery_timeout', 60)
        half_open_max = params.get('half_open_max', 3)

        if self._state == 'open':
            if time.time() - self._opened_at > recovery_timeout:
                self._state = 'half-open'
                self._success_count = 0
            else:
                return ActionResult(
                    success=False,
                    message=f"Circuit open. Try again in {int(recovery_timeout - (time.time() - self._opened_at))}s",
                    data={'circuit_state': 'open', 'failure_count': self._failure_count}
                )

        try:
            result = {'status': 'success', 'operation': operation}
            
            if self._state == 'half-open':
                self._success_count += 1
                if self._success_count >= half_open_max:
                    self._state = 'closed'
                    self._failure_count = 0
            
            return ActionResult(
                success=True,
                message="Operation succeeded",
                data={
                    'result': result,
                    'circuit_state': self._state,
                    'success_count': self._success_count
                }
            )

        except Exception as e:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == 'half-open' or self._failure_count >= failure_threshold:
                self._state = 'open'
                self._opened_at = time.time()

            return ActionResult(
                success=False,
                message=f"Operation failed: {str(e)}",
                data={
                    'circuit_state': self._state,
                    'failure_count': self._failure_count,
                    'error': str(e)
                }
            )


class RetryTimeoutAction(BaseAction):
    """Execute operation with timeout protection.
    
    Cancels operation if it exceeds time limit.
    """
    action_type = "retry_timeout"
    display_name = "超时控制"
    description = "带超时控制的执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with timeout.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, operation_args,
                   timeout, on_timeout.
        
        Returns:
            ActionResult with result or timeout error.
        """
        operation = params.get('operation', '')
        operation_args = params.get('operation_args', {})
        timeout = params.get('timeout', 30)
        on_timeout = params.get('on_timeout', 'error')

        if not operation:
            return ActionResult(success=False, message="operation is required")

        import threading

        result_container = [None]
        error_container = [None]
        completed = [False]
        lock = threading.Lock()

        def run_operation():
            try:
                result = self._execute_operation(operation, operation_args, context)
                with lock:
                    result_container[0] = result
                    completed[0] = True
            except Exception as e:
                with lock:
                    error_container[0] = str(e)
                    completed[0] = True

        thread = threading.Thread(target=run_operation)
        thread.start()
        thread.join(timeout=timeout)

        with lock:
            is_completed = completed[0]

        if not is_completed:
            if on_timeout == 'error':
                return ActionResult(
                    success=False,
                    message=f"Operation timed out after {timeout}s",
                    data={'timeout': timeout, 'operation': operation}
                )
            elif on_timeout == 'continue':
                return ActionResult(
                    success=True,
                    message=f"Operation timed out, continuing",
                    data={'timeout': timeout, 'partial_result': result_container[0]}
                )

        if error_container[0]:
            return ActionResult(
                success=False,
                message=f"Operation failed: {error_container[0]}",
                data={'error': error_container[0]}
            )

        return ActionResult(
            success=True,
            message="Operation completed",
            data={'result': result_container[0]}
        )

    def _execute_operation(self, operation: str, args: Dict, context: Any) -> Any:
        """Execute named operation."""
        return {'status': 'completed', 'operation': operation}


class RetryBatchAction(BaseAction):
    """Retry multiple operations with shared configuration.
    
    Applies retry logic across a batch of operations.
    """
    action_type = "retry_batch"
    display_name = "批量重试"
    description = "批量操作统一重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch with retry.
        
        Args:
            context: Execution context.
            params: Dict with keys: operations, max_attempts,
                   backoff_base, stop_on_error.
        
        Returns:
            ActionResult with batch results.
        """
        operations = params.get('operations', [])
        max_attempts = params.get('max_attempts', 3)
        backoff_base = params.get('backoff_base', 2)
        stop_on_error = params.get('stop_on_error', False)

        if not operations:
            return ActionResult(success=False, message="operations list is required")

        results = []
        errors = []
        success_count = 0

        for i, op in enumerate(operations):
            operation = op.get('operation', '')
            args = op.get('args', {})
            
            for attempt in range(max_attempts):
                try:
                    result = self._execute_operation(operation, args, context)
                    results.append({
                        'index': i,
                        'operation': operation,
                        'success': True,
                        'result': result,
                        'attempts': attempt + 1
                    })
                    success_count += 1
                    break

                except Exception as e:
                    last_error = str(e)
                    if attempt < max_attempts - 1:
                        time.sleep(backoff_base ** attempt)

                if attempt == max_attempts - 1:
                    errors.append({
                        'index': i,
                        'operation': operation,
                        'error': last_error,
                        'attempts': max_attempts
                    })
                    
                    if stop_on_error:
                        return ActionResult(
                            success=False,
                            message=f"Stopped on error at operation {i}",
                            data={
                                'results': results,
                                'errors': errors,
                                'total': len(operations),
                                'succeeded': success_count
                            }
                        )

        return ActionResult(
            success=len(errors) == 0,
            message=f"Batch: {success_count}/{len(operations)} succeeded",
            data={
                'results': results,
                'errors': errors,
                'total': len(operations),
                'succeeded': success_count,
                'failed': len(errors)
            }
        )

    def _execute_operation(self, operation: str, args: Dict, context: Any) -> Any:
        """Execute named operation."""
        return {'status': 'done', 'operation': operation}
