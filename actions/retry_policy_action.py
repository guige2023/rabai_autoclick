"""Retry policy action module for RabAI AutoClick.

Provides configurable retry logic with exponential backoff, jitter,
and circuit breaker pattern for resilient API calls.
"""

import sys
import os
import time
import random
from typing import Any, Dict, Optional, Callable, List, Type
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.2
    retryable_exceptions: tuple = (Exception,)


@dataclass
class CircuitState:
    """Circuit breaker state."""
    failures: int = 0
    last_failure_time: Optional[float] = None
    state: str = "closed"  # closed, open, half_open


class RetryPolicyAction(BaseAction):
    """Execute actions with configurable retry policies and circuit breaker.
    
    Supports fixed, exponential, and linear backoff strategies with optional
    jitter to prevent thundering herd. Includes circuit breaker pattern.
    """
    action_type = "retry_policy"
    display_name = "重试策略"
    description = "带重试策略的执行，支持指数退避和熔断器"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an action with retry policy.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - func: Callable to execute
                - args: Positional arguments for func
                - kwargs: Keyword arguments for func
                - strategy: 'fixed', 'exponential', or 'linear'
                - max_attempts: Maximum retry attempts (default 3)
                - initial_delay: Initial delay in seconds (default 1.0)
                - max_delay: Maximum delay in seconds (default 60.0)
                - multiplier: Backoff multiplier (default 2.0)
                - jitter: Enable random jitter (default True)
                - retryable_exceptions: Tuple of exceptions to retry on
                - circuit_breaker: Enable circuit breaker (default False)
                - circuit_threshold: Failures before opening circuit (default 5)
                - circuit_timeout: Seconds before attempting reset (default 60)
        
        Returns:
            ActionResult with execution result and retry metadata.
        """
        # Extract function and arguments
        func = params.get('func')
        if not callable(func):
            return ActionResult(success=False, message="func must be callable")
        
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        
        # Retry configuration
        strategy = params.get('strategy', 'exponential').lower()
        max_attempts = params.get('max_attempts', 3)
        initial_delay = params.get('initial_delay', 1.0)
        max_delay = params.get('max_delay', 60.0)
        multiplier = params.get('multiplier', 2.0)
        jitter = params.get('jitter', True)
        retryable = params.get('retryable_exceptions', (Exception,))
        circuit_breaker = params.get('circuit_breaker', False)
        circuit_threshold = params.get('circuit_threshold', 5)
        circuit_timeout = params.get('circuit_timeout', 60)
        
        # Validate strategy
        if strategy not in ('fixed', 'exponential', 'linear'):
            return ActionResult(
                success=False,
                message=f"Invalid strategy: {strategy}"
            )
        
        # Circuit breaker state (would normally be stored persistently)
        circuit_state = CircuitState()
        
        def calculate_delay(attempt: int) -> float:
            """Calculate delay for given attempt."""
            if strategy == 'fixed':
                delay = initial_delay
            elif strategy == 'exponential':
                delay = initial_delay * (multiplier ** (attempt - 1))
            else:  # linear
                delay = initial_delay * attempt
            
            # Cap at max_delay
            delay = min(delay, max_delay)
            
            # Add jitter
            if jitter:
                jitter_range = delay * 0.2
                delay += random.uniform(-jitter_range, jitter_range)
            
            return max(0, delay)
        
        def is_retryable(exception: Exception) -> bool:
            """Check if exception is retryable."""
            return isinstance(exception, retryable)
        
        # Execute with retries
        last_exception = None
        attempts_made = 0
        
        for attempt in range(1, max_attempts + 1):
            attempts_made = attempt
            
            # Check circuit breaker
            if circuit_breaker:
                if circuit_state.state == "open":
                    if (time.time() - circuit_state.last_failure_time) > circuit_timeout:
                        circuit_state.state = "half_open"
                    else:
                        return ActionResult(
                            success=False,
                            message=f"Circuit breaker open, retry after {circuit_timeout}s",
                            data={'circuit_state': 'open'}
                        )
            
            try:
                result = func(*args, **kwargs)
                
                # Close circuit on success
                if circuit_breaker and circuit_state.state == "half_open":
                    circuit_state.state = "closed"
                    circuit_state.failures = 0
                
                return ActionResult(
                    success=True,
                    message=f"Succeeded on attempt {attempt}",
                    data={
                        'result': result,
                        'attempts': attempt,
                        'circuit_state': circuit_state.state if circuit_breaker else None
                    }
                )
                
            except Exception as e:
                last_exception = e
                
                # Update circuit breaker
                if circuit_breaker:
                    circuit_state.failures += 1
                    circuit_state.last_failure_time = time.time()
                    
                    if circuit_state.failures >= circuit_threshold:
                        circuit_state.state = "open"
                
                # Check if should retry
                if not is_retryable(e):
                    return ActionResult(
                        success=False,
                        message=f"Non-retryable error: {e}",
                        data={
                            'error': str(e),
                            'attempts': attempt
                        }
                    )
                
                if attempt < max_attempts:
                    delay = calculate_delay(attempt)
                    time.sleep(delay)
                else:
                    break
        
        # All attempts failed
        return ActionResult(
            success=False,
            message=f"Failed after {attempts_made} attempts: {last_exception}",
            data={
                'error': str(last_exception),
                'attempts': attempts_made,
                'circuit_state': circuit_state.state if circuit_breaker else None
            }
        )


class CircuitBreakerAction(BaseAction):
    """Standalone circuit breaker for protecting calls.
    
    Monitors failure rates and opens circuit to prevent cascading failures.
    """
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "保护调用免受级联故障影响"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with circuit breaker protection.
        
        Args:
            context: Execution context.
            params: Dict with:
                - func: Callable to protect
                - threshold: Failures before opening (default 5)
                - timeout: Seconds before half-open (default 60)
                - expected_exception: Exception class to catch
        
        Returns:
            ActionResult with protected execution result.
        """
        func = params.get('func')
        threshold = params.get('threshold', 5)
        timeout = params.get('timeout', 60)
        expected = params.get('expected_exception', Exception)
        
        state = CircuitState()
        
        # Check circuit state
        current_time = time.time()
        if state.state == "open":
            if state.last_failure_time and \
               (current_time - state.last_failure_time) > timeout:
                state.state = "half_open"
            else:
                return ActionResult(
                    success=False,
                    message="Circuit breaker is open",
                    data={'state': 'open'}
                )
        
        try:
            result = func()
            if state.state == "half_open":
                state.state = "closed"
                state.failures = 0
            return ActionResult(
                success=True,
                message="Call succeeded",
                data={'result': result, 'state': state.state}
            )
        except expected as e:
            state.failures += 1
            state.last_failure_time = current_time
            if state.failures >= threshold:
                state.state = "open"
            return ActionResult(
                success=False,
                message=f"Call failed: {e}",
                data={'state': state.state, 'failures': state.failures}
            )
