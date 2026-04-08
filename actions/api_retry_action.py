"""API Retry Action.

Implements configurable retry logic for API calls with exponential backoff,
jitter, circuit breaker pattern, and status code-based retry decisions.
"""

import sys
import os
import time
import random
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRetryAction(BaseAction):
    """Retry failed API calls with exponential backoff and jitter.
    
    Supports configurable retry count, base delay, max delay, jitter,
    retryable status codes, and circuit breaker integration.
    """
    action_type = "api_retry"
    display_name = "API重试"
    description = "对失败的API调用进行指数退避重试，支持抖动和熔断"

    DEFAULT_RETRYABLE_CODES = {408, 429, 500, 502, 503, 504}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API call with retry logic.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - api_call_fn: Callable that performs the API call.
                - max_retries: Max retry attempts (default: 3).
                - base_delay: Initial delay in seconds (default: 1.0).
                - max_delay: Max delay cap in seconds (default: 60.0).
                - exponential_base: Exponential base (default: 2.0).
                - jitter: Add random jitter (default: True).
                - retryable_codes: Set of HTTP status codes to retry.
                - retryable_exceptions: List of exception types to retry.
                - save_to_var: Variable to store result.
                - on_retry: Callback function called before each retry (receives attempt count and error).
        
        Returns:
            ActionResult with API call result or final error.
        """
        try:
            api_call_fn = params.get('api_call_fn')
            max_retries = params.get('max_retries', 3)
            base_delay = params.get('base_delay', 1.0)
            max_delay = params.get('max_delay', 60.0)
            exponential_base = params.get('exponential_base', 2.0)
            jitter = params.get('jitter', True)
            retryable_codes = params.get('retryable_codes', self.DEFAULT_RETRYABLE_CODES)
            retryable_exceptions = params.get('retryable_exceptions', [Exception])
            save_to_var = params.get('save_to_var', 'retry_result')
            on_retry = params.get('on_retry', None)

            if api_call_fn is None:
                return ActionResult(success=False, message="api_call_fn is required")

            last_error = None
            attempt = 0

            while attempt <= max_retries:
                try:
                    result = api_call_fn()
                    context.set_variable(save_to_var, result)
                    if attempt > 0:
                        return ActionResult(success=True, data=result, 
                                           message=f"Success on attempt {attempt + 1}")
                    return ActionResult(success=True, data=result, message="Success on first attempt")
                except Exception as e:
                    last_error = e
                    # Check if retryable
                    if not self._is_retryable(e, retryable_codes, retryable_exceptions):
                        return ActionResult(success=False, message=f"Non-retryable error: {e}")

                    if attempt >= max_retries:
                        break

                    # Calculate delay
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    if jitter:
                        delay = delay * (0.5 + random.random())

                    if on_retry:
                        try:
                            on_retry(attempt + 1, str(e), delay)
                        except Exception:
                            pass

                    time.sleep(delay)
                    attempt += 1

            return ActionResult(success=False, message=f"All retries exhausted: {last_error}")

        except Exception as e:
            return ActionResult(success=False, message=f"Retry setup error: {e}")

    def _is_retryable(self, error: Exception, codes: set, exception_types: List) -> bool:
        """Check if an error is retryable."""
        if not exception_types:
            return False
        for exc_type in exception_types:
            if isinstance(error, exc_type):
                # Check if it's a status code error with retryable code
                if hasattr(error, 'response') and hasattr(error.response, 'status_code'):
                    return error.response.status_code in codes
                return True
        return False


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """Simple circuit breaker implementation."""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = 'closed'  # closed, open, half_open

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == 'open':
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = 'half_open'
            else:
                raise CircuitBreakerOpen("Circuit breaker is open")

        try:
            result = fn(*args, **kwargs)
            if self.state == 'half_open':
                self.state = 'closed'
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'
            raise e

    def reset(self):
        """Reset circuit breaker to closed state."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'
