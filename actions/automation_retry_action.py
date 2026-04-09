"""Automation Retry Action Module.

Provides retry logic utilities: backoff strategies, retry policies,
circuit breaker integration, and timeout handling.

Example:
    result = execute(context, {"action": "retry_with_backoff", "fn": my_func})
"""
from typing import Any, Callable, Optional, TypeVar, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
import random


T = TypeVar("T")


class BackoffStrategy:
    """Base class for backoff strategies."""
    
    def get_delay(self, attempt: int) -> float:
        """Get delay in seconds for given attempt.
        
        Args:
            attempt: Zero-based attempt number
            
        Returns:
            Delay in seconds
        """
        raise NotImplementedError


class ConstantBackoff(BackoffStrategy):
    """Constant backoff - same delay between attempts."""
    
    def __init__(self, delay: float = 1.0) -> None:
        """Initialize constant backoff.
        
        Args:
            delay: Fixed delay in seconds
        """
        self.delay = delay
    
    def get_delay(self, attempt: int) -> float:
        """Get constant delay."""
        return self.delay


class LinearBackoff(BackoffStrategy):
    """Linear backoff - delay increases linearly."""
    
    def __init__(self, base_delay: float = 1.0, increment: float = 1.0) -> None:
        """Initialize linear backoff.
        
        Args:
            base_delay: Initial delay in seconds
            increment: Delay increment per attempt
        """
        self.base_delay = base_delay
        self.increment = increment
    
    def get_delay(self, attempt: int) -> float:
        """Get linearly increasing delay."""
        return self.base_delay + attempt * self.increment


class ExponentialBackoff(BackoffStrategy):
    """Exponential backoff - delay doubles each attempt."""
    
    def __init__(
        self,
        base_delay: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
    ) -> None:
        """Initialize exponential backoff.
        
        Args:
            base_delay: Initial delay in seconds
            multiplier: Multiplier per attempt
            max_delay: Maximum delay cap
        """
        self.base_delay = base_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
    
    def get_delay(self, attempt: int) -> float:
        """Get exponentially increasing delay."""
        delay = self.base_delay * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


class JitteredBackoff(BackoffStrategy):
    """Exponential backoff with jitter for distributed systems."""
    
    def __init__(
        self,
        base_delay: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter_factor: float = 0.5,
    ) -> None:
        """Initialize jittered backoff.
        
        Args:
            base_delay: Initial delay in seconds
            multiplier: Multiplier per attempt
            max_delay: Maximum delay cap
            jitter_factor: Random factor (0.0 to 1.0)
        """
        self.base_delay = base_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter_factor = jitter_factor
    
    def get_delay(self, attempt: int) -> float:
        """Get exponentially increasing delay with jitter."""
        base = self.base_delay * (self.multiplier ** attempt)
        capped = min(base, self.max_delay)
        
        jitter_range = capped * self.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        
        return max(0, capped + jitter)


class RetryPolicy:
    """Retry policy with configurable behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
        retry_on: Optional[tuple[type[Exception], ...]] = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize retry policy.
        
        Args:
            max_attempts: Maximum retry attempts
            backoff: Backoff strategy
            retry_on: Tuple of exception types to retry
            timeout: Overall timeout in seconds
        """
        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialBackoff()
        self.retry_on = retry_on or (Exception,)
        self.timeout = timeout
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Check if should retry after exception.
        
        Args:
            attempt: Current attempt number
            exception: Exception that occurred
            
        Returns:
            True if should retry
        """
        if attempt >= self.max_attempts:
            return False
        
        return isinstance(exception, self.retry_on)
    
    def get_delay(self, attempt: int) -> float:
        """Get delay before next attempt."""
        return self.backoff.get_delay(attempt)


class RetryExecutor:
    """Executes functions with retry logic."""
    
    def __init__(self, policy: Optional[RetryPolicy] = None) -> None:
        """Initialize retry executor.
        
        Args:
            policy: Retry policy to use
        """
        self.policy = policy or RetryPolicy()
        self._attempts: list[dict[str, Any]] = []
    
    def execute(self, fn: Callable[[], T]) -> T:
        """Execute function with retry logic.
        
        Args:
            fn: Function to execute
            
        Returns:
            Function result
            
        Raises:
            Exception: If all retries exhausted
        """
        start_time = time.time()
        last_exception: Optional[Exception] = None
        
        for attempt in range(self.policy.max_attempts):
            elapsed = time.time() - start_time
            if elapsed >= self.policy.timeout:
                raise TimeoutError(f"Retry timeout after {elapsed:.2f}s")
            
            try:
                result = fn()
                self._attempts.append({
                    "attempt": attempt,
                    "status": "success",
                    "elapsed_ms": (time.time() - start_time) * 1000,
                })
                return result
            
            except Exception as e:
                last_exception = e
                
                self._attempts.append({
                    "attempt": attempt,
                    "status": "failed",
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "elapsed_ms": (time.time() - start_time) * 1000,
                })
                
                if not self.policy.should_retry(attempt, e):
                    raise
                
                delay = self.policy.get_delay(attempt)
                time.sleep(delay)
        
        if last_exception is not None:
            raise last_exception
        
        raise RuntimeError("Retry exhausted")
    
    def get_attempts(self) -> list[dict[str, Any]]:
        """Get retry attempt history."""
        return self._attempts.copy()
    
    def clear_attempts(self) -> None:
        """Clear attempt history."""
        self._attempts.clear()


@dataclass
class RetryConfig:
    """Retry configuration for actions."""
    
    max_attempts: int = 3
    initial_delay: float = 1.0
    backoff_type: str = "exponential"
    retry_on: list[str] = None
    
    def __post_init__(self) -> None:
        """Initialize defaults."""
        if self.retry_on is None:
            self.retry_on = ["Exception"]
    
    def get_backoff(self) -> BackoffStrategy:
        """Get backoff strategy."""
        if self.backoff_type == "constant":
            return ConstantBackoff(self.initial_delay)
        elif self.backoff_type == "linear":
            return LinearBackoff(self.initial_delay)
        elif self.backoff_type == "jitter":
            return JitteredBackoff(self.initial_delay)
        else:
            return ExponentialBackoff(self.initial_delay)


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute automation retry action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "backoff_delay":
        strategy_name = params.get("strategy", "exponential")
        attempt = params.get("attempt", 0)
        
        if strategy_name == "constant":
            strategy = ConstantBackoff(params.get("delay", 1.0))
        elif strategy_name == "linear":
            strategy = LinearBackoff(params.get("base_delay", 1.0))
        elif strategy_name == "jitter":
            strategy = JitteredBackoff(params.get("base_delay", 1.0))
        else:
            strategy = ExponentialBackoff(params.get("base_delay", 1.0))
        
        delay = strategy.get_delay(attempt)
        result["data"] = {"delay_seconds": delay}
    
    elif action == "create_policy":
        config = RetryConfig(
            max_attempts=params.get("max_attempts", 3),
            initial_delay=params.get("initial_delay", 1.0),
            backoff_type=params.get("backoff_type", "exponential"),
        )
        policy = RetryPolicy(
            max_attempts=config.max_attempts,
            backoff=config.get_backoff(),
        )
        result["data"] = {
            "max_attempts": policy.max_attempts,
            "backoff_type": config.backoff_type,
        }
    
    elif action == "should_retry":
        config = RetryConfig(
            max_attempts=params.get("max_attempts", 3),
        )
        policy = RetryPolicy(max_attempts=config.max_attempts)
        should_retry = policy.should_retry(
            params.get("attempt", 0),
            ValueError(params.get("error", "")),
        )
        result["data"] = {"should_retry": should_retry}
    
    elif action == "retry_status":
        executor = RetryExecutor()
        result["data"] = {"policy": "configured"}
    
    elif action == "attempt_history":
        executor = RetryExecutor()
        result["data"] = {"attempts": executor.get_attempts()}
    
    elif action == "validate_config":
        config = RetryConfig(
            max_attempts=params.get("max_attempts", 3),
            initial_delay=params.get("initial_delay", 1.0),
            backoff_type=params.get("backoff_type", "exponential"),
        )
        errors = []
        
        if config.max_attempts < 1:
            errors.append("max_attempts must be >= 1")
        if config.initial_delay < 0:
            errors.append("initial_delay must be >= 0")
        if config.backoff_type not in ("constant", "linear", "exponential", "jitter"):
            errors.append(f"Unknown backoff_type: {config.backoff_type}")
        
        result["data"] = {
            "valid": len(errors) == 0,
            "errors": errors,
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
