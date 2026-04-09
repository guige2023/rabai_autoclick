"""
Automation Retry Policy Action Module.

Implements configurable retry policies with exponential backoff,
jitter, and circuit breaker patterns for automation workflows.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import random
import threading
from datetime import datetime, timedelta


class RetryStrategy(Enum):
    """Retry strategies."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    FIBONACCI_BACKOFF = "fibonacci_backoff"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class RetryExhausted(Exception):
    """Raised when all retry attempts are exhausted."""
    
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"Retry exhausted after {attempts} attempts: {last_error}")


class CircuitOpen(Exception):
    """Raised when circuit breaker is open."""
    
    def __init__(self, circuit_name: str, retry_after: float):
        self.circuit_name = circuit_name
        self.retry_after = retry_after
        super().__init__(f"Circuit '{circuit_name}' is open. Retry after {retry_after:.2f}s")


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    
    # Circuit breaker config
    circuit_enabled: bool = False
    circuit_threshold: int = 5
    circuit_timeout: float = 30.0


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    attempts: int
    total_duration: float
    last_error: Optional[Exception] = None
    circuit_state: CircuitState = CircuitState.CLOSED


class BackoffCalculator:
    """Calculates delay between retry attempts."""
    
    @staticmethod
    def exponential(
        attempt: int,
        initial_delay: float,
        multiplier: float,
        max_delay: float
    ) -> float:
        """Exponential backoff calculation."""
        delay = initial_delay * (multiplier ** attempt)
        return min(delay, max_delay)
    
    @staticmethod
    def linear(attempt: int, delay: float) -> float:
        """Linear backoff calculation."""
        return delay * (attempt + 1)
    
    @staticmethod
    def fibonacci(attempt: int, initial_delay: float, max_delay: float) -> float:
        """Fibonacci backoff calculation."""
        a, b = 1, 1
        for _ in range(attempt):
            a, b = b, a + b
        delay = initial_delay * a
        return min(delay, max_delay)
    
    @staticmethod
    def with_jitter(delay: float, factor: float = 0.1) -> float:
        """Add random jitter to delay."""
        jitter_range = delay * factor
        return delay + random.uniform(-jitter_range, jitter_range)


class CircuitBreaker:
    """
    Circuit breaker implementation for preventing cascading failures.
    
    Example:
        circuit = CircuitBreaker(name="api", threshold=5, timeout=30)
        try:
            result = circuit.call(api_request)
        except CircuitOpen:
            wait_for_recovery()
    """
    
    def __init__(
        self,
        name: str,
        threshold: int = 5,
        timeout: float = 30.0
    ):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        
        self._lock = threading.RLock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                else:
                    retry_after = self._get_retry_after()
                    raise CircuitOpen(self.name, retry_after)
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            self.success_count += 1
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.monotonic()
            
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
            elif self.failure_count >= self.threshold:
                self.state = CircuitState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset."""
        if self.last_failure_time is None:
            return True
        elapsed = time.monotonic() - self.last_failure_time
        return elapsed >= self.timeout
    
    def _get_retry_after(self) -> float:
        """Get time until circuit might close."""
        if self.last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self.last_failure_time
        return max(0.0, self.timeout - elapsed)
    
    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self.state


class RetryPolicy:
    """
    Configurable retry policy with backoff and circuit breaker.
    
    Example:
        policy = RetryPolicy(config=RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            circuit_enabled=True
        ))
        
        result = policy.execute(lambda: api.call())
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.circuit: Optional[CircuitBreaker] = None
        
        if self.config.circuit_enabled:
            self.circuit = CircuitBreaker(
                name="default",
                threshold=self.config.circuit_threshold,
                timeout=self.config.circuit_timeout
            )
    
    def execute(self, func: Callable[[], Any]) -> RetryResult:
        """Execute function with retry policy."""
        start_time = time.monotonic()
        last_error: Optional[Exception] = None
        
        for attempt in range(self.config.max_attempts):
            try:
                if self.circuit:
                    result = self.circuit.call(func)
                else:
                    result = func()
                
                duration = time.monotonic() - start_time
                return RetryResult(
                    success=True,
                    attempts=attempt + 1,
                    total_duration=duration,
                    circuit_state=self.circuit.get_state() if self.circuit else CircuitState.CLOSED
                )
                
            except CircuitOpen:
                raise
            except Exception as e:
                last_error = e
                
                if attempt < self.config.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    time.sleep(delay)
        
        duration = time.monotonic() - start_time
        return RetryResult(
            success=False,
            attempts=self.config.max_attempts,
            total_duration=duration,
            last_error=last_error,
            circuit_state=self.circuit.get_state() if self.circuit else CircuitState.CLOSED
        )
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for this attempt."""
        if self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = BackoffCalculator.exponential(
                attempt,
                self.config.initial_delay,
                self.config.multiplier,
                self.config.max_delay
            )
        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = BackoffCalculator.linear(attempt, self.config.initial_delay)
        elif self.config.strategy == RetryStrategy.FIBONACCI_BACKOFF:
            delay = BackoffCalculator.fibonacci(
                attempt,
                self.config.initial_delay,
                self.config.max_delay
            )
        else:
            delay = self.config.initial_delay
        
        if self.config.jitter:
            delay = BackoffCalculator.with_jitter(delay, self.config.jitter_factor)
        
        return max(0, delay)


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationRetryPolicyAction(BaseAction):
    """
    Retry policy action for automation workflows.
    
    Parameters:
        strategy: Backoff strategy (exponential/linear/fixed/fibonacci)
        max_attempts: Maximum retry attempts
        initial_delay: Initial delay in seconds
        multiplier: Backoff multiplier
        circuit_enabled: Enable circuit breaker
    
    Example:
        action = AutomationRetryPolicyAction()
        result = action.execute({}, {
            "strategy": "exponential_backoff",
            "max_attempts": 3,
            "initial_delay": 1.0,
            "circuit_enabled": True
        })
    """
    
    _policies: Dict[str, RetryPolicy] = {}
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute retry policy."""
        policy_name = params.get("policy_name", "default")
        strategy_str = params.get("strategy", "exponential_backoff")
        max_attempts = params.get("max_attempts", 3)
        initial_delay = params.get("initial_delay", 1.0)
        max_delay = params.get("max_delay", 60.0)
        multiplier = params.get("multiplier", 2.0)
        jitter = params.get("jitter", True)
        circuit_enabled = params.get("circuit_enabled", False)
        
        strategy = RetryStrategy(strategy_str)
        
        config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            multiplier=multiplier,
            jitter=jitter,
            strategy=strategy,
            circuit_enabled=circuit_enabled
        )
        
        with self._lock:
            if policy_name not in self._policies:
                self._policies[policy_name] = RetryPolicy(config)
            policy = self._policies[policy_name]
        
        return {
            "policy_name": policy_name,
            "strategy": strategy_str,
            "max_attempts": max_attempts,
            "circuit_enabled": circuit_enabled,
            "configured_at": datetime.now().isoformat()
        }
