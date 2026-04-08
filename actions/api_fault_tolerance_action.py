"""
API Fault Tolerance Action Module

Provides circuit breaker, retry policies, and fault tolerance patterns for APIs.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import random


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class RetryStrategy(Enum):
    """Retry strategy."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on: tuple[type, ...] = (Exception,)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_requests: int = 3
    excluded_exceptions: tuple[type, ...] = ()


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state: CircuitState = CircuitState.CLOSED
    last_failure: Optional[datetime] = None
    last_success: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


@dataclass
class FaultToleranceResult:
    """Result of fault tolerance operation."""
    success: bool
    result: Any = None
    error: Optional[str] = None
    attempts: int = 0
    circuit_state: CircuitState = CircuitState.CLOSED
    duration_ms: float = 0


class CircuitBreaker:
    """Circuit breaker implementation."""
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self._last_state_change = datetime.now()
        self._half_open_calls = 0
    
    def _should_record_failure(self, exception: Exception) -> bool:
        """Check if this failure should be recorded."""
        for exc_type in self.config.excluded_exceptions:
            if isinstance(exception, exc_type):
                return False
        return True
    
    def record_success(self):
        """Record a successful call."""
        self.stats.successful_calls += 1
        self.stats.consecutive_successes += 1
        self.stats.consecutive_failures = 0
        self.stats.last_success = datetime.now()
        
        if self.state == CircuitState.HALF_OPEN:
            if self.stats.consecutive_successes >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)
    
    def record_failure(self, exception: Exception):
        """Record a failed call."""
        if not self._should_record_failure(exception):
            return
        
        self.stats.failed_calls += 1
        self.stats.consecutive_failures += 1
        self.stats.consecutive_successes = 0
        self.stats.last_failure = datetime.now()
        
        if self.state == CircuitState.CLOSED:
            if self.stats.consecutive_failures >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
        
        elif self.state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.OPEN)
    
    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state."""
        old_state = self.state
        self.state = new_state
        self._last_state_change = datetime.now()
        self.stats.state = new_state
        
        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self.stats.consecutive_successes = 0
            self.stats.consecutive_failures = 0
        
        if new_state == CircuitState.CLOSED:
            self.stats.consecutive_failures = 0
    
    def can_execute(self) -> bool:
        """Check if a request can be executed."""
        self.stats.total_calls += 1
        
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            elapsed = (datetime.now() - self._last_state_change).total_seconds()
            if elapsed >= self.config.timeout_seconds:
                self._transition_to(CircuitState.HALF_OPEN)
                return True
            self.stats.rejected_calls += 1
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_requests:
                self._half_open_calls += 1
                return True
            self.stats.rejected_calls += 1
            return False
        
        return False
    
    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "total_calls": self.stats.total_calls,
            "successful_calls": self.stats.successful_calls,
            "failed_calls": self.stats.failed_calls,
            "rejected_calls": self.stats.rejected_calls,
            "consecutive_failures": self.stats.consecutive_failures,
            "consecutive_successes": self.stats.consecutive_successes,
            "last_failure": self.stats.last_failure.isoformat() if self.stats.last_failure else None,
            "last_success": self.stats.last_success.isoformat() if self.stats.last_success else None
        }


class ApiFaultToleranceAction:
    """Main fault tolerance action handler."""
    
    def __init__(self):
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._default_retry_config = RetryConfig()
        self._default_circuit_config = CircuitBreakerConfig()
    
    def get_circuit_breaker(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(name, config)
        return self._circuit_breakers[name]
    
    async def execute_with_retry(
        self,
        operation: Callable[[], Awaitable[Any]],
        config: Optional[RetryConfig] = None
    ) -> FaultToleranceResult:
        """
        Execute operation with retry logic.
        
        Args:
            operation: Async operation to execute
            config: Retry configuration
            
        Returns:
            FaultToleranceResult with outcome
        """
        cfg = config or self._default_retry_config
        start_time = datetime.now()
        last_error = None
        attempt = 0
        
        while attempt < cfg.max_attempts:
            attempt += 1
            
            try:
                result = await operation()
                
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                return FaultToleranceResult(
                    success=True,
                    result=result,
                    attempts=attempt,
                    duration_ms=duration_ms
                )
                
            except Exception as e:
                last_error = e
                
                # Check if we should retry
                should_retry = any(isinstance(e, exc_type) for exc_type in cfg.retry_on)
                
                if not should_retry or attempt >= cfg.max_attempts:
                    break
                
                # Calculate delay
                delay = self._calculate_delay(attempt, cfg)
                
                # Add jitter
                if cfg.jitter:
                    jitter_amount = delay * cfg.jitter_factor
                    delay += random.uniform(-jitter_amount, jitter_amount)
                
                await asyncio.sleep(delay)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return FaultToleranceResult(
            success=False,
            error=str(last_error),
            attempts=attempt,
            duration_ms=duration_ms
        )
    
    async def execute_with_circuit_breaker(
        self,
        operation: Callable[[], Awaitable[Any]],
        circuit_name: str,
        circuit_config: Optional[CircuitBreakerConfig] = None
    ) -> FaultToleranceResult:
        """
        Execute operation with circuit breaker.
        
        Args:
            operation: Async operation to execute
            circuit_name: Name of circuit breaker
            circuit_config: Circuit breaker configuration
            
        Returns:
            FaultToleranceResult with outcome
        """
        breaker = self.get_circuit_breaker(circuit_name, circuit_config)
        start_time = datetime.now()
        
        if not breaker.can_execute():
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return FaultToleranceResult(
                success=False,
                error=f"Circuit breaker {circuit_name} is {breaker.state.value}",
                circuit_state=breaker.state,
                duration_ms=duration_ms
            )
        
        try:
            result = await operation()
            breaker.record_success()
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return FaultToleranceResult(
                success=True,
                result=result,
                circuit_state=breaker.state,
                duration_ms=duration_ms
            )
            
        except Exception as e:
            breaker.record_failure(e)
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return FaultToleranceResult(
                success=False,
                error=str(e),
                circuit_state=breaker.state,
                duration_ms=duration_ms
            )
    
    async def execute_with_fault_tolerance(
        self,
        operation: Callable[[], Awaitable[Any]],
        circuit_name: str,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None
    ) -> FaultToleranceResult:
        """
        Execute operation with both circuit breaker and retry.
        
        Circuit breaker is checked before retry logic.
        """
        breaker = self.get_circuit_breaker(circuit_name, circuit_config)
        cfg = retry_config or self._default_retry_config
        start_time = datetime.now()
        last_error = None
        attempt = 0
        
        while attempt < cfg.max_attempts:
            if not breaker.can_execute():
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                return FaultToleranceResult(
                    success=False,
                    error=f"Circuit breaker {circuit_name} is {breaker.state.value}",
                    attempts=attempt,
                    circuit_state=breaker.state,
                    duration_ms=duration_ms
                )
            
            attempt += 1
            
            try:
                result = await operation()
                breaker.record_success()
                
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                return FaultToleranceResult(
                    success=True,
                    result=result,
                    attempts=attempt,
                    circuit_state=breaker.state,
                    duration_ms=duration_ms
                )
                
            except Exception as e:
                last_error = e
                breaker.record_failure(e)
                
                should_retry = any(isinstance(e, exc_type) for exc_type in cfg.retry_on)
                
                if not should_retry or attempt >= cfg.max_attempts:
                    break
                
                delay = self._calculate_delay(attempt, cfg)
                if cfg.jitter:
                    delay += random.uniform(-delay * cfg.jitter_factor, delay * cfg.jitter_factor)
                
                await asyncio.sleep(delay)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return FaultToleranceResult(
            success=False,
            error=str(last_error),
            attempts=attempt,
            circuit_state=breaker.state,
            duration_ms=duration_ms
        )
    
    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """Calculate delay for retry attempt."""
        if config.strategy == RetryStrategy.FIXED:
            delay = config.initial_delay
        
        elif config.strategy == RetryStrategy.EXPONENTIAL:
            delay = config.initial_delay * (2 ** (attempt - 1))
        
        elif config.strategy == RetryStrategy.LINEAR:
            delay = config.initial_delay * attempt
        
        elif config.strategy == RetryStrategy.FIBONACCI:
            # Fibonacci sequence
            a, b = 1, 1
            for _ in range(attempt - 1):
                a, b = b, a + b
            delay = config.initial_delay * a
        
        else:
            delay = config.initial_delay
        
        return min(delay, config.max_delay)
    
    async def get_circuit_breaker_stats(self, name: str) -> Optional[dict[str, Any]]:
        """Get statistics for a circuit breaker."""
        breaker = self._circuit_breakers.get(name)
        if breaker:
            return breaker.get_stats()
        return None
    
    async def list_circuit_breakers(self) -> dict[str, dict[str, Any]]:
        """List all circuit breakers and their stats."""
        return {
            name: breaker.get_stats()
            for name, breaker in self._circuit_breakers.items()
        }
    
    async def reset_circuit_breaker(self, name: str) -> bool:
        """Manually reset a circuit breaker."""
        if name in self._circuit_breakers:
            breaker = self._circuit_breakers[name]
            breaker._transition_to(CircuitState.CLOSED)
            return True
        return False
    
    async def execute_batch_with_fault_tolerance(
        self,
        operations: list[tuple[str, Callable[[], Awaitable[Any]]]],
        circuit_prefix: str = "batch"
    ) -> dict[str, FaultToleranceResult]:
        """
        Execute multiple operations with fault tolerance.
        
        Each operation gets its own circuit breaker named {prefix}:{index}
        """
        results = {}
        tasks = []
        
        for i, (name, op) in enumerate(operations):
            circuit_name = f"{circuit_prefix}:{i}:{name}"
            task = self.execute_with_fault_tolerance(op, circuit_name)
            tasks.append(task)
        
        # Execute all in parallel
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, outcome in enumerate(outcomes):
            if isinstance(outcome, Exception):
                results[operations[i][0]] = FaultToleranceResult(
                    success=False,
                    error=str(outcome)
                )
            else:
                results[operations[i][0]] = outcome
        
        return results
