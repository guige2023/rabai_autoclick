"""API Fault Tolerance Action Module.

Provides fault tolerance mechanisms for API operations including:
- Circuit breaker pattern implementation
- Bulkhead isolation
- Fallback mechanisms
- Graceful degradation

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()      # Normal operation
    OPEN = auto()        # Failing fast
    HALF_OPEN = auto()   # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3


@dataclass
class CircuitMetrics:
    """Metrics for circuit breaker monitoring."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None


class CircuitBreaker:
    """Circuit breaker implementation for API fault tolerance.
    
    Prevents cascading failures by opening the circuit when
    a service is experiencing issues.
    """
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0
        self.last_failure_time: Optional[float] = None
        self.metrics = CircuitMetrics()
        self._lock = asyncio.Lock()
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function through the circuit breaker.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result of the function call
            
        Raises:
            CircuitOpenError: When circuit is open
            Exception: Original exception from failed call
        """
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    await self._transition_to_half_open()
                else:
                    self.metrics.rejected_calls += 1
                    raise CircuitOpenError(f"Circuit '{self.name}' is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return (time.time() - self.last_failure_time) >= self.config.timeout_seconds
    
    async def _transition_to_half_open(self) -> None:
        """Transition circuit to half-open state."""
        self.state = CircuitState.HALF_OPEN
        self.half_open_calls = 0
        self.metrics.state_changes += 1
        logger.info(f"Circuit '{self.name}' transitioning to HALF_OPEN")
    
    async def _on_success(self) -> None:
        """Handle successful call."""
        async with self._lock:
            self.metrics.total_calls += 1
            self.metrics.successful_calls += 1
            self.metrics.last_success_time = time.time()
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                self.half_open_calls += 1
                if self.success_count >= self.config.success_threshold:
                    await self._transition_to_closed()
            elif self.state == CircuitState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)
    
    async def _on_failure(self) -> None:
        """Handle failed call."""
        async with self._lock:
            self.metrics.total_calls += 1
            self.metrics.failed_calls += 1
            self.metrics.last_failure_time = time.time()
            self.last_failure_time = time.time()
            
            if self.state == CircuitState.HALF_OPEN:
                await self._transition_to_open()
            elif self.state == CircuitState.CLOSED:
                self.failure_count += 1
                self.success_count = 0
                if self.failure_count >= self.config.failure_threshold:
                    await self._transition_to_open()
    
    async def _transition_to_open(self) -> None:
        """Transition circuit to open state."""
        self.state = CircuitState.OPEN
        self.metrics.state_changes += 1
        logger.warning(f"Circuit '{self.name}' transitioning to OPEN")
    
    async def _transition_to_closed(self) -> None:
        """Transition circuit to closed state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.metrics.state_changes += 1
        logger.info(f"Circuit '{self.name}' transitioning to CLOSED")
    
    def get_health_status(self) -> dict[str, Any]:
        """Get current health status of the circuit breaker."""
        return {
            "name": self.name,
            "state": self.state.name,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "rejected_calls": self.metrics.rejected_calls,
            }
        }


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class BulkheadIsolation:
    """Bulkhead isolation pattern implementation.
    
    Limits concurrent executions to prevent resource exhaustion.
    """
    
    def __init__(self, max_concurrent: int = 10, max_queue_size: int = 100):
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._active_count = 0
        self._lock = asyncio.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with bulkhead isolation.
        
        Args:
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of the function
            
        Raises:
            BulkheadFullError: When bulkhead capacity is exceeded
        """
        async with self._lock:
            if self._semaphore is None:
                self._semaphore = asyncio.Semaphore(self.max_concurrent)
            if self._active_count >= self.max_concurrent:
                raise BulkheadFullError("Bulkhead capacity exceeded")
            self._active_count += 1
        
        try:
            async with self._semaphore:
                return await func(*args, **kwargs)
        finally:
            async with self._lock:
                self._active_count -= 1


class BulkheadFullError(Exception):
    """Raised when bulkhead capacity is exceeded."""
    pass


class FaultToleranceManager:
    """Manages fault tolerance across multiple services."""
    
    def __init__(self):
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._bulkheads: dict[str, BulkheadIsolation] = {}
        self._fallbacks: dict[str, Callable] = {}
        self._lock = asyncio.Lock()
    
    def get_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(name, config)
        return self._circuit_breakers[name]
    
    def get_bulkhead(self, name: str, max_concurrent: int = 10) -> BulkheadIsolation:
        """Get or create a bulkhead."""
        if name not in self._bulkheads:
            self._bulkheads[name] = BulkheadIsolation(max_concurrent)
        return self._bulkheads[name]
    
    def register_fallback(self, service: str, fallback: Callable) -> None:
        """Register a fallback function for a service."""
        self._fallbacks[service] = fallback
    
    async def execute_with_fault_tolerance(
        self,
        service: str,
        func: Callable,
        use_circuit_breaker: bool = True,
        use_bulkhead: bool = True,
        *args,
        **kwargs
    ) -> Any:
        """Execute function with all fault tolerance mechanisms.
        
        Args:
            service: Service name for circuit breaker identification
            func: Function to execute
            use_circuit_breaker: Whether to use circuit breaker
            use_bulkhead: Whether to use bulkhead isolation
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of the function
        """
        fallback = self._fallbacks.get(service)
        
        try:
            if use_circuit_breaker:
                cb = self.get_circuit_breaker(service)
                func = cb.call
            
            if use_bulkhead:
                bulkhead = self.get_bulkhead(service)
                async def wrapped():
                    return await func(*args, **kwargs)
                return await bulkhead.execute(wrapped)
            
            return await func(*args, **kwargs)
            
        except CircuitOpenError:
            logger.warning(f"Circuit open for service '{service}', attempting fallback")
            if fallback:
                return await fallback(*args, **kwargs)
            raise
        except BulkheadFullError:
            logger.error(f"Bulkhead full for service '{service}'")
            if fallback:
                return await fallback(*args, **kwargs)
            raise
        except Exception as e:
            logger.error(f"Fault tolerance error for service '{service}': {e}")
            if fallback:
                return await fallback(*args, **kwargs)
            raise
    
    def get_all_health_status(self) -> dict[str, dict[str, Any]]:
        """Get health status of all managed circuits."""
        return {
            name: cb.get_health_status()
            for name, cb in self._circuit_breakers.items()
        }
