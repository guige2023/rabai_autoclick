"""
API Circuit Breaker Action - Circuit breaker pattern for API resilience.

This module provides circuit breaker capabilities for
preventing cascading failures and enabling graceful degradation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    timeout: float = 60.0
    half_open_attempts: int = 3
    success_threshold: int = 2


class CircuitBreaker:
    """Circuit breaker for API calls."""
    
    def __init__(self, config: CircuitConfig | None = None) -> None:
        self.config = config or CircuitConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: float | None = None
        self.half_open_attempts = 0
    
    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.half_open_attempts = 0
        elif self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN
    
    def can_execute(self) -> bool:
        """Check if request can be executed."""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.config.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_attempts = 0
                    return True
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_attempts < self.config.half_open_attempts:
                self.half_open_attempts += 1
                return True
            return False
        
        return True
    
    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self.state


class APICircuitBreakerAction:
    """API circuit breaker action for automation workflows."""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0) -> None:
        self.config = CircuitConfig(failure_threshold=failure_threshold, timeout=timeout)
        self.circuit_breaker = CircuitBreaker(self.config)
    
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.circuit_breaker.get_state() == CircuitState.OPEN
    
    def record_success(self) -> None:
        """Record successful call."""
        self.circuit_breaker.record_success()
    
    def record_failure(self) -> None:
        """Record failed call."""
        self.circuit_breaker.record_failure()
    
    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        return self.circuit_breaker.can_execute()


__all__ = ["CircuitState", "CircuitConfig", "CircuitBreaker", "APICircuitBreakerAction"]
