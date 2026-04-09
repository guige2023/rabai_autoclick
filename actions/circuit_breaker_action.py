"""
Circuit Breaker Action Module.

Implements the circuit breaker pattern to prevent cascading failures
and provide fault tolerance for external service calls.
"""

from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import time
import logging
import threading

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitMetrics:
    """Metrics tracked by circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state_change_times: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 2          # Successes in half-open to close
    timeout: float = 60.0               # Seconds before trying half-open
    excluded_exceptions: tuple = ()     # Exceptions that don't count as failures
    half_open_max_calls: int = 3        # Max test calls in half-open


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.
    
    Example:
        breaker = CircuitBreaker(failure_threshold=5, timeout=30)
        
        @breaker
        def call_external_service():
            return requests.get("https://api.example.com/data")
            
        result = breaker.call(call_external_service)
    """
    
    def __init__(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
        state_change_callback: Optional[Callable[[str, CircuitState, CircuitState], None]] = None,
    ):
        self.name = name
        self.config = config or CircuitConfig()
        self.state_change_callback = state_change_callback
        
        self._state = CircuitState.CLOSED
        self._metrics = CircuitMetrics()
        self._lock = threading.RLock()
        self._last_state_change = time.time()
        self._half_open_calls = 0
        
    @property
    def state(self) -> CircuitState:
        """Get current circuit state, checking for timeout transition."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                elapsed = time.time() - self._last_state_change
                if elapsed >= self.config.timeout:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state
            
    @property
    def metrics(self) -> CircuitMetrics:
        """Get current circuit metrics."""
        with self._lock:
            return CircuitMetrics(
                total_calls=self._metrics.total_calls,
                successful_calls=self._metrics.successful_calls,
                failed_calls=self._metrics.failed_calls,
                rejected_calls=self._metrics.rejected_calls,
                consecutive_failures=self._metrics.consecutive_failures,
                consecutive_successes=self._metrics.consecutive_successes,
                last_failure_time=self._metrics.last_failure_time,
                last_success_time=self._metrics.last_success_time,
                state_change_times=self._metrics.state_change_times.copy(),
            )
            
    def _transition_to(self, new_state: CircuitState) -> None:
        """Internal state transition with callback."""
        old_state = self._state
        if old_state == new_state:
            return
            
        self._state = new_state
        self._last_state_change = time.time()
        
        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            
        self._metrics.state_change_times.append({
            "timestamp": self._last_state_change,
            "from": old_state.value,
            "to": new_state.value,
        })
        
        logger.info(f"Circuit {self.name}: {old_state.value} -> {new_state.value}")
        
        if self.state_change_callback:
            try:
                self.state_change_callback(self.name, old_state, new_state)
            except Exception as e:
                logger.error(f"Circuit callback error: {e}")
                
    def _record_success(self) -> None:
        """Record a successful call."""
        self._metrics.total_calls += 1
        self._metrics.successful_calls += 1
        self._metrics.consecutive_successes += 1
        self._metrics.consecutive_failures = 0
        self._metrics.last_success_time = time.time()
        
    def _record_failure(self) -> None:
        """Record a failed call."""
        self._metrics.total_calls += 1
        self._metrics.failed_calls += 1
        self._metrics.consecutive_failures += 1
        self._metrics.consecutive_successes = 0
        self._metrics.last_failure_time = time.time()
        
    def _record_rejection(self) -> None:
        """Record a rejected call."""
        self._metrics.rejected_calls += 1
        
    def call(
        self,
        func: Callable,
        *args,
        fallback: Optional[Callable] = None,
        **kwargs,
    ) -> Any:
        """
        Execute function through circuit breaker.
        
        Args:
            func: Function to call
            *args: Positional arguments for function
            fallback: Optional fallback function to use on rejection
            **kwargs: Keyword arguments for function
            
        Returns:
            Function result, fallback result, or raises CircuitOpenError
            
        Raises:
            CircuitOpenError: If circuit is open and no fallback provided
        """
        with self._lock:
            current_state = self.state
            
            if current_state == CircuitState.OPEN:
                self._record_rejection()
                if fallback:
                    logger.info(f"Circuit {self.name} OPEN, using fallback")
                    return fallback(*args, **kwargs)
                raise CircuitOpenError(f"Circuit {self.name} is OPEN")
                
            if current_state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self._record_rejection()
                    if fallback:
                        return fallback(*args, **kwargs)
                    raise CircuitOpenError(
                        f"Circuit {self.name} HALF_OPEN, max test calls reached"
                    )
                self._half_open_calls += 1
                
        # Execute the function
        try:
            result = func(*args, **kwargs)
            self._handle_success()
            return result
            
        except self.config.excluded_exceptions as e:
            logger.debug(f"Circuit {self.name}: excluded exception {type(e).__name__}")
            return None
            
        except Exception as e:
            self._handle_failure()
            if fallback:
                return fallback(*args, **kwargs)
            raise
            
    def _handle_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._record_success()
            
            if self._state == CircuitState.HALF_OPEN:
                if self._metrics.consecutive_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    self._metrics.consecutive_successes = 0
                    
            elif self._state == CircuitState.CLOSED:
                self._metrics.consecutive_failures = 0
                
    def _handle_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._record_failure()
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
                
            elif self._state == CircuitState.CLOSED:
                if self._metrics.consecutive_failures >= self.config.failure_threshold:
                    logger.warning(
                        f"Circuit {self.name} opening after "
                        f"{self._metrics.consecutive_failures} consecutive failures"
                    )
                    self._transition_to(CircuitState.OPEN)
                    
    def reset(self) -> None:
        """Manually reset circuit to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._metrics = CircuitMetrics()
            logger.info(f"Circuit {self.name} manually reset")
            
    def get_health_status(self) -> Dict[str, Any]:
        """Get detailed health status of circuit breaker."""
        with self._lock:
            metrics = self._metrics
            return {
                "name": self.name,
                "state": self.state.value,
                "uptime_seconds": time.time() - self._last_state_change,
                "metrics": {
                    "total_calls": metrics.total_calls,
                    "success_rate": (
                        metrics.successful_calls / metrics.total_calls
                        if metrics.total_calls > 0 else 0
                    ),
                    "rejected_calls": metrics.rejected_calls,
                    "consecutive_failures": metrics.consecutive_failures,
                    "consecutive_successes": metrics.consecutive_successes,
                },
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "success_threshold": self.config.success_threshold,
                    "timeout_seconds": self.config.timeout,
                },
                "last_failure": (
                    datetime.fromtimestamp(metrics.last_failure_time).isoformat()
                    if metrics.last_failure_time else None
                ),
                "last_success": (
                    datetime.fromtimestamp(metrics.last_success_time).isoformat()
                    if metrics.last_success_time else None
                ),
            }


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.
    
    Example:
        registry = CircuitBreakerRegistry()
        registry.register("payment-api", failure_threshold=3)
        
        breaker = registry.get("payment-api")
        result = breaker.call(payment_function)
    """
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        
    def register(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
        **kwargs,
    ) -> CircuitBreaker:
        """
        Register a new circuit breaker.
        
        Args:
            name: Unique identifier for the breaker
            config: Optional CircuitConfig
            **kwargs: Passed to CircuitConfig if not provided
            
        Returns:
            The registered CircuitBreaker
        """
        with self._lock:
            if name in self._breakers:
                return self._breakers[name]
                
            if config is None:
                config = CircuitConfig(**kwargs)
                
            breaker = CircuitBreaker(name, config)
            self._breakers[name] = breaker
            return breaker
            
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker by name."""
        return self._breakers.get(name)
        
    def get_all_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all registered breakers."""
        with self._lock:
            return {
                name: breaker.get_health_status()
                for name, breaker in self._breakers.items()
            }
            
    def unregister(self, name: str) -> None:
        """Remove a circuit breaker from registry."""
        with self._lock:
            self._breakers.pop(name, None)


# Global registry instance
_registry = CircuitBreakerRegistry()


def get_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry."""
    return _registry


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    success_threshold: int = 2,
    timeout: float = 60.0,
):
    """
    Decorator to add circuit breaker to a function.
    
    Example:
        @circuit_breaker("user-api", failure_threshold=3)
        def fetch_user(user_id):
            return api.get(f"/users/{user_id}")
    """
    def decorator(func: Callable) -> Callable:
        breaker = _registry.register(
            name,
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
        )
        
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
            
        wrapper._circuit_breaker = breaker
        wrapper.__name__ = func.__name__
        return wrapper
        
    return decorator
