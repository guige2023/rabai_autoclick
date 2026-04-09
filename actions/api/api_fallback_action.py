"""API Fallback Action Module.

Provides fallback mechanisms for API operations including
circuit breaker pattern, fallback endpoints, and graceful degradation.

Example:
    >>> from actions.api.api_fallback_action import APIFallbackAction
    >>> action = APIFallbackAction()
    >>> result = await action.execute_with_fallback(primary_func, fallback_func)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import threading
import time


class FallbackTrigger(Enum):
    """Conditions that trigger fallback."""
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network_error"
    HTTP_ERROR = "http_error"
    RATE_LIMIT = "rate_limit"
    SERVICE_UNAVAILABLE = "service_unavailable"


class FallbackStatus(Enum):
    """Status of fallback mechanism."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FALLBACK_ACTIVE = "fallback_active"
    FAILED = "failed"


@dataclass
class FallbackConfig:
    """Configuration for fallback handling.
    
    Attributes:
        enable_circuit_breaker: Enable circuit breaker pattern
        failure_threshold: Failures before opening circuit
        recovery_timeout: Time before attempting recovery
        fallback_enabled: Enable fallback functions
        fallback_cooldown: Cooldown between fallback activations
    """
    enable_circuit_breaker: bool = True
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    fallback_enabled: bool = True
    fallback_cooldown: float = 30.0


@dataclass
class CircuitState:
    """Circuit breaker state.
    
    Attributes:
        state: Current circuit state
        failure_count: Consecutive failures
        last_failure: Last failure timestamp
        last_success: Last success timestamp
    """
    state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[float] = None
    last_success: Optional[float] = None
    opened_at: Optional[float] = None


@dataclass
class FallbackResult:
    """Result of fallback operation.
    
    Attributes:
        status: Operation status
        result: Operation result
        used_fallback: Whether fallback was used
        error: Error if failed
        circuit_state: Circuit state after operation
    """
    status: FallbackStatus
    result: Any = None
    used_fallback: bool = False
    error: Optional[str] = None
    circuit_state: str = "closed"
    duration: float = 0.0


class APIFallbackAction:
    """Fallback handler for API operations.
    
    Provides circuit breaker and fallback mechanisms to ensure
    graceful degradation when primary services fail.
    
    Attributes:
        config: Fallback configuration
        _circuit_states: Circuit breaker states per endpoint
        _fallback_functions: Registered fallback functions
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[FallbackConfig] = None,
    ) -> None:
        """Initialize fallback action.
        
        Args:
            config: Fallback configuration
        """
        self.config = config or FallbackConfig()
        self._circuit_states: Dict[str, CircuitState] = {}
        self._fallback_functions: Dict[str, Callable] = {}
        self._last_fallback: Dict[str, float] = {}
        self._lock = threading.RLock()
    
    async def execute_with_fallback(
        self,
        primary_func: Callable[..., Any],
        fallback_func: Optional[Callable[..., Any]] = None,
        endpoint: str = "default",
        trigger_conditions: Optional[List[FallbackTrigger]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> FallbackResult:
        """Execute function with fallback support.
        
        Args:
            primary_func: Primary function to execute
            fallback_func: Fallback function
            endpoint: Endpoint identifier
            trigger_conditions: Conditions that trigger fallback
            *args: Positional arguments for functions
            **kwargs: Keyword arguments for functions
        
        Returns:
            FallbackResult with execution details
        """
        import time
        start = time.time()
        
        circuit = self._get_circuit(endpoint)
        trigger_conditions = trigger_conditions or [
            FallbackTrigger.TIMEOUT,
            FallbackTrigger.NETWORK_ERROR,
            FallbackTrigger.HTTP_ERROR,
        ]
        
        if circuit.state == "open":
            if self._should_attempt_recovery(circuit):
                circuit.state = "half_open"
            else:
                if fallback_func and self.config.fallback_enabled:
                    return await self._execute_fallback(
                        fallback_func, endpoint, args, kwargs, time.time() - start
                    )
                return FallbackResult(
                    status=FallbackStatus.FALLBACK_ACTIVE,
                    error="Circuit open",
                    circuit_state=circuit.state,
                    duration=time.time() - start,
                )
        
        try:
            if asyncio.iscoroutinefunction(primary_func):
                result = await primary_func(*args, **kwargs)
            else:
                result = primary_func(*args, **kwargs)
            
            self._record_success(endpoint)
            
            return FallbackResult(
                status=FallbackStatus.HEALTHY,
                result=result,
                circuit_state=circuit.state,
                duration=time.time() - start,
            )
        
        except Exception as e:
            error_type = self._classify_error(e, trigger_conditions)
            
            if error_type in trigger_conditions:
                self._record_failure(endpoint)
            else:
                self._record_success(endpoint)
            
            if fallback_func and self.config.fallback_enabled:
                return await self._execute_fallback(
                    fallback_func, endpoint, args, kwargs, time.time() - start
                )
            
            return FallbackResult(
                status=FallbackStatus.FAILED,
                error=str(e),
                circuit_state=circuit.state,
                duration=time.time() - start,
            )
    
    async def _execute_fallback(
        self,
        fallback_func: Callable,
        endpoint: str,
        args: tuple,
        kwargs: dict,
        elapsed: float,
    ) -> FallbackResult:
        """Execute fallback function.
        
        Args:
            fallback_func: Fallback function
            endpoint: Endpoint identifier
            args: Positional arguments
            kwargs: Keyword arguments
            elapsed: Elapsed time
        
        Returns:
            FallbackResult
        """
        if self._in_fallback_cooldown(endpoint):
            return FallbackResult(
                status=FallbackStatus.FAILED,
                error="Fallback in cooldown",
                used_fallback=False,
                circuit_state="open",
                duration=elapsed,
            )
        
        try:
            if asyncio.iscoroutinefunction(fallback_func):
                result = await fallback_func(*args, **kwargs)
            else:
                result = fallback_func(*args, **kwargs)
            
            self._record_fallback_activation(endpoint)
            
            return FallbackResult(
                status=FallbackStatus.FALLBACK_ACTIVE,
                result=result,
                used_fallback=True,
                circuit_state="open",
                duration=elapsed,
            )
        
        except Exception as e:
            return FallbackResult(
                status=FallbackStatus.FAILED,
                error=str(e),
                used_fallback=True,
                circuit_state="open",
                duration=elapsed,
            )
    
    def _get_circuit(self, endpoint: str) -> CircuitState:
        """Get circuit state for endpoint.
        
        Args:
            endpoint: Endpoint identifier
        
        Returns:
            CircuitState
        """
        with self._lock:
            if endpoint not in self._circuit_states:
                self._circuit_states[endpoint] = CircuitState()
            return self._circuit_states[endpoint]
    
    def _record_success(self, endpoint: str) -> None:
        """Record successful operation.
        
        Args:
            endpoint: Endpoint identifier
        """
        circuit = self._get_circuit(endpoint)
        circuit.last_success = time.time()
        circuit.failure_count = 0
        
        if circuit.state == "half_open":
            circuit.state = "closed"
    
    def _record_failure(self, endpoint: str) -> None:
        """Record failed operation.
        
        Args:
            endpoint: Endpoint identifier
        """
        circuit = self._get_circuit(endpoint)
        circuit.last_failure = time.time()
        circuit.failure_count += 1
        
        if circuit.failure_count >= self.config.failure_threshold:
            circuit.state = "open"
            circuit.opened_at = time.time()
    
    def _should_attempt_recovery(self, circuit: CircuitState) -> bool:
        """Check if should attempt recovery.
        
        Args:
            circuit: Circuit state
        
        Returns:
            True if should attempt
        """
        if not circuit.opened_at:
            return True
        return (time.time() - circuit.opened_at) >= self.config.recovery_timeout
    
    def _record_fallback_activation(self, endpoint: str) -> None:
        """Record fallback activation.
        
        Args:
            endpoint: Endpoint identifier
        """
        self._last_fallback[endpoint] = time.time()
    
    def _in_fallback_cooldown(self, endpoint: str) -> bool:
        """Check if in fallback cooldown.
        
        Args:
            endpoint: Endpoint identifier
        
        Returns:
            True if in cooldown
        """
        if endpoint not in self._last_fallback:
            return False
        return (time.time() - self._last_fallback[endpoint]) < self.config.fallback_cooldown
    
    def _classify_error(
        self,
        error: Exception,
        triggers: List[FallbackTrigger],
    ) -> Optional[FallbackTrigger]:
        """Classify error type.
        
        Args:
            error: Exception
            triggers: Trigger conditions
        
        Returns:
            FallbackTrigger if matched
        """
        error_str = str(error).lower()
        
        if "timeout" in error_str:
            return FallbackTrigger.TIMEOUT
        if "network" in error_str or "connection" in error_str:
            return FallbackTrigger.NETWORK_ERROR
        if "429" in error_str or "rate" in error_str:
            return FallbackTrigger.RATE_LIMIT
        if "503" in error_str or "unavailable" in error_str:
            return FallbackTrigger.SERVICE_UNAVAILABLE
        
        return FallbackTrigger.HTTP_ERROR
    
    def register_fallback(
        self,
        endpoint: str,
        fallback_func: Callable,
    ) -> None:
        """Register fallback function for endpoint.
        
        Args:
            endpoint: Endpoint identifier
            fallback_func: Fallback function
        """
        with self._lock:
            self._fallback_functions[endpoint] = fallback_func
    
    def reset_circuit(self, endpoint: str) -> None:
        """Reset circuit breaker for endpoint.
        
        Args:
            endpoint: Endpoint identifier
        """
        with self._lock:
            if endpoint in self._circuit_states:
                self._circuit_states[endpoint] = CircuitState()
