"""
API Fault Tolerance Action Module

Provides fault tolerance mechanisms for API calls.
Supports circuit breakers, bulkheads, and fallbacks.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
import threading

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FailureType(Enum):
    """Type of failure."""
    TIMEOUT = "timeout"
    ERROR = "error"
    RATE_LIMIT = "rate_limit"
    CONNECTION = "connection"


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_timeout_seconds: float = 30.0
    window_seconds: float = 60.0


@dataclass
class CircuitStats:
    """Circuit breaker statistics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None


@dataclass
class FallbackConfig:
    """Configuration for fallback behavior."""
    enabled: bool = True
    fallback_value: Any = None
    fallback_fn: Optional[Callable] = None
    max_fallback_attempts: int = 3
    fallback_delay_seconds: float = 0.0


@dataclass
class CallResult:
    """Result of a fault-tolerant call."""
    success: bool
    value: Any = None
    error: Optional[str] = None
    failure_type: Optional[FailureType] = None
    circuit_state: CircuitState = CircuitState.CLOSED
    from_fallback: bool = False
    duration_ms: float = 0.0


class CircuitBreaker:
    """Circuit breaker implementation."""
    
    def __init__(self, name: str, config: Optional[CircuitConfig] = None):
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._opened_at: Optional[datetime] = None
        self._stats = CircuitStats()
        self._lock = threading.Lock()
        self._failure_history: deque[datetime] = deque(maxlen=100)
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._opened_at:
                time_since_open = (datetime.now() - self._opened_at).total_seconds()
                if time_since_open >= self.config.half_open_timeout_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._stats.state_changes += 1
        return self._state
    
    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._stats.successful_calls += 1
            self._stats.last_success_time = datetime.now()
            self._failure_count = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._success_count = 0
                    self._stats.state_changes += 1
    
    def record_failure(self, failure_type: FailureType = FailureType.ERROR) -> None:
        """Record a failed call."""
        with self._lock:
            self._stats.failed_calls += 1
            self._stats.last_failure_time = datetime.now()
            self._failure_history.append(datetime.now())
            
            self._failure_count += 1
            self._success_count = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_at = datetime.now()
                self._stats.state_changes += 1
            elif self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = datetime.now()
                self._stats.state_changes += 1
    
    def can_execute(self) -> bool:
        """Check if a call can be executed."""
        return self.state != CircuitState.OPEN or (
            self._opened_at and 
            (datetime.now() - self._opened_at).total_seconds() >= self.config.half_open_timeout_seconds
        )
    
    def get_stats(self) -> CircuitStats:
        """Get circuit statistics."""
        return self._stats


class Bulkhead:
    """Bulkhead pattern implementation for resource isolation."""
    
    def __init__(self, max_concurrent: int = 10, max_queue: int = 100):
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._current_load = 0
        self._queued_requests = 0
        self._rejected_requests = 0
        self._lock = threading.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with bulkhead isolation."""
        async with self._semaphore:
            return await func(*args, **kwargs)


class ApiFaultToleranceAction:
    """
    Fault tolerance manager for API calls.
    
    Example:
        ft = ApiFaultToleranceAction()
        
        result = await ft.execute_with_fault_tolerance(
            api_call,
            fallback_fn=lambda: get_cached_data(),
            circuit_name="api_endpoint"
        )
    """
    
    def __init__(self):
        self._circuits: dict[str, CircuitBreaker] = {}
        self._fallbacks: dict[str, FallbackConfig] = {}
        self._bulkhead: Optional[Bulkhead] = None
        self._stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "rejected_calls": 0,
            "fallback_calls": 0
        }
    
    def get_or_create_circuit(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        if name not in self._circuits:
            self._circuits[name] = CircuitBreaker(name, config)
        return self._circuits[name]
    
    def set_fallback(
        self,
        circuit_name: str,
        fallback_fn: Optional[Callable] = None,
        fallback_value: Any = None
    ) -> None:
        """Set fallback for a circuit."""
        self._fallbacks[circuit_name] = FallbackConfig(
            fallback_fn=fallback_fn,
            fallback_value=fallback_value
        )
    
    def set_bulkhead(self, max_concurrent: int = 10, max_queue: int = 100) -> None:
        """Configure bulkhead."""
        self._bulkhead = Bulkhead(max_concurrent, max_queue)
    
    async def execute_with_fault_tolerance(
        self,
        func: Callable,
        circuit_name: str = "default",
        circuit_config: Optional[CircuitConfig] = None,
        fallback_config: Optional[FallbackConfig] = None,
        timeout_seconds: float = 30.0
    ) -> CallResult:
        """
        Execute a function with fault tolerance.
        
        Args:
            func: Function to execute
            circuit_name: Circuit breaker name
            circuit_config: Circuit breaker configuration
            fallback_config: Fallback configuration
            timeout_seconds: Call timeout
            
        Returns:
            CallResult with execution details
        """
        start_time = time.time()
        circuit = self.get_or_create_circuit(circuit_name, circuit_config)
        fallback = fallback_config or self._fallbacks.get(circuit_name, FallbackConfig())
        
        self._stats["total_calls"] += 1
        circuit._stats.total_calls += 1
        
        if not circuit.can_execute():
            self._stats["rejected_calls"] += 1
            circuit._stats.rejected_calls += 1
            return CallResult(
                success=False,
                error="Circuit breaker is open",
                failure_type=FailureType.ERROR,
                circuit_state=circuit.state,
                duration_ms=(time.time() - start_time) * 1000
            )
        
        try:
            if asyncio.iscoroutinefunction(func):
                if self._bulkhead:
                    result = await asyncio.wait_for(
                        self._bulkhead.execute(func),
                        timeout=timeout_seconds
                    )
                else:
                    result = await asyncio.wait_for(func(), timeout=timeout_seconds)
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: asyncio.run(func()) if asyncio.iscoroutinefunction(func) else func()
                )
            
            circuit.record_success()
            self._stats["successful_calls"] += 1
            
            return CallResult(
                success=True,
                value=result,
                circuit_state=circuit.state,
                duration_ms=(time.time() - start_time) * 1000
            )
        
        except asyncio.TimeoutError:
            circuit.record_failure(FailureType.TIMEOUT)
            self._stats["failed_calls"] += 1
            
            if fallback.enabled:
                return await self._execute_fallback(fallback, circuit.state, start_time)
            
            return CallResult(
                success=False,
                error="Call timed out",
                failure_type=FailureType.TIMEOUT,
                circuit_state=circuit.state,
                duration_ms=(time.time() - start_time) * 1000
            )
        
        except Exception as e:
            circuit.record_failure(FailureType.ERROR)
            self._stats["failed_calls"] += 1
            
            if fallback.enabled:
                return await self._execute_fallback(fallback, circuit.state, start_time)
            
            return CallResult(
                success=False,
                error=str(e),
                failure_type=FailureType.ERROR,
                circuit_state=circuit.state,
                duration_ms=(time.time() - start_time) * 1000
            )
    
    async def _execute_fallback(
        self,
        fallback: FallbackConfig,
        circuit_state: CircuitState,
        start_time: float
    ) -> CallResult:
        """Execute fallback."""
        self._stats["fallback_calls"] += 1
        
        if fallback.fallback_fn:
            try:
                if asyncio.iscoroutinefunction(fallback.fallback_fn):
                    value = await fallback.fallback_fn()
                else:
                    value = fallback.fallback_fn()
            except Exception:
                value = fallback.fallback_value
        else:
            value = fallback.fallback_value
        
        return CallResult(
            success=True,
            value=value,
            circuit_state=circuit_state,
            from_fallback=True,
            duration_ms=(time.time() - start_time) * 1000
        )
    
    def get_circuit_stats(self, circuit_name: str) -> Optional[CircuitStats]:
        """Get statistics for a specific circuit."""
        circuit = self._circuits.get(circuit_name)
        return circuit.get_stats() if circuit else None
    
    def get_all_circuit_stats(self) -> dict[str, CircuitStats]:
        """Get statistics for all circuits."""
        return {name: c.get_stats() for name, c in self._circuits.items()}
    
    def get_stats(self) -> dict[str, Any]:
        """Get overall fault tolerance statistics."""
        return {
            **self._stats,
            "circuit_count": len(self._circuits),
            "success_rate": (
                self._stats["successful_calls"] / self._stats["total_calls"]
                if self._stats["total_calls"] > 0 else 0
            )
        }
