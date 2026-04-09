"""
Circuit Breaker Action Module

Fault tolerance pattern with half-open, closed, and open states.
Configurable thresholds, timeout handling, and fallback actions.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: float = 60
    half_open_max_calls: int = 3
    window_seconds: float = 60
    monitor_period_seconds: float = 10


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics."""
    
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_state_change: Optional[float] = None


class CircuitBreaker:
    """Core circuit breaker logic."""
    
    def __init__(self, name: str, config: CircuitConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.metrics = CircuitMetrics()
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: float = 0
        self._call_times: List[float] = []
        self._half_open_calls: int = 0
        self._lock = asyncio.Lock()
    
    async def call(
        self,
        func: Callable,
        fallback: Optional[Callable] = None,
        *args,
        **kwargs
    ) -> Any:
        """Execute function through circuit breaker."""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    self.metrics.rejected_calls += 1
                    if fallback:
                        return await fallback(*args, **kwargs)
                    raise Exception(f"Circuit {self.name} is OPEN")
            
            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self.metrics.rejected_calls += 1
                    if fallback:
                        return await fallback(*args, **kwargs)
                    raise Exception(f"Circuit {self.name} half-open limit reached")
                self._half_open_calls += 1
        
        self.metrics.total_calls += 1
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            await self._on_success()
            
            return result
        
        except Exception as e:
            await self._on_failure()
            if fallback:
                return await fallback(*args, **kwargs)
            raise
    
    async def _on_success(self) -> None:
        """Handle successful call."""
        self.metrics.successful_calls += 1
        
        if self.state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._transition_to_closed()
        elif self.state == CircuitState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)
    
    async def _on_failure(self) -> None:
        """Handle failed call."""
        self.metrics.failed_calls += 1
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self._transition_to_open()
        
        elif self.state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to_open()
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset."""
        return (
            time.time() - self._last_failure_time >= self.config.timeout_seconds
        )
    
    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        if self.state != CircuitState.OPEN:
            self.state = CircuitState.OPEN
            self.metrics.state_changes += 1
            self.metrics.last_state_change = time.time()
            logger.warning(f"Circuit {self.name} transitioned to OPEN")
    
    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        self.state = CircuitState.HALF_OPEN
        self._half_open_calls = 0
        self._success_count = 0
        self.metrics.state_changes += 1
        self.metrics.last_state_change = time.time()
        logger.info(f"Circuit {self.name} transitioned to HALF_OPEN")
    
    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        self.state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self.metrics.state_changes += 1
        self.metrics.last_state_change = time.time()
        logger.info(f"Circuit {self.name} transitioned to CLOSED")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "rejected_calls": self.metrics.rejected_calls,
                "state_changes": self.metrics.state_changes
            }
        }
    
    async def reset(self) -> None:
        """Manually reset circuit breaker."""
        async with self._lock:
            self._transition_to_closed()


class CircuitBreakerRegistry:
    """Registry for multiple circuit breakers."""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()
    
    async def get_or_create(
        self,
        name: str,
        config: Optional[CircuitConfig] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker by name."""
        async with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(
                    name,
                    config or CircuitConfig()
                )
            return self._breakers[name]
    
    async def get_all_status(self) -> List[Dict]:
        """Get status of all circuit breakers."""
        return [
            await cb.get_status()
            for cb in self._breakers.values()
        ]


class CircuitBreakerAction:
    """
    Main circuit breaker action handler.
    
    Provides fault tolerance with configurable thresholds,
    automatic state management, and fallback support.
    """
    
    def __init__(self, config: Optional[CircuitConfig] = None):
        self.default_config = config or CircuitConfig()
        self.registry = CircuitBreakerRegistry()
        self._middleware: List[Callable] = []
    
    async def call(
        self,
        name: str,
        func: Callable,
        fallback: Optional[Callable] = None,
        config: Optional[CircuitConfig] = None,
        *args,
        **kwargs
    ) -> Any:
        """Execute function through circuit breaker."""
        breaker = await self.registry.get_or_create(
            name,
            config or self.default_config
        )
        
        for mw in self._middleware:
            await mw(name, breaker.state)
        
        return await breaker.call(func, fallback, *args, **kwargs)
    
    async def get_status(self, name: str) -> Optional[Dict]:
        """Get status of a specific circuit breaker."""
        breaker = await self.registry.get_or_create(name, self.default_config)
        return await breaker.get_status()
    
    async def get_all_status(self) -> List[Dict]:
        """Get status of all circuit breakers."""
        return await self.registry.get_all_status()
    
    async def reset(self, name: Optional[str] = None) -> None:
        """Reset circuit breaker(s)."""
        if name:
            breaker = await self.registry.get_or_create(name, self.default_config)
            await breaker.reset()
        else:
            for breaker in self.registry._breakers.values():
                await breaker.reset()
    
    def add_middleware(self, func: Callable) -> None:
        """Add middleware for state changes."""
        self._middleware.append(func)
