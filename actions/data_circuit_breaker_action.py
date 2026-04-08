"""Data circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern for data operations:
- DataCircuitBreaker: Circuit breaker for data operations
- DataCircuitState: Circuit state management
- DataFallbackHandler: Fallback handlers for circuit open
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class DataCircuitBreakerConfig:
    """Configuration for data circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    half_open_max_calls: int = 3
    excluded_errors: List[str] = field(default_factory=lambda: ["ValidationError", "ConfigurationError"])
    window_size: float = 60.0
    volume_threshold: int = 10


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit is open."""
    pass


class DataCircuitBreaker:
    """Circuit breaker for data operations."""
    
    def __init__(self, name: str, config: Optional[DataCircuitBreakerConfig] = None):
        self.name = name
        self.config = config or DataCircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.RLock()
        self._call_history: List[Tuple[float, bool]] = []
        self._stats = {"total_calls": 0, "successful_calls": 0, "failed_calls": 0, "rejected_calls": 0, "state_changes": 0}
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._opened_time and (time.time() - self._opened_time) >= self.config.timeout:
                    self._transition_to(CircuitState.HALF_OPEN)
                else:
                    return CircuitState.OPEN
            return self._state
    
    def _transition_to(self, new_state: CircuitState):
        """Transition to new state."""
        with self._lock:
            if self._state != new_state:
                logging.info(f"Circuit {self.name}: {self._state.value} -> {new_state.value}")
                self._state = new_state
                self._stats["state_changes"] += 1
                
                if new_state == CircuitState.HALF_OPEN:
                    self._half_open_calls = 0
                    self._success_count = 0
                elif new_state == CircuitState.CLOSED:
                    self._failure_count = 0
                    self._success_count = 0
                elif new_state == CircuitState.OPEN:
                    self._opened_time = time.time()
    
    def _is_excluded_error(self, exception: Exception) -> bool:
        """Check if error is excluded from circuit counting."""
        error_type = type(exception).__name__
        return any(exc.lower() in error_type.lower() for exc in self.config.excluded_errors)
    
    def _record_call(self, success: bool):
        """Record call outcome."""
        now = time.time()
        with self._lock:
            self._call_history.append((now, success))
            cutoff = now - self.config.window_size
            self._call_history = [(t, s) for t, s in self._call_history if t >= cutoff]
            
            self._stats["total_calls"] += 1
            if success:
                self._stats["successful_calls"] += 1
            else:
                self._stats["failed_calls"] += 1
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute call through circuit breaker."""
        current_state = self.state
        
        if current_state == CircuitState.OPEN:
            self._stats["rejected_calls"] += 1
            raise CircuitBreakerOpen(f"Circuit {self.name} is OPEN")
        
        if current_state == CircuitState.HALF_OPEN:
            with self._lock:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self._stats["rejected_calls"] += 1
                    raise CircuitBreakerOpen(f"Circuit {self.name} is HALF_OPEN and max calls reached")
                self._half_open_calls += 1
        
        try:
            result = func(*args, **kwargs)
            self._record_call(True)
            self._on_success()
            return result
        except Exception as e:
            self._record_call(False)
            if not self._is_excluded_error(e):
                self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            else:
                self._failure_count = max(0, self._failure_count - 1)
    
    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self.config.failure_threshold:
                recent_calls = sum(1 for _, _ in self._call_history if _ >= time.time() - self.config.window_size)
                if recent_calls >= self.config.volume_threshold:
                    self._transition_to(CircuitState.OPEN)
                else:
                    self._failure_count = self.config.failure_threshold // 2
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                **{k: v for k, v in self._stats.items()}
            }
    
    def reset(self):
        """Manually reset circuit breaker."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0
            self._success_count = 0
            self._call_history.clear()


class DataCircuitBreakerAction(BaseAction):
    """Data circuit breaker action."""
    action_type = "data_circuit_breaker"
    display_name = "数据熔断器"
    description = "数据操作熔断保护"
    
    def __init__(self):
        super().__init__()
        self._breakers: Dict[str, DataCircuitBreaker] = {}
        self._lock = threading.Lock()
    
    def _get_breaker(self, name: str, config: Optional[DataCircuitBreakerConfig] = None) -> DataCircuitBreaker:
        """Get or create circuit breaker."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = DataCircuitBreaker(name, config)
            return self._breakers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute operation with circuit breaker."""
        try:
            name = params.get("name", "default")
            operation = params.get("operation")
            fallback = params.get("fallback")
            
            config = DataCircuitBreakerConfig(
                failure_threshold=params.get("failure_threshold", 5),
                success_threshold=params.get("success_threshold", 2),
                timeout=params.get("timeout", 30.0),
                half_open_max_calls=params.get("half_open_max_calls", 3),
            )
            
            breaker = self._get_breaker(name, config)
            
            if operation is None:
                stats = breaker.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            try:
                result = breaker.call(operation)
                return ActionResult(success=True, data={"result": result})
            except CircuitBreakerOpen as e:
                if fallback:
                    try:
                        if callable(fallback):
                            result = fallback()
                            return ActionResult(success=True, data={"result": result, "circuit_open": True, "fallback_used": True})
                    except Exception as fe:
                        return ActionResult(success=False, message=f"Fallback also failed: {str(fe)}", data={"circuit_open": True})
                return ActionResult(success=False, message=str(e), data={"circuit_open": True})
            except Exception as e:
                return ActionResult(success=False, message=f"Operation failed: {str(e)}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataCircuitBreakerAction error: {str(e)}")
    
    def reset(self, name: Optional[str] = None) -> ActionResult:
        """Reset circuit breaker."""
        try:
            with self._lock:
                if name:
                    if name in self._breakers:
                        self._breakers[name].reset()
                else:
                    for b in self._breakers.values():
                        b.reset()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
