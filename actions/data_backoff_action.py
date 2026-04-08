"""Data backoff action module for RabAI AutoClick.

Provides backoff strategies for data operations:
- ExponentialBackoff: Exponential backoff for transient failures
- AdaptiveBackoff: Adaptive backoff based on error patterns
- DataBackoffStrategy: Composite backoff strategies for data workloads
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import random
import threading
import math
import logging
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackoffType(Enum):
    """Backoff algorithm types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    POLYNOMIAL = "polynomial"
    FIBONACCI = "fibonacci"
    CONSTANT = "constant"
    RANDOM = "random"
    DECORRELATED = "decorrelated"


@dataclass
class DataBackoffConfig:
    """Configuration for data backoff."""
    backoff_type: BackoffType = BackoffType.EXPONENTIAL
    initial_interval: float = 0.5
    max_interval: float = 60.0
    multiplier: float = 2.0
    randomization_factor: float = 0.1
    max_attempts: int = 5
    retryable_errors: List[str] = field(default_factory=lambda: [
        "TransientError", "TemporaryError", "ConnectionError", "TimeoutError",
        "ResourceBusy", "LockTimeout", "RateLimitError", "ServiceUnavailable"
    ])
    reset_on_success: bool = True
    use_fibonacci_sequence: bool = False
    polynomial_degree: float = 2.0


class ExponentialBackoff:
    """Classic exponential backoff."""
    
    def __init__(self, config: DataBackoffConfig):
        self.config = config
        self._attempt = 0
        self._lock = threading.Lock()
    
    def compute(self) -> float:
        """Compute next backoff interval."""
        with self._lock:
            self._attempt += 1
            interval = self.config.initial_interval * (self.config.multiplier ** (self._attempt - 1))
            interval = min(interval, self.config.max_interval)
            
            if self.config.randomization_factor > 0:
                delta = interval * self.config.randomization_factor
                interval += random.uniform(-delta, delta)
            
            return max(0, interval)
    
    def reset(self):
        """Reset attempt counter."""
        with self._lock:
            self._attempt = 0
    
    def get_attempt(self) -> int:
        """Get current attempt number."""
        with self._lock:
            return self._attempt


class FibonacciBackoff:
    """Fibonacci-based backoff for smoother intervals."""
    
    def __init__(self, config: DataBackoffConfig):
        self.config = config
        self._attempt = 0
        self._fib_cache: Dict[int, float] = {0: 0, 1: 1}
        self._lock = threading.Lock()
    
    def _fib(self, n: int) -> float:
        """Get nth Fibonacci number."""
        if n in self._fib_cache:
            return self._fib_cache[n]
        self._fib_cache[n] = self._fib(n - 1) + self._fib(n - 2)
        return self._fib_cache[n]
    
    def compute(self) -> float:
        """Compute next backoff interval using Fibonacci."""
        with self._lock:
            self._attempt += 1
            fib_val = self._fib(self._attempt)
            interval = self.config.initial_interval * fib_val
            interval = min(interval, self.config.max_interval)
            
            if self.config.randomization_factor > 0:
                delta = interval * self.config.randomization_factor
                interval += random.uniform(-delta, delta)
            
            return max(0, interval)
    
    def reset(self):
        """Reset attempt counter."""
        with self._lock:
            self._attempt = 0


class AdaptiveBackoff:
    """Adaptive backoff that adjusts based on error patterns."""
    
    def __init__(self, config: DataBackoffConfig):
        self.config = config
        self._attempt = 0
        self._error_history: List[bool] = []
        self._success_rate: float = 1.0
        self._lock = threading.Lock()
        self._window_size = 10
    
    def compute(self) -> float:
        """Compute adaptive backoff interval."""
        with self._lock:
            self._attempt += 1
            
            base_interval = self.config.initial_interval * (self.config.multiplier ** (self._attempt - 1))
            
            if len(self._error_history) >= self._window_size:
                self._success_rate = sum(self._error_history[-self._window_size:]) / self._window_size
            
            adaptive_multiplier = 1.0 + (1.0 - self._success_rate) * 2.0
            
            interval = base_interval * adaptive_multiplier
            interval = min(interval, self.config.max_interval)
            
            if self.config.randomization_factor > 0:
                delta = interval * self.config.randomization_factor
                interval += random.uniform(-delta, delta)
            
            return max(0, interval)
    
    def record_result(self, success: bool):
        """Record operation result for adaptation."""
        with self._lock:
            self._error_history.append(success)
            if len(self._error_history) > self._window_size * 2:
                self._error_history = self._error_history[-self._window_size:]
    
    def reset(self):
        """Reset state."""
        with self._lock:
            self._attempt = 0
            self._error_history.clear()
            self._success_rate = 1.0


class DataBackoffStrategy:
    """Manage backoff strategies for data operations."""
    
    def __init__(self, config: Optional[DataBackoffConfig] = None):
        self.config = config or DataBackoffConfig()
        self._strategies: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"retries": 0, "successes": 0, "failures": 0})
    
    def _get_or_create_strategy(self, name: str) -> Any:
        """Get or create backoff strategy."""
        with self._lock:
            if name not in self._strategies:
                bt = self.config.backoff_type
                if bt == BackoffType.FIBONACCI or self.config.use_fibonacci_sequence:
                    self._strategies[name] = FibonacciBackoff(self.config)
                elif bt == BackoffType.EXPONENTIAL:
                    self._strategies[name] = ExponentialBackoff(self.config)
                else:
                    self._strategies[name] = ExponentialBackoff(self.config)
            return self._strategies[name]
    
    def get_delay(self, name: str = "default") -> float:
        """Get next backoff delay."""
        strategy = self._get_or_create_strategy(name)
        return strategy.compute()
    
    def reset(self, name: Optional[str] = None):
        """Reset backoff state."""
        with self._lock:
            if name:
                if name in self._strategies:
                    self._strategies[name].reset()
            else:
                for s in self._strategies.values():
                    s.reset()
    
    def record_result(self, name: str, success: bool):
        """Record operation result."""
        with self._lock:
            self._stats[name]["successes" if success else "failures"] += 1
            if name in self._strategies and hasattr(self._strategies[name], 'record_result'):
                self._strategies[name].record_result(success)
    
    def get_stats(self, name: str = "default") -> Dict[str, Any]:
        """Get backoff statistics."""
        with self._lock:
            return dict(self._stats.get(name, {}))


class DataBackoffAction(BaseAction):
    """Data backoff action."""
    action_type = "data_backoff"
    display_name = "数据退避"
    description = "数据操作退避策略"
    
    def __init__(self):
        super().__init__()
        self._strategy: Optional[DataBackoffStrategy] = None
        self._lock = threading.Lock()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute backoff for data operation."""
        try:
            command = params.get("command", "get_delay")
            name = params.get("name", "default")
            operation = params.get("operation")
            operation_args = params.get("args", [])
            operation_kwargs = params.get("kwargs", {})
            
            if self._strategy is None:
                config = DataBackoffConfig(
                    initial_interval=params.get("initial_interval", 0.5),
                    max_interval=params.get("max_interval", 60.0),
                    multiplier=params.get("multiplier", 2.0),
                    randomization_factor=params.get("randomization_factor", 0.1),
                    max_attempts=params.get("max_attempts", 5),
                )
                self._strategy = DataBackoffStrategy(config)
            
            if command == "get_delay":
                delay = self._strategy.get_delay(name)
                return ActionResult(success=True, data={"delay": delay})
            
            elif command == "reset":
                self._strategy.reset(name)
                return ActionResult(success=True, message=f"Backoff reset for {name}")
            
            elif command == "record":
                success = params.get("success", True)
                self._strategy.record_result(name, success)
                return ActionResult(success=True)
            
            elif command == "execute" and operation:
                max_attempts = params.get("max_attempts", 5)
                for attempt in range(max_attempts):
                    try:
                        result = operation(*operation_args, **operation_kwargs)
                        self._strategy.record_result(name, True)
                        return ActionResult(success=True, data={"result": result, "attempts": attempt + 1})
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            self._strategy.record_result(name, False)
                            return ActionResult(success=False, message=f"Failed after {max_attempts} attempts: {str(e)}")
                        
                        delay = self._strategy.get_delay(name)
                        time.sleep(delay)
                
                return ActionResult(success=False, message="Max attempts exceeded")
            
            elif command == "stats":
                stats = self._strategy.get_stats(name)
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataBackoffAction error: {str(e)}")
