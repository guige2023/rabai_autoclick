"""API fault tolerance action module for RabAI AutoClick.

Provides fault tolerance for API operations:
- ApiFaultTolerance: Comprehensive fault tolerance for API calls
- ApiResilienceHandler: Handle API failures with resilience patterns
- ApiBulkhead: Bulkhead pattern for API isolation
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import random
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FaultToleranceStrategy(Enum):
    """Fault tolerance strategies."""
    CIRCUIT_BREAKER = "circuit_breaker"
    BULKHEAD = "bulkhead"
    TIMEOUT = "timeout"
    RETRY = "retry"
    FALLBACK = "fallback"
    RATE_LIMIT = "rate_limit"


@dataclass
class BulkheadConfig:
    """Bulkhead isolation configuration."""
    max_concurrent: int = 10
    max_queued: int = 20
    timeout: float = 30.0


@dataclass
class FaultToleranceConfig:
    """Configuration for API fault tolerance."""
    enable_circuit_breaker: bool = True
    enable_bulkhead: bool = True
    enable_timeout: bool = True
    enable_retry: bool = True
    enable_fallback: bool = True
    failure_threshold: int = 5
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0
    bulkhead_config: BulkheadConfig = field(default_factory=BulkheadConfig)


class ApiBulkhead:
    """Bulkhead pattern for API isolation."""
    
    def __init__(self, name: str, config: BulkheadConfig):
        self.name = name
        self.config = config
        self._semaphore: Optional[threading.Semaphore] = None
        self._queue: deque = deque(maxlen=config.max_queued)
        self._active_count = 0
        self._lock = threading.Lock()
        self._stats = {"total_attempts": 0, "admitted": 0, "rejected": 0, "queued": 0}
    
    def _get_semaphore(self) -> threading.Semaphore:
        """Get or create semaphore lazily."""
        if self._semaphore is None:
            with self._lock:
                if self._semaphore is None:
                    self._semaphore = threading.Semaphore(self.config.max_concurrent)
        return self._semaphore
    
    def execute(self, operation: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """Execute operation through bulkhead."""
        with self._lock:
            self._stats["total_attempts"] += 1
        
        semaphore = self._get_semaphore()
        
        acquired = semaphore.acquire(timeout=0.1)
        
        if not acquired:
            with self._lock:
                self._stats["rejected"] += 1
            return False, None
        
        with self._lock:
            self._active_count += 1
            self._stats["admitted"] += 1
        
        try:
            result = operation(*args, **kwargs)
            return True, result
        except Exception as e:
            return False, e
        finally:
            with self._lock:
                self._active_count -= 1
            semaphore.release()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get bulkhead statistics."""
        with self._lock:
            return {
                "name": self.name,
                "active_count": self._active_count,
                **{k: v for k, v in self._stats.items()},
            }


class ApiFaultTolerance:
    """Comprehensive API fault tolerance handler."""
    
    def __init__(self, name: str, config: Optional[FaultToleranceConfig] = None):
        self.name = name
        self.config = config or FaultToleranceConfig()
        self._bulkhead = ApiBulkhead(name, self.config.bulkhead_config)
        self._circuit_open = False
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        self._stats = {"total_calls": 0, "successful_calls": 0, "failed_calls": 0, "circuit_trips": 0}
    
    def _check_circuit(self) -> bool:
        """Check if circuit breaker should trip."""
        if not self.config.enable_circuit_breaker:
            return True
        
        if self._circuit_open:
            if self._last_failure_time and (time.time() - self._last_failure_time) > 60:
                with self._lock:
                    self._circuit_open = False
                    self._failure_count = 0
                return True
            return False
        
        return True
    
    def _trip_circuit(self):
        """Trip the circuit breaker."""
        with self._lock:
            self._circuit_open = True
            self._last_failure_time = time.time()
            self._stats["circuit_trips"] += 1
    
    def _record_success(self):
        """Record successful call."""
        with self._lock:
            self._stats["successful_calls"] += 1
            if self._failure_count > 0:
                self._failure_count = max(0, self._failure_count - 1)
    
    def _record_failure(self):
        """Record failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self.config.enable_circuit_breaker and self._failure_count >= self.config.failure_threshold:
                self._trip_circuit()
    
    def execute(self, operation: Callable, fallback: Optional[Callable] = None, *args, **kwargs) -> Tuple[bool, Any]:
        """Execute operation with fault tolerance."""
        with self._lock:
            self._stats["total_calls"] += 1
        
        if not self._check_circuit():
            if fallback and self.config.enable_fallback:
                try:
                    result = fallback()
                    return True, result
                except Exception:
                    return False, None
            return False, None
        
        def execute_with_timeout():
            result = [None]
            error = [None]
            
            def worker():
                try:
                    result[0] = operation()
                except Exception as e:
                    error[0] = e
            
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()
            t.join(timeout=self.config.timeout)
            
            if t.is_alive():
                raise TimeoutError(f"Operation timed out after {self.config.timeout}s")
            if error[0]:
                raise error[0]
            return result[0]
        
        last_error = None
        for attempt in range(self.config.retry_count + 1):
            try:
                if self.config.enable_bulkhead:
                    success, bulkhead_result = self._bulkhead.execute(execute_with_timeout)
                    if not success:
                        raise Exception("Bulkhead rejected")
                    result = bulkhead_result
                else:
                    result = execute_with_timeout()
                
                self._record_success()
                return True, result
                
            except Exception as e:
                last_error = e
                if attempt < self.config.retry_count and self.config.enable_retry:
                    delay = self.config.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                else:
                    self._record_failure()
        
        if fallback and self.config.enable_fallback:
            try:
                result = fallback()
                return True, result
            except Exception:
                pass
        
        with self._lock:
            self._stats["failed_calls"] += 1
        
        return False, last_error
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fault tolerance statistics."""
        with self._lock:
            bulkhead_stats = self._bulkhead.get_stats()
            return {
                "name": self.name,
                "circuit_open": self._circuit_open,
                "failure_count": self._failure_count,
                "bulkhead": bulkhead_stats,
                **{k: v for k, v in self._stats.items()},
            }
    
    def reset_circuit(self):
        """Manually reset circuit breaker."""
        with self._lock:
            self._circuit_open = False
            self._failure_count = 0


class ApiFaultToleranceAction(BaseAction):
    """API fault tolerance action."""
    action_type = "api_fault_tolerance"
    display_name = "API容错"
    description = "API调用容错处理"
    
    def __init__(self):
        super().__init__()
        self._handlers: Dict[str, ApiFaultTolerance] = {}
        self._lock = threading.Lock()
    
    def _get_handler(self, name: str, config: Optional[FaultToleranceConfig] = None) -> ApiFaultTolerance:
        """Get or create fault tolerance handler."""
        with self._lock:
            if name not in self._handlers:
                self._handlers[name] = ApiFaultTolerance(name, config)
            return self._handlers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with fault tolerance."""
        try:
            name = params.get("name", "default")
            operation = params.get("operation")
            fallback = params.get("fallback")
            
            config = FaultToleranceConfig(
                enable_circuit_breaker=params.get("enable_circuit_breaker", True),
                enable_bulkhead=params.get("enable_bulkhead", True),
                enable_timeout=params.get("enable_timeout", True),
                enable_retry=params.get("enable_retry", True),
                enable_fallback=params.get("enable_fallback", True),
                failure_threshold=params.get("failure_threshold", 5),
                timeout=params.get("timeout", 30.0),
                retry_count=params.get("retry_count", 3),
                retry_delay=params.get("retry_delay", 1.0),
            )
            
            handler = self._get_handler(name, config)
            
            if not operation:
                stats = handler.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            success, result = handler.execute(operation, fallback)
            return ActionResult(success=success, data={"result": result})
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiFaultToleranceAction error: {str(e)}")
