"""Automation Breaker Action Module.

Provides circuit breaker functionality for automation tasks with
failure tracking, state management, and automatic recovery.

Example:
    >>> from actions.automation.automation_breaker_action import AutomationBreakerAction
    >>> action = AutomationBreakerAction()
    >>> result = await action.execute_with_breaker("api_call", task)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import threading


class BreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BreakerEvent(Enum):
    """Circuit breaker events."""
    FAILURE = "failure"
    SUCCESS = "success"
    TIMEOUT = "timeout"
    ATTEMPT = "attempt"


@dataclass
class BreakerConfig:
    """Configuration for circuit breaker.
    
    Attributes:
        failure_threshold: Failures before opening
        success_threshold: Successes before closing
        timeout: Time before attempting recovery
        half_open_max_calls: Max calls in half-open
        monitoring_window: Failure monitoring window
    """
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    monitoring_window: float = 60.0


@dataclass
class BreakerMetrics:
    """Circuit breaker metrics.
    
    Attributes:
        total_calls: Total calls
        successful_calls: Successful calls
        failed_calls: Failed calls
        rejected_calls: Rejected calls
        current_state: Current breaker state
        last_failure: Last failure timestamp
    """
    total_calls: int
    successful_calls: int
    failed_calls: int
    rejected_calls: int
    current_state: BreakerState
    last_failure: Optional[float]


@dataclass
class BreakerResult:
    """Result of breaker-protected call.
    
    Attributes:
        success: Whether call succeeded
        result: Call result
        error: Error if failed
        state: Breaker state after call
        duration: Call duration
    """
    success: bool
    result: Any = None
    error: Optional[str] = None
    state: BreakerState = BreakerState.CLOSED
    duration: float = 0.0


class AutomationBreakerAction:
    """Circuit breaker for automation tasks.
    
    Provides fault tolerance with automatic state
    transitions and recovery mechanisms.
    
    Attributes:
        config: Breaker configuration
        _breakers: Named circuit breakers
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[BreakerConfig] = None,
    ) -> None:
        """Initialize breaker action.
        
        Args:
            config: Breaker configuration
        """
        self.config = config or BreakerConfig()
        self._breakers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    async def execute_with_breaker(
        self,
        name: str,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> BreakerResult:
        """Execute task with circuit breaker protection.
        
        Args:
            name: Circuit breaker name
            task: Task to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            BreakerResult
        """
        import time
        start = time.time()
        
        breaker = self._get_or_create_breaker(name)
        
        state = self._get_state(breaker)
        
        if state == BreakerState.OPEN:
            if self._should_attempt_recovery(breaker):
                self._transition_to_half_open(breaker)
                state = BreakerState.HALF_OPEN
            else:
                self._record_rejection(breaker)
                return BreakerResult(
                    success=False,
                    error="Circuit breaker open",
                    state=BreakerState.OPEN,
                    duration=time.time() - start,
                )
        
        try:
            if asyncio.iscoroutinefunction(task):
                result = await task(*args, **kwargs)
            else:
                result = task(*args, **kwargs)
            
            self._record_success(breaker)
            
            return BreakerResult(
                success=True,
                result=result,
                state=self._get_state(breaker),
                duration=time.time() - start,
            )
        
        except Exception as e:
            self._record_failure(breaker)
            
            return BreakerResult(
                success=False,
                error=str(e),
                state=self._get_state(breaker),
                duration=time.time() - start,
            )
    
    def _get_or_create_breaker(self, name: str) -> Dict[str, Any]:
        """Get or create breaker by name.
        
        Args:
            name: Breaker name
        
        Returns:
            Breaker state dictionary
        """
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = {
                    "name": name,
                    "state": BreakerState.CLOSED,
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure": None,
                    "opened_at": None,
                    "half_open_calls": 0,
                    "total_calls": 0,
                    "successful_calls": 0,
                    "failed_calls": 0,
                    "rejected_calls": 0,
                }
            return self._breakers[name]
    
    def _get_state(self, breaker: Dict[str, Any]) -> BreakerState:
        """Get current breaker state.
        
        Args:
            breaker: Breaker state
        
        Returns:
            BreakerState
        """
        state = breaker["state"]
        
        if state == BreakerState.OPEN:
            if self._should_attempt_recovery(breaker):
                return BreakerState.HALF_OPEN
        elif state == BreakerState.HALF_OPEN:
            if breaker["half_open_calls"] >= self.config.half_open_max_calls:
                return BreakerState.OPEN
        
        return state
    
    def _should_attempt_recovery(self, breaker: Dict[str, Any]) -> bool:
        """Check if should attempt recovery.
        
        Args:
            breaker: Breaker state
        
        Returns:
            True if should try
        """
        if not breaker["opened_at"]:
            return True
        return (time.time() - breaker["opened_at"]) >= self.config.timeout
    
    def _transition_to_half_open(self, breaker: Dict[str, Any]) -> None:
        """Transition breaker to half-open state.
        
        Args:
            breaker: Breaker state
        """
        breaker["state"] = BreakerState.HALF_OPEN
        breaker["half_open_calls"] = 0
        breaker["success_count"] = 0
    
    def _record_success(self, breaker: Dict[str, Any]) -> None:
        """Record successful call.
        
        Args:
            breaker: Breaker state
        """
        breaker["total_calls"] += 1
        breaker["successful_calls"] += 1
        breaker["failure_count"] = 0
        
        if breaker["state"] == BreakerState.HALF_OPEN:
            breaker["success_count"] += 1
            
            if breaker["success_count"] >= self.config.success_threshold:
                breaker["state"] = BreakerState.CLOSED
                breaker["opened_at"] = None
    
    def _record_failure(self, breaker: Dict[str, Any]) -> None:
        """Record failed call.
        
        Args:
            breaker: Breaker state
        """
        breaker["total_calls"] += 1
        breaker["failed_calls"] += 1
        breaker["failure_count"] += 1
        breaker["last_failure"] = time.time()
        breaker["success_count"] = 0
        
        if breaker["state"] == BreakerState.HALF_OPEN:
            breaker["state"] = BreakerState.OPEN
            breaker["opened_at"] = time.time()
        elif breaker["failure_count"] >= self.config.failure_threshold:
            breaker["state"] = BreakerState.OPEN
            breaker["opened_at"] = time.time()
    
    def _record_rejection(self, breaker: Dict[str, Any]) -> None:
        """Record rejected call.
        
        Args:
            breaker: Breaker state
        """
        breaker["rejected_calls"] += 1
    
    def get_breaker_state(self, name: str) -> Optional[BreakerState]:
        """Get state of named breaker.
        
        Args:
            name: Breaker name
        
        Returns:
            BreakerState or None
        """
        with self._lock:
            if name not in self._breakers:
                return None
            return self._get_state(self._breakers[name])
    
    def get_metrics(self, name: str) -> Optional[BreakerMetrics]:
        """Get metrics for named breaker.
        
        Args:
            name: Breaker name
        
        Returns:
            BreakerMetrics or None
        """
        with self._lock:
            if name not in self._breakers:
                return None
            
            breaker = self._breakers[name]
            
            return BreakerMetrics(
                total_calls=breaker["total_calls"],
                successful_calls=breaker["successful_calls"],
                failed_calls=breaker["failed_calls"],
                rejected_calls=breaker["rejected_calls"],
                current_state=self._get_state(breaker),
                last_failure=breaker["last_failure"],
            )
    
    def reset_breaker(self, name: str) -> bool:
        """Reset breaker to closed state.
        
        Args:
            name: Breaker name
        
        Returns:
            True if reset
        """
        with self._lock:
            if name not in self._breakers:
                return False
            
            self._breakers[name] = {
                "name": name,
                "state": BreakerState.CLOSED,
                "failure_count": 0,
                "success_count": 0,
                "last_failure": None,
                "opened_at": None,
                "half_open_calls": 0,
                "total_calls": 0,
                "successful_calls": 0,
                "failed_calls": 0,
                "rejected_calls": 0,
            }
            return True
    
    def list_breakers(self) -> List[str]:
        """List all breaker names.
        
        Returns:
            List of breaker names
        """
        with self._lock:
            return list(self._breakers.keys())
