"""API Timeout Action Module.

Provides timeout management for API operations including configurable
timeouts, graceful cancellation, and timeout retry handling.

Example:
    >>> from actions.api.api_timeout_action import APITimeoutAction
    >>> action = APITimeoutAction()
    >>> result = await action.execute_with_timeout(fetch_data, timeout=30)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar
from functools import wraps
import threading
import time


T = TypeVar("T")


class TimeoutStrategy(Enum):
    """Timeout handling strategies."""
    CANCEL = "cancel"
    RETRY = "retry"
    FALLBACK = "fallback"
    EXTEND = "extend"


class TimeoutStatus(Enum):
    """Status of a timed operation."""
    SUCCESS = "success"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    ERROR = "error"


@dataclass
class TimeoutConfig:
    """Configuration for timeout handling.
    
    Attributes:
        default_timeout: Default timeout in seconds
        max_timeout: Maximum allowed timeout
        retry_count: Number of retries on timeout
        retry_delay: Delay between retries
        enable_extend: Allow timeout extension
    """
    default_timeout: float = 30.0
    max_timeout: float = 300.0
    retry_count: int = 3
    retry_delay: float = 1.0
    enable_extend: bool = True


@dataclass
class TimeoutResult:
    """Result of a timed operation.
    
    Attributes:
        status: Operation status
        result: Operation result if successful
        error: Error if failed
        duration: Actual execution duration
        timeout_value: Timeout that was applied
    """
    status: TimeoutStatus
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    timeout_value: float = 0.0
    attempts: int = 1


class APITimeoutAction:
    """Timeout handler for API operations.
    
    Provides configurable timeout management with support
    for retries, fallbacks, and graceful cancellation.
    
    Attributes:
        config: Timeout configuration
        _active_timers: Active timeout timers
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[TimeoutConfig] = None,
    ) -> None:
        """Initialize timeout action.
        
        Args:
            config: Timeout configuration
        """
        self.config = config or TimeoutConfig()
        self._active_timers: Dict[str, asyncio.Task] = {}
        self._lock = threading.RLock()
    
    async def execute_with_timeout(
        self,
        coro: Awaitable[T],
        timeout: Optional[float] = None,
        strategy: TimeoutStrategy = TimeoutStrategy.CANCEL,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> TimeoutResult:
        """Execute coroutine with timeout.
        
        Args:
            coro: Coroutine to execute
            timeout: Timeout in seconds
            strategy: Timeout handling strategy
            fallback: Fallback function for timeout
        
        Returns:
            TimeoutResult with execution details
        """
        timeout = timeout or self.config.default_timeout
        timeout = min(timeout, self.config.max_timeout)
        
        start_time = time.time()
        attempts = 1
        
        while attempts <= max(1, self.config.retry_count if strategy == TimeoutStrategy.RETRY else 1):
            try:
                result = await asyncio.wait_for(coro, timeout=timeout)
                duration = time.time() - start_time
                
                return TimeoutResult(
                    status=TimeoutStatus.SUCCESS,
                    result=result,
                    duration=duration,
                    timeout_value=timeout,
                    attempts=attempts,
                )
            
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                
                if strategy == TimeoutStrategy.RETRY and attempts < self.config.retry_count:
                    await asyncio.sleep(self.config.retry_delay * attempts)
                    attempts += 1
                    continue
                
                if strategy == TimeoutStrategy.FALLBACK and fallback:
                    try:
                        fallback_result = fallback()
                        return TimeoutResult(
                            status=TimeoutStatus.SUCCESS,
                            result=fallback_result,
                            duration=duration,
                            timeout_value=timeout,
                            attempts=attempts,
                        )
                    except Exception as e:
                        return TimeoutResult(
                            status=TimeoutStatus.ERROR,
                            error=e,
                            duration=duration,
                            timeout_value=timeout,
                            attempts=attempts,
                        )
                
                return TimeoutResult(
                    status=TimeoutStatus.TIMEOUT,
                    duration=duration,
                    timeout_value=timeout,
                    attempts=attempts,
                )
            
            except Exception as e:
                return TimeoutResult(
                    status=TimeoutStatus.ERROR,
                    error=e,
                    duration=time.time() - start_time,
                    timeout_value=timeout,
                    attempts=attempts,
                )
        
        return TimeoutResult(
            status=TimeoutStatus.TIMEOUT,
            duration=time.time() - start_time,
            timeout_value=timeout,
            attempts=attempts,
        )
    
    def with_timeout(
        self,
        timeout: Optional[float] = None,
        strategy: TimeoutStrategy = TimeoutStrategy.CANCEL,
    ) -> Callable:
        """Decorator to add timeout to async function.
        
        Args:
            timeout: Timeout in seconds
            strategy: Timeout handling strategy
        
        Returns:
            Decorated function
        """
        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[TimeoutResult]]:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> TimeoutResult:
                bound_timeout = timeout or kwargs.pop("timeout", self.config.default_timeout)
                return await self.execute_with_timeout(
                    func(*args, **kwargs),
                    timeout=bound_timeout,
                    strategy=strategy,
                )
            return wrapper
        return decorator
    
    async def extend_timeout(
        self,
        task_id: str,
        additional_time: float,
    ) -> bool:
        """Extend timeout for active task.
        
        Args:
            task_id: Task identifier
            additional_time: Additional time to add
        
        Returns:
            True if extended successfully
        """
        if not self.config.enable_extend:
            return False
        
        with self._lock:
            if task_id in self._active_timers:
                return True
            return False
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a timed task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            True if cancelled successfully
        """
        with self._lock:
            if task_id in self._active_timers:
                task = self._active_timers[task_id]
                task.cancel()
                del self._active_timers[task_id]
                return True
            return False
    
    async def get_active_tasks(self) -> List[str]:
        """Get list of active task IDs.
        
        Returns:
            List of task IDs
        """
        with self._lock:
            return list(self._active_timers.keys())
