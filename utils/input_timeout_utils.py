"""Input timeout utilities for UI automation.

Provides utilities for managing timeouts, deadlines,
and operation duration tracking for UI automation.
"""

from __future__ import annotations

import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any


@dataclass
class TimeoutConfig:
    """Configuration for timeout behavior."""
    default_timeout_ms: float = 30000.0
    min_timeout_ms: float = 100.0
    max_timeout_ms: float = 300000.0
    enable_adaptive: bool = True
    adaptation_factor: float = 1.5


@dataclass
class OperationTimer:
    """Tracks the timing of an operation."""
    name: str
    start_time_ms: float
    end_time_ms: Optional[float] = None
    timeout_ms: float = 0.0
    is_complete: bool = False
    
    @property
    def duration_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        if self.end_time_ms is None:
            return (time.time() * 1000) - self.start_time_ms
        return self.end_time_ms - self.start_time_ms
    
    @property
    def remaining_ms(self) -> float:
        """Get remaining time until timeout."""
        if self.timeout_ms <= 0:
            return float('inf')
        return max(0, self.timeout_ms - self.duration_ms)
    
    @property
    def is_expired(self) -> bool:
        """Check if timeout has expired."""
        return self.timeout_ms > 0 and self.duration_ms >= self.timeout_ms


class TimeoutManager:
    """Manages timeouts for UI automation operations.
    
    Provides utilities for tracking timeouts, deadlines,
    and adaptive timeout adjustment.
    """
    
    def __init__(self, config: Optional[TimeoutConfig] = None) -> None:
        """Initialize the timeout manager.
        
        Args:
            config: Timeout configuration.
        """
        self.config = config or TimeoutConfig()
        self._active_timers: Dict[str, OperationTimer] = {}
        self._history: List[OperationTimer] = []
        self._lock = threading.Lock()
        self._adaptive_base: Dict[str, float] = {}
    
    def start_timer(
        self,
        name: str,
        timeout_ms: Optional[float] = None
    ) -> OperationTimer:
        """Start a new operation timer.
        
        Args:
            name: Operation name.
            timeout_ms: Timeout in milliseconds.
            
        Returns:
            Created timer.
        """
        timeout = timeout_ms if timeout_ms is not None else self.config.default_timeout_ms
        
        timeout = max(self.config.min_timeout_ms, min(self.config.max_timeout_ms, timeout))
        
        if self.config.enable_adaptive and name in self._adaptive_base:
            timeout = self._adaptive_base[name] * self.config.adaptation_factor
        
        with self._lock:
            timer = OperationTimer(
                name=name,
                start_time_ms=time.time() * 1000,
                timeout_ms=timeout
            )
            self._active_timers[name] = timer
            return timer
    
    def stop_timer(self, name: str) -> Optional[OperationTimer]:
        """Stop a timer and record its history.
        
        Args:
            name: Operation name.
            
        Returns:
            Stopped timer or None.
        """
        with self._lock:
            if name not in self._active_timers:
                return None
            
            timer = self._active_timers[name]
            timer.end_time_ms = time.time() * 1000
            timer.is_complete = True
            
            del self._active_timers[name]
            self._history.append(timer)
            
            if self.config.enable_adaptive:
                if name in self._adaptive_base:
                    old_avg = self._adaptive_base[name]
                    self._adaptive_base[name] = (
                        old_avg * 0.7 + timer.duration_ms * 0.3
                    )
                else:
                    self._adaptive_base[name] = timer.duration_ms
            
            return timer
    
    def get_timer(self, name: str) -> Optional[OperationTimer]:
        """Get an active timer by name.
        
        Args:
            name: Operation name.
            
        Returns:
            Timer or None.
        """
        with self._lock:
            return self._active_timers.get(name)
    
    def is_expired(self, name: str) -> bool:
        """Check if a timer has expired.
        
        Args:
            name: Operation name.
            
        Returns:
            True if expired.
        """
        with self._lock:
            timer = self._active_timers.get(name)
            if timer is None:
                return True
            return timer.is_expired
    
    def get_remaining_time(self, name: str) -> float:
        """Get remaining time for a timer.
        
        Args:
            name: Operation name.
            
        Returns:
            Remaining time in milliseconds.
        """
        with self._lock:
            timer = self._active_timers.get(name)
            if timer is None:
                return 0.0
            return timer.remaining_ms
    
    def cancel_timer(self, name: str) -> bool:
        """Cancel a timer without recording history.
        
        Args:
            name: Operation name.
            
        Returns:
            True if cancelled.
        """
        with self._lock:
            if name in self._active_timers:
                del self._active_timers[name]
                return True
            return False
    
    def get_history(
        self,
        name: Optional[str] = None,
        limit: int = 100
    ) -> List[OperationTimer]:
        """Get operation history.
        
        Args:
            name: Filter by operation name.
            limit: Maximum number of entries.
            
        Returns:
            List of historical timers.
        """
        with self._lock:
            history = self._history
            
            if name:
                history = [t for t in history if t.name == name]
            
            return history[-limit:]
    
    def get_average_duration(self, name: str) -> float:
        """Get average duration for an operation.
        
        Args:
            name: Operation name.
            
        Returns:
            Average duration in milliseconds.
        """
        with self._lock:
            history = [t for t in self._history if t.name == name]
            if not history:
                return self._adaptive_base.get(name, self.config.default_timeout_ms)
            
            return sum(t.duration_ms for t in history) / len(history)
    
    def clear_history(self) -> None:
        """Clear operation history."""
        with self._lock:
            self._history.clear()
    
    def reset_adaptive(self) -> None:
        """Reset adaptive timeout values."""
        with self._lock:
            self._adaptive_base.clear()


class Deadline:
    """Represents a deadline for an operation."""
    
    def __init__(self, timeout_ms: float) -> None:
        """Initialize deadline.
        
        Args:
            timeout_ms: Timeout in milliseconds.
        """
        self._start_ms = time.time() * 1000
        self._timeout_ms = timeout_ms
    
    @property
    def remaining_ms(self) -> float:
        """Get remaining time in milliseconds."""
        elapsed = (time.time() * 1000) - self._start_ms
        return max(0, self._timeout_ms - elapsed)
    
    @property
    def is_expired(self) -> bool:
        """Check if deadline has expired."""
        return self.remaining_ms <= 0
    
    def wait(self) -> float:
        """Wait until deadline expires.
        
        Returns:
            Actual time waited in seconds.
        """
        remaining = self.remaining_ms / 1000.0
        if remaining > 0:
            time.sleep(remaining)
        return remaining


@contextmanager
def timeout_context(
    name: str,
    timeout_ms: float,
    manager: Optional[TimeoutManager] = None
):
    """Context manager for timeout tracking.
    
    Args:
        name: Operation name.
        timeout_ms: Timeout in milliseconds.
        manager: Timeout manager (creates new if None).
        
    Yields:
        OperationTimer.
    """
    if manager is None:
        manager = TimeoutManager()
    
    timer = manager.start_timer(name, timeout_ms)
    try:
        yield timer
    finally:
        manager.stop_timer(name)


class RetryWithTimeout:
    """Retries operations until they succeed or timeout.
    
    Provides utilities for retrying operations with
    timeout and backoff strategies.
    """
    
    def __init__(
        self,
        timeout_ms: float = 30000.0,
        initial_delay_ms: float = 100.0,
        max_delay_ms: float = 5000.0,
        backoff_factor: float = 2.0,
        max_attempts: int = 0
    ) -> None:
        """Initialize retry handler.
        
        Args:
            timeout_ms: Maximum time to retry.
            initial_delay_ms: Initial delay between retries.
            max_delay_ms: Maximum delay between retries.
            backoff_factor: Multiplier for delay after each retry.
            max_attempts: Maximum attempts (0 for unlimited).
        """
        self.timeout_ms = timeout_ms
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms
        self.backoff_factor = backoff_factor
        self.max_attempts = max_attempts
    
    def execute(
        self,
        operation: Callable[[], Any],
        should_retry: Optional[Callable[[Exception], bool]] = None
    ) -> Any:
        """Execute operation with retries.
        
        Args:
            operation: Operation to execute.
            should_retry: Function to determine if error should retry.
            
        Returns:
            Operation result.
            
        Raises:
            Exception: Last exception if all retries fail.
        """
        deadline = Deadline(self.timeout_ms)
        delay = self.initial_delay_ms
        attempt = 0
        last_error: Optional[Exception] = None
        
        while True:
            attempt += 1
            
            try:
                return operation()
            except Exception as e:
                last_error = e
                
                if should_retry and not should_retry(e):
                    raise
                
                if self.max_attempts > 0 and attempt >= self.max_attempts:
                    raise
                
                if deadline.is_expired:
                    raise
                
                wait_time = min(delay, deadline.remaining_ms / 1000.0)
                if wait_time > 0:
                    time.sleep(wait_time)
                
                delay = min(delay * self.backoff_factor, self.max_delay_ms)
        
        if last_error:
            raise last_error


def wait_for_condition(
    condition: Callable[[], bool],
    timeout_ms: float = 30000.0,
    poll_interval_ms: float = 100.0,
    error_message: Optional[str] = None
) -> bool:
    """Wait for a condition to become true.
    
    Args:
        condition: Function that returns True when done.
        timeout_ms: Maximum time to wait.
        poll_interval_ms: Time between checks.
        error_message: Error message if timeout.
        
    Returns:
        True if condition met.
        
    Raises:
        TimeoutError: If timeout expires.
    """
    deadline = Deadline(timeout_ms)
    
    while not condition():
        if deadline.is_expired:
            msg = error_message or "Timeout waiting for condition"
            raise TimeoutError(msg)
        
        wait_time = min(poll_interval_ms, deadline.remaining_ms) / 1000.0
        if wait_time > 0:
            time.sleep(wait_time)
    
    return True


def wait_for_element(
    finder: Callable[[], Optional[Any]],
    timeout_ms: float = 30000.0,
    poll_interval_ms: float = 100.0
) -> Optional[Any]:
    """Wait for an element to be found.
    
    Args:
        finder: Function to find element.
        timeout_ms: Maximum time to wait.
        poll_interval_ms: Time between checks.
        
    Returns:
        Found element or None if timeout.
    """
    deadline = Deadline(timeout_ms)
    
    while True:
        element = finder()
        if element is not None:
            return element
        
        if deadline.is_expired:
            return None
        
        wait_time = min(poll_interval_ms, deadline.remaining_ms) / 1000.0
        if wait_time > 0:
            time.sleep(wait_time)
