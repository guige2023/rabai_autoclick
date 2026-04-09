"""
Timeout Handling Utilities for UI Automation.

This module provides utilities for managing timeouts in automation workflows,
including timeout configuration, timeout watchers, and timeout callbacks.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import signal
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class TimeoutError(Exception):
    """Raised when an operation times out."""
    pass


class TimeoutStrategy(Enum):
    """Timeout handling strategies."""
    RAISE = auto()      # Raise TimeoutError
    RETURN_NONE = auto()  # Return None on timeout
    CALLBACK = auto()   # Call timeout callback
    EXTEND = auto()     # Extend timeout


@dataclass
class TimeoutConfig:
    """
    Configuration for timeout handling.
    
    Attributes:
        timeout: Timeout duration in seconds
        strategy: How to handle timeout
        callback: Optional timeout callback
        on_timeout_return: Value to return on timeout (for RETURN_NONE strategy)
    """
    timeout: float = 30.0
    strategy: TimeoutStrategy = TimeoutStrategy.RAISE
    callback: Optional[Callable[[], Any]] = None
    on_timeout_return: Any = None


class TimeoutContext:
    """
    Context manager for timeout handling.
    
    Example:
        with TimeoutContext(timeout=10) as ctx:
            result = long_running_operation()
    """
    
    def __init__(
        self,
        timeout: float,
        strategy: TimeoutStrategy = TimeoutStrategy.RAISE,
        on_timeout: Optional[Callable[[], Any]] = None
    ):
        self.timeout = timeout
        self.strategy = strategy
        self.on_timeout = on_timeout
        self._start_time: Optional[float] = None
        self._remaining: float = timeout
    
    def __enter__(self) -> 'TimeoutContext':
        self._start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            elapsed = time.time() - self._start_time
            self._remaining = max(0, self.timeout - elapsed)
            
            if self.strategy == TimeoutStrategy.RAISE:
                if issubclass(exc_type, TimeoutError):
                    return True  # Suppress TimeoutError
            elif self.strategy == TimeoutStrategy.CALLBACK:
                if self.on_timeout:
                    self.on_timeout()
                return True
        
        return False
    
    @property
    def remaining(self) -> float:
        """Get remaining time."""
        if self._start_time:
            return max(0, self.timeout - (time.time() - self._start_time))
        return self.timeout


class TimeoutWatcher:
    """
    Watches for timeout conditions.
    
    Example:
        watcher = TimeoutWatcher()
        watcher.start(timeout=30)
        # Do work
        if watcher.is_expired():
            handle_timeout()
    """
    
    def __init__(self):
        self._start_time: Optional[float] = None
        self._timeout: float = 0
        self._running = False
        self._callbacks: list[Callable[[], None]] = []
    
    def start(self, timeout: float) -> None:
        """
        Start watching.
        
        Args:
            timeout: Timeout duration in seconds
        """
        self._timeout = timeout
        self._start_time = time.time()
        self._running = True
    
    def stop(self) -> None:
        """Stop watching."""
        self._running = False
    
    def reset(self) -> None:
        """Reset the watcher."""
        self._start_time = None
        self._running = False
    
    def is_expired(self) -> bool:
        """Check if timeout has expired."""
        if not self._running or self._start_time is None:
            return False
        
        elapsed = time.time() - self._start_time
        return elapsed >= self._timeout
    
    def remaining(self) -> float:
        """Get remaining time."""
        if not self._running or self._start_time is None:
            return 0
        
        elapsed = time.time() - self._start_time
        return max(0, self._timeout - elapsed)
    
    def add_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to be called on timeout."""
        self._callbacks.append(callback)
    
    def check_and_trigger(self) -> bool:
        """
        Check if expired and trigger callbacks if needed.
        
        Returns:
            True if expired and callbacks were triggered
        """
        if self.is_expired():
            for callback in self._callbacks:
                callback()
            return True
        return False


class SignalTimeout:
    """
    Signal-based timeout for Unix systems.
    
    Example:
        with SignalTimeout(5):
            long_running_operation()
    """
    
    def __init__(self, timeout: float):
        self.timeout = timeout
        self._old_handler: Optional[signal.Handler] = None
    
    def __enter__(self) -> None:
        def handler(signum, frame):
            raise TimeoutError(f"Operation timed out after {self.timeout} seconds")
        
        self._old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(int(self.timeout))
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        signal.alarm(0)
        if self._old_handler:
            signal.signal(signal.SIGALRM, self._old_handler)
        
        if exc_type is TimeoutError:
            return True
        
        return False


class TimeoutManager:
    """
    Manages multiple timeout operations.
    
    Example:
        manager = TimeoutManager()
        task1 = manager.add_timeout("task1", 30)
        task2 = manager.add_timeout("task2", 60)
        
        manager.cancel("task1")
    """
    
    def __init__(self):
        self._timeouts: dict[str, TimeoutWatcher] = {}
        self._lock = threading.Lock()
    
    def add_timeout(
        self,
        name: str,
        timeout: float,
        callback: Optional[Callable[[], None]] = None
    ) -> TimeoutWatcher:
        """
        Add a named timeout.
        
        Args:
            name: Timeout name
            timeout: Duration in seconds
            callback: Optional callback
            
        Returns:
            TimeoutWatcher
        """
        with self._lock:
            watcher = TimeoutWatcher()
            if callback:
                watcher.add_callback(callback)
            watcher.start(timeout)
            self._timeouts[name] = watcher
            return watcher
    
    def remove_timeout(self, name: str) -> bool:
        """Remove a named timeout."""
        with self._lock:
            if name in self._timeouts:
                self._timeouts[name].stop()
                del self._timeouts[name]
                return True
            return False
    
    def get_timeout(self, name: str) -> Optional[TimeoutWatcher]:
        """Get a named timeout watcher."""
        return self._timeouts.get(name)
    
    def is_expired(self, name: str) -> bool:
        """Check if a named timeout is expired."""
        watcher = self._timeouts.get(name)
        return watcher.is_expired() if watcher else True
    
    def remaining(self, name: str) -> float:
        """Get remaining time for a named timeout."""
        watcher = self._timeouts.get(name)
        return watcher.remaining() if watcher else 0
    
    def cancel_all(self) -> None:
        """Cancel all timeouts."""
        with self._lock:
            for watcher in self._timeouts.values():
                watcher.stop()
            self._timeouts.clear()


def with_timeout(
    timeout: float,
    strategy: TimeoutStrategy = TimeoutStrategy.RAISE,
    on_timeout: Optional[Callable[[], Any]] = None
) -> Callable[[Callable[[], T]], Callable[[], T]]:
    """
    Decorator to add timeout to a function.
    
    Example:
        @with_timeout(timeout=10)
        def long_running_task():
            return do_work()
    """
    def decorator(func: Callable[[], T]) -> Callable[[], T]:
        def wrapper() -> T:
            config = TimeoutConfig(
                timeout=timeout,
                strategy=strategy,
                callback=on_timeout
            )
            
            start_time = time.time()
            
            while True:
                try:
                    return func()
                except TimeoutError:
                    if strategy == TimeoutStrategy.RAISE:
                        raise
                    elif strategy == TimeoutStrategy.CALLBACK:
                        if on_timeout:
                            on_timeout()
                    elif strategy == TimeoutStrategy.RETURN_NONE:
                        return config.on_timeout_return
                
                # Check elapsed time
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    if strategy == TimeoutStrategy.RAISE:
                        raise TimeoutError(f"Function timed out after {timeout} seconds")
                    elif strategy == TimeoutStrategy.CALLBACK:
                        if on_timeout:
                            on_timeout()
                    elif strategy == TimeoutStrategy.RETURN_NONE:
                        return config.on_timeout_return
                
                time.sleep(0.01)
        
        return wrapper
    return decorator
