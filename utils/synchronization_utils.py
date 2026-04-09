"""
Synchronization Utilities for UI Automation.

This module provides utilities for synchronizing automation actions,
managing waits, and coordinating timing in workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class WaitStrategy(Enum):
    """Wait strategies for element synchronization."""
    FIXED = auto()
    EXPONENTIAL = auto()
    INCREMENTAL = auto()
    ADAPTIVE = auto()


@dataclass
class WaitConfig:
    """
    Configuration for wait operations.
    
    Attributes:
        timeout: Maximum wait time in seconds
        poll_interval: How often to check condition
        max_attempts: Maximum number of attempts
        strategy: Wait strategy to use
    """
    timeout: float = 10.0
    poll_interval: float = 0.5
    max_attempts: int = 20
    strategy: WaitStrategy = WaitStrategy.FIXED
    
    def get_interval(self, attempt: int) -> float:
        """Get the interval for a specific attempt based on strategy."""
        if self.strategy == WaitStrategy.FIXED:
            return self.poll_interval
        elif self.strategy == WaitStrategy.EXPONENTIAL:
            return min(self.poll_interval * (2 ** attempt), self.timeout)
        elif self.strategy == WaitStrategy.INCREMENTAL:
            return self.poll_interval * (attempt + 1)
        elif self.strategy == WaitStrategy.ADAPTIVE:
            # Adaptive: start fast, slow down as timeout approaches
            remaining = self.timeout - (attempt * self.poll_interval)
            return min(self.poll_interval * 2, remaining / 2) if remaining > 0 else 0.1
        return self.poll_interval


class ConditionWaiter:
    """
    Waits for a condition to be met.
    
    Example:
        waiter = ConditionWaiter(WaitConfig(timeout=30))
        element = waiter.wait_for(lambda: driver.find_element(By.ID, "btn"))
    """
    
    def __init__(self, config: Optional[WaitConfig] = None):
        self.config = config or WaitConfig()
    
    def wait_for(
        self,
        condition: Callable[[], Any],
        on_timeout: Optional[Callable[[], Any]] = None
    ) -> tuple[bool, Any]:
        """
        Wait for a condition to be met.
        
        Args:
            condition: Function that returns truthy when condition is met
            on_timeout: Optional callback when timeout occurs
            
        Returns:
            Tuple of (success, result)
        """
        start_time = time.time()
        attempt = 0
        
        while True:
            try:
                result = condition()
                if result:
                    return True, result
            except Exception:
                pass
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed >= self.config.timeout:
                if on_timeout:
                    return False, on_timeout()
                return False, None
            
            # Wait before next attempt
            interval = self.config.get_interval(attempt)
            time.sleep(interval)
            attempt += 1
    
    def wait_for_true(
        self,
        condition: Callable[[], bool],
        timeout: Optional[float] = None
    ) -> bool:
        """
        Wait for a condition to return True.
        
        Args:
            condition: Function that returns bool
            timeout: Optional override timeout
            
        Returns:
            True if condition met, False on timeout
        """
        start_time = time.time()
        use_timeout = timeout if timeout is not None else self.config.timeout
        
        while time.time() - start_time < use_timeout:
            try:
                if condition():
                    return True
            except Exception:
                pass
            time.sleep(self.config.poll_interval)
        
        return False
    
    def wait_for_value(
        self,
        get_value: Callable[[], Any],
        expected: Any,
        timeout: Optional[float] = None
    ) -> tuple[bool, Any]:
        """
        Wait for a value to match expected.
        
        Args:
            get_value: Function to get current value
            expected: Expected value
            timeout: Optional override timeout
            
        Returns:
            Tuple of (success, current_value)
        """
        start_time = time.time()
        use_timeout = timeout if timeout is not None else self.config.timeout
        
        while time.time() - start_time < use_timeout:
            try:
                value = get_value()
                if value == expected:
                    return True, value
            except Exception:
                pass
            time.sleep(self.config.poll_interval)
        
        try:
            return False, get_value()
        except Exception:
            return False, None


class EventWaiter:
    """
    Wait for events with callbacks.
    
    Example:
        waiter = EventWaiter()
        waiter.register_callback("login_complete", on_login)
        waiter.wait("login_complete", timeout=30)
    """
    
    def __init__(self):
        self._callbacks: dict[str, list[Callable[[], None]]] = {}
        self._event_flags: dict[str, threading.Event] = {}
        self._results: dict[str, Any] = {}
    
    def register_callback(
        self,
        event_name: str,
        callback: Callable[[], None]
    ) -> None:
        """Register a callback for an event."""
        self._callbacks.setdefault(event_name, []).append(callback)
        self._event_flags.setdefault(event_name, threading.Event())
    
    def trigger(self, event_name: str, result: Optional[Any] = None) -> bool:
        """
        Trigger an event.
        
        Args:
            event_name: Name of the event
            result: Optional result to store
            
        Returns:
            True if event was triggered
        """
        if event_name not in self._event_flags:
            return False
        
        self._results[event_name] = result
        
        # Call callbacks
        for callback in self._callbacks.get(event_name, []):
            try:
                callback()
            except Exception:
                pass
        
        # Signal waiting threads
        self._event_flags[event_name].set()
        
        return True
    
    def wait(
        self,
        event_name: str,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Wait for an event to be triggered.
        
        Args:
            event_name: Name of the event
            timeout: Optional timeout in seconds
            
        Returns:
            True if event was triggered, False on timeout
        """
        if event_name not in self._event_flags:
            return False
        
        return self._event_flags[event_name].wait(timeout=timeout)
    
    def get_result(self, event_name: str) -> Any:
        """Get the result associated with an event."""
        return self._results.get(event_name)
    
    def is_set(self, event_name: str) -> bool:
        """Check if an event has been triggered."""
        if event_name not in self._event_flags:
            return False
        return self._event_flags[event_name].is_set()
    
    def clear(self, event_name: str) -> None:
        """Clear an event."""
        if event_name in self._event_flags:
            self._event_flags[event_name].clear()


class AsyncEventWaiter:
    """
    Async version of EventWaiter for asyncio contexts.
    """
    
    def __init__(self):
        self._callbacks: dict[str, list[Callable[[], Any]]] = {}
        self._futures: dict[str, asyncio.Future] = {}
        self._results: dict[str, Any] = {}
    
    async def wait(
        self,
        event_name: str,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Wait for an event asynchronously.
        
        Args:
            event_name: Name of the event
            timeout: Optional timeout
            
        Returns:
            Event result
        """
        future = asyncio.get_event_loop().create_future()
        self._futures[event_name] = future
        
        try:
            if timeout:
                await asyncio.wait_for(future, timeout=timeout)
            else:
                await future
        except asyncio.TimeoutError:
            raise TimeoutError(f"Event '{event_name}' timed out after {timeout}s")
        
        return self._results.get(event_name)
    
    def trigger(self, event_name: str, result: Any = None) -> None:
        """Trigger an async event."""
        self._results[event_name] = result
        
        if event_name in self._futures:
            self._futures[event_name].set_result(result)


class Barrier:
    """
    Synchronization barrier for coordinating multiple actions.
    
    Example:
        barrier = Barrier(parties=3)
        # In 3 threads:
        barrier.wait()  # All 3 threads wait here until all arrive
    """
    
    def __init__(self, parties: int):
        self._parties = parties
        self._count = 0
        self._lock = threading.Lock()
        self._turn = threading.Condition(self._lock)
    
    def wait(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all parties to arrive.
        
        Args:
            timeout: Optional timeout
            
        Returns:
            True if all parties arrived, False on timeout
        """
        with self._turn:
            self._count += 1
            
            if self._count >= self._parties:
                self._turn.notify_all()
                return True
            
            try:
                self._turn.wait(timeout=timeout)
                return True
            except threading.TimeoutError:
                return False
    
    @property
    def parties(self) -> int:
        """Get number of parties."""
        return self._parties
    
    @property
    def waiting(self) -> int:
        """Get number of waiting parties."""
        with self._lock:
            return max(0, self._count - 1)
