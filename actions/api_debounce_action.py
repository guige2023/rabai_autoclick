"""
API Debounce Action Module

Provides debouncing functionality for API calls to prevent excessive requests.
Debouncing groups rapid successive calls into a single request.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar
from functools import wraps
import threading

T = TypeVar('T')
CallableType = TypeVar('CallableType', bound=Callable[..., Any])


@dataclass
class DebounceConfig:
    """Configuration for debounce behavior."""
    wait_ms: int = 300
    max_wait_ms: int = 5000
    leading_call: bool = False
    trailing_call: bool = True
    max_size: int = 1000


@dataclass
class DebounceCall:
    """Represents a debounced call."""
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    timestamp: float
    call_count: int = 1


class DebounceResult:
    """Result of a debounced operation."""
    
    def __init__(self, value: Any = None, error: Optional[Exception] = None,
                 call_count: int = 0, skipped: bool = False):
        self.value = value
        self.error = error
        self.call_count = call_count
        self.skipped = skipped
        self.timestamp = time.time()
    
    @property
    def success(self) -> bool:
        return self.error is None and not self.skipped
    
    def __repr__(self) -> str:
        status = "success" if self.success else "error" if self.error else "skipped"
        return f"DebounceResult({status}, call_count={self.call_count})"


class ApiDebounceAction:
    """
    Debounce manager for API calls.
    
    Groups rapid successive calls together and executes only the last one
    after the debounce period has elapsed.
    
    Example:
        debouncer = ApiDebounceAction(wait_ms=500)
        
        @debouncer.debounce()
        async def fetch_data(query: str):
            return await api.search(query)
    """
    
    def __init__(self, config: Optional[DebounceConfig] = None):
        self.config = config or DebounceConfig()
        self._pending: dict[str, DebounceCall] = {}
        self._results: dict[str, DebounceResult] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._timers: dict[str, asyncio.Task] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._history: deque[str] = deque(maxlen=self.config.max_size)
        self._stats = {
            "total_calls": 0,
            "debounced_calls": 0,
            "executed_calls": 0,
            "errors": 0
        }
        self._lock = threading.Lock()
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a given key."""
        with self._lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]
    
    def _generate_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """Generate a unique key for this call."""
        func_id = getattr(func, '__name__', str(id(func)))
        args_str = str(args) + str(sorted(kwargs.items()))
        return f"{func_id}:{hash(args_str)}"
    
    def debounce(self, key: Optional[str] = None, wait_ms: Optional[int] = None):
        """
        Decorator to debounce a function.
        
        Args:
            key: Optional custom key for grouping calls
            wait_ms: Override default wait time in milliseconds
        """
        def decorator(func: CallableType) -> CallableType:
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                debounce_key = key or self._generate_key(func, args, kwargs)
                wait = wait_ms or self.config.wait_ms
                
                self._stats["total_calls"] += 1
                
                async with self._get_lock(debounce_key):
                    self._pending[debounce_key] = DebounceCall(
                        args=args,
                        kwargs=kwargs,
                        timestamp=time.time(),
                        call_count=self._pending.get(debounce_key, DebounceCall((), {}, 0)).call_count + 1
                    )
                    
                    if debounce_key in self._timers:
                        self._timers[debounce_key].cancel()
                    
                    self._stats["debounced_calls"] += 1
                    
                    try:
                        await asyncio.sleep(wait / 1000.0)
                        
                        call = self._pending.pop(debounce_key, None)
                        if call:
                            self._stats["executed_calls"] += 1
                            result = await func(*call.args, **call.kwargs)
                            self._results[debounce_key] = DebounceResult(
                                value=result,
                                call_count=call.call_count
                            )
                            return result
                    except Exception as e:
                        self._stats["errors"] += 1
                        self._results[debounce_key] = DebounceResult(error=e)
                        raise
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                debounce_key = key or self._generate_key(func, args, kwargs)
                wait = wait_ms or self.config.wait_ms
                
                self._stats["total_calls"] += 1
                
                with self._get_lock(debounce_key):
                    self._pending[debounce_key] = DebounceCall(
                        args=args,
                        kwargs=kwargs,
                        timestamp=time.time(),
                        call_count=self._pending.get(debounce_key, DebounceCall((), {}, 0)).call_count + 1
                    )
                    
                    self._stats["debounced_calls"] += 1
                
                time.sleep(wait / 1000.0)
                
                call = self._pending.pop(debounce_key, None)
                if call:
                    self._stats["executed_calls"] += 1
                    try:
                        result = func(*call.args, **call.kwargs)
                        self._results[debounce_key] = DebounceResult(
                            value=result,
                            call_count=call.call_count
                        )
                        return result
                    except Exception as e:
                        self._stats["errors"] += 1
                        self._results[debounce_key] = DebounceResult(error=e)
                        raise
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper
        
        return decorator
    
    async def flush(self, key: str) -> Optional[DebounceResult]:
        """
        Immediately execute pending call for a key.
        
        Args:
            key: The debounce key to flush
            
        Returns:
            DebounceResult from the executed call
        """
        async with self._get_lock(key):
            call = self._pending.pop(key, None)
            if call:
                try:
                    result = await call.func(*call.args, **call.kwargs)
                    return DebounceResult(value=result, call_count=call.call_count)
                except Exception as e:
                    return DebounceResult(error=e)
        return None
    
    def get_result(self, key: str) -> Optional[DebounceResult]:
        """Get the last result for a key."""
        return self._results.get(key)
    
    def get_stats(self) -> dict[str, Any]:
        """Get debounce statistics."""
        return {
            **self._stats,
            "pending_count": len(self._pending),
            "debounce_rate": (
                self._stats["debounced_calls"] / self._stats["total_calls"]
                if self._stats["total_calls"] > 0 else 0
            )
        }
    
    def clear(self) -> None:
        """Clear all pending calls and results."""
        self._pending.clear()
        self._results.clear()
        self._history.clear()
    
    def cancel_all(self) -> None:
        """Cancel all pending timers."""
        for timer in self._timers.values():
            timer.cancel()
        self._timers.clear()


class DebounceGroup:
    """
    Group multiple debouncers together.
    
    Allows sharing debounce state across related operations.
    """
    
    def __init__(self):
        self._debouncers: dict[str, ApiDebounceAction] = {}
        self._shared_state: dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def get_debouncer(self, name: str, config: Optional[DebounceConfig] = None) -> ApiDebounceAction:
        """Get or create a named debouncer."""
        with self._lock:
            if name not in self._debouncers:
                self._debouncers[name] = ApiDebounceAction(config)
            return self._debouncers[name]
    
    def share_state(self, key: str, value: Any) -> None:
        """Share state across debouncers in the group."""
        with self._lock:
            self._shared_state[key] = value
    
    def get_shared_state(self, key: str, default: Any = None) -> Any:
        """Get shared state."""
        return self._shared_state.get(key, default)
    
    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get stats from all debouncers in the group."""
        return {name: d.get_stats() for name, d in self._debouncers.items()}
