"""
Operation Timeout Utilities

Provides utilities for timeout management
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, TypeVar
import asyncio
import time

T = TypeVar("T")


class OperationTimeout:
    """
    Manages timeouts for operations.
    
    Wraps operations with timeout enforcement
    and provides timeout callbacks.
    """

    def __init__(self, default_timeout_ms: float = 30000.0) -> None:
        self._default_timeout = default_timeout_ms / 1000.0
        self._handlers: dict[str, Callable[[], None]] = {}

    def set_default_timeout(self, timeout_ms: float) -> None:
        """Set default timeout in milliseconds."""
        self._default_timeout = timeout_ms / 1000.0

    def register_timeout_handler(
        self,
        operation_id: str,
        handler: Callable[[], None],
    ) -> None:
        """Register a handler to call on timeout."""
        self._handlers[operation_id] = handler

    async def run_with_timeout(
        self,
        operation: Callable[..., T],
        *args: Any,
        timeout_ms: float | None = None,
        **kwargs: Any,
    ) -> T:
        """
        Run operation with timeout.
        
        Args:
            operation: Callable to execute.
            timeout_ms: Timeout in milliseconds.
            
        Returns:
            Operation result.
            
        Raises:
            asyncio.TimeoutError: If operation times out.
        """
        timeout = (timeout_ms or self._default_timeout * 1000) / 1000.0
        result = await asyncio.wait_for(
            asyncio.to_thread(operation, *args, **kwargs),
            timeout=timeout,
        )
        return result

    def execute_sync(
        self,
        operation: Callable[..., T],
        *args: Any,
        timeout_ms: float | None = None,
        **kwargs: Any,
    ) -> T | None:
        """
        Run synchronous operation with timeout.
        
        Returns:
            Operation result or None if timeout.
        """
        timeout = (timeout_ms or self._default_timeout * 1000) / 1000.0
        start = time.time()

        def worker() -> T:
            return operation(*args, **kwargs)

        future = asyncio.run_coroutine_threadsafe(
            asyncio.to_thread(worker),
            asyncio.new_event_loop(),
        )
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            return None
