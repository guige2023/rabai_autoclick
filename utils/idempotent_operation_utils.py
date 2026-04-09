"""
Idempotent Operation Utilities for UI Automation.

This module provides utilities for making operations idempotent,
ensuring that executing the same operation multiple times
produces the same result.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, TypeVar, Generic
from enum import Enum
import threading


T = TypeVar("T")


class IdempotencyStatus(Enum):
    """Status of idempotent operation."""
    NEW = "new"
    COMPLETED = "completed"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"


@dataclass
class IdempotentResult(Generic[T]):
    """Result of an idempotent operation."""
    status: IdempotencyStatus
    value: Optional[T] = None
    error: Optional[str] = None
    execution_count: int = 0
    first_execution_time: Optional[float] = None
    last_execution_time: Optional[float] = None


@dataclass
class IdempotencyConfig:
    """Configuration for idempotent operations."""
    ttl_seconds: float = 3600.0
    max_retries: int = 3
    retry_delay: float = 1.0
    cache_result: bool = True


class IdempotentOperation(Generic[T]):
    """
    Wrapper for making operations idempotent.
    """

    def __init__(
        self,
        key: str,
        config: Optional[IdempotencyConfig] = None
    ):
        """
        Initialize idempotent operation.

        Args:
            key: Unique key for this operation
            config: Idempotency configuration
        """
        self.key = key
        self.config = config or IdempotencyConfig()
        self._status: IdempotencyStatus = IdempotencyStatus.NEW
        self._result: Optional[T] = None
        self._error: Optional[str] = None
        self._execution_count: int = 0
        self._first_execution_time: Optional[float] = None
        self._lock = threading.Lock()

    def execute(self, func: Callable[[], T]) -> IdempotentResult[T]:
        """
        Execute operation with idempotency.

        Args:
            func: Operation to execute

        Returns:
            IdempotentResult
        """
        with self._lock:
            current_time = time.time()

            if self._status == IdempotencyStatus.COMPLETED:
                if self.config.cache_result:
                    return IdempotentResult(
                        status=IdempotencyStatus.COMPLETED,
                        value=self._result,
                        execution_count=self._execution_count,
                        first_execution_time=self._first_execution_time,
                        last_execution_time=self._last_execution_time
                    )

            if self._status == IdempotencyStatus.IN_PROGRESS:
                return IdempotentResult(
                    status=IdempotencyStatus.IN_PROGRESS,
                    error="Operation already in progress",
                    execution_count=self._execution_count
                )

            self._status = IdempotencyStatus.IN_PROGRESS
            self._execution_count += 1

            if self._first_execution_time is None:
                self._first_execution_time = current_time

            self._last_execution_time = current_time

        try:
            result = func()

            with self._lock:
                self._result = result
                self._status = IdempotencyStatus.COMPLETED
                self._error = None

            return IdempotentResult(
                status=IdempotencyStatus.COMPLETED,
                value=result,
                execution_count=self._execution_count,
                first_execution_time=self._first_execution_time,
                last_execution_time=current_time
            )

        except Exception as e:
            with self._lock:
                self._status = IdempotencyStatus.FAILED
                self._error = str(e)

            return IdempotentResult(
                status=IdempotencyStatus.FAILED,
                error=str(e),
                execution_count=self._execution_count,
                first_execution_time=self._first_execution_time,
                last_execution_time=current_time
            )

    def reset(self) -> None:
        """Reset idempotent state."""
        with self._lock:
            self._status = IdempotencyStatus.NEW
            self._result = None
            self._error = None
            self._execution_count = 0
            self._first_execution_time = None
            self._last_execution_time = None

    @property
    def status(self) -> IdempotencyStatus:
        """Get current status."""
        return self._status


class IdempotencyManager:
    """
    Manage multiple idempotent operations.
    """

    def __init__(self, config: Optional[IdempotencyConfig] = None):
        """
        Initialize manager.

        Args:
            config: Default configuration
        """
        self.config = config or IdempotencyConfig()
        self._operations: Dict[str, IdempotentOperation] = {}
        self._lock = threading.Lock()

    def get_operation(self, key: str) -> IdempotentOperation:
        """Get or create idempotent operation."""
        with self._lock:
            if key not in self._operations:
                self._operations[key] = IdempotentOperation(key, self.config)
            return self._operations[key]

    def execute(
        self,
        key: str,
        func: Callable[[], T]
    ) -> IdempotentResult[T]:
        """
        Execute function with idempotency guarantee.

        Args:
            key: Operation key
            func: Function to execute

        Returns:
            IdempotentResult
        """
        op = self.get_operation(key)
        return op.execute(func)

    def clear(self) -> None:
        """Clear all operations."""
        with self._lock:
            self._operations.clear()


def compute_operation_key(
    *args: Any,
    **kwargs: Any
) -> str:
    """
    Compute a deterministic key for an operation.

    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Operation key string
    """
    key_parts = [str(arg) for arg in args]
    key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))

    key_str = "|".join(key_parts)
    return hashlib.sha256(key_str.encode()).hexdigest()[:16]


def idempotent(
    key: str,
    config: Optional[IdempotencyConfig] = None
) -> Callable:
    """
    Decorator for making functions idempotent.

    Args:
        key: Idempotency key
        config: Configuration

    Returns:
        Decorator function
    """
    manager = IdempotencyManager(config)
    operation = manager.get_operation(key)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            def execute():
                return func(*args, **kwargs)
            result = operation.execute(execute)
            if result.status == IdempotencyStatus.FAILED:
                raise Exception(result.error)
            return result.value
        return wrapper

    return decorator
