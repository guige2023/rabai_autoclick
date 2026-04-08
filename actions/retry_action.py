"""Retry action module for RabAI AutoClick.

Provides retry utilities:
- RetryPolicy: Configurable retry policies
- RetryExecutor: Execute with retry
- BackoffStrategy: Backoff strategies
"""

from typing import Any, Callable, Dict, List, Optional, Type
import time
import random
import threading
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackoffStrategy:
    """Base backoff strategy."""

    def next(self) -> float:
        """Get next delay."""
        raise NotImplementedError

    def reset(self) -> None:
        """Reset strategy."""
        raise NotImplementedError


class FixedBackoff(BackoffStrategy):
    """Fixed backoff."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    def next(self) -> float:
        return self.delay

    def reset(self) -> None:
        pass


class LinearBackoff(BackoffStrategy):
    """Linear backoff."""

    def __init__(self, start: float = 1.0, increment: float = 1.0):
        self.start = start
        self.increment = increment
        self._attempt = 0

    def next(self) -> float:
        delay = self.start + (self._attempt * self.increment)
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0


class ExponentialBackoff(BackoffStrategy):
    """Exponential backoff."""

    def __init__(self, base: float = 2.0, max_delay: float = 60.0, jitter: bool = True):
        self.base = base
        self.max_delay = max_delay
        self.jitter = jitter
        self._attempt = 0

    def next(self) -> float:
        delay = min(self.base ** self._attempt, self.max_delay)
        self._attempt += 1
        if self.jitter:
            delay *= (0.5 + random.random())
        return delay

    def reset(self) -> None:
        self._attempt = 0


class RetryPolicy:
    """Retry policy configuration."""

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialBackoff()
        self.retryable_exceptions = retryable_exceptions or [Exception]

    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Check if should retry."""
        if attempt >= self.max_attempts:
            return False
        return any(isinstance(exception, exc_type) for exc_type in self.retryable_exceptions)


class RetryExecutor:
    """Execute operations with retry."""

    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute with retry."""
        attempt = 0
        last_exception = None

        while attempt < self.policy.max_attempts:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if not self.policy.should_retry(attempt, e):
                    raise
                attempt += 1
                delay = self.policy.backoff.next()
                time.sleep(delay)

        if last_exception:
            raise last_exception


class RetryAction(BaseAction):
    """Retry management action."""
    action_type = "retry"
    display_name = "重试管理"
    description = "重试策略"

    def __init__(self):
        super().__init__()
        self._policies: Dict[str, RetryPolicy] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Retry error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create retry policy."""
        name = params.get("name", str(uuid.uuid4()))
        max_attempts = params.get("max_attempts", 3)
        backoff_type = params.get("backoff", "exponential")
        base = params.get("base", 2.0)
        max_delay = params.get("max_delay", 60.0)

        if backoff_type == "fixed":
            backoff = FixedBackoff(delay=params.get("delay", 1.0))
        elif backoff_type == "linear":
            backoff = LinearBackoff(start=params.get("start", 1.0), increment=params.get("increment", 1.0))
        elif backoff_type == "exponential":
            backoff = ExponentialBackoff(base=base, max_delay=max_delay)
        else:
            backoff = ExponentialBackoff()

        policy = RetryPolicy(max_attempts=max_attempts, backoff=backoff)
        self._policies[name] = policy

        return ActionResult(success=True, message=f"Retry policy created: {name}", data={"name": name})

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute with retry (simulated)."""
        name = params.get("name", "default")

        policy = self._policies.get(name)
        if not policy:
            return ActionResult(success=False, message=f"Policy not found: {name}")

        executor = RetryExecutor(policy)
        attempt = 0

        def mock_func():
            return {"executed": True, "attempt": attempt + 1}

        try:
            result = executor.execute(mock_func)
            return ActionResult(success=True, message="Executed with retry", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Failed after retries: {str(e)}")

    def _list(self) -> ActionResult:
        """List all policies."""
        names = list(self._policies.keys())
        return ActionResult(success=True, message=f"{len(names)} policies", data={"policies": names})
