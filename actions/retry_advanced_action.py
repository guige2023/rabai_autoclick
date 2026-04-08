"""Retry Advanced Action Module.

Provides advanced retry logic with exponential backoff,
jitter, and retry budgets.
"""

import time
import random
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RetryStrategy(Enum):
    """Retry strategy."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL


@dataclass
class RetryAttempt:
    """Retry attempt record."""
    attempt: int
    delay: float
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class RetryManager:
    """Manages retry logic."""

    def __init__(self):
        self._attempts: Dict[str, List[RetryAttempt]] = {}

    def calculate_delay(
        self,
        attempt: int,
        config: RetryConfig
    ) -> float:
        """Calculate delay for attempt."""
        if config.strategy == RetryStrategy.FIXED:
            delay = config.initial_delay
        elif config.strategy == RetryStrategy.EXPONENTIAL:
            delay = config.initial_delay * (config.multiplier ** (attempt - 1))
        elif config.strategy == RetryStrategy.LINEAR:
            delay = config.initial_delay * attempt
        else:
            delay = config.initial_delay

        delay = min(delay, config.max_delay)

        if config.jitter:
            delay = delay * (0.5 + random.random())

        return delay

    def execute_with_retry(
        self,
        task_id: str,
        func: Callable,
        config: RetryConfig,
        *args,
        **kwargs
    ) -> tuple[Any, List[RetryAttempt]]:
        """Execute function with retry."""
        attempts = []
        last_error = None

        for attempt in range(1, config.max_attempts + 1):
            delay = self.calculate_delay(attempt, config)

            try:
                time.sleep(delay)
                result = func(*args, **kwargs)

                attempts.append(RetryAttempt(
                    attempt=attempt,
                    delay=delay,
                    success=True
                ))

                self._attempts[task_id] = attempts
                return result, attempts

            except Exception as e:
                last_error = str(e)
                attempts.append(RetryAttempt(
                    attempt=attempt,
                    delay=delay,
                    success=False,
                    error=last_error
                ))

        self._attempts[task_id] = attempts
        return None, attempts

    def get_attempts(self, task_id: str) -> List[Dict]:
        """Get retry attempts for task."""
        attempts = self._attempts.get(task_id, [])
        return [
            {
                "attempt": a.attempt,
                "delay": a.delay,
                "success": a.success,
                "error": a.error,
                "timestamp": a.timestamp
            }
            for a in attempts
        ]


class RetryAdvancedAction(BaseAction):
    """Action for retry operations."""

    def __init__(self):
        super().__init__("retry_advanced")
        self._manager = RetryManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute retry action."""
        try:
            operation = params.get("operation", "config")

            if operation == "config":
                return self._get_config(params)
            elif operation == "attempts":
                return self._get_attempts(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get_config(self, params: Dict) -> ActionResult:
        """Get retry config."""
        config = RetryConfig(
            max_attempts=params.get("max_attempts", 3),
            initial_delay=params.get("initial_delay", 1.0),
            max_delay=params.get("max_delay", 60.0),
            multiplier=params.get("multiplier", 2.0),
            jitter=params.get("jitter", True),
            strategy=RetryStrategy(params.get("strategy", "exponential"))
        )

        delay = self._manager.calculate_delay(1, config)
        return ActionResult(success=True, data={
            "config": {
                "max_attempts": config.max_attempts,
                "initial_delay": config.initial_delay,
                "max_delay": config.max_delay,
                "multiplier": config.multiplier,
                "strategy": config.strategy.value
            },
            "calculated_delay_attempt_1": delay
        })

    def _get_attempts(self, params: Dict) -> ActionResult:
        """Get retry attempts."""
        attempts = self._manager.get_attempts(params.get("task_id", ""))
        return ActionResult(success=True, data={"attempts": attempts})
