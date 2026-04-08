"""Action delay and timing utilities.

This module provides utilities for managing delays, debouncing,
and timing between actions in automation workflows.
"""

from __future__ import annotations

import time
from typing import Callable, Dict, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum, auto

T = TypeVar("T")


class DelayStrategy(Enum):
    """Delay strategies for action timing."""
    FIXED = auto()
    LINEAR_BACKOFF = auto()
    EXPONENTIAL_BACKOFF = auto()
    JITTER = auto()
    SMART = auto()


@dataclass
class DelayConfig:
    """Configuration for action delays."""
    base_delay_ms: float = 100.0
    max_delay_ms: float = 5000.0
    backoff_factor: float = 1.5
    jitter_factor: float = 0.3
    strategy: DelayStrategy = DelayStrategy.SMART


class DelayTimer:
    """Manages delays between actions."""

    def __init__(self, config: Optional[DelayConfig] = None) -> None:
        self._config = config or DelayConfig()
        self._last_action_time: Dict[str, float] = {}
        self._attempt_counts: Dict[str, int] = {}

    def wait(self, action_name: str, override_ms: Optional[float] = None) -> float:
        """Wait appropriate delay for an action.

        Args:
            action_name: Name of the action.
            override_ms: Override delay in milliseconds.

        Returns:
            Actual delay applied in milliseconds.
        """
        now = time.perf_counter()
        last_time = self._last_action_time.get(action_name, 0.0)
        elapsed_ms = (now - last_time) * 1000

        if override_ms is not None:
            delay_ms = override_ms
        else:
            delay_ms = self._calculate_delay(action_name, elapsed_ms)

        if delay_ms > elapsed_ms:
            sleep_time = (delay_ms - elapsed_ms) / 1000.0
            time.sleep(sleep_time)

        self._last_action_time[action_name] = time.perf_counter()
        return delay_ms

    def _calculate_delay(self, action_name: str, elapsed_ms: float) -> float:
        cfg = self._config
        base = cfg.base_delay_ms

        if cfg.strategy == DelayStrategy.FIXED:
            return base

        elif cfg.strategy == DelayStrategy.LINEAR_BACKOFF:
            count = self._attempt_counts.get(action_name, 0)
            return min(base * (1 + count), cfg.max_delay_ms)

        elif cfg.strategy == DelayStrategy.EXPONENTIAL_BACKOFF:
            count = self._attempt_counts.get(action_name, 0)
            return min(base * (cfg.backoff_factor ** count), cfg.max_delay_ms)

        elif cfg.strategy == DelayStrategy.JITTER:
            import random
            jitter = base * cfg.jitter_factor * random.random()
            return base + jitter

        elif cfg.strategy == DelayStrategy.SMART:
            if elapsed_ms >= base:
                return base
            else:
                return base - elapsed_ms

        return base

    def record_attempt(self, action_name: str) -> None:
        """Record an action attempt for backoff tracking."""
        self._attempt_counts[action_name] = self._attempt_counts.get(action_name, 0) + 1

    def reset_attempt(self, action_name: str) -> None:
        """Reset attempt count after successful action."""
        self._attempt_counts[action_name] = 0

    def reset_all(self) -> None:
        """Reset all timing state."""
        self._last_action_time.clear()
        self._attempt_counts.clear()


def debounce(
    func: Callable[..., T],
    delay_ms: float,
) -> Callable[..., Optional[T]]:
    """Decorator that debounces a function call.

    Args:
        func: Function to debounce.
        delay_ms: Delay in milliseconds.

    Returns:
        Debounced function.
    """
    last_call: Dict[str, float] = {}

    def wrapper(*args: ..., **kwargs: ...) -> Optional[T]:
        key = str(args) + str(kwargs)
        now = time.perf_counter()
        if key in last_call:
            elapsed_ms = (now - last_call[key]) * 1000
            if elapsed_ms < delay_ms:
                return None
        last_call[key] = now
        return func(*args, **kwargs)

    return wrapper


def throttle(
    func: Callable[..., T],
    min_interval_ms: float,
) -> Callable[..., Optional[T]]:
    """Decorator that throttles a function call.

    Args:
        func: Function to throttle.
        min_interval_ms: Minimum interval in milliseconds.

    Returns:
        Throttled function.
    """
    last_call: Dict[str, float] = {}

    def wrapper(*args: ..., **kwargs: ...) -> Optional[T]:
        key = str(args) + str(kwargs)
        now = time.perf_counter()
        if key in last_call:
            elapsed_ms = (now - last_call[key]) * 1000
            if elapsed_ms < min_interval_ms:
                return None
        last_call[key] = now
        return func(*args, **kwargs)

    return wrapper


__all__ = [
    "DelayStrategy",
    "DelayConfig",
    "DelayTimer",
    "debounce",
    "throttle",
]
