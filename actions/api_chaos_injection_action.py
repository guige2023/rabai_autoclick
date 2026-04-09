"""
API Chaos Injection Action Module

Injects faults and failures into API calls to test resilience,
including latency, errors, aborts, and network partition simulation.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, TypeVar, Awaitable

import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ChaosMode(Enum):
    """Chaos injection modes."""

    LATENCY = auto()
    ERROR_RESPONSE = auto()
    ABORT = auto()
    TIMEOUT = auto()
    NETWORK_PARTITION = auto()
    BANDWIDTH_LIMIT = auto()
    PACKET_LOSS = auto()
    RANDOM_FAILURE = auto()


class ErrorType(Enum):
    """Types of error responses to inject."""

    HTTP_500 = 500
    HTTP_502 = 502
    HTTP_503 = 503
    HTTP_504 = 504
    HTTP_429 = 429
    CONNECTION_RESET = "connection_reset"
    CONNECTION_TIMEOUT = "connection_timeout"
    READ_TIMEOUT = "read_timeout"


@dataclass
class ChaosConfig:
    """Configuration for chaos injection."""

    enabled: bool = False
    mode: ChaosMode = ChaosMode.LATENCY
    probability: float = 0.1
    latency_ms_min: int = 100
    latency_ms_max: int = 5000
    error_type: ErrorType = ErrorType.HTTP_500
    error_message: str = "Chaos injection triggered"
    abort_probability: float = 0.05
    timeout_seconds: float = 0.1
    bandwidth_kbps: Optional[int] = None
    packet_loss_rate: float = 0.0
    target_endpoints: Optional[List[str]] = None
    excluded_endpoints: Optional[List[str]] = None


@dataclass
class ChaosResult:
    """Result of a chaos injection run."""

    endpoint: str
    chaos_triggered: bool
    mode: ChaosMode
    latency_injected_ms: Optional[float] = None
    error_type: Optional[ErrorType] = None
    original_result: Optional[Any] = None
    error_message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class ChaosEngine:
    """Manages chaos injection rules and execution."""

    def __init__(self, config: ChaosConfig) -> None:
        self.config = config
        self._stats: Dict[str, int] = {}
        self._active = config.enabled

    def enable(self) -> None:
        self._active = True

    def disable(self) -> None:
        self._active = False

    def should_inject(self, endpoint: str) -> bool:
        """Determine if chaos should be injected for this endpoint."""
        if not self._active:
            return False

        if self.config.target_endpoints:
            if endpoint not in self.config.target_endpoints:
                return False

        if self.config.excluded_endpoints:
            if endpoint in self.config.excluded_endpoints:
                return False

        return random.random() < self.config.probability

    async def inject_latency(self) -> None:
        """Inject artificial latency."""
        delay = random.uniform(
            self.config.latency_ms_min / 1000.0,
            self.config.latency_ms_max / 1000.0,
        )
        await asyncio.sleep(delay)

    def inject_error(self) -> Exception:
        """Generate an error response based on configured error type."""
        error_type = self.config.error_type
        if error_type == ErrorType.CONNECTION_RESET:
            return ConnectionError("Connection reset by peer")
        elif error_type == ErrorType.CONNECTION_TIMEOUT:
            return TimeoutError("Connection timed out")
        elif error_type == ErrorType.READ_TIMEOUT:
            return TimeoutError("Read timed out")
        else:
            return APIChaosError(
                f"Chaos injected {error_type.name}: {self.config.error_message}",
                status_code=error_type.value if isinstance(error_type.value, int) else 500,
            )

    async def inject_chaos(
        self,
        endpoint: str,
        coro: Awaitable[T],
    ) -> T:
        """Execute a coroutine with possible chaos injection."""
        if not self.should_inject(endpoint):
            return await coro

        mode = self.config.mode

        if mode == ChaosMode.LATENCY or mode == ChaosMode.RANDOM_FAILURE:
            if random.random() < 0.5:
                await self.inject_latency()

        if mode == ChaosMode.ERROR_RESPONSE or mode == ChaosMode.RANDOM_FAILURE:
            if random.random() < 0.3:
                raise self.inject_error()

        if mode == ChaosMode.ABORT:
            if random.random() < self.config.abort_probability:
                raise self.inject_error()

        if mode == ChaosMode.TIMEOUT:
            if random.random() < self.config.probability:
                await asyncio.sleep(self.config.timeout_seconds)
                raise TimeoutError("Chaos timeout triggered")

        return await coro

    def get_stats(self) -> Dict[str, int]:
        """Return chaos injection statistics."""
        return self._stats.copy()

    def record_injection(self, endpoint: str, mode: ChaosMode) -> None:
        """Record a chaos injection event."""
        key = f"{endpoint}:{mode.name}"
        self._stats[key] = self._stats.get(key, 0) + 1


class APIChaosError(Exception):
    """Exception raised when chaos is injected into an API call."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class ChaosAction:
    """Action class for API chaos injection."""

    def __init__(self, config: Optional[ChaosConfig] = None) -> None:
        self.config = config or ChaosConfig()
        self.engine = ChaosEngine(self.config)

    def execute(self, coro: Awaitable[T], endpoint: str = "unknown") -> Awaitable[T]:
        """Execute an async API call with chaos injection."""
        return self.engine.inject_chaos(endpoint, coro)

    def execute_sync(self, func: Callable[[], T], endpoint: str = "unknown") -> T:
        """Execute a sync function with chaos injection."""
        if not self.engine.should_inject(endpoint):
            return func()

        mode = self.config.mode
        if mode == ChaosMode.LATENCY:
            time.sleep(random.uniform(
                self.config.latency_ms_min / 1000.0,
                self.config.latency_ms_max / 1000.0,
            ))

        if mode == ChaosMode.ERROR_RESPONSE:
            if random.random() < self.config.probability:
                raise self.engine.inject_error()

        return func()

    def update_config(self, **kwargs: Any) -> None:
        """Update chaos configuration."""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    def reset_stats(self) -> None:
        """Reset chaos injection statistics."""
        self._stats: Dict[str, int] = {}
