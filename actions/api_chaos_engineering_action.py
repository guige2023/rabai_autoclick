"""
API Chaos Engineering Module.

Injects failures and stress conditions into API calls to test
resilience. Supports latency injection, error simulation,
network partition simulation, and timeout scenarios.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Awaitable


class ChaosType(Enum):
    """Types of chaos injection."""
    LATENCY = "latency"
    TIMEOUT = "timeout"
    ERROR_RATE = "error_rate"
    NETWORK_PARTITION = "network_partition"
    BANDWIDTH_LIMIT = "bandwidth_limit"
    DNS_FAILURE = "dns_failure"
    CONNECTION_CLOSE = "connection_close"


class ErrorMode(Enum):
    """Error injection modes."""
    RANDOM = "random"
    DETERMINISTIC = "deterministic"
    BURST = "burst"
    GRADUAL = "gradual"


@dataclass
class ChaosConfig:
    """Configuration for chaos injection."""
    chaos_type: ChaosType
    probability: float = 0.1
    intensity: float = 1.0
    duration_ms: int = 0
    error_code: int = 503
    error_message: str = "Service unavailable"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChaosResult:
    """Result of a chaos experiment."""
    experiment_id: str
    chaos_type: ChaosType
    injected: bool
    target: str
    duration_ms: float
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class ChaosEngine:
    """
    Chaos engineering engine for API resilience testing.

    Injects various failure modes to test how systems behave
    under adverse conditions.

    Example:
        engine = ChaosEngine()
        engine.add_target("api-gateway", ChaosConfig(ChaosType.LATENCY, probability=0.2))
        result = await engine.execute("api-gateway", call_api)
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self._targets: dict[str, ChaosConfig] = {}
        self._experiments: list[ChaosResult] = []
        self._active: dict[str, float] = {}
        self._state: dict[str, Any] = {}

        if seed is not None:
            random.seed(seed)

    def add_target(self, name: str, config: ChaosConfig) -> None:
        """
        Register a target endpoint or service for chaos injection.

        Args:
            name: Target identifier
            config: Chaos configuration
        """
        self._targets[name] = config

    def remove_target(self, name: str) -> bool:
        """Remove a chaos target."""
        return self._targets.pop(name, None) is not None

    async def execute(
        self,
        target: str,
        coro: Callable[[], Awaitable[Any]],
        fallback: Optional[Callable[[Exception], Awaitable[Any]]] = None
    ) -> Any:
        """
        Execute an async API call with chaos injection.

        Args:
            target: Target name (must be registered)
            coro: Async API call coroutine
            fallback: Optional fallback if chaos causes failure

        Returns:
            Result of the API call or fallback
        """
        if target not in self._targets:
            return await coro()

        config = self._targets[target]
        experiment_id = f"{target}:{time.time_ns()}"

        should_inject = self._should_inject(config)

        if not should_inject:
            return await coro()

        start = time.perf_counter()

        try:
            if config.chaos_type == ChaosType.LATENCY:
                result = await self._inject_latency(config, coro)
            elif config.chaos_type == ChaosType.TIMEOUT:
                result = await self._inject_timeout(config, coro)
            elif config.chaos_type == ChaosType.ERROR_RATE:
                result = await self._inject_error(config, coro)
            elif config.chaos_type == ChaosType.NETWORK_PARTITION:
                result = await self._inject_partition(config, coro)
            elif config.chaos_type == ChaosType.CONNECTION_CLOSE:
                result = await self._inject_connection_close(config, coro)
            else:
                result = await coro()

            duration = (time.perf_counter() - start) * 1000
            self._record(ChaosResult(
                experiment_id=experiment_id,
                chaos_type=config.chaos_type,
                injected=True,
                target=target,
                duration_ms=duration,
                metadata=config.metadata
            ))

            return result

        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            self._record(ChaosResult(
                experiment_id=experiment_id,
                chaos_type=config.chaos_type,
                injected=True,
                target=target,
                duration_ms=duration,
                error=str(e),
                metadata=config.metadata
            ))

            if fallback:
                return await fallback(e)
            raise

    def _should_inject(self, config: ChaosConfig) -> bool:
        """Determine if chaos should be injected this time."""
        if config.chaos_type == ChaosType.BURST:
            target_state = self._state.get(f"burst_{id(config)}", 0)
            if target_state >= 3:
                self._state[f"burst_{id(config)}"] = 0
                return True
            if random.random() < config.probability * 2:
                self._state[f"burst_{id(config)}"] = target_state + 1
            return False

        return random.random() < config.probability

    async def _inject_latency(
        self,
        config: ChaosConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Inject artificial latency."""
        delay_ms = int(config.duration_ms * config.intensity) if config.duration_ms > 0 else int(1000 * config.intensity)
        await asyncio.sleep(delay_ms / 1000)
        return await coro()

    async def _inject_timeout(
        self,
        config: ChaosConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate timeout."""
        delay = 0.001 * config.intensity
        try:
            return await asyncio.wait_for(coro(), timeout=delay)
        except asyncio.TimeoutError:
            raise TimeoutError(config.error_message) from None

    async def _inject_error(
        self,
        config: ChaosConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Inject random errors."""
        error = Exception(config.error_message)
        raise error

    async def _inject_partition(
        self,
        config: ChaosConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate network partition."""
        await asyncio.sleep(config.duration_ms / 2000)
        raise ConnectionError("Network partition simulated")

    async def _inject_connection_close(
        self,
        config: ChaosConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate connection being closed mid-request."""
        await asyncio.sleep(0.001)
        raise ConnectionResetError("Connection closed by server")

    def _record(self, result: ChaosResult) -> None:
        """Record experiment result."""
        self._experiments.append(result)
        if len(self._experiments) > 10000:
            self._experiments = self._experiments[-5000:]

    def get_experiments(
        self,
        target: Optional[str] = None,
        limit: int = 100
    ) -> list[ChaosResult]:
        """Get recent chaos experiment results."""
        results = self._experiments
        if target:
            results = [r for r in results if r.target == target]
        return results[-limit:]

    def get_stats(self, target: str) -> dict[str, Any]:
        """Get chaos injection statistics for a target."""
        experiments = [e for e in self._experiments if e.target == target]
        if not experiments:
            return {"total": 0, "injected": 0, "error_rate": 0.0}

        injected = sum(1 for e in experiments if e.injected)
        errors = sum(1 for e in experiments if e.error)

        return {
            "total": len(experiments),
            "injected": injected,
            "injection_rate": injected / len(experiments),
            "errors": errors,
            "error_rate": errors / len(experiments) if experiments else 0.0,
            "avg_duration_ms": sum(e.duration_ms for e in experiments) / len(experiments)
        }

    def enable(self, target: str) -> None:
        """Enable chaos injection for a target."""
        self._active[target] = time.time()

    def disable(self, target: str) -> None:
        """Disable chaos injection for a target."""
        self._active.pop(target, None)

    def is_enabled(self, target: str) -> bool:
        """Check if chaos is enabled for a target."""
        return target in self._active
