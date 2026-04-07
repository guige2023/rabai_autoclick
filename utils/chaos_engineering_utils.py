"""
Chaos engineering utilities for controlled fault injection.

Provides chaos experiments, failure injection, latency simulation,
network partition tools, and experiment orchestration.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ChaosAction(Enum):
    """Types of chaos experiment actions."""
    DELAY = auto()
    ERROR = auto()
    ABORT = auto()
    PARTITION = auto()
    BLACKHOLE = auto()
    CPU_LOAD = auto()
    MEMORY_LOAD = auto()
    KILL_PROCESS = auto()


@dataclass
class ChaosTarget:
    """Target for a chaos experiment."""
    target_type: str  # process, network, container, pod
    identifier: str
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ChaosExperiment:
    """Represents a chaos experiment."""
    name: str
    description: str
    action: ChaosAction
    target: ChaosTarget
    duration_seconds: int = 60
    level: int = 1  # 1-10 scale
    parameters: dict[str, Any] = field(default_factory=dict)
    steady_state_hypothesis: Optional[dict[str, Any]] = None

    def validate(self) -> list[str]:
        """Validate experiment configuration."""
        errors = []
        if not self.name:
            errors.append("Experiment name is required")
        if self.duration_seconds <= 0:
            errors.append("Duration must be positive")
        if self.level < 1 or self.level > 10:
            errors.append("Level must be between 1 and 10")
        return errors


@dataclass
class ExperimentResult:
    """Result of a chaos experiment."""
    experiment_name: str
    is_success: bool
    started_at: float
    ended_at: float
    steady_state_preserved: bool = True
    artifacts: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def duration(self) -> float:
        return self.ended_at - self.started_at


class LatencyInjector:
    """Injects artificial latency into operations."""

    def __init__(self, min_delay_ms: int = 10, max_delay_ms: int = 1000) -> None:
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        self._rng = random.Random()

    def delay(self, min_ms: Optional[int] = None, max_ms: Optional[int] = None) -> float:
        """Inject delay, returns actual delay in seconds."""
        delay_ms = self._rng.randint(
            min_ms or self.min_delay_ms,
            max_ms or self.max_delay_ms,
        )
        time.sleep(delay_ms / 1000)
        return delay_ms / 1000

    async def async_delay(self, min_ms: Optional[int] = None, max_ms: Optional[int] = None) -> float:
        """Inject async delay."""
        delay_ms = self._rng.randint(
            min_ms or self.min_delay_ms,
            max_ms or self.max_delay_ms,
        )
        await asyncio.sleep(delay_ms / 1000)
        return delay_ms / 1000

    def wrap(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator to wrap a function with latency injection."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self.delay()
            return func(*args, **kwargs)
        return wrapper

    def async_wrap(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator to wrap an async function with latency injection."""
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            await self.async_delay()
            return await func(*args, **kwargs)
        return wrapper


class ErrorInjector:
    """Injects errors into operations."""

    def __init__(self, error_rate: float = 0.1, error_type: type = Exception) -> None:
        self.error_rate = error_rate
        self.error_type = error_type
        self._rng = random.Random()

    def should_error(self) -> bool:
        return self._rng.random() < self.error_rate

    def inject(self, message: str = "Injected error") -> None:
        """Raise an error based on the error rate."""
        if self.should_error():
            raise self.error_type(message)

    def wrap(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator to wrap a function with error injection."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if self.should_error():
                raise self.error_type(f"Injected: {func.__name__}")
            return func(*args, **kwargs)
        return wrapper


class CPUStresser:
    """Applies CPU stress to the system."""

    def __init__(self, load_percent: int = 80, duration_seconds: int = 10) -> None:
        self.load_percent = load_percent
        self.duration_seconds = duration_seconds
        self._stop_event: Optional[asyncio.Event] = None

    async def stress(self) -> None:
        """Run CPU stress."""
        self._stop_event = asyncio.Event()
        start = time.time()
        count = 0
        while time.time() - start < self.duration_seconds and not self._stop_event.is_set():
            _ = sum(i * i for i in range(1000))
            count += 1
            await asyncio.sleep(0.001)
        logger.info("CPU stresser completed: %d iterations", count)

    def stop(self) -> None:
        """Stop CPU stress."""
        if self._stop_event:
            self._stop_event.set()


class MemoryStresser:
    """Applies memory pressure to the system."""

    def __init__(self, mb_to_allocate: int = 100) -> None:
        self.mb_to_allocate = mb_to_allocate
        self._chunks: list[bytearray] = []

    def stress(self) -> int:
        """Allocate memory, returns actual MB allocated."""
        chunk_size = 1024 * 1024  # 1MB
        allocated = 0
        for _ in range(self.mb_to_allocate):
            try:
                chunk = bytearray(chunk_size)
                chunk[0] = 1
                self._chunks.append(chunk)
                allocated += 1
            except MemoryError:
                break
        logger.info("Memory stressor allocated %d MB", allocated)
        return allocated

    def release(self) -> None:
        """Release allocated memory."""
        self._chunks.clear()
        logger.info("Memory stressor released all memory")


class NetworkChaos:
    """Network fault injection tools."""

    def __init__(self) -> None:
        self._partitions: list[tuple[str, str]] = []

    def add_partition(self, source: str, target: str) -> bool:
        """Simulate a network partition between source and target."""
        self._partitions.append((source, target))
        logger.info("Network partition added: %s <-> %s", source, target)
        return True

    def remove_partition(self, source: str, target: str) -> bool:
        """Remove a network partition."""
        try:
            self._partitions.remove((source, target))
            logger.info("Network partition removed: %s <-> %s", source, target)
            return True
        except ValueError:
            return False

    def block_host(self, host: str) -> bool:
        """Block traffic to/from a specific host."""
        import subprocess
        try:
            subprocess.run(
                ["iptables", "-A", "INPUT", "-s", host, "-j", "DROP"],
                check=True,
            )
            logger.info("Blocked host: %s", host)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Failed to block host: %s", e)
            return False

    def unblock_host(self, host: str) -> bool:
        """Unblock traffic to/from a specific host."""
        import subprocess
        try:
            subprocess.run(
                ["iptables", "-D", "INPUT", "-s", host, "-j", "DROP"],
                check=True,
            )
            logger.info("Unblocked host: %s", host)
            return True
        except subprocess.CalledProcessError:
            return False


class ChaosEngine:
    """Orchestrates chaos experiments."""

    def __init__(self) -> None:
        self._experiments: dict[str, ChaosExperiment] = {}
        self._latency = LatencyInjector()
        self._error = ErrorInjector()
        self._cpu_stresser = CPUStresser()
        self._memory_stresser = MemoryStresser()
        self._network_chaos = NetworkChaos()
        self._running: dict[str, asyncio.Task] = {}

    def register_experiment(self, experiment: ChaosExperiment) -> bool:
        """Register a chaos experiment."""
        errors = experiment.validate()
        if errors:
            logger.error("Invalid experiment: %s", errors)
            return False
        self._experiments[experiment.name] = experiment
        logger.info("Registered experiment: %s", experiment.name)
        return True

    async def run_experiment(self, name: str) -> ExperimentResult:
        """Run a registered chaos experiment."""
        if name not in self._experiments:
            return ExperimentResult(
                experiment_name=name,
                is_success=False,
                started_at=time.time(),
                ended_at=time.time(),
                error=f"Experiment {name} not found",
            )

        experiment = self._experiments[name]
        started_at = time.time()
        logger.info("Starting chaos experiment: %s", name)

        try:
            await self._execute_action(experiment)
            await asyncio.sleep(experiment.duration_seconds)
            ended_at = time.time()

            return ExperimentResult(
                experiment_name=name,
                is_success=True,
                started_at=started_at,
                ended_at=ended_at,
                steady_state_preserved=True,
            )
        except Exception as e:
            logger.error("Experiment %s failed: %s", name, e)
            return ExperimentResult(
                experiment_name=name,
                is_success=False,
                started_at=started_at,
                ended_at=time.time(),
                error=str(e),
            )

    async def _execute_action(self, experiment: ChaosExperiment) -> None:
        """Execute a chaos experiment action."""
        action = experiment.action
        params = experiment.parameters

        if action == ChaosAction.DELAY:
            min_ms = params.get("min_delay_ms", 10)
            max_ms = params.get("max_delay_ms", 1000)
            await self._latency.async_delay(min_ms, max_ms)
        elif action == ChaosAction.ERROR:
            rate = params.get("error_rate", 0.1)
            self._error.error_rate = rate
            self._error.inject(params.get("message", "Chaos injection"))
        elif action == ChaosAction.CPU_LOAD:
            load = params.get("load_percent", 80)
            duration = params.get("duration", 10)
            self._cpu_stresser.load_percent = load
            self._cpu_stresser.duration_seconds = duration
            await self._cpu_stresser.stress()
        elif action == ChaosAction.MEMORY_LOAD:
            mb = params.get("mb_to_allocate", 100)
            self._memory_stresser.mb_to_allocate = mb
            self._memory_stresser.stress()
        elif action == ChaosAction.PARTITION:
            source = params.get("source", "")
            target = params.get("target", "")
            if source and target:
                self._network_chaos.add_partition(source, target)

    async def stop_experiment(self, name: str) -> bool:
        """Stop a running experiment."""
        if name in self._running:
            self._running[name].cancel()
            del self._running[name]
            return True
        return False
