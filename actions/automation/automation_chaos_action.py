"""
Automation Chaos Action Module.

Chaos engineering utilities for automation including failure injection,
stress testing, and resilience validation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ChaosAction(Enum):
    """Types of chaos actions."""
    KILL_PROCESS = "kill_process"
    NETWORK_PARTITION = "network_partition"
    CPU_STRESS = "cpu_stress"
    MEMORY_STRESS = "memory_stress"
    DISK_STRESS = "disk_stress"
    LATENCY_INJECTION = "latency_injection"
    PACKET_LOSS = "packet_loss"
    SERVICE_UNAVAILABLE = "service_unavailable"


@dataclass
class ChaosExperiment:
    """A chaos engineering experiment."""
    name: str
    description: str
    actions: List[ChaosAction]
    duration_seconds: float
    target: str
    enabled: bool = True


@dataclass
class ExperimentResult:
    """Result of a chaos experiment."""
    experiment_name: str
    success: bool
    started_at: float
    ended_at: float
    duration_ms: float
    actions_triggered: List[str]
    system_recovered: bool
    resilience_score: float  # 0.0 to 1.0
    observations: List[str] = field(default_factory=list)


@dataclass
class ChaosMetrics:
    """Metrics from chaos engineering activities."""
    experiments_run: int = 0
    experiments_passed: int = 0
    experiments_failed: int = 0
    total_recovery_time_ms: float = 0.0
    avg_resilience_score: float = 0.0


class ChaosEngine:
    """Core chaos engineering engine."""

    def __init__(self) -> None:
        self._experiments: Dict[str, ChaosExperiment] = {}
        self._metrics = ChaosMetrics()
        self._running = False

    def add_experiment(self, experiment: ChaosExperiment) -> None:
        """Register a chaos experiment."""
        self._experiments[experiment.name] = experiment
        logger.info(f"Registered chaos experiment: {experiment.name}")

    async def run_experiment(
        self,
        name: str,
        validate_fn: Optional[Callable[[], bool]] = None,
    ) -> ExperimentResult:
        """Run a registered chaos experiment."""
        experiment = self._experiments.get(name)
        if not experiment:
            raise KeyError(f"Experiment '{name}' not found")

        started = time.time()
        actions_triggered: List[str] = []

        logger.info(f"Starting chaos experiment: {name}")

        for action in experiment.actions:
            try:
                await self._execute_action(action, experiment.duration_seconds)
                actions_triggered.append(action.value)
            except Exception as e:
                logger.error(f"Chaos action {action.value} failed: {e}")

        # Wait for system to potentially recover
        await asyncio.sleep(experiment.duration_seconds)

        # Validate system health
        system_recovered = True
        resilience_score = 1.0

        if validate_fn:
            try:
                system_recovered = validate_fn()
                resilience_score = 0.8 if system_recovered else 0.2
            except Exception:
                resilience_score = 0.0

        ended = time.time()
        result = ExperimentResult(
            experiment_name=name,
            success=system_recovered,
            started_at=started,
            ended_at=ended,
            duration_ms=(ended - started) * 1000,
            actions_triggered=actions_triggered,
            system_recovered=system_recovered,
            resilience_score=resilience_score,
        )

        self._update_metrics(result)
        return result

    async def _execute_action(
        self,
        action: ChaosAction,
        duration_seconds: float,
    ) -> None:
        """Execute a single chaos action."""
        logger.debug(f"Executing chaos action: {action.value}")

        if action == ChaosAction.LATENCY_INJECTION:
            delay = random.uniform(0.1, 2.0) * duration_seconds
            await asyncio.sleep(delay)

        elif action == ChaosAction.SERVICE_UNAVAILABLE:
            # Simulate service being unavailable
            await asyncio.sleep(duration_seconds)

        elif action == ChaosAction.CPU_STRESS:
            # Simple CPU stress via busy loop
            end = time.time() + min(duration_seconds, 5.0)
            while time.time() < end:
                _ = sum(i * i for i in range(1000))

        elif action == ChaosAction.MEMORY_STRESS:
            # Allocate some memory temporarily
            data = [bytearray(100000) for _ in range(100)]
            await asyncio.sleep(min(duration_seconds, 5.0))
            del data

        elif action == ChaosAction.NETWORK_PARTITION:
            await asyncio.sleep(duration_seconds)

        elif action == ChaosAction.PACKET_LOSS:
            await asyncio.sleep(duration_seconds)

        elif action == ChaosAction.KILL_PROCESS:
            await asyncio.sleep(duration_seconds)

        elif action == ChaosAction.DISK_STRESS:
            await asyncio.sleep(duration_seconds)

    def _update_metrics(self, result: ExperimentResult) -> None:
        """Update chaos metrics."""
        self._metrics.experiments_run += 1
        if result.success:
            self._metrics.experiments_passed += 1
        else:
            self._metrics.experiments_failed += 1

        n = self._metrics.experiments_run
        old_avg = self._metrics.avg_resilience_score
        self._metrics.avg_resilience_score = (old_avg * (n - 1) + result.resilience_score) / n


class AutomationChaosAction:
    """
    Chaos engineering for automation workflows.

    Provides controlled chaos experiments to validate system resilience.

    Example:
        chaos = AutomationChaosAction()
        chaos.add_experiment(ChaosExperiment(
            name="latency-test",
            description="Test latency tolerance",
            actions=[ChaosAction.LATENCY_INJECTION, ChaosAction.SERVICE_UNAVAILABLE],
            duration_seconds=5.0,
            target="api-service",
        ))

        result = await chaos.run_chaos("latency-test", validate_fn=health_check)
    """

    def __init__(self) -> None:
        self.engine = ChaosEngine()

    def add_experiment(
        self,
        name: str,
        description: str,
        actions: List[ChaosAction],
        duration_seconds: float,
        target: str = "",
    ) -> ChaosExperiment:
        """Add a chaos experiment."""
        experiment = ChaosExperiment(
            name=name,
            description=description,
            actions=actions,
            duration_seconds=duration_seconds,
            target=target,
        )
        self.engine.add_experiment(experiment)
        return experiment

    async def run_chaos(
        self,
        name: str,
        validate_fn: Optional[Callable[[], bool]] = None,
    ) -> ExperimentResult:
        """Run a chaos experiment."""
        return await self.engine.run_experiment(name, validate_fn)

    async def run_steady_state(
        self,
        health_check: Callable[[], bool],
        duration_seconds: float = 60.0,
    ) -> bool:
        """Run a steady-state hypothesis check."""
        start = time.time()
        passed = 0
        total = 0

        while time.time() - start < duration_seconds:
            try:
                if health_check():
                    passed += 1
                total += 1
            except Exception:
                total += 1
            await asyncio.sleep(5.0)

        return (passed / max(1, total)) > 0.95

    def get_metrics(self) -> ChaosMetrics:
        """Get chaos engineering metrics."""
        return self.engine._metrics
