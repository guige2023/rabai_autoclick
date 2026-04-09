"""
Automation Chaos Action Module

Provides chaos engineering capabilities for automation workflows including
fault injection, failure simulation, and resilience testing.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ChaosActionType(Enum):
    """Types of chaos actions."""

    DELAY = "delay"
    TIMEOUT = "timeout"
    ERROR = "error"
    FAILURE = "failure"
    NETWORK_PARTITION = "network_partition"
    CPU_STRESS = "cpu_stress"
    MEMORY_STRESS = "memory_stress"
    KILL_PROCESS = "kill_process"


class ChaosStatus(Enum):
    """Chaos experiment status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FAILED = "failed"


@dataclass
class ChaosTarget:
    """Target for chaos injection."""

    target_id: str
    target_type: str
    name: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChaosAction:
    """A chaos action to inject."""

    action_id: str
    action_type: ChaosActionType
    target_id: str
    duration_seconds: float
    probability: float = 1.0
    parameters: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class ChaosExperiment:
    """A chaos engineering experiment."""

    experiment_id: str
    name: str
    description: str
    actions: List[ChaosAction]
    status: ChaosStatus = ChaosStatus.PENDING
    targets: List[ChaosTarget] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class ChaosResult:
    """Result of a chaos action."""

    action_id: str
    target_id: str
    action_type: ChaosActionType
    success: bool
    injected: bool
    error: Optional[str] = None
    latency_ms: float = 0.0


@dataclass
class ChaosConfig:
    """Configuration for chaos engineering."""

    default_probability: float = 0.1
    default_duration_seconds: float = 10.0
    enable_cpu_stress: bool = True
    enable_memory_stress: bool = True
    max_concurrent_actions: int = 5
    auto_recovery: bool = True


class FaultInjector:
    """Injects various types of faults."""

    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()

    async def inject_delay(
        self,
        target: ChaosTarget,
        duration_ms: float,
    ) -> ChaosResult:
        """Inject network delay."""
        start = time.time()
        try:
            await asyncio.sleep(duration_ms / 1000)
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.DELAY,
                success=True,
                injected=True,
                latency_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.DELAY,
                success=False,
                injected=False,
                error=str(e),
            )

    async def inject_error(
        self,
        target: ChaosTarget,
        error_type: str,
    ) -> ChaosResult:
        """Inject an error response."""
        start = time.time()
        try:
            raise Exception(f"Injected {error_type} error")
        except Exception as e:
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.ERROR,
                success=False,
                injected=True,
                error=str(e),
                latency_ms=(time.time() - start) * 1000,
            )

    async def inject_timeout(
        self,
        target: ChaosTarget,
        timeout_seconds: float,
    ) -> ChaosResult:
        """Inject a timeout."""
        start = time.time()
        try:
            await asyncio.sleep(timeout_seconds)
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.TIMEOUT,
                success=True,
                injected=True,
            )
        except Exception as e:
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.TIMEOUT,
                success=False,
                injected=False,
                error=str(e),
            )

    async def inject_cpu_stress(
        self,
        target: ChaosTarget,
        duration_seconds: float,
        intensity: float = 0.5,
    ) -> ChaosResult:
        """Inject CPU stress."""
        start = time.time()
        try:
            end_time = time.time() + duration_seconds
            while time.time() < end_time:
                _ = sum(i * i for i in range(1000))
                await asyncio.sleep(0.001)
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.CPU_STRESS,
                success=True,
                injected=True,
                latency_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ChaosResult(
                action_id="",
                target_id=target.target_id,
                action_type=ChaosActionType.CPU_STRESS,
                success=False,
                injected=False,
                error=str(e),
            )


class AutomationChaosAction:
    """
    Chaos engineering action for automation workflows.

    Features:
    - Multiple fault injection types (delay, error, timeout, stress)
    - Probability-based chaos injection
    - Experiment orchestration
    - Target management
    - Result tracking and reporting
    - Auto-recovery support

    Usage:
        chaos = AutomationChaosAction(config)
        experiment = chaos.create_experiment("test-latency")
        chaos.add_action(experiment, ChaosAction(...))
        results = await chaos.run_experiment(experiment)
    """

    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()
        self._injector = FaultInjector(self.config)
        self._experiments: Dict[str, ChaosExperiment] = {}
        self._targets: Dict[str, ChaosTarget] = {}
        self._stats = {
            "experiments_run": 0,
            "actions_injected": 0,
            "actions_triggered": 0,
            "experiments_failed": 0,
        }

    def create_target(
        self,
        target_type: str,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ChaosTarget:
        """Create a chaos target."""
        target_id = f"target_{uuid.uuid4().hex[:8]}"
        target = ChaosTarget(
            target_id=target_id,
            target_type=target_type,
            name=name,
            metadata=metadata or {},
        )
        self._targets[target_id] = target
        return target

    def create_experiment(
        self,
        name: str,
        description: str = "",
    ) -> ChaosExperiment:
        """Create a new chaos experiment."""
        experiment_id = f"exp_{uuid.uuid4().hex[:12]}"
        experiment = ChaosExperiment(
            experiment_id=experiment_id,
            name=name,
            description=description,
            actions=[],
        )
        self._experiments[experiment_id] = experiment
        return experiment

    def add_action(
        self,
        experiment: ChaosExperiment,
        action_type: ChaosActionType,
        target_id: str,
        duration_seconds: Optional[float] = None,
        probability: float = 1.0,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> ChaosAction:
        """Add an action to an experiment."""
        action = ChaosAction(
            action_id=f"action_{uuid.uuid4().hex[:8]}",
            action_type=action_type,
            target_id=target_id,
            duration_seconds=duration_seconds or self.config.default_duration_seconds,
            probability=probability,
            parameters=parameters or {},
        )
        experiment.actions.append(action)
        return action

    async def run_experiment(
        self,
        experiment: ChaosExperiment,
    ) -> ChaosExperiment:
        """
        Run a chaos experiment.

        Args:
            experiment: Experiment to run

        Returns:
            Updated experiment with results
        """
        logger.info(f"Starting chaos experiment: {experiment.experiment_id}")
        experiment.status = ChaosStatus.RUNNING
        experiment.started_at = time.time()
        self._stats["experiments_run"] += 1

        try:
            for action in experiment.actions:
                if not action.enabled:
                    continue

                if random.random() > action.probability:
                    logger.info(f"Skipping action {action.action_id} (probability)")
                    continue

                target = self._targets.get(action.target_id)
                if target is None:
                    continue

                result = await self._inject_action(action, target)
                experiment.results.append(result.__dict__)
                self._stats["actions_triggered"] += 1

                if result.injected:
                    self._stats["actions_injected"] += 1

            experiment.status = ChaosStatus.COMPLETED

        except Exception as e:
            logger.error(f"Experiment failed: {e}")
            experiment.status = ChaosStatus.FAILED
            self._stats["experiments_failed"] += 1

        experiment.completed_at = time.time()
        return experiment

    async def _inject_action(
        self,
        action: ChaosAction,
        target: ChaosTarget,
    ) -> ChaosResult:
        """Inject a single chaos action."""
        action_id = action.action_id
        target_id = target.target_id

        if action.action_type == ChaosActionType.DELAY:
            duration_ms = action.parameters.get("delay_ms", 100)
            result = await self._injector.inject_delay(target, duration_ms)
        elif action.action_type == ChaosActionType.ERROR:
            error_type = action.parameters.get("error_type", "generic")
            result = await self._injector.inject_error(target, error_type)
        elif action.action_type == ChaosActionType.TIMEOUT:
            timeout = action.duration_seconds
            result = await self._injector.inject_timeout(target, timeout)
        elif action.action_type == ChaosActionType.CPU_STRESS:
            intensity = action.parameters.get("intensity", 0.5)
            result = await self._injector.inject_cpu_stress(target, action.duration_seconds, intensity)
        else:
            result = ChaosResult(
                action_id=action_id,
                target_id=target_id,
                action_type=action.action_type,
                success=False,
                injected=False,
                error="Unknown action type",
            )

        result.action_id = action_id
        return result

    def abort_experiment(self, experiment_id: str) -> bool:
        """Abort a running experiment."""
        experiment = self._experiments.get(experiment_id)
        if experiment and experiment.status == ChaosStatus.RUNNING:
            experiment.status = ChaosStatus.ABORTED
            return True
        return False

    def get_experiment(self, experiment_id: str) -> Optional[ChaosExperiment]:
        """Get an experiment by ID."""
        return self._experiments.get(experiment_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get chaos engineering statistics."""
        return {
            **self._stats.copy(),
            "total_experiments": len(self._experiments),
            "total_targets": len(self._targets),
        }


async def demo_chaos():
    """Demonstrate chaos engineering."""
    config = ChaosConfig(default_probability=1.0)
    chaos = AutomationChaosAction(config)

    target = chaos.create_target("api", "test-api")
    experiment = chaos.create_experiment("latency-test", "Test network latency")

    chaos.add_action(
        experiment,
        ChaosActionType.DELAY,
        target.target_id,
        probability=1.0,
        parameters={"delay_ms": 50},
    )

    result = await chaos.run_experiment(experiment)
    print(f"Experiment status: {result.status.value}")
    print(f"Actions triggered: {len(result.results)}")
    print(f"Stats: {chaos.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_chaos())
