"""
Automation Chaos Engineering Module.

Injects failures into automation workflows to test resilience.
Supports timeout simulation, error injection, resource exhaustion,
and network failure scenarios.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Awaitable


class ChaosScenario(Enum):
    """Chaos engineering scenarios for automation."""
    TIMEOUT = "timeout"
    RANDOM_ERROR = "random_error"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    NETWORK_PARTITION = "network_partition"
    DELAY_INJECTION = "delay_injection"
    STATE_CORRUPTION = "state_corruption"
    KILL_PROCESS = "kill_process"


@dataclass
class ChaosScenarioConfig:
    """Configuration for a chaos scenario."""
    scenario: ChaosScenario
    probability: float = 0.1
    intensity: float = 1.0
    duration_ms: int = 0
    error_type: str = "RuntimeError"
    error_message: str = "Chaos injected failure"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChaosExperimentResult:
    """Result of a chaos experiment."""
    experiment_id: str
    scenario: ChaosScenario
    target: str
    chaos_injected: bool
    execution_time_ms: float
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BlastRadius:
    """Assessment of potential blast radius."""
    affected_tasks: int
    affected_workflows: int
    estimated_recovery_time_ms: float
    risk_level: str


class AutomationChaosEngine:
    """
    Chaos engineering engine for automation workflows.

    Injects various failure modes into automation processes
    to test resilience and recovery procedures.

    Example:
        engine = AutomationChaosEngine()
        engine.add_scenario(ChaosScenario.TIMEOUT, probability=0.2, duration_ms=5000)
        result = await engine.execute("my_task", my_async_task)
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self._scenarios: dict[str, ChaosScenarioConfig] = {}
        self._experiment_history: list[ChaosExperimentResult] = []
        self._targets: dict[str, str] = {}
        self._aborted_tasks: set[str] = set()

        if seed is not None:
            random.seed(seed)

    def add_scenario(
        self,
        scenario: ChaosScenario,
        probability: float = 0.1,
        intensity: float = 1.0,
        **kwargs: Any
    ) -> ChaosScenarioConfig:
        """
        Add a chaos scenario configuration.

        Args:
            scenario: Type of chaos scenario
            probability: Probability of injection (0.0-1.0)
            intensity: Severity multiplier
            **kwargs: Additional scenario-specific parameters

        Returns:
            Created ChaosScenarioConfig
        """
        config = ChaosScenarioConfig(
            scenario=scenario,
            probability=probability,
            intensity=intensity,
            **kwargs
        )
        self._scenarios[scenario.value] = config
        return config

    def remove_scenario(self, scenario: ChaosScenario) -> bool:
        """Remove a chaos scenario."""
        return self._scenarios.pop(scenario.value, None) is not None

    def enable_target(self, target: str, scenario_type: str) -> None:
        """Enable chaos for a specific target."""
        self._targets[target] = scenario_type

    def disable_target(self, target: str) -> None:
        """Disable chaos for a specific target."""
        self._targets.pop(target, None)

    async def execute(
        self,
        task_name: str,
        coro: Callable[[], Awaitable[Any]],
        scenario: Optional[ChaosScenario] = None
    ) -> Any:
        """
        Execute a task with potential chaos injection.

        Args:
            task_name: Name of the task
            coro: Async task coroutine
            scenario: Specific scenario to use, or random from enabled

        Returns:
            Task result or chaos-induced error
        """
        scenario_name = scenario.value if scenario else self._targets.get(task_name)
        if not scenario_name or scenario_name not in self._scenarios:
            return await coro()

        config = self._scenarios[scenario_name]
        experiment_id = f"{task_name}:{time.time_ns()}"

        should_inject = random.random() < config.probability

        if not should_inject:
            return await coro()

        start = time.perf_counter()

        try:
            result = await self._inject_chaos(task_name, config, coro)
            duration_ms = (time.perf_counter() - start) * 1000

            self._record(ChaosExperimentResult(
                experiment_id=experiment_id,
                scenario=config.scenario,
                target=task_name,
                chaos_injected=True,
                execution_time_ms=duration_ms
            ))

            return result

        except Exception as e:
            duration_ms = (time.perf_counter() - start) * 1000
            self._record(ChaosExperimentResult(
                experiment_id=experiment_id,
                scenario=config.scenario,
                target=task_name,
                chaos_injected=True,
                execution_time_ms=duration_ms,
                error=str(e)
            ))
            raise

    async def _inject_chaos(
        self,
        task_name: str,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Inject chaos based on scenario type."""
        if config.scenario == ChaosScenario.TIMEOUT:
            return await self._inject_timeout(config, coro)
        elif config.scenario == ChaosScenario.RANDOM_ERROR:
            return self._inject_random_error(config)
        elif config.scenario == ChaosScenario.DELAY_INJECTION:
            return await self._inject_delay(config, coro)
        elif config.scenario == ChaosScenario.STATE_CORRUPTION:
            return await self._inject_state_corruption(config, coro)
        elif config.scenario == ChaosScenario.RESOURCE_EXHAUSTION:
            return await self._inject_resource_exhaustion(config, coro)
        elif config.scenario == ChaosScenario.NETWORK_PARTITION:
            return await self._inject_network_partition(config, coro)
        else:
            return await coro()

    async def _inject_timeout(
        self,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Inject timeout failure."""
        timeout_s = (config.duration_ms / 1000) * config.intensity if config.duration_ms > 0 else 0.001
        try:
            return await asyncio.wait_for(coro(), timeout=timeout_s)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task timed out after {timeout_s}s (chaos)") from None

    def _inject_random_error(self, config: ChaosScenarioConfig) -> Any:
        """Inject a random error."""
        error_class = getattr(__builtins__, config.error_type, RuntimeError)
        raise error_class(config.error_message)

    async def _inject_delay(
        self,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Inject artificial delay."""
        delay_s = (config.duration_ms / 1000) * config.intensity if config.duration_ms > 0 else 1.0
        await asyncio.sleep(delay_s)
        return await coro()

    async def _inject_state_corruption(
        self,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate state corruption during execution."""
        await asyncio.sleep(0.001)
        raise RuntimeError("State corruption detected - workflow violated integrity constraints")

    async def _inject_resource_exhaustion(
        self,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate resource exhaustion."""
        await asyncio.sleep(0.001)
        raise MemoryError("Out of memory - resource exhaustion scenario triggered")

    async def _inject_network_partition(
        self,
        config: ChaosScenarioConfig,
        coro: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Simulate network partition."""
        await asyncio.sleep(0.001)
        raise ConnectionError("Network partition - cannot reach dependency")

    def _record(self, result: ChaosExperimentResult) -> None:
        """Record experiment result."""
        self._experiment_history.append(result)
        if len(self._experiment_history) > 10000:
            self._experiment_history = self._experiment_history[-5000:]

    def get_experiment_history(
        self,
        scenario: Optional[ChaosScenario] = None,
        limit: int = 100
    ) -> list[ChaosExperimentResult]:
        """Get experiment history."""
        results = self._experiment_history
        if scenario:
            results = [r for r in results if r.scenario == scenario]
        return results[-limit:]

    def get_statistics(self) -> dict[str, Any]:
        """Get chaos experiment statistics."""
        if not self._experiment_history:
            return {"total_experiments": 0}

        total = len(self._experiment_history)
        injected = sum(1 for r in self._experiment_history if r.chaos_injected)

        by_scenario: dict[str, dict[str, Any]] = {}
        for result in self._experiment_history:
            scenario_name = result.scenario.value
            if scenario_name not in by_scenario:
                by_scenario[scenario_name] = {"total": 0, "errors": 0, "avg_duration_ms": 0}

            by_scenario[scenario_name]["total"] += 1
            if result.error:
                by_scenario[scenario_name]["errors"] += 1
            by_scenario[scenario_name]["avg_duration_ms"] += result.execution_time_ms

        for scenario_data in by_scenario.values():
            if scenario_data["total"] > 0:
                scenario_data["avg_duration_ms"] /= scenario_data["total"]

        return {
            "total_experiments": total,
            "chaos_injected": injected,
            "injection_rate": injected / total if total > 0 else 0,
            "by_scenario": by_scenario
        }

    def assess_blast_radius(
        self,
        target: str,
        scenario: ChaosScenario
    ) -> BlastRadius:
        """Assess the potential blast radius of a chaos scenario."""
        affected_tasks = random.randint(1, 5)
        affected_workflows = random.randint(1, 3)
        recovery_time_ms = random.uniform(1000, 30000)

        risk = "low"
        if scenario in (ChaosScenario.RESOURCE_EXHAUSTION, ChaosScenario.NETWORK_PARTITION):
            risk = "high"
        elif scenario == ChaosScenario.STATE_CORRUPTION:
            risk = "medium"

        return BlastRadius(
            affected_tasks=affected_tasks,
            affected_workflows=affected_workflows,
            estimated_recovery_time_ms=recovery_time_ms,
            risk_level=risk
        )
