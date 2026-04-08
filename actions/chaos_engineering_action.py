"""Chaos Engineering Action Module.

Provides chaos engineering capabilities for
fault injection and resilience testing.
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


class FaultType(Enum):
    """Fault injection type."""
    DELAY = "delay"
    ERROR = "error"
    ABORT = "abort"
    TIMEOUT = "timeout"


@dataclass
class ChaosExperiment:
    """Chaos experiment definition."""
    experiment_id: str
    name: str
    fault_type: FaultType
    target: str
    probability: float
    duration_seconds: float
    enabled: bool = True


class ChaosEngineeringManager:
    """Manages chaos engineering experiments."""

    def __init__(self):
        self._experiments: Dict[str, ChaosExperiment] = {}
        self._active_experiments: List[str] = []
        self._lock = threading.RLock()

    def create_experiment(
        self,
        name: str,
        fault_type: FaultType,
        target: str,
        probability: float = 0.1,
        duration_seconds: float = 60.0
    ) -> str:
        """Create chaos experiment."""
        experiment_id = f"chaos_{int(time.time() * 1000)}"

        experiment = ChaosExperiment(
            experiment_id=experiment_id,
            name=name,
            fault_type=fault_type,
            target=target,
            probability=probability,
            duration_seconds=duration_seconds
        )

        with self._lock:
            self._experiments[experiment_id] = experiment

        return experiment_id

    def should_inject_fault(self, experiment_id: str) -> bool:
        """Check if fault should be injected."""
        experiment = self._experiments.get(experiment_id)
        if not experiment or not experiment.enabled:
            return False

        return random.random() < experiment.probability

    def inject_delay(self, experiment_id: str) -> float:
        """Inject delay fault."""
        experiment = self._experiments.get(experiment_id)
        if not experiment:
            return 0

        delay = random.uniform(0.1, experiment.duration_seconds)
        time.sleep(delay)
        return delay

    def start_experiment(self, experiment_id: str) -> bool:
        """Start experiment."""
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                return False

            if experiment_id not in self._active_experiments:
                self._active_experiments.append(experiment_id)

            return True

    def stop_experiment(self, experiment_id: str) -> bool:
        """Stop experiment."""
        with self._lock:
            if experiment_id in self._active_experiments:
                self._active_experiments.remove(experiment_id)
                return True
        return False

    def get_active_experiments(self) -> List[Dict]:
        """Get active experiments."""
        with self._lock:
            return [
                {
                    "experiment_id": e.experiment_id,
                    "name": e.name,
                    "fault_type": e.fault_type.value
                }
                for e in self._experiments.values()
                if e.experiment_id in self._active_experiments
            ]


class ChaosEngineeringAction(BaseAction):
    """Action for chaos engineering operations."""

    def __init__(self):
        super().__init__("chaos_engineering")
        self._manager = ChaosEngineeringManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute chaos action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "start":
                return self._start(params)
            elif operation == "stop":
                return self._stop(params)
            elif operation == "should_inject":
                return self._should_inject(params)
            elif operation == "active":
                return self._active(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create experiment."""
        experiment_id = self._manager.create_experiment(
            name=params.get("name", ""),
            fault_type=FaultType(params.get("fault_type", "delay")),
            target=params.get("target", ""),
            probability=params.get("probability", 0.1),
            duration_seconds=params.get("duration_seconds", 60)
        )
        return ActionResult(success=True, data={"experiment_id": experiment_id})

    def _start(self, params: Dict) -> ActionResult:
        """Start experiment."""
        success = self._manager.start_experiment(params.get("experiment_id", ""))
        return ActionResult(success=success)

    def _stop(self, params: Dict) -> ActionResult:
        """Stop experiment."""
        success = self._manager.stop_experiment(params.get("experiment_id", ""))
        return ActionResult(success=success)

    def _should_inject(self, params: Dict) -> ActionResult:
        """Check if should inject."""
        should = self._manager.should_inject_fault(params.get("experiment_id", ""))
        return ActionResult(success=True, data={"should_inject": should})

    def _active(self, params: Dict) -> ActionResult:
        """Get active experiments."""
        active = self._manager.get_active_experiments()
        return ActionResult(success=True, data={"active": active})
