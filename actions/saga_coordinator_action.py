"""Saga Coordinator Action Module.

Provides saga pattern coordination for
distributed transaction management.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SagaStatus(Enum):
    """Saga status."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


class StepStatus(Enum):
    """Step status."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


@dataclass
class SagaStep:
    """Saga step."""
    step_id: str
    name: str
    forward: Callable
    compensate: Optional[Callable] = None
    status: StepStatus = StepStatus.PENDING


@dataclass
class Saga:
    """Saga instance."""
    saga_id: str
    name: str
    status: SagaStatus = SagaStatus.RUNNING
    steps: List[SagaStep] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


class SagaCoordinator:
    """Coordinates saga execution."""

    def __init__(self):
        self._sagas: Dict[str, Saga] = {}
        self._lock = threading.RLock()

    def create_saga(
        self,
        name: str,
        steps: List[Dict]
    ) -> str:
        """Create a saga."""
        saga_id = f"saga_{int(time.time() * 1000)}"

        saga_steps = []
        for step_def in steps:
            saga_steps.append(SagaStep(
                step_id=step_def.get("id", ""),
                name=step_def.get("name", ""),
                forward=step_def.get("forward", lambda: None),
                compensate=step_def.get("compensate")
            ))

        saga = Saga(
            saga_id=saga_id,
            name=name,
            steps=saga_steps
        )

        with self._lock:
            self._sagas[saga_id] = saga

        return saga_id

    def execute(self, saga_id: str) -> tuple[bool, List[Dict]]:
        """Execute saga."""
        saga = self._sagas.get(saga_id)
        if not saga:
            return False, [{"error": "Saga not found"}]

        completed_steps = []
        errors = []

        for step in saga.steps:
            step.status = StepStatus.EXECUTING

            try:
                step.forward()
                step.status = StepStatus.COMPLETED
                completed_steps.append({
                    "step_id": step.step_id,
                    "status": "completed"
                })

            except Exception as e:
                step.status = StepStatus.FAILED
                errors.append({
                    "step_id": step.step_id,
                    "error": str(e)
                })

                saga.status = SagaStatus.FAILED
                return False, errors

        saga.status = SagaStatus.COMPLETED
        saga.completed_at = time.time()
        return True, completed_steps

    def compensate(self, saga_id: str) -> tuple[bool, List[Dict]]:
        """Compensate saga."""
        saga = self._sagas.get(saga_id)
        if not saga:
            return False, [{"error": "Saga not found"}]

        saga.status = SagaStatus.COMPENSATING
        compensated = []
        errors = []

        for step in reversed(saga.steps):
            if step.status != StepStatus.COMPLETED:
                continue

            if not step.compensate:
                continue

            try:
                step.compensate()
                step.status = StepStatus.COMPENSATED
                compensated.append({
                    "step_id": step.step_id,
                    "status": "compensated"
                })

            except Exception as e:
                errors.append({
                    "step_id": step.step_id,
                    "error": str(e)
                })

        saga.status = SagaStatus.COMPENSATED
        saga.completed_at = time.time()

        return len(errors) == 0, compensated + errors


class SagaCoordinatorAction(BaseAction):
    """Action for saga operations."""

    def __init__(self):
        super().__init__("saga_coordinator")
        self._coordinator = SagaCoordinator()

    def execute(self, params: Dict) -> ActionResult:
        """Execute saga action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "compensate":
                return self._compensate(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create saga."""
        saga_id = self._coordinator.create_saga(
            name=params.get("name", ""),
            steps=params.get("steps", [])
        )
        return ActionResult(success=True, data={"saga_id": saga_id})

    def _execute(self, params: Dict) -> ActionResult:
        """Execute saga."""
        success, steps = self._coordinator.execute(params.get("saga_id", ""))
        return ActionResult(success=success, data={
            "success": success,
            "steps": steps
        })

    def _compensate(self, params: Dict) -> ActionResult:
        """Compensate saga."""
        success, steps = self._coordinator.compensate(params.get("saga_id", ""))
        return ActionResult(success=success, data={
            "success": success,
            "steps": steps
        })
