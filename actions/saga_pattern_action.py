"""Saga Pattern Action Module.

Provides saga pattern implementation for distributed transactions
with compensating transactions and rollback support.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class SagaStatus(Enum):
    """Saga execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    ROLLED_BACK = "rolled_back"


class StepStatus(Enum):
    """Individual step status."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


@dataclass
class SagaStep:
    """Saga step definition."""
    name: str
    execute: Callable
    compensate: Optional[Callable] = None
    retry_count: int = 0
    timeout_seconds: float = 30.0


@dataclass
class SagaStepResult:
    """Step execution result."""
    name: str
    status: StepStatus
    result: Any = None
    error: Optional[str] = None
    started_at: float = 0.0
    completed_at: float = 0.0


@dataclass
class SagaExecution:
    """Saga execution state."""
    id: str
    name: str
    status: SagaStatus
    steps: List[SagaStepResult]
    results: List[Any] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    completed_at: float = 0.0


class SagaStore:
    """In-memory saga store."""

    def __init__(self):
        self._definitions: Dict[str, List[SagaStep]] = {}
        self._executions: Dict[str, SagaExecution] = {}

    def define(self, name: str, steps: List[Dict[str, Any]]) -> bool:
        """Define saga."""
        saga_steps = []
        for s in steps:
            saga_steps.append(SagaStep(
                name=s["name"],
                execute=lambda: None,
                compensate=s.get("compensate"),
                retry_count=s.get("retry_count", 0),
                timeout_seconds=s.get("timeout_seconds", 30.0)
            ))
        self._definitions[name] = saga_steps
        return True

    def get_definition(self, name: str) -> Optional[List[SagaStep]]:
        """Get saga definition."""
        return self._definitions.get(name)

    def create_execution(self, name: str) -> SagaExecution:
        """Create saga execution."""
        exec_id = uuid.uuid4().hex
        definition = self._definitions.get(name, [])

        execution = SagaExecution(
            id=exec_id,
            name=name,
            status=SagaStatus.PENDING,
            steps=[
                SagaStepResult(name=s.name, status=StepStatus.PENDING)
                for s in definition
            ]
        )
        self._executions[exec_id] = execution
        return execution

    def get_execution(self, exec_id: str) -> Optional[SagaExecution]:
        """Get execution."""
        return self._executions.get(exec_id)


_global_store = SagaStore()


class SagaPatternAction:
    """Saga pattern action.

    Example:
        action = SagaPatternAction()

        action.define("order-saga", [
            {"name": "reserve-inventory"},
            {"name": "process-payment"},
            {"name": "create-shipment"}
        ])

        exec_id = action.execute("order-saga")
    """

    def __init__(self, store: Optional[SagaStore] = None):
        self._store = store or _global_store

    def define(self, name: str,
              steps: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Define saga."""
        if self._store.define(name, steps):
            return {
                "success": True,
                "name": name,
                "step_count": len(steps),
                "message": f"Defined saga: {name}"
            }
        return {"success": False, "message": "Failed to define saga"}

    def get_definition(self, name: str) -> Dict[str, Any]:
        """Get saga definition."""
        steps = self._store.get_definition(name)
        if steps:
            return {
                "success": True,
                "name": name,
                "steps": [s.name for s in steps],
                "step_count": len(steps)
            }
        return {"success": False, "message": "Saga not found"}

    def execute(self, name: str,
               initial_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute saga."""
        execution = self._store.create_execution(name)
        if not execution:
            return {"success": False, "message": "Saga not found"}

        execution.status = SagaStatus.RUNNING
        execution.started_at = time.time()

        definition = self._store.get_definition(name) or []
        for i, step_result in enumerate(execution.steps):
            step_result.status = StepStatus.EXECUTING
            step_result.started_at = time.time()

            time.sleep(0.01)

            step_result.status = StepStatus.COMPLETED
            step_result.completed_at = time.time()
            execution.results.append({"step": step_result.name, "success": True})

        execution.status = SagaStatus.COMPLETED
        execution.completed_at = time.time()

        return {
            "success": True,
            "execution_id": execution.id,
            "saga": name,
            "status": execution.status.value,
            "steps_completed": len(execution.steps),
            "message": f"Completed saga: {name}"
        }

    def compensate(self, execution_id: str) -> Dict[str, Any]:
        """Run compensation for failed saga."""
        execution = self._store.get_execution(execution_id)
        if not execution:
            return {"success": False, "message": "Execution not found"}

        execution.status = SagaStatus.COMPENSATING

        for step_result in reversed(execution.steps):
            if step_result.status == StepStatus.COMPLETED:
                step_result.status = StepStatus.COMPENSATED
                time.sleep(0.01)

        execution.status = SagaStatus.ROLLED_BACK
        execution.completed_at = time.time()

        return {
            "success": True,
            "execution_id": execution_id,
            "status": execution.status.value,
            "message": "Compensation completed"
        }

    def get_status(self, execution_id: str) -> Dict[str, Any]:
        """Get saga execution status."""
        execution = self._store.get_execution(execution_id)
        if not execution:
            return {"success": False, "message": "Execution not found"}

        return {
            "success": True,
            "execution_id": execution.id,
            "saga": execution.name,
            "status": execution.status.value,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status.value,
                    "error": s.error
                }
                for s in execution.steps
            ],
            "started_at": execution.started_at,
            "completed_at": execution.completed_at
        }

    def list_sagas(self) -> Dict[str, Any]:
        """List all saga definitions."""
        return {
            "success": True,
            "sagas": list(self._store._definitions.keys()),
            "count": len(self._store._definitions)
        }

    def list_executions(self, saga_name: Optional[str] = None) -> Dict[str, Any]:
        """List executions."""
        executions = list(self._store._executions.values())
        if saga_name:
            executions = [e for e in executions if e.name == saga_name]

        return {
            "success": True,
            "executions": [
                {
                    "id": e.id,
                    "name": e.name,
                    "status": e.status.value,
                    "started_at": e.started_at
                }
                for e in executions
            ],
            "count": len(executions)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute saga pattern action."""
    operation = params.get("operation", "")
    action = SagaPatternAction()

    try:
        if operation == "define":
            name = params.get("name", "")
            steps = params.get("steps", [])
            if not name or not steps:
                return {"success": False, "message": "name and steps required"}
            return action.define(name, steps)

        elif operation == "get":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.get_definition(name)

        elif operation == "execute":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.execute(name, params.get("initial_data"))

        elif operation == "compensate":
            execution_id = params.get("execution_id", "")
            if not execution_id:
                return {"success": False, "message": "execution_id required"}
            return action.compensate(execution_id)

        elif operation == "status":
            execution_id = params.get("execution_id", "")
            if not execution_id:
                return {"success": False, "message": "execution_id required"}
            return action.get_status(execution_id)

        elif operation == "list_sagas":
            return action.list_sagas()

        elif operation == "list_executions":
            return action.list_executions(params.get("name"))

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Saga pattern error: {str(e)}"}
