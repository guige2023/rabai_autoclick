"""Process automation action module for RabAI AutoClick.

Provides process automation:
- ProcessBuilderAction: Build automation processes from steps
- ProcessExecutorAction: Execute automation processes
- ProcessMonitorAction: Monitor process execution
- ProcessOptimizerAction: Optimize process performance
- ProcessRepositoryAction: Store and retrieve process definitions
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessState(Enum):
    """Process execution states."""
    IDLE = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class ProcessStep:
    """A single step in a process."""

    def __init__(
        self,
        step_id: str,
        name: str,
        action_type: str,
        params: Dict[str, Any],
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.id = step_id
        self.name = name
        self.action_type = action_type
        self.params = params
        self.retry_policy = retry_policy or {"max_retries": 0}
        self.timeout_seconds = timeout_seconds
        self.status: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Optional[Any] = None
        self.error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "action_type": self.action_type,
            "params": self.params,
            "retry_policy": self.retry_policy,
            "timeout_seconds": self.timeout_seconds,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
        }


class ProcessBuilderAction(BaseAction):
    """Build automation processes from steps."""
    action_type = "process_builder"
    display_name = "流程构建器"
    description = "从步骤构建自动化流程"

    def __init__(self) -> None:
        super().__init__()
        self._process_templates: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "build":
                return self._build_process(params)
            elif action == "add_step":
                return self._add_step(params)
            elif action == "validate":
                return self._validate_process(params)
            elif action == "save_template":
                return self._save_template(params)
            elif action == "load_template":
                return self._load_template(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Process building failed: {e}")

    def _build_process(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        steps = params.get("steps", [])
        if not name:
            return ActionResult(success=False, message="name is required")

        process_id = str(uuid.uuid4())
        process = {
            "id": process_id,
            "name": name,
            "steps": [
                ProcessStep(
                    step_id=s.get("id", str(uuid.uuid4())),
                    name=s.get("name", f"Step_{i}"),
                    action_type=s.get("action_type", ""),
                    params=s.get("params", {}),
                    retry_policy=s.get("retry_policy"),
                    timeout_seconds=s.get("timeout_seconds", 30),
                ).to_dict()
                for i, s in enumerate(steps)
            ],
            "step_count": len(steps),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "state": ProcessState.IDLE.name,
        }
        return ActionResult(success=True, message=f"Process '{name}' built with {len(steps)} steps", data=process)

    def _add_step(self, params: Dict[str, Any]) -> ActionResult:
        process = params.get("process", {})
        step = params.get("step", {})
        steps = process.get("steps", [])
        steps.append(step)
        process["steps"] = steps
        process["step_count"] = len(steps)
        return ActionResult(success=True, message=f"Step added, total: {len(steps)}", data=process)

    def _validate_process(self, params: Dict[str, Any]) -> ActionResult:
        process = params.get("process", {})
        steps = process.get("steps", [])
        errors: List[str] = []
        for i, step in enumerate(steps):
            if not step.get("action_type"):
                errors.append(f"Step {i}: missing action_type")
            if not step.get("name"):
                errors.append(f"Step {i}: missing name")
        return ActionResult(
            success=len(errors) == 0,
            message=f"Process validation: {'PASSED' if not errors else 'FAILED'}",
            data={"valid": len(errors) == 0, "errors": errors},
        )

    def _save_template(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        process = params.get("process", {})
        if not name:
            return ActionResult(success=False, message="name is required")
        self._process_templates[name] = process
        return ActionResult(success=True, message=f"Template saved: {name}")

    def _load_template(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name not in self._process_templates:
            return ActionResult(success=False, message=f"Template not found: {name}")
        return ActionResult(success=True, message=f"Template loaded: {name}", data=self._process_templates[name])


class ProcessExecutorAction(BaseAction):
    """Execute automation processes."""
    action_type = "process_executor"
    display_name = "流程执行器"
    description = "执行自动化流程"

    def __init__(self) -> None:
        super().__init__()
        self._executions: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "run":
                return self._run_process(params)
            elif action == "pause":
                return self._pause_process(params)
            elif action == "resume":
                return self._resume_process(params)
            elif action == "cancel":
                return self._cancel_process(params)
            elif action == "status":
                return self._get_status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Process execution failed: {e}")

    def _run_process(self, params: Dict[str, Any]) -> ActionResult:
        process = params.get("process", {})
        execution_id = str(uuid.uuid4())
        self._executions[execution_id] = {
            "id": execution_id,
            "process_name": process.get("name", ""),
            "state": ProcessState.RUNNING.name,
            "current_step": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
            "steps_completed": 0,
            "total_steps": process.get("step_count", 0),
        }
        return ActionResult(
            success=True,
            message=f"Process '{process.get('name')}' started",
            data={"execution_id": execution_id},
        )

    def _pause_process(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id in self._executions:
            self._executions[execution_id]["state"] = ProcessState.PAUSED.name
            return ActionResult(success=True, message="Process paused")
        return ActionResult(success=False, message="Execution not found")

    def _resume_process(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id in self._executions:
            self._executions[execution_id]["state"] = ProcessState.RUNNING.name
            return ActionResult(success=True, message="Process resumed")
        return ActionResult(success=False, message="Execution not found")

    def _cancel_process(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id in self._executions:
            self._executions[execution_id]["state"] = ProcessState.CANCELLED.name
            return ActionResult(success=True, message="Process cancelled")
        return ActionResult(success=False, message="Execution not found")

    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id and execution_id in self._executions:
            return ActionResult(success=True, message="Execution status", data=self._executions[execution_id])
        return ActionResult(success=True, message="All executions", data={"executions": self._executions})


class ProcessMonitorAction(BaseAction):
    """Monitor process execution."""
    action_type = "process_monitor"
    display_name = "流程监控器"
    description = "监控流程执行状态"

    def __init__(self) -> None:
        super().__init__()
        self._metrics: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            execution_id = params.get("execution_id", "")
            metrics = params.get("metrics", {})
            if not execution_id:
                return ActionResult(success=False, message="execution_id is required")
            self._metrics.setdefault(execution_id, []).append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metrics": metrics,
            })
            return ActionResult(success=True, message=f"Metrics recorded for {execution_id[:8]}", data=self._metrics.get(execution_id, []))
        except Exception as e:
            return ActionResult(success=False, message=f"Process monitoring failed: {e}")


class ProcessOptimizerAction(BaseAction):
    """Optimize process performance."""
    action_type = "process_optimizer"
    display_name = "流程优化器"
    description = "优化流程执行性能"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            process = params.get("process", {})
            metrics = params.get("metrics", {})
            optimizations = []

            if metrics.get("parallel_eligible_steps"):
                optimizations.append({
                    "type": "PARALLELIZATION",
                    "description": f"Run {metrics['parallel_eligible_steps']} steps in parallel",
                    "estimated_speedup": "2-5x",
                })

            if metrics.get("redundant_delays"):
                optimizations.append({
                    "type": "DELAY_REDUCTION",
                    "description": f"Remove {metrics['redundant_delays']} unnecessary delays",
                    "estimated_speedup": "1.1-1.5x",
                })

            if metrics.get("retry_loops"):
                optimizations.append({
                    "type": "RETRY_OPTIMIZATION",
                    "description": "Flatten retry loops",
                    "estimated_speedup": "1.2-2x",
                })

            return ActionResult(
                success=True,
                message=f"Generated {len(optimizations)} optimizations",
                data={"optimizations": optimizations, "estimated_total_speedup": f"{sum(float(o.get('estimated_speedup', '1x').rstrip('x')) for o in optimizations):.1f}x"},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process optimization failed: {e}")


class ProcessRepositoryAction(BaseAction):
    """Store and retrieve process definitions."""
    action_type = "process_repository"
    display_name = "流程仓库"
    description = "存储和检索流程定义"

    def __init__(self) -> None:
        super().__init__()
        self._processes: Dict[str, Dict[str, Any]] = {}
        self._categories: Dict[str, List[str]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "save":
                return self._save(params)
            elif action == "load":
                return self._load(params)
            elif action == "delete":
                return self._delete(params)
            elif action == "list":
                return self._list(params)
            elif action == "search":
                return self._search(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Process repository failed: {e}")

    def _save(self, params: Dict[str, Any]) -> ActionResult:
        process = params.get("process", {})
        category = params.get("category", "default")
        name = process.get("name", "")
        if not name:
            return ActionResult(success=False, message="process.name is required")
        process["category"] = category
        self._processes[name] = process
        self._categories.setdefault(category, []).append(name)
        return ActionResult(success=True, message=f"Process '{name}' saved to '{category}'")

    def _load(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name not in self._processes:
            return ActionResult(success=False, message=f"Process not found: {name}")
        return ActionResult(success=True, message=f"Process loaded: {name}", data=self._processes[name])

    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name in self._processes:
            category = self._processes[name].get("category", "default")
            del self._processes[name]
            if category in self._categories:
                self._categories[category] = [n for n in self._categories[category] if n != name]
            return ActionResult(success=True, message=f"Process deleted: {name}")
        return ActionResult(success=False, message=f"Process not found: {name}")

    def _list(self, params: Dict[str, Any]) -> ActionResult:
        category = params.get("category", None)
        if category:
            names = self._categories.get(category, [])
            return ActionResult(success=True, message=f"{len(names)} processes in '{category}'", data={"names": names})
        return ActionResult(success=True, message=f"{len(self._processes)} total processes", data={"categories": self._categories})

    def _search(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "").lower()
        results = [name for name in self._processes if query in name.lower()]
        return ActionResult(success=True, message=f"Found {len(results)} processes", data={"results": results})
