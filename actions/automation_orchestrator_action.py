"""Automation orchestrator action module for RabAI AutoClick.

Provides workflow orchestration:
- WorkflowOrchestratorAction: Orchestrate complex workflows
- StepCoordinatorAction: Coordinate workflow steps
- DependencyResolverAction: Resolve step dependencies
- ExecutionPlanAction: Plan workflow execution
"""

import time
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowOrchestratorAction(BaseAction):
    """Orchestrate complex workflows."""
    action_type = "automation_workflow_orchestrator"
    display_name = "工作流编排器"
    description = "编排复杂工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "execute")
            workflow = params.get("workflow", {})
            steps = workflow.get("steps", [])

            if action == "execute":
                if not steps:
                    return ActionResult(success=False, message="No steps to execute")

                execution_order = self._topological_sort(steps)
                execution_results = []

                for step in execution_order:
                    step_name = step.get("name", "unnamed")
                    step_action = step.get("action", {})
                    depends_on = step.get("depends_on", [])
                    timeout = step.get("timeout", 60)

                    depends_satisfied = all(
                        any(r.get("step") == dep for r in execution_results if r.get("success"))
                        for dep in depends_on
                    )

                    if not depends_satisfied:
                        execution_results.append({
                            "step": step_name,
                            "success": False,
                            "error": "Dependencies not satisfied"
                        })
                        continue

                    success = step_action.get("success", True)
                    execution_results.append({
                        "step": step_name,
                        "success": success,
                        "duration_ms": 100
                    })

                failed_steps = [r for r in execution_results if not r["success"]]
                all_success = len(failed_steps) == 0

                return ActionResult(
                    success=all_success,
                    data={
                        "execution_results": execution_results,
                        "total_steps": len(steps),
                        "executed_steps": len(execution_results),
                        "failed_steps": len(failed_steps),
                        "status": WorkflowStatus.COMPLETED.value if all_success else WorkflowStatus.FAILED.value
                    },
                    message=f"Workflow executed: {len(execution_results)}/{len(steps)} steps, {len(failed_steps)} failed"
                )

            elif action == "validate":
                errors = []
                step_names = set()

                for step in steps:
                    name = step.get("name")
                    if not name:
                        errors.append("Step without name found")
                    elif name in step_names:
                        errors.append(f"Duplicate step name: {name}")
                    else:
                        step_names.add(name)

                    depends_on = step.get("depends_on", [])
                    for dep in depends_on:
                        if dep not in step_names and dep not in [s.get("name") for s in steps]:
                            errors.append(f"Step '{name}' depends on unknown step: {dep}")

                circular_deps = self._detect_circular_dependencies(steps)
                if circular_deps:
                    errors.append(f"Circular dependencies detected: {circular_deps}")

                return ActionResult(
                    success=len(errors) == 0,
                    data={
                        "valid": len(errors) == 0,
                        "errors": errors,
                        "steps_count": len(steps)
                    },
                    message=f"Workflow validation: {'passed' if len(errors) == 0 else f'{len(errors)} errors'}"
                )

            elif action == "plan":
                execution_order = self._topological_sort(steps)
                return ActionResult(
                    success=True,
                    data={
                        "execution_plan": execution_order,
                        "total_steps": len(steps),
                        "parallelizable_steps": self._find_parallel_steps(steps)
                    },
                    message=f"Execution plan: {len(execution_order)} steps in order"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Workflow orchestrator error: {str(e)}")

    def _topological_sort(self, steps: List[Dict]) -> List[Dict]:
        step_map = {s.get("name"): s for s in steps}
        in_degree = {s.get("name"): 0 for s in steps}

        for step in steps:
            for dep in step.get("depends_on", []):
                if dep in in_degree:
                    in_degree[step.get("name")] += 1

        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(step_map[current])

            for step in steps:
                if current in step.get("depends_on", []):
                    in_degree[step.get("name")] -= 1
                    if in_degree[step.get("name")] == 0:
                        queue.append(step.get("name"))

        return result

    def _detect_circular_dependencies(self, steps: List[Dict]) -> List[str]:
        step_map = {s.get("name"): s for s in steps}
        visited = set()
        rec_stack = set()
        cycle = []

        def dfs(name: str) -> bool:
            visited.add(name)
            rec_stack.add(name)

            step = step_map.get(name)
            if step:
                for dep in step.get("depends_on", []):
                    if dep not in visited:
                        if dfs(dep):
                            cycle.append(name)
                            return True
                    elif dep in rec_stack:
                        cycle.append(name)
                        return True

            rec_stack.remove(name)
            return False

        for step in steps:
            name = step.get("name")
            if name and name not in visited:
                if dfs(name):
                    return cycle

        return []

    def _find_parallel_steps(self, steps: List[Dict]) -> List[List[str]]:
        parallel_groups = []
        step_map = {s.get("name"): s for s in steps}
        in_degree = {s.get("name"): len(s.get("depends_on", [])) for s in steps}

        while len(parallel_groups) < len(steps):
            current_group = [name for name, degree in in_degree.items() if degree == 0 and name not in [s for g in parallel_groups for s in g]]
            if not current_group:
                break
            parallel_groups.append(current_group)

            for step in steps:
                for dep in step.get("depends_on", []):
                    if dep in current_group:
                        in_degree[step.get("name")] -= 1

        return parallel_groups

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"workflow": {}, "steps": []}


class StepCoordinatorAction(BaseAction):
    """Coordinate workflow steps."""
    action_type = "automation_step_coordinator"
    display_name = "步骤协调器"
    description = "协调工作流步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "coordinate")
            step = params.get("step", {})
            execution_context = params.get("execution_context", {})

            if action == "coordinate":
                step_name = step.get("name", "unnamed")
                step_type = step.get("type", "action")
                config = step.get("config", {})

                return ActionResult(
                    success=True,
                    data={
                        "coordinated_step": step_name,
                        "step_type": step_type,
                        "config": config,
                        "execution_context": execution_context
                    },
                    message=f"Coordinated step: {step_name}"
                )

            elif action == "batch":
                steps = params.get("steps", [])
                batch_size = params.get("batch_size", 5)

                batches = []
                for i in range(0, len(steps), batch_size):
                    batches.append(steps[i:i + batch_size])

                return ActionResult(
                    success=True,
                    data={
                        "batches": batches,
                        "batch_count": len(batches),
                        "batch_size": batch_size
                    },
                    message=f"Coordinated {len(steps)} steps into {len(batches)} batches"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Step coordinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"step": {}, "execution_context": {}, "steps": [], "batch_size": 5}


class DependencyResolverAction(BaseAction):
    """Resolve step dependencies."""
    action_type = "automation_dependency_resolver"
    display_name = "依赖解析器"
    description = "解析步骤依赖"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            resolution_strategy = params.get("resolution_strategy", " breadth_first")

            if not steps:
                return ActionResult(success=False, message="No steps provided")

            resolved_order = []
            unresolved = set(step.get("name") for step in steps)
            resolved_deps = {}

            while unresolved:
                ready = []
                for step in steps:
                    name = step.get("name")
                    if name not in unresolved:
                        continue

                    depends_on = step.get("depends_on", [])
                    if all(dep not in unresolved for dep in depends_on):
                        ready.append(name)

                if not ready:
                    return ActionResult(success=False, message="Circular dependency detected")

                resolved_order.extend(ready)
                for name in ready:
                    unresolved.remove(name)
                    resolved_deps[name] = steps[[s.get("name") for s in steps].index(name)].get("depends_on", [])

            return ActionResult(
                success=True,
                data={
                    "resolved_order": resolved_order,
                    "total_steps": len(steps),
                    "resolution_strategy": resolution_strategy,
                    "dependency_graph": resolved_deps
                },
                message=f"Resolved dependencies: {len(resolved_order)} steps in order"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Dependency resolver error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["steps"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"resolution_strategy": "breadth_first"}


class ExecutionPlanAction(BaseAction):
    """Plan workflow execution."""
    action_type = "automation_execution_plan"
    display_name = "执行计划器"
    description = "规划工作流执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow = params.get("workflow", {})
            steps = workflow.get("steps", [])
            optimization_level = params.get("optimization_level", "none")

            if not steps:
                return ActionResult(success=False, message="No steps to plan")

            execution_phases = []
            step_map = {s.get("name"): s for s in steps}
            in_degree = {s.get("name"): len(s.get("depends_on", [])) for s in steps}

            phase_num = 0
            while in_degree:
                current_phase = [name for name, degree in in_degree.items() if degree == 0]

                if not current_phase:
                    break

                execution_phases.append({
                    "phase": phase_num,
                    "steps": current_phase,
                    "can_parallelize": len(current_phase) > 1
                })

                for name in current_phase:
                    del in_degree[name]
                    for step in steps:
                        if name in step.get("depends_on", []):
                            in_degree[step.get("name")] -= 1

                phase_num += 1

            estimated_duration = sum(phase.get("estimated_duration", 100) for phase in execution_phases)
            max_parallel = max(len(phase.get("steps", [])) for phase in execution_phases) if execution_phases else 0

            return ActionResult(
                success=True,
                data={
                    "execution_phases": execution_phases,
                    "total_phases": len(execution_phases),
                    "estimated_duration_ms": estimated_duration,
                    "max_parallelizable_steps": max_parallel,
                    "optimization_level": optimization_level
                },
                message=f"Execution plan: {len(execution_phases)} phases, max {max_parallel} parallel steps"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Execution plan error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["workflow"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"steps": [], "optimization_level": "none"}
