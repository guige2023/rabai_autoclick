"""API Orchestrator Action Module.

Provides coordinated multi-API workflows with dependency management,
parallel execution, and result aggregation.
"""

from __future__ import annotations

import sys
import os
import time
import json
import threading
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import urllib.request
import urllib.parse
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StepStatus(Enum):
    """Status of an orchestration step."""
    PENDING = "pending"
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepType(Enum):
    """Types of orchestration steps."""
    API_REQUEST = "api_request"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    CONDITION = "condition"
    PARALLEL = "parallel"
    SEQUENCE = "sequence"
    LOOP = "loop"


@dataclass
class OrchestrationStep:
    """A step in an API orchestration workflow."""
    step_id: str
    step_type: StepType
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 60.0
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    workflow_id: str
    status: str
    steps_completed: int
    steps_failed: int
    total_time: float
    step_results: Dict[str, Any]


class ApiOrchestratorAction(BaseAction):
    """Orchestrate complex multi-API workflows.

    Manages workflows with parallel execution, dependency chains,
    conditional branching, and result aggregation.
    """
    action_type = "api_orchestrator"
    display_name = "API编排器"
    description = "编排多API协同工作流，支持并行执行和依赖管理"

    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, List[OrchestrationStep]] = {}
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute orchestration operation.

        Args:
            context: Execution context.
            params: Dict with keys: action, workflow_id, steps, etc.

        Returns:
            ActionResult with workflow execution result.
        """
        action = params.get("action", "run")

        if action == "define":
            return self._define_workflow(context, params)
        elif action == "run":
            return self._run_workflow(context, params)
        elif action == "status":
            return self._get_workflow_status(params)
        elif action == "parallel":
            return self._run_parallel_steps(context, params)
        elif action == "chain":
            return self._run_chained_steps(context, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _define_workflow(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Define a reusable workflow."""
        import uuid

        workflow_id = params.get("workflow_id", str(uuid.uuid4())[:8])
        name = params.get("name", "Unnamed Workflow")
        steps_config = params.get("steps", [])
        save_to_var = params.get("save_to_var", None)

        steps = []
        for i, step_config in enumerate(steps_config):
            step_type_str = step_config.get("type", "api_request").upper()
            try:
                step_type = StepType[step_type_str]
            except KeyError:
                step_type = StepType.API_REQUEST

            step = OrchestrationStep(
                step_id=step_config.get("id", f"step_{i}"),
                step_type=step_type,
                name=step_config.get("name", f"Step {i}"),
                config=step_config.get("config", {}),
                depends_on=step_config.get("depends_on", []),
                max_retries=step_config.get("max_retries", 3),
                timeout=step_config.get("timeout", 60.0)
            )
            steps.append(step)

        with self._lock:
            self._workflows[workflow_id] = steps

        result_data = {
            "workflow_id": workflow_id,
            "name": name,
            "steps": len(steps),
            "step_ids": [s.step_id for s in steps]
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Workflow '{workflow_id}' defined with {len(steps)} steps",
            data=result_data
        )

    def _run_workflow(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a defined workflow."""
        workflow_id = params.get("workflow_id", "")
        context_vars = params.get("context_vars", {})
        save_to_var = params.get("save_to_var", None)

        with self._lock:
            steps = self._workflows.get(workflow_id, [])
            if not steps:
                return ActionResult(
                    success=False,
                    message=f"Workflow '{workflow_id}' not found"
                )
            steps = [s for s in steps]

        workflow_start = time.time()
        ready_queue = deque()
        completed: Set[str] = set()
        failed_steps: Set[str] = set()

        for step in steps:
            if not step.depends_on:
                ready_queue.append(step)

        results: Dict[str, Any] = {}

        while ready_queue and len(completed) + len(failed_steps) < len(steps):
            batch = []
            while ready_queue:
                step = ready_queue.popleft()
                if step.status == StepStatus.PENDING:
                    batch.append(step)

            if not batch:
                break

            for step in batch:
                step.status = StepStatus.RUNNING
                step.started_at = time.time()

            with self._lock:
                step_results = self._execute_steps(batch, context_vars, results)

            for step, result_data in zip(batch, step_results):
                step.completed_at = time.time()
                step.result = result_data
                results[step.step_id] = result_data

                if result_data.get("success"):
                    step.status = StepStatus.COMPLETED
                    completed.add(step.step_id)
                    for s in steps:
                        if s.status == StepStatus.PENDING:
                            if all(dep in completed for dep in s.depends_on):
                                ready_queue.append(s)
                else:
                    step.error = result_data.get("error")
                    if step.retry_count < step.max_retries:
                        step.retry_count += 1
                        step.status = StepStatus.PENDING
                        ready_queue.append(step)
                    else:
                        step.status = StepStatus.FAILED
                        failed_steps.add(step.step_id)

        total_time = time.time() - workflow_start

        result_data = {
            "workflow_id": workflow_id,
            "status": "completed" if not failed_steps else "failed",
            "steps_completed": len(completed),
            "steps_failed": len(failed_steps),
            "total_time": total_time,
            "step_results": results
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=not failed_steps,
            message=f"Workflow '{workflow_id}': "
                    f"{len(completed)} completed, {len(failed_steps)} failed "
                    f"in {total_time:.2f}s",
            data=result_data
        )

    def _execute_steps(self, steps: List[OrchestrationStep],
                       context_vars: Dict, results: Dict) -> List[Dict]:
        """Execute a batch of steps."""
        output = []
        for step in steps:
            result = {"success": False, "step_id": step.step_id}

            try:
                if step.step_type == StepType.API_REQUEST:
                    result = self._execute_api_step(step, context_vars, results)
                elif step.step_type == StepType.TRANSFORM:
                    result = self._execute_transform_step(step, context_vars, results)
                elif step.step_type == StepType.AGGREGATE:
                    result = self._execute_aggregate_step(step, context_vars, results)
                elif step.step_type == StepType.CONDITION:
                    result = self._execute_condition_step(step, context_vars, results)
                else:
                    result = {"success": True, "step_id": step.step_id, "skipped": True}

            except Exception as e:
                result = {"success": False, "step_id": step.step_id, "error": str(e)}

            output.append(result)

        return output

    def _execute_api_step(self, step: OrchestrationStep,
                          context_vars: Dict, results: Dict) -> Dict:
        """Execute an API request step."""
        config = step.config
        url = self._resolve_template(config.get("url", ""), context_vars, results)
        method = config.get("method", "GET").upper()
        headers = config.get("headers", {})
        data = config.get("data")
        timeout = config.get("timeout", step.timeout)

        resolved_headers = {}
        for k, v in headers.items():
            resolved_headers[k] = self._resolve_template(str(v), context_vars, results)

        try:
            full_url = url
            if config.get("params"):
                encoded = urllib.parse.urlencode(config["params"])
                sep = "&" if "?" in url else "?"
                full_url = f"{url}{sep}{encoded}"

            body = None
            if data and method in ("POST", "PUT", "PATCH"):
                resolved_data = self._resolve_template(json.dumps(data), context_vars, results)
                body = resolved_data.encode("utf-8")
                resolved_headers.setdefault("Content-Type", "application/json")

            req = urllib.request.Request(
                full_url, data=body,
                headers=resolved_headers,
                method=method
            )

            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read()
                ct = response.headers.get("Content-Type", "")
                if "application/json" in ct:
                    body_out = json.loads(raw.decode("utf-8"))
                else:
                    body_out = raw.decode("utf-8", errors="replace")

                return {
                    "success": True,
                    "step_id": step.step_id,
                    "status": response.status,
                    "body": body_out,
                    "elapsed": response.status
                }

        except urllib.error.HTTPError as e:
            return {"success": False, "step_id": step.step_id, "error": f"HTTP {e.code}: {e.reason}"}
        except urllib.error.URLError as e:
            return {"success": False, "step_id": step.step_id, "error": str(e.reason)}
        except Exception as e:
            return {"success": False, "step_id": step.step_id, "error": str(e)}

    def _execute_transform_step(self, step: OrchestrationStep,
                                context_vars: Dict, results: Dict) -> Dict:
        """Execute a data transformation step."""
        config = step.config
        source_step = config.get("source", None)
        transform_type = config.get("transform_type", "identity")

        source_data = results.get(source_step, {}).get("body") if source_step else None

        if source_data is None:
            return {"success": True, "step_id": step.step_id, "data": None, "skipped": True}

        if transform_type == "pick":
            fields = config.get("fields", [])
            if isinstance(source_data, dict):
                transformed = {k: source_data.get(k) for k in fields if k in source_data}
            else:
                transformed = source_data
        elif transform_type == "omit":
            fields = config.get("fields", [])
            if isinstance(source_data, dict):
                transformed = {k: v for k, v in source_data.items() if k not in fields}
            else:
                transformed = source_data
        elif transform_type == "map":
            mapping = config.get("mapping", {})
            transformed = {mapping.get(k, k): v for k, v in source_data.items()} \
                if isinstance(source_data, dict) else source_data
        else:
            transformed = source_data

        return {"success": True, "step_id": step.step_id, "data": transformed}

    def _execute_aggregate_step(self, step: OrchestrationStep,
                                context_vars: Dict, results: Dict) -> Dict:
        """Execute an aggregation step."""
        config = step.config
        sources = config.get("sources", [])
        agg_type = config.get("agg_type", "merge")

        source_values = []
        for src in sources:
            data = results.get(src, {}).get("body")
            if data is not None:
                source_values.append(data)

        if agg_type == "merge":
            if all(isinstance(v, dict) for v in source_values):
                aggregated = {}
                for v in source_values:
                    aggregated.update(v)
            else:
                aggregated = source_values
        elif agg_type == "concat":
            aggregated = source_values
        elif agg_type == "first":
            aggregated = source_values[0] if source_values else None
        elif agg_type == "last":
            aggregated = source_values[-1] if source_values else None
        else:
            aggregated = source_values

        return {"success": True, "step_id": step.step_id, "data": aggregated}

    def _execute_condition_step(self, step: OrchestrationStep,
                                 context_vars: Dict, results: Dict) -> Dict:
        """Execute a conditional step."""
        config = step.config
        source_step = config.get("source", None)
        condition = config.get("condition", "")
        true_steps = config.get("true_steps", [])
        false_steps = config.get("false_steps", [])

        source_data = results.get(source_step, {}).get("body") if source_step else None

        eval_result = False
        if isinstance(source_data, dict) and condition:
            try:
                for k, v in source_data.items():
                    cond = condition.replace(f"${k}", repr(v))
                eval_result = eval(cond, {"__builtins__": {}}, {})
            except Exception:
                eval_result = bool(source_data)
        else:
            eval_result = bool(source_data)

        return {
            "success": True,
            "step_id": step.step_id,
            "condition_result": eval_result,
            "branches": true_steps if eval_result else false_steps
        }

    def _resolve_template(self, template: str, context_vars: Dict,
                         results: Dict) -> str:
        """Resolve variable references in a template string."""
        if not isinstance(template, str):
            return template

        for step_id, result in results.items():
            placeholder = f"${{results.{step_id}}}"
            if placeholder in template:
                body = result.get("body", result)
                template = template.replace(placeholder, json.dumps(body))

        for var_name, var_val in context_vars.items():
            placeholder = f"${{vars.{var_name}}}"
            if placeholder in template:
                template = template.replace(placeholder, str(var_val))

        return template

    def _run_parallel_steps(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run multiple API steps in parallel."""
        steps_config = params.get("steps", [])
        timeout = params.get("timeout", 60.0)
        save_to_var = params.get("save_to_var", None)

        import uuid
        steps = []
        for i, cfg in enumerate(steps_config):
            step = OrchestrationStep(
                step_id=cfg.get("id", f"parallel_{i}"),
                step_type=StepType.API_REQUEST,
                name=cfg.get("name", f"Parallel Step {i}"),
                config=cfg,
                timeout=cfg.get("timeout", timeout)
            )
            steps.append(step)

        start = time.time()
        results_list = self._execute_steps(steps, {}, {})
        elapsed = time.time() - start

        results_dict = {r["step_id"]: r for r in results_list}
        success_count = sum(1 for r in results_list if r.get("success"))

        result_data = {
            "total": len(steps),
            "successful": success_count,
            "failed": len(steps) - success_count,
            "elapsed": elapsed,
            "results": results_dict
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=success_count == len(steps),
            message=f"Parallel execution: {success_count}/{len(steps)} succeeded "
                    f"in {elapsed:.2f}s",
            data=result_data
        )

    def _run_chained_steps(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run steps sequentially, passing output to input."""
        steps_config = params.get("steps", [])
        save_to_var = params.get("save_to_var", None)

        steps = []
        for i, cfg in enumerate(steps_config):
            step = OrchestrationStep(
                step_id=cfg.get("id", f"chain_{i}"),
                step_type=StepType.API_REQUEST,
                name=cfg.get("name", f"Chain Step {i}"),
                config=cfg,
                timeout=cfg.get("timeout", 60.0)
            )
            steps.append(step)

        context_vars = params.get("context_vars", {})
        results: Dict[str, Any] = {}
        start = time.time()

        for i, step in enumerate(steps):
            step.depends_on = [steps[i - 1].step_id] if i > 0 else []
            step_results = self._execute_steps([step], context_vars, results)
            results[step.step_id] = step_results[0]
            if not step_results[0].get("success"):
                return ActionResult(
                    success=False,
                    message=f"Chain failed at step {step.step_id}: "
                            f"{step_results[0].get('error')}",
                    data={"failed_at": step.step_id, "results": results}
                )

        elapsed = time.time() - start
        result_data = {
            "steps": len(steps),
            "elapsed": elapsed,
            "results": results
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Chain of {len(steps)} steps completed in {elapsed:.2f}s",
            data=result_data
        )

    def _get_workflow_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get status of a workflow or step."""
        workflow_id = params.get("workflow_id", None)
        save_to_var = params.get("save_to_var", None)

        with self._lock:
            if workflow_id:
                steps = self._workflows.get(workflow_id, [])
                data = {
                    "workflow_id": workflow_id,
                    "total_steps": len(steps),
                    "steps": [
                        {"id": s.step_id, "status": s.status.value,
                         "type": s.step_type.value, "name": s.name}
                        for s in steps
                    ]
                }
            else:
                data = {
                    "workflows": list(self._workflows.keys()),
                    "total": len(self._workflows)
                }

        if save_to_var:
            context.variables[save_to_var] = data

        return ActionResult(success=True, message="Status retrieved", data=data)

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "workflow_id": None,
            "name": "Workflow",
            "steps": [],
            "context_vars": {},
            "timeout": 60.0,
            "save_to_var": None
        }
