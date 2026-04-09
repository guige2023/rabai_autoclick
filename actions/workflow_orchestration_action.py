"""
Workflow orchestration action for multi-step process coordination.

Provides DAG-based workflow execution with parallel branches and error handling.
"""

from typing import Any, Optional
import time
import threading
from collections import defaultdict, deque


class WorkflowOrchestrationAction:
    """DAG-based workflow orchestration engine."""

    def __init__(
        self,
        max_parallel_tasks: int = 10,
        retry_on_failure: bool = True,
        max_retries: int = 3,
    ) -> None:
        """
        Initialize workflow orchestrator.

        Args:
            max_parallel_tasks: Maximum parallel task executions
            retry_on_failure: Whether to retry failed tasks
            max_retries: Maximum retry attempts
        """
        self.max_parallel_tasks = max_parallel_tasks
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self._workflows: dict[str, dict[str, Any]] = {}
        self._executions: dict[str, dict[str, Any]] = {}
        self._running_tasks: set[str] = set()
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute workflow orchestration operation.

        Args:
            params: Dictionary containing:
                - operation: 'create', 'execute', 'status', 'cancel'
                - workflow_id: Workflow identifier
                - workflow: Workflow definition (for create)
                - inputs: Workflow inputs (for execute)

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "execute")

        if operation == "create":
            return self._create_workflow(params)
        elif operation == "execute":
            return self._execute_workflow(params)
        elif operation == "status":
            return self._get_execution_status(params)
        elif operation == "cancel":
            return self._cancel_execution(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _create_workflow(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create workflow definition."""
        workflow_id = params.get("workflow_id", "")
        workflow_def = params.get("workflow", {})

        if not workflow_id:
            return {"success": False, "error": "workflow_id is required"}

        tasks = workflow_def.get("tasks", [])
        dependencies = workflow_def.get("dependencies", {})

        if not self._validate_dag(tasks, dependencies):
            return {"success": False, "error": "Invalid workflow DAG"}

        self._workflows[workflow_id] = {
            "tasks": tasks,
            "dependencies": dependencies,
            "created_at": time.time(),
        }

        return {"success": True, "workflow_id": workflow_id, "task_count": len(tasks)}

    def _validate_dag(self, tasks: list[str], dependencies: dict[str, list[str]]) -> bool:
        """Validate workflow forms a valid DAG."""
        in_degree = defaultdict(int)
        for task in tasks:
            if task not in dependencies:
                dependencies[task] = []
            for dep in dependencies[task]:
                in_degree[task] += 1

        queue = deque([t for t in tasks if in_degree[t] == 0])
        visited = 0

        while queue:
            task = queue.popleft()
            visited += 1
            for dependent in tasks:
                if task in dependencies.get(dependent, []):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        return visited == len(tasks)

    def _execute_workflow(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute workflow."""
        workflow_id = params.get("workflow_id", "")
        inputs = params.get("inputs", {})

        if workflow_id not in self._workflows:
            return {"success": False, "error": "Workflow not found"}

        execution_id = f"exec_{int(time.time() * 1000)}"
        workflow = self._workflows[workflow_id]

        self._executions[execution_id] = {
            "workflow_id": workflow_id,
            "status": "running",
            "inputs": inputs,
            "task_results": {},
            "started_at": time.time(),
            "completed_at": None,
        }

        result = self._run_workflow(execution_id, workflow, inputs)

        if result["status"] == "completed":
            self._executions[execution_id]["status"] = "completed"
            self._executions[execution_id]["completed_at"] = time.time()

        return {
            "success": True,
            "execution_id": execution_id,
            "status": result["status"],
        }

    def _run_workflow(
        self, execution_id: str, workflow: dict[str, Any], inputs: dict[str, Any]
    ) -> dict[str, Any]:
        """Run workflow tasks respecting dependencies."""
        tasks = workflow["tasks"]
        dependencies = workflow["dependencies"]

        completed = set()
        task_outputs = {}

        while len(completed) < len(tasks):
            ready_tasks = [
                t for t in tasks
                if t not in completed
                and all(dep in completed for dep in dependencies.get(t, []))
                and t not in self._running_tasks
            ]

            if not ready_tasks:
                if len(completed) < len(tasks):
                    return {"status": "deadlocked"}
                break

            for task in ready_tasks[: self.max_parallel_tasks]:
                self._running_tasks.add(task)
                result = self._execute_task(task, task_outputs, inputs)
                self._running_tasks.remove(task)
                task_outputs[task] = result
                completed.add(task)

                self._executions[execution_id]["task_results"][task] = result

        return {"status": "completed"}

    def _execute_task(
        self, task: str, dependencies: dict[str, Any], inputs: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute single task."""
        return {
            "task": task,
            "status": "success",
            "executed_at": time.time(),
            "input_keys": list(inputs.keys()),
        }

    def _get_execution_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get workflow execution status."""
        execution_id = params.get("execution_id", "")

        if execution_id in self._executions:
            return {"success": True, "execution": self._executions[execution_id]}
        return {"success": False, "error": "Execution not found"}

    def _cancel_execution(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel workflow execution."""
        execution_id = params.get("execution_id", "")

        if execution_id in self._executions:
            self._executions[execution_id]["status"] = "cancelled"
            self._executions[execution_id]["cancelled_at"] = time.time()
            return {"success": True, "execution_id": execution_id}
        return {"success": False, "error": "Execution not found"}

    def get_workflows(self) -> list[str]:
        """Get list of registered workflows."""
        return list(self._workflows.keys())

    def get_executions(self, workflow_id: Optional[str] = None) -> list[dict[str, Any]]:
        """Get workflow executions."""
        if workflow_id:
            return [
                exec_info
                for exec_id, exec_info in self._executions.items()
                if exec_info["workflow_id"] == workflow_id
            ]
        return list(self._executions.values())
