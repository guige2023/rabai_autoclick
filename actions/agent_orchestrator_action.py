"""
Agent orchestrator action for multi-agent coordination.

Provides task delegation, result aggregation, and agent lifecycle management.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading
import uuid


class AgentOrchestratorAction:
    """Multi-agent orchestration for coordinated task execution."""

    def __init__(
        self,
        max_concurrent_agents: int = 5,
        result_timeout: float = 60.0,
        enable_aggregation: bool = True,
    ) -> None:
        """
        Initialize agent orchestrator.

        Args:
            max_concurrent_agents: Maximum parallel agent executions
            result_timeout: Timeout for collecting agent results
            enable_aggregation: Enable result aggregation
        """
        self.max_concurrent_agents = max_concurrent_agents
        self.result_timeout = result_timeout
        self.enable_aggregation = enable_aggregation

        self._agents: Dict[str, Dict[str, Any]] = {}
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._results: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute orchestration operation.

        Args:
            params: Dictionary containing:
                - operation: 'register', 'delegate', 'aggregate', 'status'
                - agent_id: Agent identifier
                - task: Task to delegate
                - tasks: List of tasks for parallel execution

        Returns:
            Dictionary with orchestration result
        """
        operation = params.get("operation", "delegate")

        if operation == "register":
            return self._register_agent(params)
        elif operation == "delegate":
            return self._delegate_task(params)
        elif operation == "delegate_all":
            return self._delegate_all(params)
        elif operation == "aggregate":
            return self._aggregate_results(params)
        elif operation == "status":
            return self._get_status(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _register_agent(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register an agent with the orchestrator."""
        agent_id = params.get("agent_id", str(uuid.uuid4()))
        agent_config = params.get("config", {})

        self._agents[agent_id] = {
            "id": agent_id,
            "config": agent_config,
            "status": "idle",
            "registered_at": time.time(),
            "tasks_completed": 0,
        }

        return {"success": True, "agent_id": agent_id, "status": "registered"}

    def _delegate_task(self, params: dict[str, Any]) -> dict[str, Any]:
        """Delegate task to a specific agent."""
        agent_id = params.get("agent_id", "")
        task = params.get("task", {})
        task_id = params.get("task_id", str(uuid.uuid4()))

        if agent_id not in self._agents:
            return {"success": False, "error": f"Agent '{agent_id}' not found"}

        agent = self._agents[agent_id]
        agent["status"] = "busy"

        self._tasks[task_id] = {
            "id": task_id,
            "agent_id": agent_id,
            "task": task,
            "status": "running",
            "created_at": time.time(),
        }

        try:
            result = self._execute_agent_task(agent_id, task)

            self._tasks[task_id]["status"] = "completed"
            self._tasks[task_id]["completed_at"] = time.time()
            self._tasks[task_id]["result"] = result

            self._agents[agent_id]["status"] = "idle"
            self._agents[agent_id]["tasks_completed"] += 1

            return {
                "success": True,
                "task_id": task_id,
                "agent_id": agent_id,
                "result": result,
            }

        except Exception as e:
            self._tasks[task_id]["status"] = "failed"
            self._tasks[task_id]["error"] = str(e)
            self._agents[agent_id]["status"] = "idle"

            return {"success": False, "task_id": task_id, "error": str(e)}

    def _delegate_all(self, params: dict[str, Any]) -> dict[str, Any]:
        """Delegate multiple tasks to available agents."""
        tasks = params.get("tasks", [])
        parallel = params.get("parallel", True)

        if not tasks:
            return {"success": False, "error": "Tasks are required"}

        if parallel:
            return self._delegate_parallel(tasks)
        else:
            return self._delegate_sequential(tasks)

    def _delegate_parallel(self, tasks: List[Dict]) -> dict[str, Any]:
        """Execute tasks in parallel across agents."""
        results = []
        active_threads = []

        for task_def in tasks[:self.max_concurrent_agents]:
            thread = threading.Thread(
                target=self._execute_and_store, args=(task_def, results)
            )
            thread.start()
            active_threads.append(thread)

        for thread in active_threads:
            thread.join(timeout=self.result_timeout)

        return {
            "success": True,
            "completed": len(results),
            "total": len(tasks),
            "results": results,
        }

    def _delegate_sequential(self, tasks: List[Dict]) -> dict[str, Any]:
        """Execute tasks sequentially."""
        results = []

        for task_def in tasks:
            task_id = task_def.get("task_id", str(uuid.uuid4()))
            agent_id = task_def.get("agent_id", list(self._agents.keys())[0])

            result = self._delegate_task({
                "agent_id": agent_id,
                "task": task_def.get("task", task_def),
                "task_id": task_id,
            })

            results.append(result)

        return {
            "success": True,
            "completed": len(results),
            "total": len(tasks),
            "results": results,
        }

    def _execute_and_store(self, task_def: Dict, results_store: List) -> None:
        """Execute task and store result."""
        task_id = task_def.get("task_id", str(uuid.uuid4()))
        agent_id = task_def.get("agent_id", list(self._agents.keys())[0])

        result = self._delegate_task({
            "agent_id": agent_id,
            "task": task_def.get("task", task_def),
            "task_id": task_id,
        })

        results_store.append(result)

    def _execute_agent_task(self, agent_id: str, task: Dict) -> Any:
        """Execute task on specified agent (simulated)."""
        return {"success": True, "message": f"Task executed by {agent_id}"}

    def _aggregate_results(self, params: dict[str, Any]) -> dict[str, Any]:
        """Aggregate results from multiple agents."""
        task_ids = params.get("task_ids", [])
        aggregation_fn = params.get("aggregation_fn", "merge")

        if not task_ids:
            return {"success": False, "error": "Task IDs are required"}

        results = [self._tasks.get(tid, {}).get("result") for tid in task_ids]
        results = [r for r in results if r is not None]

        if aggregation_fn == "merge":
            aggregated = self._merge_results(results)
        elif aggregation_fn == "concat":
            aggregated = self._concat_results(results)
        elif aggregation_fn == "pick_first":
            aggregated = results[0] if results else None
        else:
            aggregated = results

        return {
            "success": True,
            "aggregated": aggregated,
            "result_count": len(results),
        }

    def _merge_results(self, results: List[Any]) -> Dict:
        """Merge result dictionaries."""
        merged = {}
        for result in results:
            if isinstance(result, dict):
                merged.update(result)
        return merged

    def _concat_results(self, results: List[Any]) -> List:
        """Concatenate result lists."""
        concatenated = []
        for result in results:
            if isinstance(result, list):
                concatenated.extend(result)
            else:
                concatenated.append(result)
        return concatenated

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get orchestrator status."""
        return {
            "success": True,
            "registered_agents": len(self._agents),
            "active_agents": sum(1 for a in self._agents.values() if a["status"] == "busy"),
            "total_tasks": len(self._tasks),
            "completed_tasks": sum(1 for t in self._tasks.values() if t["status"] == "completed"),
            "failed_tasks": sum(1 for t in self._tasks.values() if t["status"] == "failed"),
        }
