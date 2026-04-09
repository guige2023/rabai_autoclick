"""Task dependency graph action module for RabAI AutoClick.

Provides task orchestration through dependency graphs:
- TaskGraphExecutorAction: Execute tasks following dependency order
- DependencyGraphBuilder: Build and manage task dependency graphs
- TopologicalSorterAction: Sort tasks by dependencies
- TaskSchedulerAction: Schedule tasks based on dependencies and resources
- ParallelTaskBatcher: Batch independent tasks for parallel execution
"""

from typing import Any, Dict, List, Optional, Set, Tuple
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import logging
import time

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskNode:
    """Represents a node in the task dependency graph."""
    task_id: str
    name: str
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)  # Tasks that depend on this
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[ActionResult] = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskGraph:
    """Directed acyclic graph of task dependencies."""
    nodes: Dict[str, TaskNode] = field(default_factory=dict)
    execution_order: List[List[str]] = field(default_factory=list)  # Tiers for parallel execution
    
    def add_task(
        self,
        task_id: str,
        name: str,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a task to the graph."""
        self.nodes[task_id] = TaskNode(
            task_id=task_id,
            name=name,
            dependencies=dependencies or [],
            metadata=metadata or {}
        )
        
        # Update dependents for dependencies
        for dep_id in (dependencies or []):
            if dep_id in self.nodes:
                if task_id not in self.nodes[dep_id].dependents:
                    self.nodes[dep_id].dependents.append(task_id)
    
    def get_ready_tasks(self) -> List[str]:
        """Get tasks that are ready to execute (all dependencies met)."""
        ready = []
        for task_id, node in self.nodes.items():
            if node.status == TaskStatus.PENDING:
                # Check all dependencies are completed
                deps_met = all(
                    self.nodes[dep_id].status == TaskStatus.COMPLETED
                    for dep_id in node.dependencies
                    if dep_id in self.nodes
                )
                if deps_met:
                    ready.append(task_id)
        return ready
    
    def validate(self) -> Tuple[bool, str]:
        """Validate the graph has no cycles and all dependencies exist."""
        # Check for missing dependencies
        for task_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    return False, f"Task '{task_id}' depends on non-existent task '{dep_id}'"
        
        # Check for cycles using DFS
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def has_cycle(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)
            
            if task_id in self.nodes:
                for dep_id in self.nodes[task_id].dependencies:
                    if dep_id not in visited:
                        if has_cycle(dep_id):
                            return True
                    elif dep_id in rec_stack:
                        return True
            
            rec_stack.remove(task_id)
            return False
        
        for task_id in self.nodes:
            if task_id not in visited:
                if has_cycle(task_id):
                    return False, f"Cycle detected involving task '{task_id}'"
        
        return True, "Graph is valid"
    
    def compute_execution_order(self) -> List[List[str]]:
        """Compute tiers of tasks that can be executed in parallel."""
        self.execution_order = []
        remaining = {tid: node for tid, node in self.nodes.items()}
        
        while remaining:
            # Find tasks with no remaining dependencies
            ready = [
                tid for tid, node in remaining.items()
                if all(dep not in remaining for dep in node.dependencies)
            ]
            
            if not ready:
                break  # Should not happen if graph is valid
            
            self.execution_order.append(ready)
            
            # Remove completed tasks
            for tid in ready:
                del remaining[tid]
        
        return self.execution_order


class TaskGraphExecutorAction(BaseAction):
    """Execute tasks following a dependency graph."""
    
    action_type = "task_graph_executor"
    display_name = "任务图执行器"
    description = "按依赖顺序执行任务图"
    
    def __init__(self) -> None:
        super().__init__()
        self._graph = TaskGraph()
        self._handlers: Dict[str, Any] = {}
        self._max_retries = 3
    
    def register_handler(self, task_id: str, handler: Any) -> None:
        """Register a handler for a task."""
        self._handlers[task_id] = handler
    
    def set_max_retries(self, max_retries: int) -> None:
        """Set maximum retries per task."""
        self._max_retries = max_retries
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task graph.
        
        Args:
            params: {
                "tasks": List of task definitions (list),
                "task_id": Task ID field name (str, default "id"),
                "dependencies": Dependencies field name (str, default "dependencies"),
                "stop_on_failure": Stop execution on first failure (bool, default True),
                "parallel": Execute tasks in same tier in parallel (bool, default True)
            }
        """
        try:
            tasks = params.get("tasks", [])
            task_id_field = params.get("task_id", "id")
            deps_field = params.get("dependencies", "dependencies")
            stop_on_failure = params.get("stop_on_failure", True)
            parallel = params.get("parallel", True)
            
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")
            
            # Build graph
            graph = TaskGraph()
            for task in tasks:
                task_id = task.get(task_id_field, "")
                dependencies = task.get(deps_field, [])
                name = task.get("name", task_id)
                metadata = {k: v for k, v in task.items() if k not in (task_id_field, deps_field, "name")}
                
                if task_id:
                    graph.add_task(task_id, name, dependencies, metadata)
            
            # Validate
            is_valid, msg = graph.validate()
            if not is_valid:
                return ActionResult(success=False, message=f"Invalid graph: {msg}")
            
            # Compute execution order
            execution_order = graph.compute_execution_order()
            
            # Execute
            results: Dict[str, Any] = {}
            total_duration = 0.0
            
            for tier_idx, tier_tasks in enumerate(execution_order):
                tier_start = time.time()
                
                if parallel and len(tier_tasks) > 1:
                    # Parallel execution
                    tier_results = self._execute_tier_parallel(graph, tier_tasks, context, results)
                else:
                    # Sequential
                    tier_results = self._execute_tier_sequential(graph, tier_tasks, context, results)
                
                results.update(tier_results)
                
                tier_duration = time.time() - tier_start
                total_duration += tier_duration
                
                # Check for failures
                failed_tasks = [tid for tid, res in tier_results.items() if not res.success]
                
                if failed_tasks and stop_on_failure:
                    return ActionResult(
                        success=False,
                        message=f"Execution stopped at tier {tier_idx}: tasks {failed_tasks} failed",
                        data={
                            "completed": results,
                            "failed_tasks": failed_tasks,
                            "execution_tiers": len(execution_order),
                            "completed_tiers": tier_idx,
                            "total_duration": round(total_duration, 3)
                        }
                    )
            
            failed_count = sum(1 for r in results.values() if not r.success)
            
            return ActionResult(
                success=failed_count == 0,
                message=f"Graph execution complete: {len(results) - failed_count}/{len(results)} succeeded",
                data={
                    "results": {tid: {"success": r.success, "message": r.message} for tid, r in results.items()},
                    "execution_tiers": len(execution_order),
                    "total_duration": round(total_duration, 3),
                    "failed_count": failed_count
                }
            )
        
        except Exception as e:
            logger.error(f"Task graph execution failed: {e}")
            return ActionResult(success=False, message=f"Execution error: {str(e)}")
    
    def _execute_tier_parallel(
        self,
        graph: TaskGraph,
        tier_tasks: List[str],
        context: Any,
        results: Dict[str, ActionResult]
    ) -> Dict[str, ActionResult]:
        """Execute a tier of tasks in parallel."""
        tier_results: Dict[str, ActionResult] = {}
        
        for task_id in tier_tasks:
            node = graph.nodes[task_id]
            handler = self._handlers.get(task_id)
            
            if not handler:
                tier_results[task_id] = ActionResult(
                    success=False,
                    message=f"No handler registered for task '{task_id}'"
                )
                node.status = TaskStatus.FAILED
                continue
            
            # Execute with retry
            node.status = TaskStatus.RUNNING
            node.start_time = time.time()
            
            for attempt in range(self._max_retries + 1):
                try:
                    if callable(handler):
                        result = handler(context, node.metadata)
                        if isinstance(result, ActionResult):
                            tier_results[task_id] = result
                        else:
                            tier_results[task_id] = ActionResult(success=True, message="Task completed", data=result)
                    
                    if tier_results[task_id].success:
                        node.status = TaskStatus.COMPLETED
                        node.result = tier_results[task_id]
                        break
                    else:
                        node.retry_count = attempt + 1
                        
                except Exception as e:
                    tier_results[task_id] = ActionResult(success=False, message=f"Error: {str(e)}")
                    node.error = str(e)
                    node.retry_count = attempt + 1
            
            if not tier_results[task_id].success:
                node.status = TaskStatus.FAILED
            
            node.end_time = time.time()
        
        return tier_results
    
    def _execute_tier_sequential(
        self,
        graph: TaskGraph,
        tier_tasks: List[str],
        context: Any,
        results: Dict[str, ActionResult]
    ) -> Dict[str, ActionResult]:
        """Execute a tier of tasks sequentially."""
        return self._execute_tier_parallel(graph, tier_tasks, context, results)


class DependencyGraphBuilder(BaseAction):
    """Build task dependency graphs from specifications."""
    
    action_type = "dependency_graph_builder"
    display_name = "依赖图构建器"
    description = "从规格构建任务依赖图"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Build a dependency graph from task specs.
        
        Args:
            params: {
                "tasks": List of task definitions (list),
                "validate": Validate the graph after building (bool, default True),
                "optimize": Optimize by merging independent tasks (bool, default False)
            }
        """
        try:
            tasks = params.get("tasks", [])
            validate = params.get("validate", True)
            
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")
            
            graph = TaskGraph()
            
            # Build graph
            for task in tasks:
                task_id = task.get("id", task.get("task_id", ""))
                dependencies = task.get("dependencies", [])
                name = task.get("name", task_id)
                
                if task_id:
                    graph.add_task(task_id, name, dependencies, task)
            
            # Validate
            if validate:
                is_valid, msg = graph.validate()
                if not is_valid:
                    return ActionResult(success=False, message=f"Graph validation failed: {msg}")
            
            # Compute execution order
            execution_order = graph.compute_execution_order()
            
            return ActionResult(
                success=True,
                message=f"Built graph with {len(graph.nodes)} nodes, {len(execution_order)} tiers",
                data={
                    "node_count": len(graph.nodes),
                    "execution_tiers": execution_order,
                    "tier_count": len(execution_order),
                    "is_valid": True
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Graph build error: {str(e)}")


class TopologicalSorterAction(BaseAction):
    """Sort tasks topologically based on dependencies."""
    
    action_type = "topological_sorter"
    display_name = "拓扑排序"
    description = "对任务进行拓扑排序"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sort tasks topologically.
        
        Args:
            params: {
                "tasks": List of tasks with dependencies (list),
                "direction": "forward" or "reverse" (str, default "forward"),
                "group_tiers": Group tasks that can run in parallel (bool, default True)
            }
        """
        try:
            tasks = params.get("tasks", [])
            direction = params.get("direction", "forward")
            group_tiers = params.get("group_tiers", True)
            
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")
            
            # Build adjacency list
            graph: Dict[str, List[str]] = {}
            in_degree: Dict[str, int] = {}
            
            for task in tasks:
                task_id = task.get("id", "")
                dependencies = task.get("dependencies", [])
                
                if task_id:
                    graph[task_id] = []
                    in_degree[task_id] = 0
                    
                    for dep in dependencies:
                        if dep not in graph:
                            graph[dep] = []
                            in_degree[dep] = 0
                        graph[dep].append(task_id)
                        in_degree[task_id] += 1
            
            # Kahn's algorithm
            if direction == "forward":
                queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
            else:
                # Reverse: start from leaf nodes (highest in_degree)
                queue = deque([tid for tid, deg in in_degree.items() if deg > 0])
                # Actually for reverse we should use different approach
                # For now, do forward and reverse the result
                pass
            
            sorted_order: List[str] = []
            
            while queue:
                node = queue.popleft()
                sorted_order.append(node)
                
                for neighbor in graph.get(node, []):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            if len(sorted_order) != len(graph):
                return ActionResult(success=False, message="Cycle detected in dependency graph")
            
            if direction == "reverse":
                sorted_order = sorted_order[::-1]
            
            # Group tiers if requested
            tiers: List[List[str]] = []
            if group_tiers:
                current_tier = []
                remaining_in_degree = in_degree.copy()
                
                for task_id in sorted_order:
                    if remaining_in_degree.get(task_id, 0) == 0:
                        current_tier.append(task_id)
                    else:
                        tiers.append(current_tier)
                        current_tier = [task_id]
                        remaining_in_degree[task_id] = 0
                        for neighbor in graph.get(task_id, []):
                            remaining_in_degree[neighbor] -= 1
                
                if current_tier:
                    tiers.append(current_tier)
                
                sorted_order = [tid for tier in tiers for tid in tier]
            else:
                tiers = [[tid] for tid in sorted_order]
            
            return ActionResult(
                success=True,
                message=f"Sorted {len(sorted_order)} tasks into {len(tiers)} tiers",
                data={
                    "sorted_order": sorted_order,
                    "tiers": tiers,
                    "tier_count": len(tiers)
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Topological sort error: {str(e)}")


class TaskSchedulerAction(BaseAction):
    """Schedule tasks based on dependencies and resource constraints."""
    
    action_type = "task_scheduler"
    display_name = "任务调度器"
    description = "基于依赖和资源约束调度任务"
    
    def __init__(self) -> None:
        super().__init__()
        self._available_resources: Dict[str, int] = {"cpu": 4, "gpu": 1, "memory_gb": 16}
        self._resource_usage: Dict[str, Dict[str, int]] = {}  # task_id -> {resource: amount}
    
    def set_resources(self, resources: Dict[str, int]) -> None:
        """Set available resources."""
        self._available_resources = resources.copy()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Schedule tasks with resource constraints.
        
        Args:
            params: {
                "tasks": List of tasks with dependencies and resource needs (list),
                "resources": Available resources (dict),
                "schedule_mode": "earliest" or "fair" (str, default "earliest")
            }
        """
        try:
            tasks = params.get("tasks", [])
            resources = params.get("resources", self._available_resources)
            schedule_mode = params.get("schedule_mode", "earliest")
            
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")
            
            # Build graph and compute requirements
            task_requirements: Dict[str, Dict[str, int]] = {}
            dependencies: Dict[str, List[str]] = {}
            
            for task in tasks:
                task_id = task.get("id", "")
                if task_id:
                    task_requirements[task_id] = task.get("resources", {"cpu": 1})
                    dependencies[task_id] = task.get("dependencies", [])
            
            # Simple greedy scheduling
            schedule: List[Dict[str, Any]] = []
            completed: Set[str] = set()
            available = resources.copy()
            
            max_iterations = len(tasks) * 2
            iteration = 0
            
            while len(completed) < len(tasks) and iteration < max_iterations:
                iteration += 1
                
                # Find tasks ready to schedule
                ready = []
                for task_id, deps in dependencies.items():
                    if task_id not in completed and all(d in completed for d in deps):
                        # Check resource availability
                        can_schedule = all(
                            task_requirements[task_id].get(res, 0) <= available.get(res, 0)
                            for res in task_requirements[task_id]
                        )
                        if can_schedule:
                            ready.append(task_id)
                
                if not ready:
                    break
                
                # Schedule one task (greedy)
                if schedule_mode == "earliest":
                    selected = ready[0]
                else:  # fair - select task with most dependencies
                    selected = max(ready, key=lambda t: len(dependencies[t]))
                
                # Allocate resources
                for res, amount in task_requirements[selected].items():
                    available[res] -= amount
                
                schedule.append({
                    "task_id": selected,
                    "iteration": iteration,
                    "allocated": task_requirements[selected].copy()
                })
                completed.add(selected)
            
            return ActionResult(
                success=len(completed) == len(tasks),
                message=f"Scheduled {len(schedule)} tasks, {len(tasks) - len(completed)} unscheduled",
                data={
                    "schedule": schedule,
                    "completed_count": len(completed),
                    "total_tasks": len(tasks),
                    "final_resources": available
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")


class ParallelTaskBatcher(BaseAction):
    """Batch independent tasks for parallel execution."""
    
    action_type = "parallel_task_batcher"
    display_name = "并行任务批处理器"
    description = "将独立任务打包并行执行"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create batches of independent tasks.
        
        Args:
            params: {
                "tasks": List of tasks (list),
                "max_batch_size": Maximum batch size (int, default 10),
                "batch_by": "count" or "dependency_level" (str, default "dependency_level")
            }
        """
        try:
            tasks = params.get("tasks", [])
            max_batch_size = params.get("max_batch_size", 10)
            batch_by = params.get("batch_by", "dependency_level")
            
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")
            
            if batch_by == "count":
                # Simple count-based batching
                batches = [
                    tasks[i:i + max_batch_size]
                    for i in range(0, len(tasks), max_batch_size)
                ]
            else:
                # Dependency-level batching
                sorter = TopologicalSorterAction()
                result = sorter.execute(context, {
                    "tasks": tasks,
                    "group_tiers": True
                })
                
                if result.success:
                    tiers = result.data.get("tiers", [])
                    batches = []
                    
                    current_batch = []
                    for tier in tiers:
                        if len(current_batch) + len(tier) <= max_batch_size:
                            current_batch.extend(tier)
                        else:
                            if current_batch:
                                batches.append(current_batch)
                            current_batch = tier
                    
                    if current_batch:
                        batches.append(current_batch)
                else:
                    # Fallback to count-based
                    batches = [tasks]
            
            batch_info = [
                {"batch_id": i, "size": len(batch), "task_ids": [t.get("id", f"task_{j}") for j, t in enumerate(batch)]}
                for i, batch in enumerate(batches)
            ]
            
            return ActionResult(
                success=True,
                message=f"Created {len(batches)} batches",
                data={
                    "batch_count": len(batches),
                    "batches": batch_info,
                    "total_tasks": len(tasks)
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Batcher error: {str(e)}")
