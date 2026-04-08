"""Automation Coordinator action module for RabAI AutoClick.

Coordinates multiple automation tasks, manages dependencies,
and tracks execution order.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class AutomationCoordinatorAction(BaseAction):
    """Coordinate execution of multiple automation tasks.

    Manages task dependencies, execution order, parallel
    execution, and result aggregation.
    """
    action_type = "automation_coordinator"
    display_name = "自动化协调器"
    description = "协调多个自动化任务的执行顺序和依赖"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Coordinate task execution.

        Args:
            context: Execution context.
            params: Dict with keys: tasks (list), mode (sequential/parallel),
                   stop_on_failure, aggregate_results.

        Returns:
            ActionResult with coordinated execution results.
        """
        start_time = time.time()
        try:
            tasks = params.get('tasks', [])
            mode = params.get('mode', 'sequential')
            stop_on_failure = params.get('stop_on_failure', False)
            aggregate_results = params.get('aggregate_results', True)

            if not tasks:
                return ActionResult(
                    success=False,
                    message="No tasks provided",
                    duration=time.time() - start_time,
                )

            results = []
            if mode == 'parallel':
                results = self._execute_parallel(tasks, stop_on_failure, context)
            else:
                results = self._execute_sequential(tasks, stop_on_failure, context)

            all_success = all(r.get('success', False) for r in results)
            total_duration = time.time() - start_time

            return ActionResult(
                success=all_success,
                message=f"Coordinator: {len(results)} tasks, {'all succeeded' if all_success else 'some failed'}",
                data={
                    'total': len(results),
                    'succeeded': sum(1 for r in results if r.get('success')),
                    'failed': sum(1 for r in results if not r.get('success')),
                    'results': results,
                },
                duration=total_duration,
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Coordinator error: {str(e)}",
                duration=time.time() - start_time,
            )

    def _execute_sequential(
        self,
        tasks: List[Dict],
        stop_on_failure: bool,
        context: Any
    ) -> List[Dict]:
        """Execute tasks sequentially."""
        results = []
        for task in tasks:
            task_name = task.get('name', 'unnamed')
            action = task.get('action')
            task_params = task.get('params', {})

            task_start = time.time()
            try:
                if callable(action):
                    result = action(context, task_params)
                elif hasattr(context, 'execute_action'):
                    result = context.execute_action(action, task_params)
                else:
                    result = ActionResult(success=False, message=f"No executor for action: {action}")

                if isinstance(result, ActionResult):
                    task_result = {
                        'name': task_name,
                        'action': action,
                        'success': result.success,
                        'message': result.message,
                        'data': result.data,
                        'duration': result.duration,
                    }
                else:
                    task_result = {
                        'name': task_name,
                        'action': action,
                        'success': False,
                        'message': 'Invalid result type',
                    }
            except Exception as e:
                task_result = {
                    'name': task_name,
                    'action': action,
                    'success': False,
                    'message': str(e),
                    'traceback': traceback.format_exc(),
                    'duration': time.time() - task_start,
                }

            results.append(task_result)
            if stop_on_failure and not task_result['success']:
                # Fill remaining with skipped
                for remaining in tasks[len(results):]:
                    results.append({
                        'name': remaining.get('name', 'unnamed'),
                        'action': remaining.get('action'),
                        'success': False,
                        'message': 'skipped due to earlier failure',
                        'status': 'skipped',
                    })
                break

        return results

    def _execute_parallel(
        self,
        tasks: List[Dict],
        stop_on_failure: bool,
        context: Any
    ) -> List[Dict]:
        """Execute tasks in parallel using threads."""
        import concurrent.futures

        def run_task(task: Dict) -> Dict:
            task_name = task.get('name', 'unnamed')
            action = task.get('action')
            task_params = task.get('params', {})
            task_start = time.time()

            try:
                if callable(action):
                    result = action(context, task_params)
                elif hasattr(context, 'execute_action'):
                    result = context.execute_action(action, task_params)
                else:
                    result = ActionResult(success=False, message=f"No executor for action: {action}")

                if isinstance(result, ActionResult):
                    return {
                        'name': task_name,
                        'action': action,
                        'success': result.success,
                        'message': result.message,
                        'data': result.data,
                        'duration': result.duration,
                    }
                return {
                    'name': task_name,
                    'action': action,
                    'success': False,
                    'message': 'Invalid result type',
                    'duration': time.time() - task_start,
                }
            except Exception as e:
                return {
                    'name': task_name,
                    'action': action,
                    'success': False,
                    'message': str(e),
                    'duration': time.time() - task_start,
                }

        results = []
        max_workers = min(len(tasks), 10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_task, task): task for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        # Reorder to match input order
        name_to_result = {r['name']: r for r in results}
        ordered = [name_to_result.get(t.get('name', ''), {}) for t in tasks]

        # Check if should stop on failure
        if stop_on_failure:
            first_failure = next((r for r in ordered if not r.get('success', True)), None)
            if first_failure:
                for r in ordered:
                    if r.get('status') is None:
                        r['status'] = 'skipped'

        return ordered


class AutomationDependencyGraphAction(BaseAction):
    """Build and execute a dependency graph of automation tasks.

    Resolves dependencies, detects cycles, and executes
    tasks in topological order.
    """
    action_type = "automation_dependency_graph"
    display_name = "自动化依赖图"
    description = "构建和执行任务依赖图"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tasks with dependencies.

        Args:
            context: Execution context.
            params: Dict with keys: tasks (list of {name, deps, action, params}).

        Returns:
            ActionResult with graph execution results.
        """
        start_time = time.time()
        try:
            tasks = params.get('tasks', [])

            # Build adjacency list
            graph: Dict[str, List[str]] = {}
            task_map: Dict[str, Dict] = {}
            for task in tasks:
                name = task.get('name', '')
                deps = task.get('deps', [])
                graph[name] = deps
                task_map[name] = task

            # Detect cycles
            visited: Dict[str, int] = {}  # 0=unvisited, 1=visiting, 2=done

            def has_cycle(node: str) -> bool:
                if visited.get(node) == 1:
                    return True
                if visited.get(node) == 2:
                    return False
                visited[node] = 1
                for dep in graph.get(node, []):
                    if has_cycle(dep):
                        return True
                visited[node] = 2
                return False

            for task_name in graph:
                if has_cycle(task_name):
                    return ActionResult(
                        success=False,
                        message=f"Cycle detected involving '{task_name}'",
                        duration=time.time() - start_time,
                    )

            # Topological sort (Kahn's algorithm)
            in_degree = {name: 0 for name in graph}
            for deps in graph.values():
                for dep in deps:
                    if dep in in_degree:
                        in_degree[dep] += 1

            queue = [name for name, degree in in_degree.items() if degree == 0]
            execution_order = []

            while queue:
                node = queue.pop(0)
                execution_order.append(node)
                for other, deps in graph.items():
                    if node in deps:
                        in_degree[other] -= 1
                        if in_degree[other] == 0:
                            queue.append(other)

            if len(execution_order) != len(graph):
                return ActionResult(
                    success=False,
                    message="Could not resolve all dependencies",
                    duration=time.time() - start_time,
                )

            # Execute in topological order
            results = []
            for task_name in execution_order:
                task = task_map[task_name]
                action = task.get('action')
                task_params = task.get('params', {})
                task_start = time.time()

                try:
                    if callable(action):
                        result = action(context, task_params)
                    elif hasattr(context, 'execute_action'):
                        result = context.execute_action(action, task_params)
                    else:
                        result = ActionResult(success=False, message=f"No executor for: {action}")

                    if isinstance(result, ActionResult):
                        task_result = {
                            'name': task_name,
                            'success': result.success,
                            'message': result.message,
                            'data': result.data,
                            'duration': result.duration,
                        }
                    else:
                        task_result = {'name': task_name, 'success': False, 'message': 'Invalid result'}
                except Exception as e:
                    task_result = {
                        'name': task_name,
                        'success': False,
                        'message': str(e),
                        'duration': time.time() - task_start,
                    }

                results.append(task_result)
                if not task_result['success']:
                    # Mark dependent tasks as skipped
                    pass

            duration = time.time() - start_time
            return ActionResult(
                success=all(r.get('success', False) for r in results),
                message=f"Dependency graph: {len(results)} tasks executed",
                data={'execution_order': execution_order, 'results': results},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Dependency graph error: {str(e)}",
                duration=duration,
            )
