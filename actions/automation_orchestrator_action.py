"""Automation Orchestrator Action Module.

Provides workflow orchestration and task coordination capabilities.
"""

import time
import asyncio
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OrchestrationMode(Enum):
    """Orchestration execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    FANOUT = "fanout"
    PIPELINE = "pipeline"


class AutomationOrchestratorAction(BaseAction):
    """Orchestrate complex automation workflows.
    
    Manages task dependencies, execution order, and error handling.
    """
    action_type = "automation_orchestrator"
    display_name = "自动化编排"
    description = "编排复杂自动化工作流程"
    
    def __init__(self):
        super().__init__()
        self._workflows = {}
        self._executions = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute orchestration.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id, tasks, mode, options.
        
        Returns:
            ActionResult with orchestration result.
        """
        workflow_id = params.get('workflow_id', f'wf_{int(time.time())}')
        tasks = params.get('tasks', [])
        mode = params.get('mode', 'sequential')
        options = params.get('options', {})
        
        if not tasks:
            return ActionResult(
                success=False,
                data=None,
                error="No tasks specified"
            )
        
        try:
            if mode == 'sequential':
                result = self._execute_sequential(workflow_id, tasks, options)
            elif mode == 'parallel':
                result = self._execute_parallel(workflow_id, tasks, options)
            elif mode == 'fanout':
                result = self._execute_fanout(workflow_id, tasks, options)
            elif mode == 'pipeline':
                result = self._execute_pipeline(workflow_id, tasks, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown mode: {mode}"
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Orchestration failed: {str(e)}"
            )
    
    def _execute_sequential(
        self,
        workflow_id: str,
        tasks: List[Dict],
        options: Dict
    ) -> ActionResult:
        """Execute tasks sequentially."""
        results = []
        start_time = time.time()
        
        for i, task in enumerate(tasks):
            task_result = self._execute_task(task, i, options)
            results.append(task_result)
            
            if not task_result['success'] and not options.get('continue_on_error', False):
                break
        
        total_duration = time.time() - start_time
        success_count = sum(1 for r in results if r['success'])
        
        return ActionResult(
            success=success_count == len(results),
            data={
                'workflow_id': workflow_id,
                'mode': 'sequential',
                'results': results,
                'success_count': success_count,
                'total_tasks': len(tasks),
                'duration': total_duration
            },
            error=None if success_count == len(results) else "Some tasks failed"
        )
    
    def _execute_parallel(
        self,
        workflow_id: str,
        tasks: List[Dict],
        options: Dict
    ) -> ActionResult:
        """Execute tasks in parallel."""
        import concurrent.futures
        
        start_time = time.time()
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=options.get('max_workers', 4)) as executor:
            futures = {
                executor.submit(self._execute_task, task, i, options): i
                for i, task in enumerate(tasks)
            }
            
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        'index': idx,
                        'success': False,
                        'error': str(e)
                    })
        
        total_duration = time.time() - start_time
        success_count = sum(1 for r in results if r['success'])
        
        return ActionResult(
            success=success_count == len(results),
            data={
                'workflow_id': workflow_id,
                'mode': 'parallel',
                'results': results,
                'success_count': success_count,
                'total_tasks': len(tasks),
                'duration': total_duration
            },
            error=None if success_count == len(results) else "Some tasks failed"
        )
    
    def _execute_fanout(
        self,
        workflow_id: str,
        tasks: List[Dict],
        options: Dict
    ) -> ActionResult:
        """Execute fanout pattern (one input, many outputs)."""
        input_data = options.get('input_data', None)
        start_time = time.time()
        
        results = []
        for i, task in enumerate(tasks):
            task_with_input = task.copy()
            if input_data is not None:
                task_with_input['input'] = input_data
            result = self._execute_task(task_with_input, i, options)
            results.append(result)
        
        total_duration = time.time() - start_time
        success_count = sum(1 for r in results if r['success'])
        
        return ActionResult(
            success=success_count == len(results),
            data={
                'workflow_id': workflow_id,
                'mode': 'fanout',
                'results': results,
                'success_count': success_count,
                'total_tasks': len(tasks),
                'duration': total_duration
            },
            error=None if success_count == len(results) else "Some tasks failed"
        )
    
    def _execute_pipeline(
        self,
        workflow_id: str,
        tasks: List[Dict],
        options: Dict
    ) -> ActionResult:
        """Execute pipeline pattern (output of one is input of next)."""
        start_time = time.time()
        pipeline_data = options.get('initial_data', None)
        results = []
        
        for i, task in enumerate(tasks):
            task_with_input = task.copy()
            if pipeline_data is not None:
                task_with_input['input'] = pipeline_data
            
            result = self._execute_task(task_with_input, i, options)
            results.append(result)
            
            if not result['success']:
                break
            
            pipeline_data = result.get('output')
        
        total_duration = time.time() - start_time
        success_count = sum(1 for r in results if r['success'])
        
        return ActionResult(
            success=success_count == len(results),
            data={
                'workflow_id': workflow_id,
                'mode': 'pipeline',
                'results': results,
                'success_count': success_count,
                'total_tasks': len(tasks),
                'duration': total_duration,
                'final_output': pipeline_data
            },
            error=None if success_count == len(results) else "Some tasks failed"
        )
    
    def _execute_task(self, task: Dict, index: int, options: Dict) -> Dict:
        """Execute a single task."""
        task_type = task.get('type', 'echo')
        task_input = task.get('input', None)
        
        try:
            if task_type == 'echo':
                output = task_input
            elif task_type == 'transform':
                output = self._transform_data(task_input, task.get('transforms', []))
            elif task_type == 'filter':
                output = self._filter_data(task_input, task.get('condition', {}))
            elif task_type == 'aggregate':
                output = self._aggregate_data(task_input, task.get('group_by', []))
            else:
                output = task_input
            
            return {
                'index': index,
                'task_type': task_type,
                'success': True,
                'input': task_input,
                'output': output
            }
        except Exception as e:
            return {
                'index': index,
                'task_type': task_type,
                'success': False,
                'error': str(e)
            }
    
    def _transform_data(self, data: Any, transforms: List) -> Any:
        """Transform data based on transform specifications."""
        result = data
        for transform in transforms:
            t_type = transform.get('type', 'identity')
            if t_type == 'uppercase' and isinstance(result, str):
                result = result.upper()
            elif t_type == 'lowercase' and isinstance(result, str):
                result = result.lower()
        return result
    
    def _filter_data(self, data: Any, condition: Dict) -> Any:
        """Filter data based on condition."""
        if not isinstance(data, list):
            return data
        
        field = condition.get('field', '')
        value = condition.get('value', None)
        
        return [item for item in data if isinstance(item, dict) and item.get(field) == value]
    
    def _aggregate_data(self, data: Any, group_by: List) -> Any:
        """Aggregate data by grouping fields."""
        if not isinstance(data, list) or not group_by:
            return data
        
        groups = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            key = str(item.get(group_by[0], ''))
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        return [{"group": k, "items": v, "count": len(v)} for k, v in groups.items()]


class TaskCoordinatorAction(BaseAction):
    """Coordinate multiple automation tasks.
    
    Manages task dependencies and execution coordination.
    """
    action_type = "task_coordinator"
    display_name = "任务协调器"
    description = "协调多个自动化任务"
    
    def __init__(self):
        super().__init__()
        self._task_graph = {}
        self._execution_order = []
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute task coordination.
        
        Args:
            context: Execution context.
            params: Dict with keys: tasks, dependencies.
        
        Returns:
            ActionResult with coordinated execution result.
        """
        tasks = params.get('tasks', [])
        dependencies = params.get('dependencies', {})
        
        if not tasks:
            return ActionResult(
                success=False,
                data=None,
                error="No tasks to coordinate"
            )
        
        try:
            # Build task graph
            self._build_graph(tasks, dependencies)
            
            # Compute execution order
            execution_order = self._topological_sort()
            
            # Execute in order
            results = self._execute_ordered(tasks, execution_order)
            
            return ActionResult(
                success=True,
                data={
                    'execution_order': execution_order,
                    'results': results
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Coordination failed: {str(e)}"
            )
    
    def _build_graph(self, tasks: List, dependencies: Dict):
        """Build task dependency graph."""
        self._task_graph = {task.get('id', i): set() for i, task in enumerate(tasks)}
        
        for task_id, deps in dependencies.items():
            if task_id in self._task_graph:
                self._task_graph[task_id] = set(deps)
    
    def _topological_sort(self) -> List:
        """Compute topological sort of task graph."""
        visited = set()
        order = []
        
        def visit(task_id):
            if task_id in visited:
                return
            visited.add(task_id)
            for dep in self._task_graph.get(task_id, []):
                visit(dep)
            order.append(task_id)
        
        for task_id in self._task_graph:
            visit(task_id)
        
        return order
    
    def _execute_ordered(self, tasks: List, order: List) -> List:
        """Execute tasks in computed order."""
        task_map = {task.get('id', i): task for i, task in enumerate(tasks)}
        results = []
        
        for task_id in order:
            task = task_map.get(task_id, {})
            results.append({
                'task_id': task_id,
                'executed': True,
                'task': task
            })
        
        return results


def register_actions():
    """Register all Automation Orchestrator actions."""
    return [
        AutomationOrchestratorAction,
        TaskCoordinatorAction,
    ]
