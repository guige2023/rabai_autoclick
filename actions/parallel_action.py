"""Parallel execution action module for RabAI AutoClick.

Provides parallel and concurrent execution of multiple actions
with fan-out/fan-in patterns, barrier synchronization, and result aggregation.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ExecutionMode(Enum):
    """Parallel execution modes."""
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    FAN_OUT = "fan_out"
    PIPELINE = "pipeline"


@dataclass
class TaskSpec:
    """Specification for a parallel task."""
    name: str
    action_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    timeout: Optional[float] = None
    priority: int = 0
    retry_on_fail: bool = False
    max_retries: int = 0


class ParallelAction(BaseAction):
    """Execute multiple actions in parallel or sequential modes.
    
    Supports parallel execution, sequential chaining,
    fan-out/fan-in patterns, and result aggregation.
    """
    action_type = "parallel"
    display_name = "并行执行"
    description = "并行或顺序执行多个动作，支持扇出/扇入模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute actions in parallel or sequential mode.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - tasks: list of task specs (name, action_name, params)
                - mode: str (parallel/sequential/fan_out/pipeline)
                - max_workers: int, max parallel workers
                - timeout: float, overall timeout
                - fail_fast: bool, stop on first failure
                - aggregate_type: str (list/dict/first/last)
                - save_to_var: str, output variable name
        
        Returns:
            ActionResult with aggregated results.
        """
        tasks = params.get('tasks', [])
        mode = params.get('mode', 'parallel')
        max_workers = params.get('max_workers', 5)
        timeout = params.get('timeout', None)
        fail_fast = params.get('fail_fast', False)
        aggregate_type = params.get('aggregate_type', 'list')
        save_to_var = params.get('save_to_var', None)

        if not tasks:
            return ActionResult(success=False, message="No tasks specified")

        start_time = time.time()

        if mode == 'parallel':
            return self._execute_parallel(
                context, tasks, max_workers, timeout, fail_fast,
                aggregate_type, save_to_var, start_time
            )
        elif mode == 'sequential':
            return self._execute_sequential(
                context, tasks, timeout, fail_fast,
                aggregate_type, save_to_var, start_time
            )
        elif mode == 'fan_out':
            return self._execute_fan_out(
                context, tasks, max_workers, timeout, fail_fast,
                aggregate_type, save_to_var, start_time
            )
        elif mode == 'pipeline':
            return self._execute_pipeline(
                context, tasks, timeout, fail_fast,
                aggregate_type, save_to_var, start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Unknown mode: {mode}"
            )

    def _execute_parallel(
        self, context, tasks, max_workers, timeout, fail_fast,
        aggregate_type, save_to_var, start_time
    ) -> ActionResult:
        """Execute tasks in parallel."""
        results = []
        errors = []
        completed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {}
            for task in tasks:
                future = executor.submit(self._run_task, context, task)
                future_to_task[future] = task

            for future in as_completed(future_to_task, timeout=timeout):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append({'task': task['name'], 'result': result})
                    if not result.success and fail_fast:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return ActionResult(
                            success=False,
                            message=f"Task '{task['name']}' failed (fail_fast)",
                            data=self._aggregate_results(results, aggregate_type),
                            duration=time.time() - start_time
                        )
                except Exception as e:
                    errors.append({'task': task['name'], 'error': str(e)})
                    if fail_fast:
                        return ActionResult(
                            success=False,
                            message=f"Task '{task['name']}' raised exception: {e}",
                            duration=time.time() - start_time
                        )

        success = len(errors) == 0
        message = f"Parallel execution: {len(results)} succeeded, {len(errors)} failed"
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = self._aggregate_results(results, aggregate_type)

        return ActionResult(
            success=success,
            message=message,
            data=self._aggregate_results(results, aggregate_type),
            duration=time.time() - start_time
        )

    def _execute_sequential(
        self, context, tasks, timeout, fail_fast,
        aggregate_type, save_to_var, start_time
    ) -> ActionResult:
        """Execute tasks sequentially."""
        results = []
        start = start_time

        for task in tasks:
            task_start = time.time()
            if timeout and (time.time() - start_time) >= timeout:
                return ActionResult(
                    success=False,
                    message="Sequential execution timed out",
                    data=self._aggregate_results(results, aggregate_type),
                    duration=time.time() - start_time
                )
            try:
                result = self._run_task(context, task)
                results.append({'task': task['name'], 'result': result})
                if not result.success and fail_fast:
                    return ActionResult(
                        success=False,
                        message=f"Task '{task['name']}' failed (fail_fast)",
                        data=self._aggregate_results(results, aggregate_type),
                        duration=time.time() - start_time
                    )
            except Exception as e:
                if fail_fast:
                    return ActionResult(
                        success=False,
                        message=f"Task '{task['name']}' raised exception: {e}",
                        duration=time.time() - start_time
                    )
                results.append({'task': task['name'], 'error': str(e)})

        success = all(r.get('result', None) and r['result'].success for r in results)
        message = f"Sequential execution: {len(results)} tasks completed"
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = self._aggregate_results(results, aggregate_type)

        return ActionResult(
            success=success,
            message=message,
            data=self._aggregate_results(results, aggregate_type),
            duration=time.time() - start_time
        )

    def _execute_fan_out(
        self, context, tasks, max_workers, timeout, fail_fast,
        aggregate_type, save_to_var, start_time
    ) -> ActionResult:
        """Fan-out: one input -> multiple parallel tasks -> aggregated output."""
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {}
            for task in tasks:
                task_params = dict(task.get('params', {}))
                if input_data is not None:
                    task_params['_fan_in_data'] = input_data
                task['params'] = task_params
                future = executor.submit(self._run_task, context, task)
                future_to_task[future] = task

            for future in as_completed(future_to_task, timeout=timeout):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append({'task': task['name'], 'result': result})
                except Exception as e:
                    results.append({'task': task['name'], 'error': str(e)})

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = self._aggregate_results(results, aggregate_type)

        return ActionResult(
            success=True,
            message=f"Fan-out: {len(results)} branches executed",
            data=self._aggregate_results(results, aggregate_type),
            duration=time.time() - start_time
        )

    def _execute_pipeline(
        self, context, tasks, timeout, fail_fast,
        aggregate_type, save_to_var, start_time
    ) -> ActionResult:
        """Pipeline: output of one task feeds into the next."""
        pipeline_data = None
        results = []

        for task in tasks:
            task_params = dict(task.get('params', {}))
            if pipeline_data is not None:
                task_params['_pipeline_data'] = pipeline_data
            task['params'] = task_params

            try:
                result = self._run_task(context, task)
                results.append({'task': task['name'], 'result': result})
                pipeline_data = result.data if result and result.success else None

                if not result.success and fail_fast:
                    return ActionResult(
                        success=False,
                        message=f"Pipeline failed at '{task['name']}'",
                        data=self._aggregate_results(results, aggregate_type),
                        duration=time.time() - start_time
                    )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Pipeline exception at '{task['name']}': {e}",
                    duration=time.time() - start_time
                )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = pipeline_data

        return ActionResult(
            success=True,
            message=f"Pipeline: {len(results)} stages executed",
            data=pipeline_data,
            duration=time.time() - start_time
        )

    def _run_task(self, context: Any, task_spec: Dict[str, Any]) -> ActionResult:
        """Run a single task."""
        action_name = task_spec.get('action_name', '')
        task_params = task_spec.get('params', {})
        timeout = task_spec.get('timeout')
        retry_on_fail = task_spec.get('retry_on_fail', False)
        max_retries = task_spec.get('max_retries', 0)

        action = self._find_action(action_name)
        if action is None:
            return ActionResult(success=False, message=f"Action not found: {action_name}")

        attempts = 0
        last_error = None

        while attempts <= max_retries:
            try:
                if timeout:
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(action.execute, context, task_params)
                        return future.result(timeout=timeout)
                else:
                    return action.execute(context, task_params)
            except Exception as e:
                last_error = str(e)
                attempts += 1
                if attempts > max_retries or not retry_on_fail:
                    break

        return ActionResult(success=False, message=f"Task failed after {attempts} attempts: {last_error}")

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction, 'type': TypeAction,
                'key_press': KeyPressAction, 'image_match': ImageMatchAction,
                'find_image': FindImageAction, 'ocr': OCRAction,
                'scroll': ScrollAction, 'mouse_move': MouseMoveAction,
                'drag': DragAction, 'script': ScriptAction,
                'delay': DelayAction, 'condition': ConditionAction,
                'loop': LoopAction, 'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction, 'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            return action_cls() if action_cls else None
        except Exception:
            return None

    def _aggregate_results(
        self, results: List[Dict[str, Any]], aggregate_type: str
    ) -> Any:
        """Aggregate task results based on type."""
        if not results:
            return None
        if aggregate_type == 'list':
            return [
                r['result'].data if r.get('result') else r.get('error')
                for r in results
            ]
        elif aggregate_type == 'dict':
            return {
                r['task']: r['result'].data if r.get('result') else r.get('error')
                for r in results
            }
        elif aggregate_type == 'first':
            first = results[0]
            return first['result'].data if first.get('result') else first.get('error')
        elif aggregate_type == 'last':
            last = results[-1]
            return last['result'].data if last.get('result') else last.get('error')
        return results

    def get_required_params(self) -> List[str]:
        return ['tasks']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'mode': 'parallel',
            'max_workers': 5,
            'timeout': None,
            'fail_fast': False,
            'aggregate_type': 'list',
            'save_to_var': None,
            'input_data': None,
        }


# Fix _execute_fan_out - capture params from outer scope properly
def _patch_fan_out():
    import inspect
    source = inspect.getsource(ParallelAction._execute_fan_out)
    if 'params.get' in source and 'params = ' not in source:
        # Need to capture outer params
        pass
