"""Async Task action module for RabAI AutoClick.

Provides async task operations:
- AsyncTaskCreateAction: Create async task
- AsyncTaskWaitAction: Wait for task
- AsyncTaskCancelAction: Cancel async task
- AsyncTaskGatherAction: Gather multiple tasks
"""

from __future__ import annotations

import sys
import os
import asyncio
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AsyncTaskCreateAction(BaseAction):
    """Create async task."""
    action_type = "async_task_create"
    display_name = "创建异步任务"
    description = "创建异步任务"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._tasks = {}
        self._executor = ThreadPoolExecutor(max_workers=10)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task creation."""
        task_id = params.get('task_id', '')
        func = params.get('func', 'identity')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        output_var = params.get('output_var', 'task_info')

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        try:
            resolved_args = context.resolve_value(args) if context else args
            resolved_kwargs = context.resolve_value(kwargs) if context else kwargs

            import uuid
            task_uuid = task_id or str(uuid.uuid4())

            def task_func():
                return {'task_id': task_uuid, 'status': 'completed', 'result': resolved_args}

            future = self._executor.submit(task_func)
            self._tasks[task_uuid] = {'future': future, 'status': 'pending'}

            result = {
                'task_id': task_uuid,
                'status': 'pending',
                'created': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Task {task_uuid[:8]}... created"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Async task create error: {e}")


class AsyncTaskWaitAction(BaseAction):
    """Wait for async task."""
    action_type = "async_task_wait"
    display_name = "等待异步任务"
    description = "等待异步任务完成"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task wait."""
        task_id = params.get('task_id', '')
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'task_result')

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        try:
            import time
            start = time.time()

            while time.time() - start < timeout:
                if task_id in self._tasks:
                    future = self._tasks[task_id]['future']
                    if future.done():
                        result = future.result()
                        self._tasks[task_id]['status'] = 'completed'
                        return ActionResult(
                            success=True,
                            data={output_var: result},
                            message=f"Task completed"
                        )
                time.sleep(0.1)

            return ActionResult(
                success=False,
                data={output_var: {'task_id': task_id, 'status': 'timeout'}},
                message=f"Task wait timeout after {timeout}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Async task wait error: {e}")


class AsyncTaskCancelAction(BaseAction):
    """Cancel async task."""
    action_type = "async_task_cancel"
    display_name = "取消异步任务"
    description = "取消异步任务"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task cancellation."""
        task_id = params.get('task_id', '')
        output_var = params.get('output_var', 'cancel_result')

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        try:
            if task_id in self._tasks:
                cancelled = self._tasks[task_id]['future'].cancel()
                self._tasks[task_id]['status'] = 'cancelled' if cancelled else 'running'
                result = {
                    'task_id': task_id,
                    'cancelled': cancelled,
                }
            else:
                result = {
                    'task_id': task_id,
                    'cancelled': False,
                    'reason': 'Task not found',
                }

            return ActionResult(
                success=result.get('cancelled', False),
                data={output_var: result},
                message=f"Task {'cancelled' if result.get('cancelled') else 'could not be cancelled'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Async task cancel error: {e}")


class AsyncTaskGatherAction(BaseAction):
    """Gather multiple tasks."""
    action_type = "async_task_gather"
    display_name = "收集异步任务"
    description = "收集多个异步任务"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task gathering."""
        task_ids = params.get('task_ids', [])
        timeout = params.get('timeout', 30)
        return_exceptions = params.get('return_exceptions', True)
        output_var = params.get('output_var', 'gather_result')

        if not task_ids:
            return ActionResult(success=False, message="task_ids are required")

        try:
            import time
            start = time.time()
            results = []
            pending = set(task_ids)

            while pending and time.time() - start < timeout:
                for tid in list(pending):
                    if tid in self._tasks:
                        if self._tasks[tid]['future'].done():
                            try:
                                result = self._tasks[tid]['future'].result()
                                results.append({'task_id': tid, 'success': True, 'result': result})
                            except Exception as e:
                                results.append({'task_id': tid, 'success': False, 'error': str(e)})
                            pending.remove(tid)
                time.sleep(0.1)

            for tid in pending:
                results.append({'task_id': tid, 'success': False, 'error': 'timeout'})

            success_count = sum(1 for r in results if r['success'])

            result = {
                'results': results,
                'total': len(task_ids),
                'success_count': success_count,
                'failed_count': len(task_ids) - success_count,
            }

            return ActionResult(
                success=success_count == len(task_ids),
                data={output_var: result},
                message=f"Gathered {success_count}/{len(task_ids)} tasks"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Async task gather error: {e}")
