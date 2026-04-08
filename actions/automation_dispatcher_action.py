"""Automation Dispatcher action module for RabAI AutoClick.

Dispatches automation tasks to workers with load balancing,
priority queuing, and throttling.
"""

import time
import sys
import os
import json
import threading
from typing import Any, Dict, List, Optional, Callable
from queue import Queue, PriorityQueue
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationDispatcherAction(BaseAction):
    """Dispatch automation tasks to workers.

    Priority-based dispatching with load balancing,
    worker pools, and task tracking.
    """
    action_type = "automation_dispatcher"
    display_name = "自动化调度器"
    description = "调度自动化任务到工作池"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Dispatch tasks.

        Args:
            context: Execution context.
            params: Dict with keys: tasks, worker_count,
                   dispatch_mode (priority/round_robin/broadcast),
                   max_queue_size, task_timeout.

        Returns:
            ActionResult with dispatch results.
        """
        start_time = time.time()
        try:
            tasks = params.get('tasks', [])
            worker_count = params.get('worker_count', 4)
            dispatch_mode = params.get('dispatch_mode', 'priority')
            max_queue_size = params.get('max_queue_size', 1000)
            task_timeout = params.get('task_timeout', 60)

            if not tasks:
                return ActionResult(
                    success=False,
                    message="No tasks to dispatch",
                    duration=time.time() - start_time,
                )

            # Initialize task queue
            if dispatch_mode == 'priority':
                task_queue = PriorityQueue(maxsize=max_queue_size)
            else:
                task_queue = Queue(maxsize=max_queue_size)

            results = []
            completed = []
            lock = threading.Lock()

            def worker(worker_id: int):
                while True:
                    try:
                        item = task_queue.get(timeout=1)
                        if item is None:
                            break
                        priority, task = item if dispatch_mode == 'priority' else (0, item)
                        result = self._execute_task(task, task_timeout, context)
                        with lock:
                            completed.append({
                                'worker_id': worker_id,
                                'task': task.get('name', 'unnamed'),
                                'result': result,
                            })
                        task_queue.task_done()
                    except Exception:
                        break

            # Enqueue tasks
            for i, task in enumerate(tasks):
                priority = task.get('priority', 5)
                if dispatch_mode == 'priority':
                    task_queue.put((priority, task))
                elif dispatch_mode == 'round_robin':
                    task_queue.put(task)
                else:
                    task_queue.put(task)

            # Start workers
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(worker, i) for i in range(worker_count)]

                # Wait for queue to drain
                task_queue.join()

                # Stop workers
                for _ in range(worker_count):
                    task_queue.put(None)
                for f in as_completed(futures):
                    pass

            success_count = sum(1 for c in completed if c['result'].get('success', False) if isinstance(c['result'], dict))

            duration = time.time() - start_time
            return ActionResult(
                success=success_count == len(tasks),
                message=f"Dispatched {len(tasks)} tasks to {worker_count} workers",
                data={
                    'total': len(tasks),
                    'successful': success_count,
                    'failed': len(tasks) - success_count,
                    'completed': completed,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Dispatcher error: {str(e)}",
                duration=duration,
            )

    def _execute_task(self, task: Dict, timeout: int, context: Any) -> Dict:
        """Execute a single task."""
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
                result = ActionResult(success=False, message=f"Unknown action: {action}")

            if isinstance(result, ActionResult):
                return {
                    'success': result.success,
                    'message': result.message,
                    'data': result.data,
                    'duration': result.duration,
                }
            return {'success': False, 'message': 'Invalid result type', 'duration': time.time() - task_start}
        except Exception as e:
            return {'success': False, 'message': str(e), 'duration': time.time() - task_start}


class AutomationRegistryAction(BaseAction):
    """Registry for automation actions and workflows.

    Register, discover, and manage available automations.
    """
    action_type = "automation_registry"
    display_name = "自动化注册表"
    description = "注册和管理自动化动作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage automation registry.

        Args:
            context: Execution context.
            params: Dict with keys: action (register/discover/list/unregister),
                   name, automation, tags, category.

        Returns:
            ActionResult with registry result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'list')
            name = params.get('name', '')
            automation = params.get('automation')
            tags = params.get('tags', [])
            category = params.get('category', 'general')

            if not hasattr(context, '_automation_registry'):
                context._automation_registry = {}

            registry = context._automation_registry

            if action == 'register':
                if not name or not automation:
                    return ActionResult(
                        success=False,
                        message="name and automation are required",
                        duration=time.time() - start_time,
                    )
                registry[name] = {
                    'name': name,
                    'automation': automation,
                    'tags': tags,
                    'category': category,
                    'registered_at': time.time(),
                }
                return ActionResult(
                    success=True,
                    message=f"Registered automation: {name}",
                    data={'name': name, 'tags': tags, 'category': category},
                    duration=time.time() - start_time,
                )

            elif action == 'unregister':
                if name in registry:
                    del registry[name]
                return ActionResult(
                    success=True,
                    message=f"Unregistered: {name}",
                    duration=time.time() - start_time,
                )

            elif action == 'discover':
                query = params.get('query', '')
                matching = []
                for reg_name, reg_data in registry.items():
                    if query:
                        if query.lower() in reg_name.lower():
                            matching.append(reg_data)
                    elif tags:
                        if any(t in reg_data.get('tags', []) for t in tags):
                            matching.append(reg_data)
                    elif category and reg_data.get('category') == category:
                        matching.append(reg_data)
                return ActionResult(
                    success=True,
                    message=f"Found {len(matching)} automations",
                    data={'automations': matching},
                    duration=time.time() - start_time,
                )

            else:  # list
                all_regs = list(registry.values())
                return ActionResult(
                    success=True,
                    message=f"Registry has {len(all_regs)} automations",
                    data={'automations': all_regs, 'count': len(all_regs)},
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Registry error: {str(e)}",
                duration=duration,
            )
