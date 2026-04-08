"""Async task executor action module for RabAI AutoClick.

Provides async/await task operations:
- AsyncRunAction: Run async tasks
- AsyncGatherAction: Gather multiple async results
- AsyncTimeoutAction: Add timeout to async operations
- AsyncCancelAction: Cancel async operations
"""

import asyncio
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class AsyncTask:
    """Represents an async task."""
    task_id: str
    name: str
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    _future: Optional[Any] = None


class AsyncTaskManager:
    """Manages async tasks."""
    def __init__(self):
        self._tasks: Dict[str, AsyncTask] = {}
        self._lock = threading.Lock()

    def create(self, name: str) -> AsyncTask:
        task = AsyncTask(task_id=str(uuid.uuid4()), name=name)
        with self._lock:
            self._tasks[task.task_id] = task
        return task

    def get(self, task_id: str) -> Optional[AsyncTask]:
        with self._lock:
            return self._tasks.get(task_id)

    def update(self, task_id: str, **kwargs) -> bool:
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks[task_id]
                for k, v in kwargs.items():
                    if hasattr(task, k):
                        setattr(task, k, v)
                return True
        return False

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "task_id": t.task_id,
                    "name": t.name,
                    "status": t.status,
                    "created_at": t.created_at.isoformat(),
                    "started_at": t.started_at.isoformat() if t.started_at else None,
                    "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                    "error": t.error
                }
                for t in self._tasks.values()
            ]


_manager = AsyncTaskManager()


class AsyncRunAction(BaseAction):
    """Run an async task."""
    action_type = "async_run"
    display_name = "异步运行"
    description = "运行异步任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            coroutine_ref = params.get("coroutine_ref", None)
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 30)

            if not name:
                return ActionResult(success=False, message="name is required")

            task = _manager.create(name)
            _manager.update(task.task_id, status="running", started_at=datetime.utcnow())

            def run_coro():
                try:
                    if coroutine_ref:
                        result = asyncio.run(coroutine_ref())
                    else:
                        result = {"status": "completed", "task_id": task.task_id}
                    _manager.update(task.task_id, status="completed", completed_at=datetime.utcnow(), result=result)
                except Exception as e:
                    _manager.update(task.task_id, status="failed", completed_at=datetime.utcnow(), error=str(e))

            thread = threading.Thread(target=run_coro)
            thread.start()

            if blocking:
                thread.join(timeout=timeout)
                current_task = _manager.get(task.task_id)
                return ActionResult(
                    success=current_task.status == "completed" if current_task else False,
                    message=f"Task {name}: {current_task.status if current_task else 'unknown'}",
                    data={"task_id": task.task_id, "status": current_task.status if current_task else None}
                )

            return ActionResult(
                success=True,
                message=f"Task '{name}' started",
                data={"task_id": task.task_id, "status": "running"}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Async run failed: {str(e)}")


class AsyncGatherAction(BaseAction):
    """Gather results from multiple async tasks."""
    action_type = "async_gather"
    display_name = "异步聚合"
    description = "聚合多个异步任务结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_ids = params.get("task_ids", [])
            timeout = params.get("timeout", 30)
            return_exceptions = params.get("return_exceptions", False)

            if not task_ids:
                return ActionResult(success=False, message="task_ids are required")

            results = []
            start_time = time.time()
            for tid in task_ids:
                elapsed = time.time() - start_time
                remaining = timeout - elapsed
                if remaining <= 0:
                    break
                task = _manager.get(tid)
                if not task:
                    if return_exceptions:
                        results.append({"task_id": tid, "status": "not_found"})
                    continue
                if task.status == "completed":
                    results.append({"task_id": tid, "status": "completed", "result": task.result})
                elif task.status == "failed":
                    if return_exceptions:
                        results.append({"task_id": tid, "status": "failed", "error": task.error})
                    else:
                        return ActionResult(success=False, message=f"Task {tid} failed: {task.error}")
                else:
                    if remaining > 0:
                        time.sleep(min(1, remaining))
                    current = _manager.get(tid)
                    if current and current.status == "completed":
                        results.append({"task_id": tid, "status": "completed", "result": current.result})
                    else:
                        results.append({"task_id": tid, "status": "timeout"})

            return ActionResult(
                success=True,
                message=f"Gathered {len(results)} task results",
                data={"results": results, "count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Async gather failed: {str(e)}")


class AsyncTimeoutAction(BaseAction):
    """Add timeout to async operations."""
    action_type = "async_timeout"
    display_name = "异步超时"
    description = "为异步操作添加超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            timeout = params.get("timeout", 30)

            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            task = _manager.get(task_id)
            if not task:
                return ActionResult(success=False, message=f"Task {task_id} not found")

            start_wait = time.time()
            while task.status == "running":
                elapsed = time.time() - start_wait
                if elapsed >= timeout:
                    return ActionResult(
                        success=False,
                        message=f"Task {task_id} timed out after {timeout}s",
                        data={"task_id": task_id, "status": task.status, "elapsed": elapsed}
                    )
                time.sleep(0.5)
                task = _manager.get(task_id)

            return ActionResult(
                success=task.status == "completed",
                message=f"Task {task_id}: {task.status}",
                data={"task_id": task_id, "status": task.status, "elapsed": time.time() - start_wait}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Async timeout failed: {str(e)}")


class AsyncCancelAction(BaseAction):
    """Cancel an async task."""
    action_type = "async_cancel"
    display_name = "异步取消"
    description = "取消异步任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            force = params.get("force", False)

            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            task = _manager.get(task_id)
            if not task:
                return ActionResult(success=False, message=f"Task {task_id} not found")

            if task.status in ("completed", "failed"):
                return ActionResult(
                    success=False,
                    message=f"Task {task_id} is already {task.status}",
                    data={"task_id": task_id, "status": task.status}
                )

            if force:
                _manager.update(task_id, status="cancelled", completed_at=datetime.utcnow())
            else:
                _manager.update(task_id, status="cancelling")

            return ActionResult(
                success=True,
                message=f"Task {task_id} cancellation requested",
                data={"task_id": task_id, "status": "cancelling" if not force else "cancelled"}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Async cancel failed: {str(e)}")
