"""Automation executor action module for RabAI AutoClick.

Provides automation executor operations:
- ExecutorSubmitAction: Submit task to executor
- ExecutorBatchAction: Submit batch tasks
- ExecutorCancelAction: Cancel a task
- ExecutorStatusAction: Check task status
- ExecutorResultAction: Get task result
- ExecutorShutdownAction: Shutdown executor
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExecutorSubmitAction(BaseAction):
    """Submit a task to executor."""
    action_type = "executor_submit"
    display_name = "任务提交"
    description = "提交任务到执行器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_name = params.get("task_name", "")
            task_params = params.get("params", {})
            priority = params.get("priority", 0)
            timeout = params.get("timeout", 300)

            if not task_name:
                return ActionResult(success=False, message="task_name is required")

            task_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "executor_tasks"):
                context.executor_tasks = {}
            context.executor_tasks[task_id] = {
                "task_id": task_id,
                "task_name": task_name,
                "params": task_params,
                "priority": priority,
                "status": "queued",
                "submitted_at": time.time(),
                "timeout": timeout,
            }

            return ActionResult(
                success=True,
                data={"task_id": task_id, "task_name": task_name, "status": "queued"},
                message=f"Task {task_id} submitted",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Executor submit failed: {e}")


class ExecutorBatchAction(BaseAction):
    """Submit batch tasks to executor."""
    action_type = "executor_batch"
    display_name = "批量任务提交"
    description = "批量提交任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tasks = params.get("tasks", [])
            if not tasks:
                return ActionResult(success=False, message="tasks list is required")

            if not hasattr(context, "executor_tasks"):
                context.executor_tasks = {}

            submitted = []
            for task in tasks:
                task_id = str(uuid.uuid4())[:8]
                context.executor_tasks[task_id] = {
                    "task_id": task_id,
                    "task_name": task.get("task_name", "unknown"),
                    "params": task.get("params", {}),
                    "priority": task.get("priority", 0),
                    "status": "queued",
                    "submitted_at": time.time(),
                }
                submitted.append(task_id)

            return ActionResult(
                success=True,
                data={"submitted_count": len(submitted), "task_ids": submitted},
                message=f"Batch submitted {len(submitted)} tasks",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Executor batch failed: {e}")


class ExecutorCancelAction(BaseAction):
    """Cancel a task."""
    action_type = "executor_cancel"
    display_name = "任务取消"
    description = "取消执行中的任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            if not hasattr(context, "executor_tasks") or task_id not in context.executor_tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")

            task = context.executor_tasks[task_id]
            if task["status"] in ("completed", "failed", "cancelled"):
                return ActionResult(success=False, message=f"Task {task_id} already {task['status']}")

            task["status"] = "cancelled"
            task["cancelled_at"] = time.time()

            return ActionResult(success=True, data={"task_id": task_id, "status": "cancelled"}, message=f"Task {task_id} cancelled")
        except Exception as e:
            return ActionResult(success=False, message=f"Executor cancel failed: {e}")


class ExecutorStatusAction(BaseAction):
    """Check task status."""
    action_type = "executor_status"
    display_name = "任务状态"
    description = "查询任务状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            if not hasattr(context, "executor_tasks") or task_id not in context.executor_tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")

            task = context.executor_tasks[task_id]
            return ActionResult(
                success=True,
                data={"task_id": task_id, "status": task["status"], "submitted_at": task["submitted_at"]},
                message=f"Task {task_id}: {task['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Executor status failed: {e}")


class ExecutorResultAction(BaseAction):
    """Get task result."""
    action_type = "executor_result"
    display_name = "任务结果"
    description = "获取任务执行结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            block = params.get("block", False)
            timeout = params.get("timeout", 10)

            if not task_id:
                return ActionResult(success=False, message="task_id is required")

            if not hasattr(context, "executor_tasks") or task_id not in context.executor_tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")

            task = context.executor_tasks[task_id]
            if task["status"] == "running" and block:
                time.sleep(min(timeout, 5))

            if task["status"] == "completed":
                return ActionResult(
                    success=True,
                    data={"task_id": task_id, "result": task.get("result", {}), "status": task["status"]},
                    message=f"Task {task_id} result retrieved",
                )
            else:
                return ActionResult(
                    success=True,
                    data={"task_id": task_id, "status": task["status"]},
                    message=f"Task {task_id} not completed yet",
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Executor result failed: {e}")


class ExecutorShutdownAction(BaseAction):
    """Shutdown executor."""
    action_type = "executor_shutdown"
    display_name = "执行器关闭"
    description = "关闭执行器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            force = params.get("force", False)
            timeout = params.get("timeout", 30)

            tasks = getattr(context, "executor_tasks", {})
            pending = sum(1 for t in tasks.values() if t["status"] == "queued")
            running = sum(1 for t in tasks.values() if t["status"] == "running")

            if not force and running > 0:
                return ActionResult(
                    success=False,
                    data={"pending": pending, "running": running},
                    message=f"Cannot shutdown: {running} tasks still running",
                )

            for task in tasks.values():
                if task["status"] == "queued":
                    task["status"] = "cancelled"

            return ActionResult(
                success=True,
                data={"force": force, "pending_cancelled": pending, "running": running},
                message="Executor shutdown complete",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Executor shutdown failed: {e}")
