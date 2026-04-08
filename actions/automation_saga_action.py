"""Automation saga action module for RabAI AutoClick.

Provides saga pattern for distributed transactions:
- SagaCreateAction: Create saga
- SagaStepAction: Add saga step
- SagaExecuteAction: Execute saga
- SagaCompensateAction: Compensate failed saga
- SagaStatusAction: Get saga status
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SagaCreateAction(BaseAction):
    """Create a saga."""
    action_type = "saga_create"
    display_name = "创建Saga"
    description = "创建分布式事务Saga"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            compensation_mode = params.get("compensation_mode", "backward")

            if not name:
                return ActionResult(success=False, message="name is required")

            saga_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "sagas"):
                context.sagas = {}
            context.sagas[saga_id] = {
                "saga_id": saga_id,
                "name": name,
                "compensation_mode": compensation_mode,
                "steps": [],
                "status": "created",
                "completed_steps": [],
                "failed_step": None,
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"saga_id": saga_id, "name": name, "compensation_mode": compensation_mode},
                message=f"Saga {saga_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Saga create failed: {e}")


class SagaStepAction(BaseAction):
    """Add step to saga."""
    action_type = "saga_step"
    display_name = "添加Saga步骤"
    description = "向Saga添加步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            saga_id = params.get("saga_id", "")
            step_name = params.get("step_name", "")
            compensation_action = params.get("compensation_action", "")
            order = params.get("order", 0)

            if not saga_id or not step_name:
                return ActionResult(success=False, message="saga_id and step_name are required")

            sagas = getattr(context, "sagas", {})
            if saga_id not in sagas:
                return ActionResult(success=False, message=f"Saga {saga_id} not found")

            step_id = str(uuid.uuid4())[:8]
            sagas[saga_id]["steps"].append({
                "step_id": step_id,
                "name": step_name,
                "compensation": compensation_action,
                "order": order,
                "status": "pending",
            })
            sagas[saga_id]["steps"].sort(key=lambda s: s["order"])

            return ActionResult(
                success=True,
                data={"saga_id": saga_id, "step_id": step_id, "step_name": step_name, "step_count": len(sagas[saga_id]["steps"])},
                message=f"Step '{step_name}' added to saga {saga_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Saga step failed: {e}")


class SagaExecuteAction(BaseAction):
    """Execute saga."""
    action_type = "saga_execute"
    display_name = "执行Saga"
    description = "执行Saga事务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            saga_id = params.get("saga_id", "")
            if not saga_id:
                return ActionResult(success=False, message="saga_id is required")

            sagas = getattr(context, "sagas", {})
            if saga_id not in sagas:
                return ActionResult(success=False, message=f"Saga {saga_id} not found")

            saga = sagas[saga_id]
            saga["status"] = "running"
            completed = len(saga["completed_steps"])
            saga["completed_steps"].append(saga["steps"][completed]["name"]) if completed < len(saga["steps"]) else None

            if completed >= len(saga["steps"]) - 1:
                saga["status"] = "completed"

            return ActionResult(
                success=True,
                data={"saga_id": saga_id, "status": saga["status"], "completed_steps": completed + 1, "total_steps": len(saga["steps"])},
                message=f"Saga {saga_id}: {saga['status']}, {completed + 1}/{len(saga['steps'])} steps",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Saga execute failed: {e}")


class SagaCompensateAction(BaseAction):
    """Compensate failed saga."""
    action_type = "saga_compensate"
    display_name = "Saga补偿"
    description = "补偿失败的Saga步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            saga_id = params.get("saga_id", "")
            if not saga_id:
                return ActionResult(success=False, message="saga_id is required")

            sagas = getattr(context, "sagas", {})
            if saga_id not in sagas:
                return ActionResult(success=False, message=f"Saga {saga_id} not found")

            saga = sagas[saga_id]
            saga["status"] = "compensating"
            compensated = []
            for step in reversed(saga["completed_steps"]):
                compensated.append(step)

            saga["status"] = "compensated"

            return ActionResult(
                success=True,
                data={"saga_id": saga_id, "compensated_steps": compensated, "count": len(compensated)},
                message=f"Saga {saga_id} compensated: {len(compensated)} steps rolled back",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Saga compensate failed: {e}")


class SagaStatusAction(BaseAction):
    """Get saga status."""
    action_type = "saga_status"
    display_name = "Saga状态"
    description = "获取Saga状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            saga_id = params.get("saga_id", "")
            if not saga_id:
                return ActionResult(success=False, message="saga_id is required")

            sagas = getattr(context, "sagas", {})
            if saga_id not in sagas:
                return ActionResult(success=False, message=f"Saga {saga_id} not found")

            saga = sagas[saga_id]
            return ActionResult(
                success=True,
                data={
                    "saga_id": saga_id,
                    "name": saga["name"],
                    "status": saga["status"],
                    "completed_steps": saga["completed_steps"],
                    "failed_step": saga.get("failed_step"),
                },
                message=f"Saga {saga_id}: {saga['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Saga status failed: {e}")
