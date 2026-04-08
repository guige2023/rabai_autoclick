"""Data materialization action module for RabAI AutoClick.

Provides data materialization operations:
- MaterializeAction: Materialize a view
- MaterializeRefreshAction: Refresh materialized data
- MaterializeStatusAction: Check materialization status
- MaterializeDropAction: Drop materialized view
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MaterializeAction(BaseAction):
    """Materialize a view or query result."""
    action_type = "materialize"
    display_name = "物化数据"
    description = "物化视图或查询结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            query = params.get("query", "")
            storage_type = params.get("storage_type", "table")

            if not name:
                return ActionResult(success=False, message="name is required")

            mat_id = hashlib.md5(f"{name}:{time.time()}".encode()).hexdigest()[:12]

            if not hasattr(context, "materializations"):
                context.materializations = {}
            context.materializations[mat_id] = {
                "materialization_id": mat_id,
                "name": name,
                "query": query,
                "storage_type": storage_type,
                "status": "materialized",
                "materialized_at": time.time(),
                "row_count": 0,
                "size_bytes": 0,
            }

            return ActionResult(
                success=True,
                data={"materialization_id": mat_id, "name": name, "status": "materialized"},
                message=f"Materialized {name} as {mat_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Materialize failed: {e}")


class MaterializeRefreshAction(BaseAction):
    """Refresh materialized data."""
    action_type = "materialize_refresh"
    display_name = "刷新物化"
    description = "刷新物化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            materialization_id = params.get("materialization_id", "")
            if not materialization_id:
                return ActionResult(success=False, message="materialization_id is required")

            mats = getattr(context, "materializations", {})
            if materialization_id not in mats:
                return ActionResult(success=False, message=f"Materialization {materialization_id} not found")

            mat = mats[materialization_id]
            mat["status"] = "refreshing"
            mat["refreshed_at"] = time.time()
            mat["status"] = "materialized"

            return ActionResult(
                success=True,
                data={"materialization_id": materialization_id, "refreshed_at": mat["refreshed_at"]},
                message=f"Materialization {materialization_id} refreshed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Materialize refresh failed: {e}")


class MaterializeStatusAction(BaseAction):
    """Check materialization status."""
    action_type = "materialize_status"
    display_name = "物化状态"
    description = "检查物化状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            materialization_id = params.get("materialization_id", "")
            if not materialization_id:
                return ActionResult(success=False, message="materialization_id is required")

            mats = getattr(context, "materializations", {})
            if materialization_id not in mats:
                return ActionResult(success=False, message=f"Materialization {materialization_id} not found")

            mat = mats[materialization_id]
            return ActionResult(
                success=True,
                data={
                    "materialization_id": materialization_id,
                    "name": mat["name"],
                    "status": mat["status"],
                    "materialized_at": mat.get("materialized_at"),
                    "refreshed_at": mat.get("refreshed_at"),
                },
                message=f"Materialization {materialization_id}: {mat['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Materialize status failed: {e}")


class MaterializeDropAction(BaseAction):
    """Drop materialized view."""
    action_type = "materialize_drop"
    display_name = "删除物化"
    description = "删除物化视图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            materialization_id = params.get("materialization_id", "")
            if not materialization_id:
                return ActionResult(success=False, message="materialization_id is required")

            mats = getattr(context, "materializations", {})
            if materialization_id not in mats:
                return ActionResult(success=False, message=f"Materialization {materialization_id} not found")

            mat_name = mats[materialization_id]["name"]
            del mats[materialization_id]

            return ActionResult(success=True, data={"materialization_id": materialization_id, "name": mat_name}, message=f"Materialization {mat_name} dropped")
        except Exception as e:
            return ActionResult(success=False, message=f"Materialize drop failed: {e}")
