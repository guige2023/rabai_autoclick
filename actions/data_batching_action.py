"""Data batching action module for RabAI AutoClick.

Provides data batching operations:
- BatchCreateAction: Create a batch
- BatchAddAction: Add items to batch
- BatchFlushAction: Flush batch
- BatchSizeAction: Check batch size
- BatchClearAction: Clear batch
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchCreateAction(BaseAction):
    """Create a new batch."""
    action_type = "batch_create"
    display_name = "创建批次"
    description = "创建新批次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            max_size = params.get("max_size", 100)
            flush_interval = params.get("flush_interval", 60)

            batch_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "batches"):
                context.batches = {}
            context.batches[batch_id] = {
                "batch_id": batch_id,
                "name": name,
                "max_size": max_size,
                "flush_interval": flush_interval,
                "items": [],
                "created_at": time.time(),
                "flushed_count": 0,
            }

            return ActionResult(
                success=True,
                data={"batch_id": batch_id, "name": name, "max_size": max_size},
                message=f"Batch {batch_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch create failed: {e}")


class BatchAddAction(BaseAction):
    """Add items to batch."""
    action_type = "batch_add"
    display_name = "添加批次项"
    description = "向批次添加项目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_id = params.get("batch_id", "")
            items = params.get("items", [])

            if not batch_id:
                return ActionResult(success=False, message="batch_id is required")
            if not items:
                return ActionResult(success=False, message="items is required")

            batches = getattr(context, "batches", {})
            if batch_id not in batches:
                return ActionResult(success=False, message=f"Batch {batch_id} not found")

            batch = batches[batch_id]
            batch["items"].extend(items)

            while len(batch["items"]) > batch["max_size"]:
                batch["items"].pop(0)

            return ActionResult(
                success=True,
                data={"batch_id": batch_id, "added": len(items), "current_size": len(batch["items"])},
                message=f"Added {len(items)} to batch {batch_id}: {len(batch['items'])}/{batch['max_size']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch add failed: {e}")


class BatchFlushAction(BaseAction):
    """Flush batch items."""
    action_type = "batch_flush"
    display_name = "刷新批次"
    description = "刷新批次项目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_id = params.get("batch_id", "")
            if not batch_id:
                return ActionResult(success=False, message="batch_id is required")

            batches = getattr(context, "batches", {})
            if batch_id not in batches:
                return ActionResult(success=False, message=f"Batch {batch_id} not found")

            batch = batches[batch_id]
            flushed_items = batch["items"][:]
            batch["items"].clear()
            batch["flushed_count"] += len(flushed_items)
            batch["last_flushed_at"] = time.time()

            return ActionResult(
                success=True,
                data={"batch_id": batch_id, "flushed_count": len(flushed_items), "total_flushed": batch["flushed_count"]},
                message=f"Flushed {len(flushed_items)} items from batch {batch_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch flush failed: {e}")


class BatchSizeAction(BaseAction):
    """Check batch size."""
    action_type = "batch_size"
    display_name = "批次大小"
    description = "检查批次大小"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_id = params.get("batch_id", "")
            if not batch_id:
                return ActionResult(success=False, message="batch_id is required")

            batches = getattr(context, "batches", {})
            if batch_id not in batches:
                return ActionResult(success=False, message=f"Batch {batch_id} not found")

            batch = batches[batch_id]
            return ActionResult(
                success=True,
                data={"batch_id": batch_id, "current_size": len(batch["items"]), "max_size": batch["max_size"], "full": len(batch["items"]) >= batch["max_size"]},
                message=f"Batch {batch_id}: {len(batch['items'])}/{batch['max_size']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch size failed: {e}")


class BatchClearAction(BaseAction):
    """Clear batch."""
    action_type = "batch_clear"
    display_name = "清空批次"
    description = "清空批次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_id = params.get("batch_id", "")
            if not batch_id:
                return ActionResult(success=False, message="batch_id is required")

            batches = getattr(context, "batches", {})
            if batch_id not in batches:
                return ActionResult(success=False, message=f"Batch {batch_id} not found")

            batch = batches[batch_id]
            cleared_count = len(batch["items"])
            batch["items"].clear()

            return ActionResult(success=True, data={"batch_id": batch_id, "cleared": cleared_count}, message=f"Cleared {cleared_count} items from batch {batch_id}")
        except Exception as e:
            return ActionResult(success=False, message=f"Batch clear failed: {e}")
