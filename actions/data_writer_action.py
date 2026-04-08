"""Data writer action module for RabAI AutoClick.

Provides data writing operations:
- WriteAppendAction: Append data
- WriteUpsertAction: Upsert data
- WriteReplaceAction: Replace data
- WriteBatchAction: Batch write
- WriteTransactionAction: Transactional write
"""

import time
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WriteAppendAction(BaseAction):
    """Append data to storage."""
    action_type = "write_append"
    display_name = "追加写入"
    description = "追加数据到存储"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            storage_key = params.get("storage_key", "default")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not hasattr(context, "_data_stores"):
                context._data_stores = {}
            if storage_key not in context._data_stores:
                context._data_stores[storage_key] = []
            context._data_stores[storage_key].extend(data)

            return ActionResult(
                success=True,
                data={"appended": len(data), "total": len(context._data_stores[storage_key]), "storage_key": storage_key},
                message=f"Appended {len(data)} items to '{storage_key}' (total: {len(context._data_stores[storage_key])})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Write append failed: {e}")


class WriteUpsertAction(BaseAction):
    """Upsert data (insert or update)."""
    action_type = "write_upsert"
    display_name = "Upsert写入"
    description = "插入或更新数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            storage_key = params.get("storage_key", "default")
            key_field = params.get("key_field", "id")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not hasattr(context, "_data_stores"):
                context._data_stores = {}
            if storage_key not in context._data_stores:
                context._data_stores[storage_key] = []

            store = context._data_stores[storage_key]
            store_index = {item.get(key_field): i for i, item in enumerate(store) if key_field in item}

            inserted = 0
            updated = 0
            for item in data:
                key = item.get(key_field)
                if key in store_index:
                    store[store_index[key]] = item
                    updated += 1
                else:
                    store.append(item)
                    inserted += 1

            return ActionResult(
                success=True,
                data={"inserted": inserted, "updated": updated, "total": len(store), "storage_key": storage_key},
                message=f"Upsert: {inserted} inserted, {updated} updated in '{storage_key}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Write upsert failed: {e}")


class WriteReplaceAction(BaseAction):
    """Replace all data in storage."""
    action_type = "write_replace"
    display_name = "替换写入"
    description = "替换存储中的所有数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            storage_key = params.get("storage_key", "default")

            if not hasattr(context, "_data_stores"):
                context._data_stores = {}

            previous_count = len(context._data_stores.get(storage_key, []))
            context._data_stores[storage_key] = data

            return ActionResult(
                success=True,
                data={"previous_count": previous_count, "new_count": len(data), "storage_key": storage_key},
                message=f"Replaced '{storage_key}': {previous_count} → {len(data)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Write replace failed: {e}")


class WriteBatchAction(BaseAction):
    """Batch write operations."""
    action_type = "write_batch"
    display_name = "批量写入"
    description = "批量写入操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batches = params.get("batches", [])
            storage_key = params.get("storage_key", "default")

            if not batches:
                return ActionResult(success=False, message="batches list is required")

            total_written = 0
            for batch in batches:
                items = batch.get("items", [])
                mode = batch.get("mode", "append")

                if mode == "append":
                    if not hasattr(context, "_data_stores"):
                        context._data_stores = {}
                    if storage_key not in context._data_stores:
                        context._data_stores[storage_key] = []
                    context._data_stores[storage_key].extend(items)
                    total_written += len(items)

            return ActionResult(
                success=True,
                data={"total_written": total_written, "batch_count": len(batches), "storage_key": storage_key},
                message=f"Batch write: wrote {total_written} items in {len(batches)} batches",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Write batch failed: {e}")


class WriteTransactionAction(BaseAction):
    """Transactional write."""
    action_type = "write_transaction"
    display_name = "事务写入"
    description = "事务性写入"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operations = params.get("operations", [])
            rollback_on_error = params.get("rollback_on_error", True)

            if not operations:
                return ActionResult(success=False, message="operations list is required")

            if not hasattr(context, "_data_stores"):
                context._data_stores = {}
            if not hasattr(context, "_data_stores_backup"):
                context._data_stores_backup = {}

            backup = {k: list(v) for k, v in context._data_stores.items()}
            committed = []
            failed = None

            for op in operations:
                try:
                    storage_key = op.get("storage_key", "default")
                    data = op.get("data", [])
                    mode = op.get("mode", "append")

                    if storage_key not in context._data_stores:
                        context._data_stores[storage_key] = []

                    if mode == "append":
                        context._data_stores[storage_key].extend(data)
                    elif mode == "replace":
                        context._data_stores[storage_key] = data

                    committed.append(op)
                except Exception as e:
                    failed = str(e)
                    if rollback_on_error:
                        context._data_stores = backup
                    break

            if failed:
                return ActionResult(
                    success=False,
                    data={"committed": len(committed), "failed_at": len(committed), "error": failed},
                    message=f"Transaction failed at operation {len(committed)}: {failed}",
                )

            return ActionResult(
                success=True,
                data={"committed": len(committed), "total_operations": len(operations)},
                message=f"Transaction committed: {len(committed)}/{len(operations)} operations",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Write transaction failed: {e}")
