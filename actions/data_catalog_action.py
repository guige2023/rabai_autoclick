"""Data catalog action module for RabAI AutoClick.

Provides data catalog operations:
- CatalogRegisterAction: Register a dataset
- CatalogSearchAction: Search catalog
- CatalogSchemaAction: Get/update dataset schema
- CatalogLineageAction: Track data lineage
- CatalogVersionAction: Version dataset
- CatalogUnregisterAction: Unregister dataset
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


class CatalogRegisterAction(BaseAction):
    """Register a dataset in catalog."""
    action_type = "catalog_register"
    display_name = "数据集注册"
    description = "在目录中注册数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            dataset_type = params.get("type", "table")
            schema = params.get("schema", {})
            owner = params.get("owner", "")
            tags = params.get("tags", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            catalog_id = str(uuid.uuid4())[:8]
            dataset_id = hashlib.md5(name.encode()).hexdigest()[:12]

            if not hasattr(context, "data_catalog"):
                context.data_catalog = {}
            context.data_catalog[dataset_id] = {
                "dataset_id": dataset_id,
                "name": name,
                "type": dataset_type,
                "schema": schema,
                "owner": owner,
                "tags": tags,
                "registered_at": time.time(),
                "version": "1.0.0",
            }

            return ActionResult(
                success=True,
                data={"dataset_id": dataset_id, "name": name, "type": dataset_type},
                message=f"Dataset {name} registered as {dataset_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog register failed: {e}")


class CatalogSearchAction(BaseAction):
    """Search data catalog."""
    action_type = "catalog_search"
    display_name = "目录搜索"
    description = "搜索数据目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", "")
            filters = params.get("filters", {})
            limit = params.get("limit", 20)

            if not query and not filters:
                return ActionResult(success=False, message="query or filters is required")

            catalog = getattr(context, "data_catalog", {})
            results = []
            query_lower = query.lower()

            for ds in catalog.values():
                if query_lower in ds.get("name", "").lower():
                    results.append(ds)
                elif query_lower in str(ds.get("tags", [])).lower():
                    results.append(ds)

            results = results[:limit]

            return ActionResult(
                success=True,
                data={"query": query, "results": results, "count": len(results)},
                message=f"Found {len(results)} matching datasets",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog search failed: {e}")


class CatalogSchemaAction(BaseAction):
    """Get or update dataset schema."""
    action_type = "catalog_schema"
    display_name = "目录Schema"
    description = "获取或更新数据集Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset_id = params.get("dataset_id", "")
            new_schema = params.get("schema", None)
            mode = params.get("mode", "get")

            if not dataset_id:
                return ActionResult(success=False, message="dataset_id is required")

            catalog = getattr(context, "data_catalog", {})
            if dataset_id not in catalog:
                return ActionResult(success=False, message=f"Dataset {dataset_id} not found")

            if mode == "get":
                return ActionResult(
                    success=True,
                    data={"dataset_id": dataset_id, "schema": catalog[dataset_id].get("schema", {})},
                    message=f"Schema for {dataset_id}",
                )
            elif mode == "update" and new_schema:
                catalog[dataset_id]["schema"] = new_schema
                catalog[dataset_id]["schema_updated_at"] = time.time()
                return ActionResult(
                    success=True,
                    data={"dataset_id": dataset_id, "schema": new_schema},
                    message=f"Schema updated for {dataset_id}",
                )
            else:
                return ActionResult(success=False, message="Invalid mode or missing schema")
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog schema failed: {e}")


class CatalogLineageAction(BaseAction):
    """Track data lineage."""
    action_type = "catalog_lineage"
    display_name = "数据血缘"
    description = "追踪数据血缘"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset_id = params.get("dataset_id", "")
            upstream = params.get("upstream", [])
            downstream = params.get("downstream", [])

            if not dataset_id:
                return ActionResult(success=False, message="dataset_id is required")

            lineage_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "data_lineage"):
                context.data_lineage = {}
            context.data_lineage[lineage_id] = {
                "lineage_id": lineage_id,
                "dataset_id": dataset_id,
                "upstream": upstream,
                "downstream": downstream,
                "tracked_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"lineage_id": lineage_id, "upstream_count": len(upstream), "downstream_count": len(downstream)},
                message=f"Lineage tracked for {dataset_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog lineage failed: {e}")


class CatalogVersionAction(BaseAction):
    """Version a dataset."""
    action_type = "catalog_version"
    display_name = "数据集版本"
    description = "版本化管理数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset_id = params.get("dataset_id", "")
            version_note = params.get("note", "")

            if not dataset_id:
                return ActionResult(success=False, message="dataset_id is required")

            catalog = getattr(context, "data_catalog", {})
            if dataset_id not in catalog:
                return ActionResult(success=False, message=f"Dataset {dataset_id} not found")

            ds = catalog[dataset_id]
            current = ds.get("version", "1.0.0")
            parts = current.split(".")
            parts[-1] = str(int(parts[-1]) + 1)
            new_version = ".".join(parts)

            if "versions" not in ds:
                ds["versions"] = []
            ds["versions"].append({"version": new_version, "note": version_note, "created_at": time.time()})
            ds["version"] = new_version

            return ActionResult(
                success=True,
                data={"dataset_id": dataset_id, "old_version": current, "new_version": new_version},
                message=f"Dataset {dataset_id} versioned to {new_version}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog version failed: {e}")


class CatalogUnregisterAction(BaseAction):
    """Unregister a dataset."""
    action_type = "catalog_unregister"
    display_name = "注销数据集"
    description = "从目录注销数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset_id = params.get("dataset_id", "")
            cascade = params.get("cascade", False)

            if not dataset_id:
                return ActionResult(success=False, message="dataset_id is required")

            catalog = getattr(context, "data_catalog", {})
            if dataset_id not in catalog:
                return ActionResult(success=False, message=f"Dataset {dataset_id} not found")

            ds_name = catalog[dataset_id]["name"]
            del catalog[dataset_id]

            return ActionResult(
                success=True,
                data={"dataset_id": dataset_id, "name": ds_name},
                message=f"Dataset {ds_name} unregistered",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Catalog unregister failed: {e}")
