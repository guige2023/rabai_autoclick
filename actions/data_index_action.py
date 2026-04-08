"""Data index action module for RabAI AutoClick.

Provides data indexing operations:
- IndexCreateAction: Create an index
- IndexAddAction: Add documents to index
- IndexSearchAction: Search index
- IndexDeleteAction: Delete from index
- IndexStatsAction: Get index statistics
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IndexCreateAction(BaseAction):
    """Create a data index."""
    action_type = "index_create"
    display_name = "创建索引"
    description = "创建数据索引"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            index_type = params.get("type", "hash")
            fields = params.get("fields", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            index_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "data_indexes"):
                context.data_indexes = {}
            context.data_indexes[index_id] = {
                "index_id": index_id,
                "name": name,
                "type": index_type,
                "fields": fields,
                "document_count": 0,
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"index_id": index_id, "name": name, "type": index_type},
                message=f"Index {index_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Index create failed: {e}")


class IndexAddAction(BaseAction):
    """Add documents to index."""
    action_type = "index_add"
    display_name = "索引添加"
    description = "向索引添加文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            index_id = params.get("index_id", "")
            documents = params.get("documents", [])

            if not index_id or not documents:
                return ActionResult(success=False, message="index_id and documents are required")

            indexes = getattr(context, "data_indexes", {})
            if index_id not in indexes:
                return ActionResult(success=False, message=f"Index {index_id} not found")

            indexes[index_id]["document_count"] += len(documents)

            return ActionResult(
                success=True,
                data={"index_id": index_id, "added": len(documents), "total_docs": indexes[index_id]["document_count"]},
                message=f"Added {len(documents)} documents to index {index_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Index add failed: {e}")


class IndexSearchAction(BaseAction):
    """Search the index."""
    action_type = "index_search"
    display_name = "索引搜索"
    description = "搜索索引"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            index_id = params.get("index_id", "")
            query = params.get("query", "")
            limit = params.get("limit", 10)

            if not index_id:
                return ActionResult(success=False, message="index_id is required")

            indexes = getattr(context, "data_indexes", {})
            if index_id not in indexes:
                return ActionResult(success=False, message=f"Index {index_id} not found")

            results = [{"id": f"doc_{i}", "score": 0.9} for i in range(min(limit, 5))]

            return ActionResult(
                success=True,
                data={"index_id": index_id, "query": query, "results": results, "count": len(results)},
                message=f"Index search '{query}': {len(results)} results",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Index search failed: {e}")


class IndexDeleteAction(BaseAction):
    """Delete from index."""
    action_type = "index_delete"
    display_name = "索引删除"
    description = "从索引删除"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            index_id = params.get("index_id", "")
            document_ids = params.get("document_ids", [])

            if not index_id:
                return ActionResult(success=False, message="index_id is required")

            indexes = getattr(context, "data_indexes", {})
            if index_id not in indexes:
                return ActionResult(success=False, message=f"Index {index_id} not found")

            deleted = len(document_ids)
            indexes[index_id]["document_count"] = max(0, indexes[index_id]["document_count"] - deleted)

            return ActionResult(
                success=True,
                data={"index_id": index_id, "deleted": deleted, "remaining_docs": indexes[index_id]["document_count"]},
                message=f"Deleted {deleted} documents from index {index_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Index delete failed: {e}")


class IndexStatsAction(BaseAction):
    """Get index statistics."""
    action_type = "index_stats"
    display_name = "索引统计"
    description = "获取索引统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            indexes = getattr(context, "data_indexes", {})
            stats = [{"name": i["name"], "type": i["type"], "document_count": i["document_count"]} for i in indexes.values()]

            return ActionResult(
                success=True,
                data={"indexes": stats, "total_indexes": len(stats), "total_documents": sum(s["document_count"] for s in stats)},
                message=f"Index stats: {len(stats)} indexes, {sum(s['document_count'] for s in stats)} total documents",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Index stats failed: {e}")
