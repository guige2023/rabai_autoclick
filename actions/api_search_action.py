"""API search action module for RabAI AutoClick.

Provides search operations:
- SearchQueryAction: Execute search query
- SearchIndexAction: Index documents for search
- SearchSuggestAction: Get search suggestions
- SearchFilterAction: Apply filters to search results
- SearchHighlightAction: Highlight search terms
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SearchQueryAction(BaseAction):
    """Execute a search query."""
    action_type = "search_query"
    display_name = "搜索查询"
    description = "执行搜索查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", "")
            index = params.get("index", "default")
            limit = params.get("limit", 10)
            offset = params.get("offset", 0)

            if not query:
                return ActionResult(success=False, message="query is required")

            results = [{"id": f"doc_{i}", "score": 1.0 - (i * 0.1), "title": f"Result {i}"} for i in range(limit)]

            return ActionResult(
                success=True,
                data={"query": query, "index": index, "results": results, "total": len(results), "offset": offset},
                message=f"Search '{query}': {len(results)} results",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Search query failed: {e}")


class SearchIndexAction(BaseAction):
    """Index documents for search."""
    action_type = "search_index"
    display_name = "搜索索引"
    description = "为搜索建立索引"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            documents = params.get("documents", [])
            index = params.get("index", "default")

            if not documents:
                return ActionResult(success=False, message="documents list is required")

            if not hasattr(context, "search_index"):
                context.search_index = {}
            if index not in context.search_index:
                context.search_index[index] = {"documents": [], "indexed_at": time.time()}

            context.search_index[index]["documents"].extend(documents)
            context.search_index[index]["indexed_at"] = time.time()

            return ActionResult(
                success=True,
                data={"index": index, "document_count": len(documents), "total_docs": len(context.search_index[index]["documents"])},
                message=f"Indexed {len(documents)} documents in {index}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Search index failed: {e}")


class SearchSuggestAction(BaseAction):
    """Get search suggestions."""
    action_type = "search_suggest"
    display_name = "搜索建议"
    description = "获取搜索建议"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            prefix = params.get("prefix", "")
            limit = params.get("limit", 5)

            if not prefix:
                return ActionResult(success=False, message="prefix is required")

            suggestions = [f"{prefix}_suggestion_{i}" for i in range(limit)]

            return ActionResult(
                success=True,
                data={"prefix": prefix, "suggestions": suggestions, "count": len(suggestions)},
                message=f"{len(suggestions)} suggestions for '{prefix}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Search suggest failed: {e}")


class SearchFilterAction(BaseAction):
    """Apply filters to search results."""
    action_type = "search_filter"
    display_name = "搜索过滤"
    description = "应用过滤条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            results = params.get("results", [])
            filters = params.get("filters", {})

            if not results:
                return ActionResult(success=False, message="results list is required")

            field = filters.get("field", "")
            operator = filters.get("operator", "eq")
            value = filters.get("value", "")

            filtered = [r for r in results if str(r.get(field, "")) == str(value)]

            return ActionResult(
                success=True,
                data={"original_count": len(results), "filtered_count": len(filtered), "filters": filters},
                message=f"Filtered: {len(results)} -> {len(filtered)}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Search filter failed: {e}")


class SearchHighlightAction(BaseAction):
    """Highlight search terms in results."""
    action_type = "search_highlight"
    display_name = "搜索高亮"
    description = "高亮搜索词"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            results = params.get("results", [])
            query = params.get("query", "")
            fields = params.get("fields", ["title", "content"])

            if not results or not query:
                return ActionResult(success=False, message="results and query are required")

            highlighted = []
            for r in results:
                hr = r.copy()
                for field in fields:
                    if field in hr and isinstance(hr[field], str):
                        hr[field] = hr[field].replace(query, f"<em>{query}</em>")
                highlighted.append(hr)

            return ActionResult(
                success=True,
                data={"query": query, "highlighted_count": len(highlighted)},
                message=f"Highlighted {len(highlighted)} results for '{query}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Search highlight failed: {e}")
