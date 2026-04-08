"""Data searching action module for RabAI AutoClick.

Provides data searching operations:
- BinarySearchAction: Binary search on sorted data
- LinearSearchAction: Linear search with filters
- SearchIndexAction: Build and search index
- FuzzySearchAction: Fuzzy text search
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from difflib import SequenceMatcher

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BinarySearchAction(BaseAction):
    """Binary search on sorted data."""
    action_type = "binary_search"
    display_name: "二分搜索"
    description: "在排序数据上进行二分搜索"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            target = params.get("target")
            key = params.get("key", None)
            sorted_order = params.get("sorted_order", "asc")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="Empty data")

            if key:
                try:
                    data = sorted(data, key=lambda x: float(x.get(key, 0)) if x.get(key) is not None else float("-inf"), reverse=(sorted_order == "desc"))
                except (ValueError, TypeError):
                    data = sorted(data, key=lambda x: str(x.get(key, "")), reverse=(sorted_order == "desc"))

            left, right = 0, len(data) - 1
            found = False
            index = -1

            while left <= right:
                mid = (left + right) // 2
                mid_val = data[mid].get(key) if key and isinstance(data[mid], dict) else data[mid]

                if str(mid_val) == str(target):
                    found = True
                    index = mid
                    break
                elif str(mid_val) < str(target):
                    left = mid + 1
                else:
                    right = mid - 1

            return ActionResult(
                success=found,
                message=f"Binary search: {'Found' if found else 'Not found'} target '{target}' at index {index}",
                data={"found": found, "index": index, "target": target},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BinarySearch error: {e}")


class LinearSearchAction(BaseAction):
    """Linear search with filters."""
    action_type = "linear_search"
    display_name: "线性搜索"
    description: "带过滤条件的线性搜索"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            condition = params.get("condition", {})
            field = condition.get("field")
            operator = condition.get("operator", "eq")
            value = condition.get("value")

            if not isinstance(data, list):
                data = [data]

            matches = []
            for i, item in enumerate(data):
                if not isinstance(item, dict):
                    if field is None:
                        item_val = item
                    else:
                        continue
                else:
                    item_val = item.get(field)

                matched = False
                if operator == "eq":
                    matched = item_val == value
                elif operator == "ne":
                    matched = item_val != value
                elif operator == "gt":
                    matched = item_val is not None and item_val > value
                elif operator == "lt":
                    matched = item_val is not None and item_val < value
                elif operator == "ge":
                    matched = item_val is not None and item_val >= value
                elif operator == "le":
                    matched = item_val is not None and item_val <= value
                elif operator == "contains":
                    matched = value in str(item_val) if item_val else False
                elif operator == "startswith":
                    matched = str(item_val).startswith(str(value)) if item_val else False
                elif operator == "endswith":
                    matched = str(item_val).endswith(str(value)) if item_val else False
                elif operator == "in":
                    matched = item_val in value if isinstance(value, (list, tuple)) else False

                if matched:
                    matches.append({"index": i, "item": item})

            return ActionResult(
                success=True,
                message=f"Linear search: found {len(matches)} matches",
                data={"matches": matches, "match_count": len(matches)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"LinearSearch error: {e}")


class SearchIndexAction(BaseAction):
    """Build and search index."""
    action_type = "search_index"
    display_name: "搜索索引"
    description: "构建和搜索索引"

    def __init__(self):
        super().__init__()
        self._index: Dict[str, List[int]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "build")
            data = params.get("data", [])
            field = params.get("field", "text")

            if not isinstance(data, list):
                data = [data]

            if action == "build":
                self._index.clear()
                for i, item in enumerate(data):
                    if isinstance(item, dict):
                        text = str(item.get(field, ""))
                    else:
                        text = str(item)
                    words = text.lower().split()
                    for word in words:
                        if word not in self._index:
                            self._index[word] = []
                        self._index[word].append(i)

                return ActionResult(
                    success=True,
                    message=f"Built index with {len(self._index)} terms from {len(data)} items",
                    data={"term_count": len(self._index), "indexed_items": len(data)},
                )

            elif action == "search":
                query = params.get("query", "").lower()
                if not query:
                    return ActionResult(success=False, message="query is required")

                query_words = query.split()
                doc_scores: Dict[int, int] = {}
                for word in query_words:
                    if word in self._index:
                        for doc_idx in self._index[word]:
                            doc_scores[doc_idx] = doc_scores.get(doc_idx, 0) + 1

                results = [{"index": idx, "score": score} for idx, score in doc_scores.items()]
                results.sort(key=lambda x: x["score"], reverse=True)

                return ActionResult(
                    success=True,
                    message=f"Search '{query}': found {len(results)} results",
                    data={"results": results[:20], "result_count": len(results), "query": query},
                )

            elif action == "suggest":
                prefix = params.get("prefix", "").lower()
                if not prefix:
                    return ActionResult(success=False, message="prefix is required")
                suggestions = [term for term in self._index.keys() if term.startswith(prefix)][:10]
                return ActionResult(success=True, message=f"{len(suggestions)} suggestions", data={"suggestions": suggestions})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"SearchIndex error: {e}")


class FuzzySearchAction(BaseAction):
    """Fuzzy text search."""
    action_type = "fuzzy_search"
    display_name: "模糊搜索"
    description: "模糊文本搜索"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            query = params.get("query", "")
            field = params.get("field", "text")
            threshold = params.get("threshold", 0.6)

            if not isinstance(data, list):
                data = [data]

            if not query:
                return ActionResult(success=False, message="query is required")

            results = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    text = str(item.get(field, ""))
                else:
                    text = str(item)

                ratio = SequenceMatcher(None, query.lower(), text.lower()).ratio()
                if ratio >= threshold:
                    results.append({"index": i, "item": item, "score": round(ratio, 4)})

            results.sort(key=lambda x: x["score"], reverse=True)

            return ActionResult(
                success=True,
                message=f"Fuzzy search '{query}': found {len(results)} results (threshold={threshold})",
                data={"results": results, "result_count": len(results), "query": query, "threshold": threshold},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FuzzySearch error: {e}")
