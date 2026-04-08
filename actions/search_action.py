"""
Search utilities - full-text search, fuzzy matching, pagination, sorting, filtering.
"""
from typing import Any, Dict, List, Optional, Tuple, Callable
import re
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _tokenize(text: str) -> List[str]:
    return re.findall(r"\w+", text.lower())


def _fuzzy_match(query: str, target: str, threshold: float = 0.6) -> Tuple[bool, float]:
    query = query.lower()
    target = target.lower()
    if query in target:
        return True, 1.0
    q_tokens = _tokenize(query)
    t_tokens = _tokenize(target)
    if not q_tokens or not t_tokens:
        return False, 0.0
    matches = sum(1 for qt in q_tokens if any(qt in tt or tt in qt for tt in t_tokens))
    score = matches / len(q_tokens)
    return score >= threshold, score


def _inverted_index(documents: List[Dict[str, Any]], text_fields: List[str]) -> Dict[str, List[int]]:
    index: Dict[str, List[int]] = defaultdict(list)
    for i, doc in enumerate(documents):
        text = " ".join(str(doc.get(f, "")) for f in text_fields).lower()
        tokens = set(_tokenize(text))
        for token in tokens:
            index[token].append(i)
    return dict(index)


def _search_index(query: str, index: Dict[str, List[int]], documents: List[Dict[str, Any]]) -> List[Tuple[int, float]]:
    tokens = _tokenize(query)
    doc_scores: Dict[int, float] = defaultdict(float)
    for token in tokens:
        if token in index:
            for doc_idx in index[token]:
                doc_scores[doc_idx] += 1.0
    results = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    return results


class SearchAction(BaseAction):
    """Search operations.

    Provides full-text search, fuzzy matching, inverted index, pagination, sorting, filtering.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "search")
        query = params.get("query", "")
        documents = params.get("documents", [])
        data = params.get("data", documents)

        try:
            if operation == "search":
                if not query:
                    return {"success": False, "error": "query required"}
                text_fields = params.get("fields", ["text", "content", "title", "description"])
                index = _inverted_index(data, text_fields)
                results = _search_index(query, index, data)
                page = int(params.get("page", 0))
                page_size = int(params.get("page_size", 10))
                start = page * page_size
                end = start + page_size
                paged = results[start:end]
                found = [{"doc": data[i], "score": float(s)} for i, s in paged]
                return {
                    "success": True,
                    "results": found,
                    "total": len(results),
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (len(results) + page_size - 1) // page_size,
                }

            elif operation == "fuzzy_search":
                if not query:
                    return {"success": False, "error": "query required"}
                threshold = float(params.get("threshold", 0.6))
                text_field = params.get("field", "text")
                results = []
                for doc in data:
                    text = str(doc.get(text_field, ""))
                    matched, score = _fuzzy_match(query, text, threshold)
                    if matched:
                        results.append({"doc": doc, "score": round(score, 3)})
                results.sort(key=lambda x: x["score"], reverse=True)
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "filter":
                if not query:
                    return {"success": False, "error": "query required"}
                field = params.get("field", "")
                results = [doc for doc in data if query.lower() in str(doc.get(field, "")).lower()]
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "sort":
                field = params.get("field", "")
                reverse = params.get("reverse", False)
                if not field:
                    return {"success": False, "error": "field required"}
                try:
                    sorted_data = sorted(data, key=lambda x: x.get(field, ""), reverse=reverse)
                except Exception:
                    sorted_data = data
                return {"success": True, "data": sorted_data, "count": len(sorted_data)}

            elif operation == "paginate":
                page = int(params.get("page", 0))
                page_size = int(params.get("page_size", 10))
                start = page * page_size
                end = start + page_size
                paged = data[start:end]
                return {
                    "success": True,
                    "data": paged,
                    "count": len(paged),
                    "page": page,
                    "page_size": page_size,
                    "total": len(data),
                    "total_pages": (len(data) + page_size - 1) // page_size,
                }

            elif operation == "group_by":
                field = params.get("field", "")
                if not field:
                    return {"success": False, "error": "field required"}
                groups: Dict[str, List[Any]] = defaultdict(list)
                for doc in data:
                    key = str(doc.get(field, ""))
                    groups[key].append(doc)
                return {"success": True, "groups": dict(groups), "group_count": len(groups)}

            elif operation == "autocomplete":
                if not query:
                    return {"success": False, "error": "query required"}
                field = params.get("field", "text")
                limit = int(params.get("limit", 5))
                suggestions = set()
                q_lower = query.lower()
                for doc in data:
                    text = str(doc.get(field, ""))
                    if q_lower in text.lower():
                        suggestions.add(text)
                        if len(suggestions) >= limit:
                            break
                return {"success": True, "suggestions": list(suggestions)[:limit], "count": len(suggestions)}

            elif operation == "highlight":
                if not query:
                    return {"success": False, "error": "query required"}
                field = params.get("field", "text")
                results = []
                for doc in data:
                    text = str(doc.get(field, ""))
                    if query.lower() in text.lower():
                        highlighted = re.sub(f"({re.escape(query)})", r"**\1**", text, flags=re.IGNORECASE)
                        result = dict(doc)
                        result[f"{field}_highlighted"] = highlighted
                        results.append(result)
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "aggregate":
                field = params.get("field", "")
                agg_func = params.get("func", "count")
                if not field:
                    return {"success": False, "error": "field required"}
                values = [doc.get(field) for doc in data if field in doc]
                if agg_func == "count":
                    result = len(values)
                elif agg_func == "unique":
                    result = len(set(values))
                elif agg_func == "min":
                    nums = [v for v in values if isinstance(v, (int, float))]
                    result = min(nums) if nums else None
                elif agg_func == "max":
                    nums = [v for v in values if isinstance(v, (int, float))]
                    result = max(nums) if nums else None
                elif agg_func == "avg":
                    nums = [v for v in values if isinstance(v, (int, float))]
                    result = sum(nums) / len(nums) if nums else None
                else:
                    result = len(values)
                return {"success": True, "field": field, "aggregate": result, "func": agg_func}

            elif operation == "deduplicate":
                field = params.get("field", "")
                seen = set()
                unique = []
                removed = 0
                for doc in data:
                    key = doc.get(field, doc) if field else doc
                    if str(key) not in seen:
                        seen.add(str(key))
                        unique.append(doc)
                    else:
                        removed += 1
                return {"success": True, "data": unique, "count": len(unique), "removed": removed}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"SearchAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for search operations."""
    return SearchAction().execute(context, params)
