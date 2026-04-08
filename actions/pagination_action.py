"""
Pagination utilities - cursor-based, offset pagination, page navigation, infinite scroll.
"""
from typing import Any, Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class PaginationAction(BaseAction):
    """Pagination operations.

    Provides offset pagination, cursor pagination, page navigation, total calculation.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "paginate")
        data = params.get("data", [])
        page = int(params.get("page", 0))
        page_size = int(params.get("page_size", 10))

        try:
            if operation == "paginate":
                total = len(data)
                total_pages = max(1, (total + page_size - 1) // page_size)
                start = page * page_size
                end = start + page_size
                paged = data[start:end]
                return {
                    "success": True,
                    "data": paged,
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages,
                    "has_next": page < total_pages - 1,
                    "has_prev": page > 0,
                }

            elif operation == "offset_limit":
                offset = int(params.get("offset", 0))
                limit = int(params.get("limit", page_size))
                paged = data[offset:offset + limit]
                return {"success": True, "data": paged, "offset": offset, "limit": limit, "total": len(data)}

            elif operation == "cursor_paginate":
                cursor = params.get("cursor")
                page_size = int(params.get("page_size", 10))
                sort_field = params.get("sort_field", "id")
                sort_dir = params.get("sort_dir", "asc")
                if cursor is None:
                    start_idx = 0
                else:
                    try:
                        start_idx = int(cursor)
                    except ValueError:
                        start_idx = 0
                paged = data[start_idx:start_idx + page_size]
                next_cursor = str(start_idx + page_size) if start_idx + page_size < len(data) else None
                return {
                    "success": True,
                    "data": paged,
                    "next_cursor": next_cursor,
                    "page_size": page_size,
                    "total": len(data),
                }

            elif operation == "total_pages":
                total = len(data)
                page_size = int(params.get("page_size", 10))
                total_pages = max(1, (total + page_size - 1) // page_size)
                return {"success": True, "total": total, "page_size": page_size, "total_pages": total_pages}

            elif operation == "navigation":
                total = len(data)
                total_pages = max(1, (total + page_size - 1) // page_size)
                current = page
                window = int(params.get("window", 2))
                pages = []
                for p in range(max(0, current - window), min(total_pages, current + window + 1)):
                    pages.append({"number": p, "is_current": p == current})
                return {"success": True, "pages": pages, "current": current, "total_pages": total_pages, "has_next": current < total_pages - 1, "has_prev": current > 0}

            elif operation == "slice":
                start = int(params.get("start", 0))
                end = int(params.get("end", page_size))
                sliced = data[start:end]
                return {"success": True, "data": sliced, "start": start, "end": end, "total": len(data)}

            elif operation == "batch":
                batch_size = int(params.get("batch_size", page_size))
                batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
                return {"success": True, "batches": batches, "count": len(batches), "batch_size": batch_size}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"PaginationAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return PaginationAction().execute(context, params)
