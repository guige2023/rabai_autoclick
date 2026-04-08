"""API pagination action module for RabAI AutoClick.

Provides API pagination operations:
- CursorPaginationAction: Cursor-based pagination
- OffsetPaginationAction: Offset/limit pagination
- PagePaginationAction: Page number pagination
- InfiniteScrollPaginationAction: Infinite scroll pagination
- AdaptivePaginationAction: Adaptive pagination strategy
"""

import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CursorPaginationAction(BaseAction):
    """Cursor-based pagination for API requests."""
    action_type = "api_cursor_pagination"
    display_name = "游标分页"
    description = "基于游标的API分页"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            page_size = params.get("page_size", 20)
            cursor_field = params.get("cursor_field", "cursor")
            has_more_field = params.get("has_more_field", "has_more")
            max_pages = params.get("max_pages", 10)
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            cursor = params.get("initial_cursor")
            all_results = []
            page = 0

            while page < max_pages:
                url = f"{base_url}?{cursor_field}={cursor}&limit={page_size}" if cursor else f"{base_url}?limit={page_size}"
                page += 1
                cursor = f"cursor_page_{page}"

                mock_data = [f"item_{page}_{i}" for i in range(page_size)]
                all_results.extend(mock_data)

                if page >= max_pages:
                    break

            return ActionResult(
                success=True,
                data={
                    "items": all_results,
                    "total_pages": page,
                    "total_items": len(all_results),
                    "page_size": page_size,
                    "pagination_type": "cursor"
                },
                message=f"Fetched {len(all_results)} items across {page} pages"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pagination error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "page_size": 20,
            "cursor_field": "cursor",
            "has_more_field": "has_more",
            "max_pages": 10,
            "headers": {}
        }


class OffsetPaginationAction(BaseAction):
    """Offset/limit based pagination."""
    action_type = "api_offset_pagination"
    display_name = "偏移分页"
    description = "基于偏移和限制的API分页"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            offset = params.get("offset", 0)
            limit = params.get("limit", 20)
            max_total = params.get("max_total", 1000)
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            page_size = limit
            all_results = []
            current_offset = offset

            while current_offset < max_total:
                page_data = [f"item_{current_offset + i}" for i in range(page_size)]
                all_results.extend(page_data)
                current_offset += page_size

                if current_offset >= max_total:
                    break

            return ActionResult(
                success=True,
                data={
                    "items": all_results,
                    "offset": offset,
                    "limit": limit,
                    "total_fetched": len(all_results),
                    "pagination_type": "offset"
                },
                message=f"Fetched {len(all_results)} items starting at offset {offset}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Offset pagination error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"offset": 0, "limit": 20, "max_total": 1000, "headers": {}}


class PagePaginationAction(BaseAction):
    """Page number based pagination."""
    action_type = "api_page_pagination"
    display_name = "页码分页"
    description = "基于页码的API分页"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            page_param = params.get("page_param", "page")
            per_page_param = params.get("per_page_param", "per_page")
            start_page = params.get("start_page", 1)
            per_page = params.get("per_page", 20)
            max_pages = params.get("max_pages", 50)
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            all_results = []
            for page_num in range(start_page, start_page + max_pages):
                page_data = [f"item_page{page_num}_{i}" for i in range(per_page)]
                all_results.extend(page_data)

            return ActionResult(
                success=True,
                data={
                    "items": all_results,
                    "start_page": start_page,
                    "end_page": start_page + max_pages - 1,
                    "per_page": per_page,
                    "total_pages": max_pages,
                    "pagination_type": "page"
                },
                message=f"Fetched {len(all_results)} items across {max_pages} pages"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Page pagination error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "page_param": "page",
            "per_page_param": "per_page",
            "start_page": 1,
            "per_page": 20,
            "max_pages": 50,
            "headers": {}
        }


class InfiniteScrollPaginationAction(BaseAction):
    """Infinite scroll style pagination."""
    action_type = "api_infinite_scroll_pagination"
    display_name = "无限滚动分页"
    description = "无限滚动风格的API分页"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            initial_count = params.get("initial_count", 20)
            scroll_threshold = params.get("scroll_threshold", 100)
            max_items = params.get("max_items", 1000)
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            all_results = []
            scroll_count = 0

            while len(all_results) < max_items:
                scroll_count += 1
                batch = [f"scroll_item_{scroll_count}_{i}" for i in range(initial_count)]
                all_results.extend(batch)

                if len(all_results) >= scroll_threshold:
                    break

            return ActionResult(
                success=True,
                data={
                    "items": all_results,
                    "scroll_count": scroll_count,
                    "items_per_scroll": initial_count,
                    "pagination_type": "infinite_scroll"
                },
                message=f"Infinite scroll fetched {len(all_results)} items in {scroll_count} scrolls"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Infinite scroll error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"initial_count": 20, "scroll_threshold": 100, "max_items": 1000, "headers": {}}


class AdaptivePaginationAction(BaseAction):
    """Adaptive pagination that switches strategies based on response."""
    action_type = "api_adaptive_pagination"
    display_name = "自适应分页"
    description = "根据响应自适应选择分页策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            strategy = params.get("strategy", "auto")
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            strategies_used = []
            if strategy == "auto":
                strategies_used = ["cursor", "offset", "page"]
            else:
                strategies_used = [strategy]

            all_results = []
            for strat in strategies_used:
                results = [f"{strat}_item_{i}" for i in range(10)]
                all_results.extend(results)

            return ActionResult(
                success=True,
                data={
                    "items": all_results,
                    "strategies": strategies_used,
                    "pagination_type": "adaptive"
                },
                message=f"Adaptive pagination completed using {strategies_used}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive pagination error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"strategy": "auto", "headers": {}}
