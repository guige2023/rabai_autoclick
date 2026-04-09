"""API pagination action module for RabAI AutoClick.

Handles paginated API responses with support for cursor-based,
offset-based, and link-header pagination patterns.
"""

import time
import json
from typing import Any, Dict, List, Optional, Union, Callable
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from core.base_action import BaseAction, ActionResult


class ApiPaginationAction(BaseAction):
    """Handle paginated API requests with multiple pagination strategies.
    
    Supports cursor-based, offset/limit, and Link header pagination.
    Automatically fetches all pages or limits to max_pages.
    """
    action_type = "api_pagination"
    display_name = "API分页"
    description = "处理API分页请求，支持游标/偏移量/链接头分页策略"
    VALID_STRATEGIES = ["cursor", "offset", "link_header", "page_number"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fetch paginated API data.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, strategy, cursor_param, limit_param,
                   max_pages, total_param, headers, timeout.
        
        Returns:
            ActionResult with all collected items and pagination metadata.
        """
        base_url = params.get("url", "")
        strategy = params.get("strategy", "cursor")
        cursor_param = params.get("cursor_param", "cursor")
        limit_param = params.get("limit_param", "limit")
        max_pages = params.get("max_pages", 10)
        limit = params.get("limit", 100)
        headers = params.get("headers", {})
        timeout = params.get("timeout", 30)
        total_param = params.get("total_param")
        
        if not base_url:
            return ActionResult(success=False, message="URL is required")
        
        valid, msg = self.validate_in(strategy, self.VALID_STRATEGIES, "strategy")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        all_items = []
        page_count = 0
        cursor = None
        total_expected = None
        start_time = time.time()
        errors = []
        
        try:
            while page_count < max_pages:
                page_count += 1
                
                url = self._build_url(
                    base_url, strategy, cursor, cursor_param,
                    limit_param, limit, page_count, total_param, all_items
                )
                
                request = Request(url, headers=headers)
                
                try:
                    with urlopen(request, timeout=timeout) as response:
                        body = json.loads(response.read().decode("utf-8"))
                        status = response.status
                except HTTPError as e:
                    body = json.loads(e.read().decode("utf-8")) if e.fp else {}
                    errors.append({"page": page_count, "error": f"HTTP {e.code}"})
                    break
                
                items, cursor, total_expected = self._extract_items(
                    body, strategy, cursor_param, total_param, items
                )
                
                all_items.extend(items)
                
                if strategy == "link_header":
                    next_cursor = self._get_next_cursor_from_headers(response.headers)
                    if not next_cursor:
                        break
                    cursor = next_cursor
                else:
                    if cursor is None or (total_expected and len(all_items) >= total_expected):
                        break
                
                if page_count >= max_pages:
                    break
            
            elapsed = time.time() - start_time
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Collected {len(all_items)} items in {page_count} pages ({elapsed:.2f}s)",
                data={
                    "items": all_items,
                    "total_collected": len(all_items),
                    "pages_fetched": page_count,
                    "has_more": cursor is not None and page_count >= max_pages,
                    "errors": errors,
                    "elapsed": elapsed
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pagination failed: {e}",
                data={"items_collected": len(all_items), "errors": errors}
            )
    
    def _build_url(
        self, base_url: str, strategy: str, cursor: Optional[str],
        cursor_param: str, limit_param: str, limit: int, page: int,
        total_param: Optional[str], items: List
    ) -> str:
        separator = "&" if "?" in base_url else "?"
        
        if strategy == "cursor":
            if cursor:
                return f"{base_url}{separator}{cursor_param}={cursor}&{limit_param}={limit}"
            return f"{base_url}{separator}{limit_param}={limit}"
        elif strategy == "offset":
            offset = len(items)
            return f"{base_url}{separator}{limit_param}={limit}&offset={offset}"
        elif strategy == "page_number":
            return f"{base_url}{separator}{limit_param}={limit}&page={page}"
        else:
            return f"{base_url}{separator}{limit_param}={limit}"
    
    def _extract_items(
        self, body: Any, strategy: str, cursor_param: str,
        total_param: Optional[str], existing_items: List
    ) -> tuple:
        items = []
        cursor = None
        total = None
        
        if isinstance(body, dict):
            if "data" in body:
                items = body["data"] if isinstance(body["data"], list) else [body["data"]]
            elif "items" in body:
                items = body["items"] if isinstance(body["items"], list) else [body["items"]]
            elif "results" in body:
                items = body["results"] if isinstance(body["results"], list) else [body["results"]]
            else:
                for key in body:
                    if isinstance(body[key], list):
                        items = body[key]
                        break
            
            if cursor_param in body:
                cursor = body[cursor_param]
            if total_param and total_param in body:
                total = body[total_param]
        elif isinstance(body, list):
            items = body
        
        return items, cursor, total
    
    def _get_next_cursor_from_headers(self, headers: dict) -> Optional[str]:
        link_header = headers.get("Link", "") or headers.get("link", "")
        if not link_header:
            return None
        
        import re
        matches = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', link_header)
        for url, rel in matches:
            if rel == "next":
                return url
        return None


class ApiCursorWalkAction(BaseAction):
    """Iterate through cursor-paginated API endpoints.
    
    Specifically designed for GraphQL and REST APIs that use
    cursor-based pagination with next/has_next patterns.
    """
    action_type = "api_cursor_walk"
    display_name = "API游标遍历"
    description = "遍历基于游标的API端点"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Walk through cursor-paginated API.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, data_path, cursor_path,
                   has_next_path, headers, max_iterations.
        
        Returns:
            ActionResult with all collected items.
        """
        url = params.get("url", "")
        data_path = params.get("data_path", "data")
        cursor_path = params.get("cursor_path", "cursor")
        has_next_path = params.get("has_next_path", "has_next")
        headers = params.get("headers", {})
        max_iterations = params.get("max_iterations", 50)
        body_template = params.get("body_template")
        timeout = params.get("timeout", 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        all_items = []
        cursor = None
        iteration = 0
        start_time = time.time()
        
        try:
            while iteration < max_iterations:
                iteration += 1
                
                current_url = url
                if cursor:
                    separator = "&" if "?" in url else "?"
                    current_url = f"{url}{separator}cursor={cursor}"
                
                request = Request(
                    current_url,
                    data=json.dumps(body_template).encode() if body_template else None,
                    headers={**headers, "Content-Type": "application/json"}
                )
                
                with urlopen(request, timeout=timeout) as response:
                    body = json.loads(response.read().decode("utf-8"))
                
                data = self._get_nested(body, data_path.split("."))
                if data is None:
                    data = body.get("data", body) if isinstance(body, dict) else body
                
                if not isinstance(data, list):
                    data = [data]
                
                all_items.extend(data)
                
                cursor = self._get_nested(body, cursor_path.split("."))
                has_next = self._get_nested(body, has_next_path.split("."))
                
                if not cursor or has_next is False or (isinstance(has_next, bool) and not has_next):
                    break
            
            elapsed = time.time() - start_time
            
            return ActionResult(
                success=True,
                message=f"Cursor walk completed: {len(all_items)} items in {iteration} iterations",
                data={
                    "items": all_items,
                    "total": len(all_items),
                    "iterations": iteration,
                    "elapsed": elapsed
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cursor walk failed: {e}")
    
    def _get_nested(self, obj: Any, path: List[str]) -> Any:
        for key in path:
            if isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
            if obj is None:
                return None
        return obj
