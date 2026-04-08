"""Pagination action module for RabAI AutoClick.

Provides cursor-based and offset-based pagination handling for API requests
with automatic page fetching and result aggregation.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class PageInfo:
    """Information about a single page."""
    page_num: int
    cursor: Optional[str] = None
    offset: int = 0
    has_more: bool = True
    total_count: Optional[int] = None


@dataclass
class PaginationState:
    """State for tracking pagination progress."""
    results: List[Any] = field(default_factory=list)
    pages: List[PageInfo] = field(default_factory=list)
    total_fetched: int = 0
    current_page: int = 0
    exhausted: bool = False


class PaginationAction(BaseAction):
    """Handle paginated API requests with cursor or offset pagination.
    
    Supports automatic page fetching, result aggregation, and stop conditions
    based on total count, maximum pages, or empty response.
    """
    action_type = "pagination"
    display_name = "分页获取"
    description = "自动获取分页数据，支持游标和偏移量模式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute paginated API requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - fetch_func: Callable that takes (page_num, cursor/offset) 
                  and returns (items, has_more)
                - pagination_type: 'cursor' or 'offset'
                - page_size: Items per page (default 100)
                - max_pages: Maximum pages to fetch (default 10)
                - max_items: Maximum total items to fetch
                - stop_on_empty: Stop when a page returns empty (default True)
        
        Returns:
            ActionResult with all fetched items and pagination metadata.
        """
        # Extract parameters
        fetch_func = params.get('fetch_func')
        if not callable(fetch_func):
            return ActionResult(
                success=False, 
                message="fetch_func must be a callable"
            )
        
        pagination_type = params.get('pagination_type', 'offset').lower()
        if pagination_type not in ('cursor', 'offset'):
            return ActionResult(
                success=False,
                message="pagination_type must be 'cursor' or 'offset'"
            )
        
        page_size = params.get('page_size', 100)
        max_pages = params.get('max_pages', 10)
        max_items = params.get('max_items')
        stop_on_empty = params.get('stop_on_empty', True)
        
        # Initialize state
        state = PaginationState()
        cursor = None
        offset = 0
        
        try:
            while not state.exhausted:
                # Check page limit
                if state.current_page >= max_pages:
                    break
                
                # Check item limit
                if max_items and state.total_fetched >= max_items:
                    break
                
                # Fetch current page
                remaining = max_items - state.total_fetched if max_items else page_size
                fetch_size = min(page_size, remaining)
                
                if pagination_type == 'cursor':
                    items, has_more = fetch_func(
                        page_num=state.current_page + 1,
                        cursor=cursor,
                        page_size=fetch_size
                    )
                else:
                    items, has_more = fetch_func(
                        page_num=state.current_page + 1,
                        offset=offset,
                        page_size=fetch_size
                    )
                
                # Track page info
                page_info = PageInfo(
                    page_num=state.current_page + 1,
                    cursor=cursor,
                    offset=offset,
                    has_more=has_more
                )
                state.pages.append(page_info)
                
                # Process items
                if not items or (stop_on_empty and len(items) == 0):
                    state.exhausted = True
                    break
                
                # Add items (respecting max_items limit)
                items_to_add = items
                if max_items:
                    space_left = max_items - state.total_fetched
                    items_to_add = items[:space_left]
                
                state.results.extend(items_to_add)
                state.total_fetched += len(items_to_add)
                state.current_page += 1
                
                # Update cursor/offset for next page
                if pagination_type == 'cursor' and items:
                    cursor = self._extract_cursor(items[-1])
                else:
                    offset += len(items)
                
                # Check if more pages exist
                if not has_more or len(items) < fetch_size:
                    state.exhausted = True
                
                # Extract total count if available
                if hasattr(items, '__len__') and len(items) == 0:
                    pass
                elif max_items and state.total_fetched >= max_items:
                    break
            
            return ActionResult(
                success=True,
                message=f"Fetched {state.total_fetched} items across {state.current_page} pages",
                data={
                    'items': state.results,
                    'total_fetched': state.total_fetched,
                    'pages': [
                        {
                            'page_num': p.page_num,
                            'has_more': p.has_more
                        } for p in state.pages
                    ],
                    'exhausted': state.exhausted
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pagination failed: {e}",
                data={'partial_results': state.results}
            )
    
    def _extract_cursor(self, item: Any) -> Optional[str]:
        """Extract cursor from an item.
        
        Tries common cursor field names.
        """
        if isinstance(item, dict):
            for key in ('cursor', 'next_cursor', 'after', 'page_token', 'id'):
                if key in item:
                    return str(item[key])
        return None


class OffsetPaginationAction(BaseAction):
    """Simple offset-based pagination for APIs that support skip/limit."""
    action_type = "offset_pagination"
    display_name = "偏移分页"
    description = "基于偏移量的简单分页"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute offset-based pagination.
        
        Args:
            context: Execution context.
            params: Dict with:
                - api_call: Callable(offset, limit) -> list
                - total: Total number of items (if known)
                - limit: Page size (default 100)
                - max_results: Maximum results to return
        
        Returns:
            ActionResult with aggregated results.
        """
        api_call = params.get('api_call')
        if not callable(api_call):
            return ActionResult(success=False, message="api_call must be callable")
        
        limit = params.get('limit', 100)
        max_results = params.get('max_results')
        total = params.get('total')
        
        results = []
        offset = 0
        fetched = 0
        
        while True:
            # Check max results
            if max_results and fetched >= max_results:
                break
            
            # Check total if known
            if total and offset >= total:
                break
            
            # Fetch batch
            batch_size = min(limit, max_results - fetched) if max_results else limit
            try:
                batch = api_call(offset=offset, limit=batch_size)
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"API call failed at offset {offset}: {e}",
                    data={'partial_results': results}
                )
            
            if not batch:
                break
            
            results.extend(batch)
            fetched += len(batch)
            offset += len(batch)
            
            if len(batch) < batch_size:
                break
        
        return ActionResult(
            success=True,
            message=f"Fetched {len(results)} items",
            data={'items': results, 'total': len(results)}
        )
