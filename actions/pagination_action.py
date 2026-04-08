"""Pagination action module for RabAI AutoClick.

Provides cursor-based and offset-based pagination actions
for API requests and data processing.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Generator
from dataclasses import dataclass, field
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Page:
    """A single page of results.
    
    Attributes:
        page_number: Current page number.
        items: Items in this page.
        has_next: Whether more pages exist.
        has_prev: Whether previous pages exist.
        total_items: Total items across all pages.
        metadata: Additional pagination metadata.
    """
    page_number: int
    items: List[Any]
    has_next: bool
    has_prev: bool
    total_items: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CursorState:
    """State for cursor-based pagination.
    
    Attributes:
        cursor: Current cursor value.
        page_number: Current page number.
        items: Items fetched so far.
        exhausted: Whether all pages have been fetched.
    """
    cursor: Optional[str]
    page_number: int
    items: List[Any]
    exhausted: bool


class OffsetPaginator:
    """Offset-based pagination handler."""
    
    def __init__(self, page_size: int = 10):
        """Initialize paginator.
        
        Args:
            page_size: Number of items per page.
        """
        self.page_size = page_size
    
    def get_offset(self, page: int) -> int:
        """Calculate offset for page.
        
        Args:
            page: Page number (1-indexed).
        
        Returns:
            Offset value.
        """
        return (page - 1) * self.page_size
    
    def get_total_pages(self, total_items: int) -> int:
        """Calculate total pages.
        
        Args:
            total_items: Total number of items.
        
        Returns:
            Total page count.
        """
        return (total_items + self.page_size - 1) // self.page_size
    
    def paginate_list(self, items: List[Any], page: int = 1) -> Page:
        """Paginate a list.
        
        Args:
            items: Full list of items.
            page: Page number (1-indexed).
        
        Returns:
            Page with items.
        """
        total = len(items)
        total_pages = self.get_total_pages(total)
        
        start = self.get_offset(page)
        end = start + self.page_size
        
        page_items = items[start:end]
        
        return Page(
            page_number=page,
            items=page_items,
            has_next=page < total_pages,
            has_prev=page > 1,
            total_items=total
        )


class CursorPaginator:
    """Cursor-based pagination handler."""
    
    def __init__(self, fetcher: Callable[[Optional[str]], Dict[str, Any]]):
        """Initialize cursor paginator.
        
        Args:
            fetcher: Function that fetches a page given cursor.
                    Returns dict with 'items', 'next_cursor', 'exhausted'.
        """
        self.fetcher = fetcher
        self._state: Optional[CursorState] = None
        self._lock = threading.Lock()
    
    def initialize(self, initial_cursor: str = None) -> List[Any]:
        """Initialize and fetch first page.
        
        Args:
            initial_cursor: Optional starting cursor.
        
        Returns:
            First page of items.
        """
        with self._lock:
            result = self.fetcher(initial_cursor)
            
            self._state = CursorState(
                cursor=result.get('next_cursor'),
                page_number=1,
                items=result.get('items', []),
                exhausted=result.get('exhausted', False)
            )
            
            return self._state.items
    
    def next_page(self) -> Optional[List[Any]]:
        """Fetch next page.
        
        Returns:
            Next page of items or None if exhausted.
        """
        with self._lock:
            if self._state is None or self._state.exhausted:
                return None
            
            result = self.fetcher(self._state.cursor)
            
            self._state.cursor = result.get('next_cursor')
            self._state.page_number += 1
            self._state.exhausted = result.get('exhausted', False)
            self._state.items.extend(result.get('items', []))
            
            return result.get('items', [])
    
    def get_all(self) -> List[Any]:
        """Fetch all pages.
        
        Returns:
            All items from all pages.
        """
        if self._state is None:
            self.initialize()
        
        while not self._state.exhausted:
            self.next_page()
        
        return self._state.items
    
    def get_state(self) -> Optional[CursorState]:
        """Get current pagination state."""
        with self._lock:
            return self._state
    
    def reset(self) -> None:
        """Reset pagination state."""
        with self._lock:
            self._state = None


class PageIterator:
    """Iterator over paginated data."""
    
    def __init__(self, paginator: OffsetPaginator, items: List[Any]):
        """Initialize page iterator.
        
        Args:
            paginator: OffsetPaginator instance.
            items: Full list of items.
        """
        self.paginator = paginator
        self.items = items
        self._current_page = 0
    
    def __iter__(self):
        self._current_page = 0
        return self
    
    def __next__(self) -> Page:
        self._current_page += 1
        total_pages = self.paginator.get_total_pages(len(self.items))
        
        if self._current_page > total_pages:
            raise StopIteration
        
        return self.paginator.paginate_list(self.items, self._current_page)


class PaginateListAction(BaseAction):
    """Paginate a list of items."""
    action_type = "paginate_list"
    display_name = "列表分页"
    description = "对列表进行分页"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Paginate list.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, page, page_size.
        
        Returns:
            ActionResult with paginated page.
        """
        items = params.get('items', [])
        page = params.get('page', 1)
        page_size = params.get('page_size', 10)
        
        if not isinstance(items, list):
            return ActionResult(success=False, message="items must be a list")
        
        if page < 1:
            return ActionResult(success=False, message="page must be >= 1")
        
        try:
            paginator = OffsetPaginator(page_size=page_size)
            result_page = paginator.paginate_list(items, page)
            
            return ActionResult(
                success=True,
                message=f"Page {page} of {paginator.get_total_pages(len(items))}",
                data={
                    "page_number": result_page.page_number,
                    "items": result_page.items,
                    "has_next": result_page.has_next,
                    "has_prev": result_page.has_prev,
                    "total_items": result_page.total_items,
                    "total_pages": paginator.get_total_pages(len(items))
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pagination error: {str(e)}")


class GetPageAction(BaseAction):
    """Get a specific page from a list."""
    action_type = "get_page"
    display_name = "获取页面"
    description = "获取指定页"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get page.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, page_number, page_size.
        
        Returns:
            ActionResult with page items.
        """
        items = params.get('items', [])
        page_number = params.get('page_number', 1)
        page_size = params.get('page_size', 10)
        
        if not isinstance(items, list):
            return ActionResult(success=False, message="items must be a list")
        
        try:
            paginator = OffsetPaginator(page_size=page_size)
            result_page = paginator.paginate_list(items, page_number)
            
            return ActionResult(
                success=True,
                message=f"Retrieved page {page_number}",
                data={
                    "items": result_page.items,
                    "page_number": result_page.page_number,
                    "page_size": page_size,
                    "has_next": result_page.has_next,
                    "has_prev": result_page.has_prev
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Get page error: {str(e)}")


class PageNavigationAction(BaseAction):
    """Navigate through pages."""
    action_type = "page_navigation"
    display_name = "分页导航"
    description = "分页导航操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Navigate pages.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, page_size, action (first/next/prev/last).
        
        Returns:
            ActionResult with page data.
        """
        items = params.get('items', [])
        page_size = params.get('page_size', 10)
        action = params.get('action', 'first')
        
        if not isinstance(items, list):
            return ActionResult(success=False, message="items must be a list")
        
        try:
            paginator = OffsetPaginator(page_size=page_size)
            total_pages = paginator.get_total_pages(len(items))
            
            current_page = getattr(context, '_pagination_page', 1) if context else 1
            
            if action == 'first':
                page = 1
            elif action == 'last':
                page = total_pages
            elif action == 'next':
                page = min(current_page + 1, total_pages)
            elif action == 'prev':
                page = max(current_page - 1, 1)
            else:
                page = current_page
            
            if context:
                context._pagination_page = page
            
            result_page = paginator.paginate_list(items, page)
            
            return ActionResult(
                success=True,
                message=f"Navigated to page {page}",
                data={
                    "page_number": result_page.page_number,
                    "items": result_page.items,
                    "has_next": result_page.has_next,
                    "has_prev": result_page.has_prev,
                    "total_pages": total_pages
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Navigation error: {str(e)}")


class IteratorPagesAction(BaseAction):
    """Iterate over all pages."""
    action_type = "iterator_pages"
    display_name = "页面迭代"
    description = "遍历所有页面"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Iterate pages.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, page_size.
        
        Returns:
            ActionResult with page iterator info.
        """
        items = params.get('items', [])
        page_size = params.get('page_size', 10)
        
        if not isinstance(items, list):
            return ActionResult(success=False, message="items must be a list")
        
        try:
            paginator = OffsetPaginator(page_size=page_size)
            total_pages = paginator.get_total_pages(len(items))
            
            pages = []
            for p in range(1, total_pages + 1):
                page = paginator.paginate_list(items, p)
                pages.append({
                    "page_number": page.page_number,
                    "items": page.items,
                    "has_next": page.has_next,
                    "has_prev": page.has_prev
                })
            
            return ActionResult(
                success=True,
                message=f"Iterated {total_pages} pages",
                data={
                    "pages": pages,
                    "total_pages": total_pages,
                    "total_items": len(items)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Iterator error: {str(e)}")


class PageInfoAction(BaseAction):
    """Get pagination info without fetching data."""
    action_type = "page_info"
    display_name = "分页信息"
    description = "获取分页信息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get pagination info.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, page_size.
        
        Returns:
            ActionResult with pagination metadata.
        """
        items = params.get('items', [])
        page_size = params.get('page_size', 10)
        
        if not isinstance(items, list):
            return ActionResult(success=False, message="items must be a list")
        
        try:
            paginator = OffsetPaginator(page_size=page_size)
            total_pages = paginator.get_total_pages(len(items))
            
            return ActionResult(
                success=True,
                message=f"Total: {len(items)} items in {total_pages} pages",
                data={
                    "total_items": len(items),
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "current_page": 1,
                    "has_next": total_pages > 1,
                    "has_prev": False
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Page info error: {str(e)}")
