"""API Pagination Action Module.

Provides pagination handling for API requests and responses.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIPaginationAction(BaseAction):
    """Handle paginated API requests.
    
    Supports offset, cursor, and page-based pagination.
    """
    action_type = "api_pagination"
    display_name = "API分页"
    description = "处理API的分页请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pagination operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: strategy, fetch_func, options.
        
        Returns:
            ActionResult with paginated results.
        """
        strategy = params.get('strategy', 'offset')
        fetch_func_ref = params.get('fetch_func', None)
        options = params.get('options', {})
        
        try:
            if strategy == 'offset':
                return self._offset_pagination(params)
            elif strategy == 'cursor':
                return self._cursor_pagination(params)
            elif strategy == 'page':
                return self._page_pagination(params)
            elif strategy == 'all':
                return self._fetch_all_pages(params)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown strategy: {strategy}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Pagination failed: {str(e)}"
            )
    
    def _offset_pagination(self, params: Dict) -> ActionResult:
        """Offset-based pagination."""
        base_url = params.get('base_url', '')
        page_size = params.get('page_size', 100)
        max_pages = params.get('max_pages', 10)
        offset_param = params.get('offset_param', 'offset')
        limit_param = params.get('limit_param', 'limit')
        
        results = []
        for offset in range(0, max_pages * page_size, page_size):
            page_url = f"{base_url}?{offset_param}={offset}&{limit_param}={page_size}"
            # Simulate fetch
            results.append({'page_offset': offset, 'url': page_url})
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'offset',
                'pages': results,
                'total_pages': len(results)
            },
            error=None
        )
    
    def _cursor_pagination(self, params: Dict) -> ActionResult:
        """Cursor-based pagination."""
        base_url = params.get('base_url', '')
        cursor_param = params.get('cursor_param', 'cursor')
        max_cursors = params.get('max_cursors', 10)
        
        cursor = None
        results = []
        
        for _ in range(max_cursors):
            if cursor:
                url = f"{base_url}?{cursor_param}={cursor}"
            else:
                url = base_url
            
            # Simulate getting next cursor
            cursor = f"cursor_{len(results) + 1}"
            results.append({'cursor': cursor, 'url': url})
            
            if len(results) >= max_cursors:
                break
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'cursor',
                'pages': results,
                'total_pages': len(results)
            },
            error=None
        )
    
    def _page_pagination(self, params: Dict) -> ActionResult:
        """Page-based pagination."""
        base_url = params.get('base_url', '')
        page_param = params.get('page_param', 'page')
        page_size = params.get('page_size', 100)
        total_pages = params.get('total_pages', 10)
        
        results = []
        for page in range(1, total_pages + 1):
            url = f"{base_url}?{page_param}={page}&page_size={page_size}"
            results.append({'page': page, 'url': url})
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'page',
                'pages': results,
                'total_pages': len(results)
            },
            error=None
        )
    
    def _fetch_all_pages(self, params: Dict) -> ActionResult:
        """Fetch all pages until no more data."""
        strategy = params.get('strategy', 'offset')
        max_pages = params.get('max_pages', 100)
        stop_condition = params.get('stop_condition', 'no_more_data')
        
        all_data = []
        page_count = 0
        
        for _ in range(max_pages):
            page_count += 1
            # Simulate page fetch
            page_data = [{'page': page_count, 'item': i} for i in range(10)]
            all_data.extend(page_data)
            
            if page_count >= max_pages:
                break
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'fetch_all',
                'total_pages': page_count,
                'total_items': len(all_data),
                'all_data': all_data[:100]  # Truncate for display
            },
            error=None
        )


class APIInfiniteScrollAction(BaseAction):
    """Handle infinite scroll patterns.
    
    Manages loading more data as user scrolls.
    """
    action_type = "api_infinite_scroll"
    display_name = "API无限滚动"
    description = "处理无限滚动加载模式"
    
    def __init__(self):
        super().__init__()
        self._scroll_state: Dict[str, Dict] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute infinite scroll operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: scroll_id, action, data.
        
        Returns:
            ActionResult with scroll operation result.
        """
        scroll_id = params.get('scroll_id', 'default')
        action = params.get('action', 'load_more')
        
        if action == 'init':
            return self._init_scroll(scroll_id, params)
        elif action == 'load_more':
            return self._load_more(scroll_id, params)
        elif action == 'has_more':
            return self._has_more(scroll_id)
        elif action == 'reset':
            return self._reset_scroll(scroll_id)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _init_scroll(self, scroll_id: str, params: Dict) -> ActionResult:
        """Initialize infinite scroll."""
        self._scroll_state[scroll_id] = {
            'page': 0,
            'has_more': True,
            'items': []
        }
        
        return ActionResult(
            success=True,
            data={
                'scroll_id': scroll_id,
                'initialized': True
            },
            error=None
        )
    
    def _load_more(self, scroll_id: str, params: Dict) -> ActionResult:
        """Load more items."""
        if scroll_id not in self._scroll_state:
            return ActionResult(
                success=False,
                data=None,
                error="Scroll not initialized"
            )
        
        state = self._scroll_state[scroll_id]
        state['page'] += 1
        
        # Simulate loading more data
        new_items = [{'id': len(state['items']) + i, 'page': state['page']} for i in range(10)]
        state['items'].extend(new_items)
        
        # Check if more available
        state['has_more'] = state['page'] < 10
        
        return ActionResult(
            success=True,
            data={
                'scroll_id': scroll_id,
                'new_items': new_items,
                'total_items': len(state['items']),
                'has_more': state['has_more'],
                'current_page': state['page']
            },
            error=None
        )
    
    def _has_more(self, scroll_id: str) -> ActionResult:
        """Check if more data is available."""
        if scroll_id not in self._scroll_state:
            return ActionResult(
                success=False,
                data=None,
                error="Scroll not initialized"
            )
        
        return ActionResult(
            success=True,
            data={
                'scroll_id': scroll_id,
                'has_more': self._scroll_state[scroll_id]['has_more']
            },
            error=None
        )
    
    def _reset_scroll(self, scroll_id: str) -> ActionResult:
        """Reset scroll state."""
        if scroll_id in self._scroll_state:
            del self._scroll_state[scroll_id]
        
        return ActionResult(
            success=True,
            data={'scroll_id': scroll_id, 'reset': True},
            error=None
        )


class APIBatchProcessorAction(BaseAction):
    """Process API requests in batches.
    
    Groups multiple requests for efficient processing.
    """
    action_type = "api_batch_processor"
    display_name = "API批量处理器"
    description = "批量处理API请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch processing.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests, batch_size, strategy.
        
        Returns:
            ActionResult with batch results.
        """
        requests = params.get('requests', [])
        batch_size = params.get('batch_size', 10)
        strategy = params.get('strategy', 'sequential')
        
        if not requests:
            return ActionResult(
                success=False,
                data=None,
                error="No requests to process"
            )
        
        # Split into batches
        batches = [
            requests[i:i + batch_size]
            for i in range(0, len(requests), batch_size)
        ]
        
        results = []
        for i, batch in enumerate(batches):
            # Simulate batch processing
            batch_results = [
                {'request': req, 'batch': i, 'result': 'success'}
                for req in batch
            ]
            results.extend(batch_results)
        
        return ActionResult(
            success=True,
            data={
                'total_requests': len(requests),
                'batch_count': len(batches),
                'batch_size': batch_size,
                'results': results
            },
            error=None
        )


def register_actions():
    """Register all API Pagination actions."""
    return [
        APIPaginationAction,
        APIInfiniteScrollAction,
        APIBatchProcessorAction,
    ]
