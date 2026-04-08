"""Pagination action module for RabAI AutoClick.

Provides pagination handling for API responses with
cursor-based, offset-based, and page-based pagination.
"""

import sys
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PaginationType(Enum):
    """Pagination types."""
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"
    LINK_HEADER = "link_header"
    NEXT_TOKEN = "next_token"


@dataclass
class PaginationState:
    """Pagination state for a request."""
    total: Optional[int]
    page: int
    page_size: int
    offset: int
    total_pages: Optional[int]
    has_next: bool
    has_prev: bool
    next_cursor: Optional[str] = None
    next_offset: Optional[int] = None


class PaginationAction(BaseAction):
    """Handle paginated API responses.
    
    Supports offset-based, cursor-based, page-based,
    and link-header pagination with automatic traversal.
    """
    action_type = "pagination"
    display_name = "分页处理"
    description = "API分页处理：偏移/游标/页面/链接头分页"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Handle paginated requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (init/fetch_next/fetch_all/extract_page_info)
                - pagination_type: str (offset/page/cursor/link_header/next_token)
                - response: dict, API response (for extract_page_info)
                - page_size: int, items per page
                - offset: int, starting offset
                - cursor: str, cursor for cursor-based pagination
                - page: int, page number for page-based
                - max_pages: int, max pages to fetch for fetch_all
                - save_to_var: str
        
        Returns:
            ActionResult with pagination state or next page data.
        """
        operation = params.get('operation', 'init')
        pagination_type = params.get('pagination_type', 'offset')
        response = params.get('response', None)
        page_size = params.get('page_size', 20)
        offset = params.get('offset', 0)
        cursor = params.get('cursor', None)
        page = params.get('page', 1)
        max_pages = params.get('max_pages', 100)
        save_to_var = params.get('save_to_var', None)

        if operation == 'init':
            return self._init_pagination(pagination_type, page_size, offset, cursor, page, save_to_var)
        elif operation == 'fetch_next':
            return self._fetch_next(params, pagination_type, save_to_var)
        elif operation == 'fetch_all':
            return self._fetch_all(params, pagination_type, max_pages, save_to_var)
        elif operation == 'extract_page_info':
            return self._extract_page_info(response, pagination_type, page_size, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _init_pagination(
        self, pagination_type: str, page_size: int,
        offset: int, cursor: str, page: int, save_to_var: Optional[str]
    ) -> ActionResult:
        """Initialize pagination state."""
        if pagination_type == 'offset':
            state = PaginationState(
                total=None,
                page=1,
                page_size=page_size,
                offset=offset,
                total_pages=None,
                has_next=True,
                has_prev=offset > 0,
                next_offset=offset + page_size
            )
        elif pagination_type == 'page':
            state = PaginationState(
                total=None,
                page=page,
                page_size=page_size,
                offset=(page - 1) * page_size,
                total_pages=None,
                has_next=True,
                has_prev=page > 1,
            )
        elif pagination_type == 'cursor':
            state = PaginationState(
                total=None,
                page=1,
                page_size=page_size,
                offset=0,
                total_pages=None,
                has_next=True,
                has_prev=False,
                next_cursor=cursor
            )
        else:
            state = PaginationState(
                total=None,
                page=1,
                page_size=page_size,
                offset=0,
                total_pages=None,
                has_next=True,
                has_prev=False,
            )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'type': pagination_type,
                'page_size': state.page_size,
                'offset': state.offset,
                'page': state.page,
                'has_next': state.has_next,
                'next_cursor': state.next_cursor,
            }

        return ActionResult(
            success=True,
            message=f"Initialized {pagination_type} pagination",
            data={'type': pagination_type, 'page_size': page_size, 'offset': state.offset}
        )

    def _fetch_next(self, params: Dict, pagination_type: str, save_to_var: Optional[str]) -> ActionResult:
        """Calculate next page parameters."""
        current_offset = params.get('current_offset', 0)
        current_page = params.get('current_page', 1)
        current_cursor = params.get('current_cursor', None)
        page_size = params.get('page_size', 20)
        total = params.get('total', None)

        if pagination_type == 'offset':
            next_offset = current_offset + page_size
            has_next = total is None or next_offset < total
            result = {'offset': next_offset, 'has_next': has_next}

        elif pagination_type == 'page':
            next_page = current_page + 1
            has_next = total is None or next_page <= (total + page_size - 1) // page_size
            result = {'page': next_page, 'offset': (next_page - 1) * page_size, 'has_next': has_next}

        elif pagination_type == 'cursor':
            result = {'cursor': current_cursor, 'has_next': current_cursor is not None}

        elif pagination_type == 'next_token':
            result = {'next_token': current_cursor, 'has_next': current_cursor is not None}

        else:
            result = {'has_next': False}

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Next page: {result}",
            data=result
        )

    def _fetch_all(self, params: Dict, pagination_type: str, max_pages: int, save_to_var: Optional[str]) -> ActionResult:
        """Fetch all pages (for known iteration count)."""
        page_size = params.get('page_size', 20)
        total = params.get('total', None)
        start_page = params.get('start_page', 1)
        start_offset = params.get('start_offset', 0)

        if pagination_type == 'offset':
            total_pages = (total + page_size - 1) // page_size if total else max_pages
            offsets = list(range(start_offset, total_pages * page_size, page_size))[:max_pages]
            result = {'offsets': offsets, 'count': len(offsets)}

        elif pagination_type == 'page':
            total_pages = (total + page_size - 1) // page_size if total else max_pages
            pages = list(range(start_page, total_pages + 1))[:max_pages]
            result = {'pages': pages, 'count': len(pages)}

        else:
            result = {'max_iterations': max_pages}

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Will fetch up to {result.get('count', max_pages)} pages",
            data=result
        )

    def _extract_page_info(
        self, response: Any, pagination_type: str,
        default_page_size: int, save_to_var: Optional[str]
    ) -> ActionResult:
        """Extract pagination metadata from API response."""
        if response is None:
            return ActionResult(success=False, message="response is required")

        info = {'type': pagination_type, 'has_next': False}

        if isinstance(response, dict):
            # Try common pagination field names
            if 'total' in response:
                info['total'] = response['total']
            if 'page' in response:
                info['page'] = response['page']
            if 'page_size' in response:
                info['page_size'] = response['page_size']
            if 'per_page' in response:
                info['page_size'] = response['per_page']

            # Cursor/next token
            if 'next_cursor' in response:
                info['next_cursor'] = response['next_cursor']
                info['has_next'] = True
            elif 'cursor' in response:
                info['cursor'] = response['cursor']
                info['has_next'] = True
            elif 'next_token' in response:
                info['next_token'] = response['next_token']
                info['has_next'] = True

            # Link header (would need headers)
            if 'links' in response:
                links = response['links']
                if 'next' in links:
                    info['has_next'] = True

            # Check arrays that suggest pagination
            if 'data' in response and isinstance(response['data'], list):
                info['items'] = len(response['data'])

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = info

        return ActionResult(
            success=True,
            message="Extracted page info",
            data=info
        )

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pagination_type': 'offset',
            'response': None,
            'page_size': 20,
            'offset': 0,
            'cursor': None,
            'page': 1,
            'max_pages': 100,
            'current_offset': 0,
            'current_page': 1,
            'current_cursor': None,
            'total': None,
            'start_page': 1,
            'start_offset': 0,
            'save_to_var': None,
        }
