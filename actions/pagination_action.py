"""Pagination action module for RabAI AutoClick.

Handles paginated API responses with support for offset, cursor,
and page-based pagination strategies.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PaginationAction(BaseAction):
    """Pagination action for traversing paginated API resources.
    
    Supports multiple pagination strategies:
    - offset: skip/limit based pagination
    - cursor: opaque cursor-based pagination
    - page: page number based pagination
    - link: Hypermedia link-based pagination (RFC 5988)
    """
    action_type = "pagination"
    display_name = "分页遍历"
    description = "支持offset/cursor/page/link分页策略"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute paginated API request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                url, strategy (offset|cursor|page|link),
                offset_key, limit_key, cursor_key, page_key,
                max_pages, max_items, headers, body.
        
        Returns:
            ActionResult with all collected items and pagination info.
        """
        strategy = params.get('strategy', 'offset')
        max_pages = params.get('max_pages', 100)
        max_items = params.get('max_items', 10000)
        
        if strategy == 'offset':
            return self._paginate_offset(params, max_pages, max_items)
        elif strategy == 'cursor':
            return self._paginate_cursor(params, max_pages, max_items)
        elif strategy == 'page':
            return self._paginate_page(params, max_pages, max_items)
        elif strategy == 'link':
            return self._paginate_link(params, max_pages, max_items)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown pagination strategy: {strategy}"
            )
    
    def _paginate_offset(
        self,
        params: Dict[str, Any],
        max_pages: int,
        max_items: int
    ) -> ActionResult:
        """Paginate using offset/limit pattern."""
        base_url = params['url']
        headers = params.get('headers', {})
        body = params.get('body')
        offset_key = params.get('offset_key', 'offset')
        limit_key = params.get('limit_key', 'limit')
        limit = params.get('limit', 100)
        items_key = params.get('items_key', 'data')
        
        all_items = []
        offset = params.get('offset', 0)
        
        for page in range(max_pages):
            if len(all_items) >= max_items:
                break
            
            page_limit = min(limit, max_items - len(all_items))
            url = self._build_offset_url(base_url, offset, page_limit, offset_key, limit_key)
            
            response = self._fetch_page(url, headers, body, params.get('method', 'GET'))
            if not response['success']:
                return response
            
            items = self._extract_items(response['data'], items_key)
            if not items:
                break
            
            all_items.extend(items)
            offset += len(items)
            
            if len(items) < page_limit:
                break
        
        return ActionResult(
            success=True,
            message=f"Collected {len(all_items)} items across {page + 1} pages",
            data={
                'items': all_items,
                'total': len(all_items),
                'pages': page + 1
            }
        )
    
    def _paginate_cursor(
        self,
        params: Dict[str, Any],
        max_pages: int,
        max_items: int
    ) -> ActionResult:
        """Paginate using cursor pattern."""
        base_url = params['url']
        headers = params.get('headers', {})
        body = params.get('body')
        cursor_key = params.get('cursor_key', 'cursor')
        items_key = params.get('items_key', 'data')
        next_key = params.get('next_key', 'next_cursor')
        
        all_items = []
        cursor = params.get('cursor')
        
        for page in range(max_pages):
            if len(all_items) >= max_items:
                break
            
            url = self._build_cursor_url(base_url, cursor, cursor_key)
            
            response = self._fetch_page(url, headers, body, params.get('method', 'GET'))
            if not response['success']:
                return response
            
            data = response['data']
            items = self._extract_items(data, items_key)
            if not items:
                break
            
            all_items.extend(items)
            
            next_cursor = self._get_nested(data, next_key)
            if not next_cursor or next_cursor == cursor:
                break
            
            cursor = next_cursor
        
        return ActionResult(
            success=True,
            message=f"Collected {len(all_items)} items across {page + 1} pages",
            data={
                'items': all_items,
                'total': len(all_items),
                'pages': page + 1,
                'next_cursor': cursor
            }
        )
    
    def _paginate_page(
        self,
        params: Dict[str, Any],
        max_pages: int,
        max_items: int
    ) -> ActionResult:
        """Paginate using page number pattern."""
        base_url = params['url']
        headers = params.get('headers', {})
        body = params.get('body')
        page_key = params.get('page_key', 'page')
        per_page_key = params.get('per_page_key', 'per_page')
        per_page = params.get('per_page', 100)
        items_key = params.get('items_key', 'data')
        
        all_items = []
        page = params.get('page', 1)
        
        for page_num in range(page, page + max_pages):
            if len(all_items) >= max_items:
                break
            
            page_limit = min(per_page, max_items - len(all_items))
            url = self._build_page_url(base_url, page_num, page_limit, page_key, per_page_key)
            
            response = self._fetch_page(url, headers, body, params.get('method', 'GET'))
            if not response['success']:
                return response
            
            items = self._extract_items(response['data'], items_key)
            if not items:
                break
            
            all_items.extend(items)
            
            if len(items) < page_limit:
                break
        
        return ActionResult(
            success=True,
            message=f"Collected {len(all_items)} items across {page_num - page + 1} pages",
            data={
                'items': all_items,
                'total': len(all_items),
                'pages': page_num - page + 1
            }
        )
    
    def _paginate_link(
        self,
        params: Dict[str, Any],
        max_pages: int,
        max_items: int
    ) -> ActionResult:
        """Paginate using RFC 5988 Link header."""
        url = params['url']
        headers = params.get('headers', {})
        body = params.get('body')
        items_key = params.get('items_key', 'data')
        
        all_items = []
        
        for page in range(max_pages):
            if len(all_items) >= max_items:
                break
            
            response = self._fetch_page(url, headers, body, params.get('method', 'GET'))
            if not response['success']:
                return response
            
            items = self._extract_items(response['data'], items_key)
            if not items:
                break
            
            all_items.extend(items)
            
            next_url = response.get('headers', {}).get('link', '')
            if not next_url:
                break
            
            next_url = self._parse_link_header(next_url)
            if not next_url:
                break
            
            url = next_url
        
        return ActionResult(
            success=True,
            message=f"Collected {len(all_items)} items across {page + 1} pages",
            data={
                'items': all_items,
                'total': len(all_items),
                'pages': page + 1
            }
        )
    
    def _build_offset_url(
        self,
        base: str,
        offset: int,
        limit: int,
        offset_key: str,
        limit_key: str
    ) -> str:
        """Build URL with offset/limit params."""
        sep = '&' if '?' in base else '?'
        return f"{base}{sep}{offset_key}={offset}&{limit_key}={limit}"
    
    def _build_cursor_url(self, base: str, cursor: Optional[str], cursor_key: str) -> str:
        """Build URL with cursor param."""
        sep = '&' if '?' in base else '?'
        if cursor:
            return f"{base}{sep}{cursor_key}={cursor}"
        return base
    
    def _build_page_url(
        self,
        base: str,
        page: int,
        per_page: int,
        page_key: str,
        per_page_key: str
    ) -> str:
        """Build URL with page params."""
        sep = '&' if '?' in base else '?'
        return f"{base}{sep}{page_key}={page}&{per_page_key}={per_page}"
    
    def _fetch_page(
        self,
        url: str,
        headers: Dict[str, str],
        body: Optional[Any],
        method: str
    ) -> ActionResult:
        """Fetch a single page."""
        import json
        
        data = None
        if body:
            if isinstance(body, dict):
                data = json.dumps(body).encode('utf-8')
                headers = {**headers, 'Content-Type': 'application/json'}
            else:
                data = body.encode('utf-8') if isinstance(body, str) else body
        
        try:
            req = Request(url, data=data, headers=headers, method=method)
            with urlopen(req, timeout=30) as resp:
                body_bytes = resp.read()
                return {
                    'success': True,
                    'data': json.loads(body_bytes.decode('utf-8', errors='replace')),
                    'headers': dict(resp.headers)
                }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _extract_items(self, data: Any, key: str) -> List[Any]:
        """Extract items from response data using dot notation key."""
        if not key:
            return data if isinstance(data, list) else []
        
        parts = key.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, [])
            else:
                return []
        return current if isinstance(current, list) else []
    
    def _get_nested(self, data: Dict, key: str) -> Optional[Any]:
        """Get nested value from dict using dot notation."""
        parts = key.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
    
    def _parse_link_header(self, link_header: str) -> Optional[str]:
        """Parse RFC 5988 Link header and extract next URL."""
        import re
        match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
        return match.group(1) if match else None
