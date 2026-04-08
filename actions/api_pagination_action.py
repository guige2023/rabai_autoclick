"""API pagination action module for RabAI AutoClick.

Provides comprehensive API pagination with support for
multiple pagination styles and automatic data collection.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiPaginationAction(BaseAction):
    """API pagination action for fetching paginated data.
    
    Supports offset, cursor, page, link header, and token-based
    pagination with configurable limits and rate limiting.
    """
    action_type = "api_pagination"
    display_name = "API分页器"
    description = "API分页数据获取"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pagination fetch.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                url: Base URL
                method: HTTP method
                headers: Request headers
                pagination_type: offset|cursor|page|link|token
                max_pages: Maximum pages to fetch
                max_items: Maximum total items
                offset_key: Param name for offset
                limit_key: Param name for limit
                cursor_key: Param name for cursor
                page_key: Param name for page
                limit: Items per page
                data_key: JSON path to data array in response
                next_key: JSON path to next cursor/page token.
        
        Returns:
            ActionResult with all fetched items.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        pagination_type = params.get('pagination_type', 'offset')
        max_pages = params.get('max_pages', 100)
        max_items = params.get('max_items', 100000)
        limit = params.get('limit', 100)
        data_key = params.get('data_key', 'data')
        next_key = params.get('next_key', 'next_cursor')
        timeout = params.get('timeout', 30)
        delay = params.get('delay', 0)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        all_items = []
        page = 0
        offset = 0
        cursor = None
        next_cursor = None
        
        while page < max_pages and len(all_items) < max_items:
            page_url = self._build_page_url(
                url, pagination_type, offset, cursor, page + 1,
                params.get('offset_key', 'offset'),
                params.get('limit_key', 'limit'),
                params.get('cursor_key', 'cursor'),
                params.get('page_key', 'page'),
                limit
            )
            
            response = self._fetch_page(page_url, method, headers, timeout)
            
            if not response['success']:
                return ActionResult(
                    success=False,
                    message=f"Failed to fetch page {page + 1}: {response.get('error')}",
                    data={
                        'items': all_items,
                        'pages_fetched': page,
                        'total_items': len(all_items)
                    }
                )
            
            data = response.get('data', {})
            items = self._extract_items(data, data_key)
            
            if not items:
                break
            
            all_items.extend(items)
            page += 1
            
            if len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break
            
            next_token = self._extract_next_token(data, next_key, pagination_type)
            
            if next_token is None:
                break
            
            if pagination_type == 'offset':
                offset += len(items)
            elif pagination_type in ('cursor', 'token'):
                cursor = next_token
            elif pagination_type == 'link':
                url = next_token
                if not url:
                    break
            
            if delay > 0 and page < max_pages:
                time.sleep(delay)
        
        return ActionResult(
            success=True,
            message=f"Fetched {len(all_items)} items across {page} pages",
            data={
                'items': all_items,
                'total_items': len(all_items),
                'pages_fetched': page,
                'has_more': page >= max_pages or len(all_items) >= max_items
            }
        )
    
    def _build_page_url(
        self,
        base_url: str,
        pagination_type: str,
        offset: int,
        cursor: Optional[str],
        page_num: int,
        offset_key: str,
        limit_key: str,
        cursor_key: str,
        page_key: str,
        limit: int
    ) -> str:
        """Build URL for next page."""
        if '?' in base_url:
            sep = '&'
        else:
            sep = '?'
        
        if pagination_type == 'offset':
            return f"{base_url}{sep}{offset_key}={offset}&{limit_key}={limit}"
        elif pagination_type == 'cursor':
            if cursor:
                return f"{base_url}{sep}{cursor_key}={cursor}&{limit_key}={limit}"
            return f"{base_url}{sep}{limit_key}={limit}"
        elif pagination_type == 'page':
            return f"{base_url}{sep}{page_key}={page_num}&{limit_key}={limit}"
        elif pagination_type == 'link':
            return base_url
        
        return base_url
    
    def _fetch_page(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        timeout: int
    ) -> Dict[str, Any]:
        """Fetch a single page."""
        data = None
        
        try:
            req = Request(url, method=method, headers=headers)
            with urlopen(req, timeout=timeout) as response:
                body = response.read()
                return {
                    'success': True,
                    'data': json.loads(body.decode('utf-8', errors='replace')),
                    'status': response.status,
                    'headers': dict(response.headers)
                }
        except HTTPError as e:
            body = e.read() if e.fp else b''
            return {
                'success': False,
                'error': f"HTTP {e.code}: {e.reason}",
                'status': e.code,
                'data': body.decode('utf-8', errors='replace') if body else ''
            }
        except URLError as e:
            return {
                'success': False,
                'error': str(e.reason)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _extract_items(self, data: Any, data_key: str) -> List[Any]:
        """Extract items from response data."""
        if not data_key:
            return data if isinstance(data, list) else []
        
        parts = data_key.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, [])
            else:
                return []
        
        return current if isinstance(current, list) else []
    
    def _extract_next_token(
        self,
        data: Dict,
        next_key: str,
        pagination_type: str
    ) -> Optional[str]:
        """Extract next cursor/token from response."""
        if pagination_type == 'link':
            headers = data.get('headers', {}) if isinstance(data, dict) else {}
            link_header = headers.get('Link', '') or headers.get('link', '')
            
            if not link_header:
                return None
            
            import re
            match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
            return match.group(1) if match else None
        
        parts = next_key.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        
        return str(current) if current else None
