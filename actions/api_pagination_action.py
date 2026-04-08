"""API pagination action module for RabAI AutoClick.

Provides pagination utilities for REST APIs:
cursor-based, offset-based, and page-based pagination.
"""

import sys
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CursorPaginateAction(BaseAction):
    """Paginate through API results using cursor.
    
    Automatically follows next_cursor or next_page_token
    fields to fetch all pages.
    """
    action_type = "cursor_paginate"
    display_name = "游标分页"
    description = "使用游标自动翻页获取所有API结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Paginate through cursor-based API.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str (initial URL)
                - cursor_field: str (field containing next cursor)
                - request_config: dict (method, headers, body)
                - max_pages: int (maximum pages to fetch)
                - stop_on_empty: bool (stop when no more cursor)
                - save_to_var: str
        
        Returns:
            ActionResult with all paginated results.
        """
        url = params.get('url', '')
        cursor_field = params.get('cursor_field', 'next_cursor')
        request_config = params.get('request_config', {})
        max_pages = params.get('max_pages', 100)
        stop_on_empty = params.get('stop_on_empty', True)
        save_to_var = params.get('save_to_var', 'paginated_result')

        if not url:
            return ActionResult(success=False, message="URL is required")

        method = request_config.get('method', 'GET')
        headers = request_config.get('headers', {})
        body = request_config.get('body', None)

        all_items = []
        current_url = url
        page_count = 0

        while page_count < max_pages:
            page_count += 1

            try:
                response = self._do_request(current_url, method, headers, body)
                
                if not response.get('success'):
                    return ActionResult(
                        success=False,
                        message=f"Request failed at page {page_count}: {response.get('error')}"
                    )

                resp_data = response.get('body', {})
                
                # Extract items (try common patterns)
                items = self._extract_items(resp_data)
                all_items.extend(items)

                # Get next cursor
                cursor = self._get_nested_field(resp_data, cursor_field)
                
                if not cursor:
                    if stop_on_empty:
                        break
                    # Try common alternatives
                    for alt in ['next_page_token', 'cursor', 'pageToken', 'next']:
                        cursor = self._get_nested_field(resp_data, alt)
                        if cursor:
                            break

                if not cursor:
                    break

                # Build next URL
                if '?' in current_url:
                    current_url = f"{current_url.split('?')[0]}?{cursor_field}={cursor}"
                else:
                    current_url = f"{current_url}?{cursor_field}={cursor}"

            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Pagination error at page {page_count}: {e}"
                )

        result = {
            'total_pages': page_count,
            'total_items': len(all_items),
            'items': all_items,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Fetched {len(all_items)} items in {page_count} pages"
        )

    def _do_request(self, url: str, method: str, headers: Dict, body: Any) -> Dict:
        """Execute HTTP request."""
        import urllib.request
        import urllib.error
        import json

        body_bytes = None
        if body and method != 'GET':
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
            else:
                body_bytes = str(body).encode('utf-8')

        req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read()
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except:
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {'success': True, 'body': body_data, 'status': resp.status}
        except urllib.error.HTTPError as e:
            return {'success': False, 'error': f"HTTP {e.code}", 'status': e.code}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _extract_items(self, data: Any) -> List:
        """Extract items from response data."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ['items', 'data', 'results', 'records', 'list', 'rows']:
                if key in data:
                    val = data[key]
                    if isinstance(val, list):
                        return val
            # Return the whole dict as single item
            return [data]
        return []

    def _get_nested_field(self, data: Any, field: str) -> Any:
        """Get nested field from data using dot notation."""
        if not field:
            return None
        parts = field.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current


class OffsetPaginateAction(BaseAction):
    """Paginate through API results using offset/limit.
    
    Automatically increment offset until all results
    are fetched or max limit is reached.
    """
    action_type = "offset_paginate"
    display_name = "偏移分页"
    description = "使用偏移量自动翻页获取所有API结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Paginate through offset-based API.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str (base URL without pagination params)
                - offset_param: str (offset param name, default offset)
                - limit_param: str (limit param name, default limit)
                - limit: int (items per page)
                - max_items: int (maximum total items)
                - total_field: str (field containing total count)
                - request_config: dict
                - save_to_var: str
        
        Returns:
            ActionResult with all paginated results.
        """
        url = params.get('url', '')
        offset_param = params.get('offset_param', 'offset')
        limit_param = params.get('limit_param', 'limit')
        limit = params.get('limit', 100)
        max_items = params.get('max_items', 10000)
        total_field = params.get('total_field', 'total')
        request_config = params.get('request_config', {})
        save_to_var = params.get('save_to_var', 'offset_result')

        if not url:
            return ActionResult(success=False, message="URL is required")

        method = request_config.get('method', 'GET')
        headers = request_config.get('headers', {})
        body = request_config.get('body', None)

        all_items = []
        offset = 0
        page_count = 0
        total = None

        while offset < max_items:
            page_count += 1

            # Build URL with pagination params
            sep = '&' if '?' in url else '?'
            page_url = f"{url}{sep}{offset_param}={offset}&{limit_param}={limit}"

            try:
                response = self._do_request(page_url, method, headers, body)
                
                if not response.get('success'):
                    return ActionResult(
                        success=False,
                        message=f"Request failed at offset {offset}: {response.get('error')}"
                    )

                resp_data = response.get('body', {})
                items = self._extract_items(resp_data)
                
                if not items:
                    break

                all_items.extend(items)

                # Check total if available
                if total is None and total_field:
                    total = self._get_nested_field(resp_data, total_field)
                    if isinstance(total, (int, float)):
                        max_items = min(max_items, int(total))

                if len(items) < limit:
                    break

                offset += limit

            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Pagination error at offset {offset}: {e}"
                )

        result = {
            'total_pages': page_count,
            'total_items': len(all_items),
            'items': all_items,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Fetched {len(all_items)} items in {page_count} pages"
        )

    def _do_request(self, url: str, method: str, headers: Dict, body: Any) -> Dict:
        """Execute HTTP request."""
        import urllib.request
        import urllib.error
        import json

        body_bytes = None
        if body and method != 'GET':
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
            else:
                body_bytes = str(body).encode('utf-8')

        req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read()
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except:
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {'success': True, 'body': body_data, 'status': resp.status}
        except urllib.error.HTTPError as e:
            return {'success': False, 'error': f"HTTP {e.code}", 'status': e.code}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _extract_items(self, data: Any) -> List:
        """Extract items from response data."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ['items', 'data', 'results', 'records', 'list', 'rows']:
                if key in data:
                    val = data[key]
                    if isinstance(val, list):
                        return val
            return [data]
        return []

    def _get_nested_field(self, data: Any, field: str) -> Any:
        """Get nested field from data."""
        if not field:
            return None
        parts = field.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current


class PagePaginateAction(BaseAction):
    """Paginate through API results using page numbers.
    
    Automatically increment page number until all results
    are fetched.
    """
    action_type = "page_paginate"
    display_name = "页码分页"
    description = "使用页码自动翻页获取所有API结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Paginate through page-based API.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str (base URL)
                - page_param: str (page param name, default page)
                - page_size_param: str (page size param, default page_size)
                - page_size: int (items per page)
                - max_pages: int (maximum pages)
                - total_pages_field: str (field with total pages)
                - has_next_field: str (field indicating more pages)
                - request_config: dict
                - save_to_var: str
        
        Returns:
            ActionResult with all paginated results.
        """
        url = params.get('url', '')
        page_param = params.get('page_param', 'page')
        page_size_param = params.get('page_size_param', 'page_size')
        page_size = params.get('page_size', 100)
        max_pages = params.get('max_pages', 100)
        total_pages_field = params.get('total_pages_field', 'total_pages')
        has_next_field = params.get('has_next_field', 'has_more')
        request_config = params.get('request_config', {})
        save_to_var = params.get('save_to_var', 'page_result')

        if not url:
            return ActionResult(success=False, message="URL is required")

        method = request_config.get('method', 'GET')
        headers = request_config.get('headers', {})
        body = request_config.get('body', None)

        all_items = []
        current_page = 1
        page_count = 0

        while current_page <= max_pages:
            page_count += 1

            sep = '&' if '?' in url else '?'
            page_url = f"{url}{sep}{page_param}={current_page}&{page_size_param}={page_size}"

            try:
                response = self._do_request(page_url, method, headers, body)
                
                if not response.get('success'):
                    return ActionResult(
                        success=False,
                        message=f"Request failed at page {current_page}: {response.get('error')}"
                    )

                resp_data = response.get('body', {})
                items = self._extract_items(resp_data)
                
                if not items:
                    break

                all_items.extend(items)

                # Check if more pages
                if current_page >= max_pages:
                    break

                has_more = self._get_nested_field(resp_data, has_next_field)
                total_pages = self._get_nested_field(resp_data, total_pages_field)

                if has_more is False:
                    break
                if isinstance(total_pages, (int, float)) and current_page >= total_pages:
                    break

                current_page += 1

            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Pagination error at page {current_page}: {e}"
                )

        result = {
            'total_pages': page_count,
            'total_items': len(all_items),
            'items': all_items,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Fetched {len(all_items)} items in {page_count} pages"
        )

    def _do_request(self, url: str, method: str, headers: Dict, body: Any) -> Dict:
        """Execute HTTP request."""
        import urllib.request
        import urllib.error
        import json

        body_bytes = None
        if body and method != 'GET':
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
            else:
                body_bytes = str(body).encode('utf-8')

        req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read()
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except:
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {'success': True, 'body': body_data, 'status': resp.status}
        except urllib.error.HTTPError as e:
            return {'success': False, 'error': f"HTTP {e.code}", 'status': e.code}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _extract_items(self, data: Any) -> List:
        """Extract items from response data."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ['items', 'data', 'results', 'records', 'list', 'rows']:
                if key in data:
                    val = data[key]
                    if isinstance(val, list):
                        return val
            return [data]
        return []

    def _get_nested_field(self, data: Any, field: str) -> Any:
        """Get nested field from data."""
        if not field:
            return None
        parts = field.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current
