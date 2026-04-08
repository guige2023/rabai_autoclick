"""
API Pagination Action Module.

Handles API pagination with offset, cursor, page-based,
and link-header pagination strategies.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PaginationType(Enum):
    """Pagination types."""
    OFFSET = "offset"
    CURSOR = "cursor"
    PAGE = "page"
    LINK_HEADER = "link_header"
    NEXT_TOKEN = "next_token"
    HAS_MORE = "has_more"


@dataclass
class PaginationConfig:
    """Configuration for pagination."""
    pagination_type: PaginationType = PaginationType.OFFSET
    page_size: int = 100
    max_pages: int = 100
    max_total: Optional[int] = None
    offset_param: str = "offset"
    limit_param: str = "limit"
    page_param: str = "page"
    cursor_param: str = "cursor"
    next_token_path: str = "next_cursor"
    data_path: str = "data"
    total_path: Optional[str] = None
    has_more_field: str = "has_more"


@dataclass
class Page:
    """A single page of results."""
    page_number: int
    items: List[Any]
    total_items: int
    has_next: bool
    has_previous: bool
    next_cursor: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ApiPaginationAction(BaseAction):
    """API pagination action.
    
    Fetches paginated API data with configurable strategies,
    automatic cursor following, and result aggregation.
    """
    action_type = "api_pagination"
    display_name = "API分页"
    description = "API分页数据获取"
    
    def __init__(self):
        super().__init__()
        self._config = PaginationConfig()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Fetch paginated API data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - base_url: The API base URL
                - method: HTTP method (default GET)
                - headers: Request headers
                - body: Request body (for POST)
                - pagination_type: Pagination strategy
                - page_size: Items per page
                - max_pages: Maximum pages to fetch
                - max_total: Maximum total items
                - timeout: Request timeout
                
        Returns:
            ActionResult with aggregated paginated data.
        """
        start_time = time.time()
        
        base_url = params.get("base_url", "")
        method = params.get("method", "GET")
        headers = params.get("headers", {})
        body = params.get("body")
        timeout = params.get("timeout", 30.0)
        
        config_dict = params.get("config", {})
        config = PaginationConfig(
            pagination_type=PaginationType(config_dict.get("pagination_type", "offset")),
            page_size=config_dict.get("page_size", 100),
            max_pages=config_dict.get("max_pages", 100),
            max_total=config_dict.get("max_total"),
            offset_param=config_dict.get("offset_param", "offset"),
            limit_param=config_dict.get("limit_param", "limit"),
            page_param=config_dict.get("page_param", "page"),
            cursor_param=config_dict.get("cursor_param", "cursor"),
            next_token_path=config_dict.get("next_token_path", "next_cursor"),
            data_path=config_dict.get("data_path", "data"),
            total_path=config_dict.get("total_path"),
            has_more_field=config_dict.get("has_more_field", "has_more")
        )
        
        if not base_url:
            return ActionResult(
                success=False,
                message="Missing required parameter: base_url",
                duration=time.time() - start_time
            )
        
        all_items = []
        page_number = 0
        total_fetched = 0
        pages = []
        errors = []
        
        try:
            if config.pagination_type == PaginationType.OFFSET:
                result = self._fetch_offset_pages(
                    base_url, method, headers, body, config, timeout
                )
            elif config.pagination_type == PaginationType.CURSOR:
                result = self._fetch_cursor_pages(
                    base_url, method, headers, body, config, timeout
                )
            elif config.pagination_type == PaginationType.PAGE:
                result = self._fetch_page_pages(
                    base_url, method, headers, body, config, timeout
                )
            elif config.pagination_type == PaginationType.LINK_HEADER:
                result = self._fetch_link_header_pages(
                    base_url, method, headers, body, config, timeout
                )
            elif config.pagination_type == PaginationType.NEXT_TOKEN:
                result = self._fetch_token_pages(
                    base_url, method, headers, body, config, timeout
                )
            elif config.pagination_type == PaginationType.HAS_MORE:
                result = self._fetch_has_more_pages(
                    base_url, method, headers, body, config, timeout
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown pagination type: {config.pagination_type}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Fetched {result['total_items']} items across {result['total_pages']} pages",
                data=result,
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pagination failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _build_url(self, base_url: str, params: Dict[str, str]) -> str:
        """Build URL with query parameters."""
        if not params:
            return base_url
        
        separator = "&" if "?" in base_url else "?"
        query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        return f"{base_url}{separator}{query}"
    
    def _get_items_from_response(self, body: Any, config: PaginationConfig) -> Tuple[List[Any], Dict[str, Any]]:
        """Extract items and metadata from API response."""
        if isinstance(body, dict):
            items = body.get(config.data_path, body.get("data", body.get("items", body.get("results", []))))
            metadata = {k: v for k, v in body.items() if k not in [config.data_path, "data", "items", "results"]}
            return items, metadata
        elif isinstance(body, list):
            return body, {}
        return [], {}
    
    def _get_total_from_response(self, body: Any, config: PaginationConfig) -> int:
        """Get total count from response if available."""
        if isinstance(body, dict) and config.total_path:
            parts = config.total_path.split(".")
            value = body
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return 0
            return int(value) if value else 0
        return 0
    
    def _fetch_offset_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using offset-based pagination."""
        all_items = []
        total_items = 0
        offset = 0
        page_number = 0
        max_total = config.max_total or float('inf')
        
        while offset < max_total and page_number < config.max_pages:
            params = {
                config.offset_param: str(offset),
                config.limit_param: str(config.page_size)
            }
            url = self._build_url(base_url, params)
            
            response = self._make_request(url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            if page_number == 0:
                total_items = self._get_total_from_response(response["body"], config) or len(items)
            
            if not items or len(items) < config.page_size:
                break
            
            offset += len(items)
            page_number += 1
            
            if config.max_total and offset >= config.max_total:
                break
        
        return {
            "items": all_items[:config.max_total] if config.max_total else all_items,
            "total_items": total_items or len(all_items),
            "total_pages": page_number + 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _fetch_cursor_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using cursor-based pagination."""
        all_items = []
        cursor = None
        page_number = 0
        
        while page_number < config.max_pages:
            params = {config.limit_param: str(config.page_size)}
            if cursor:
                params[config.cursor_param] = cursor
            
            url = self._build_url(base_url, params)
            response = self._make_request(url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            cursor = metadata.get(config.cursor_param) or metadata.get("next_cursor")
            if not cursor:
                break
            
            if not items:
                break
            
            page_number += 1
        
        return {
            "items": all_items,
            "total_items": len(all_items),
            "total_pages": page_number + 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _fetch_page_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using page number pagination."""
        all_items = []
        page_number = 1
        
        while page_number <= config.max_pages:
            params = {
                config.page_param: str(page_number),
                config.limit_param: str(config.page_size)
            }
            
            url = self._build_url(base_url, params)
            response = self._make_request(url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            if not items or len(items) < config.page_size:
                break
            
            page_number += 1
        
        return {
            "items": all_items,
            "total_items": len(all_items),
            "total_pages": page_number - 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _fetch_link_header_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using HTTP Link header."""
        all_items = []
        page_number = 0
        current_url = base_url
        
        while page_number < config.max_pages and current_url:
            response = self._make_request(current_url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            link_header = response.get("headers", {}).get("Link", "")
            current_url = self._extract_next_link(link_header)
            
            if not current_url:
                break
            
            page_number += 1
        
        return {
            "items": all_items,
            "total_items": len(all_items),
            "total_pages": page_number + 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _extract_next_link(self, link_header: str) -> Optional[str]:
        """Extract next URL from Link header."""
        if not link_header:
            return None
        for part in link_header.split(","):
            if 'rel="next"' in part or "rel='next'" in part:
                start = part.find("<")
                end = part.find(">")
                if start >= 0 and end > start:
                    return part[start + 1:end]
        return None
    
    def _fetch_token_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using next token pagination."""
        all_items = []
        next_token = None
        page_number = 0
        
        while page_number < config.max_pages:
            params = {config.limit_param: str(config.page_size)} if config.limit_param else {}
            if next_token:
                params[config.cursor_param] = next_token
            
            url = self._build_url(base_url, params)
            response = self._make_request(url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            next_token = self._get_nested_field(response["body"], config.next_token_path)
            if not next_token:
                break
            
            if not items:
                break
            
            page_number += 1
        
        return {
            "items": all_items,
            "total_items": len(all_items),
            "total_pages": page_number + 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _fetch_has_more_pages(
        self, base_url: str, method: str, headers: Dict, body: Any, config: PaginationConfig, timeout: float
    ) -> Dict[str, Any]:
        """Fetch pages using has_more flag."""
        all_items = []
        has_more = True
        page_number = 0
        
        while has_more and page_number < config.max_pages:
            params = {config.limit_param: str(config.page_size)}
            
            url = self._build_url(base_url, params)
            response = self._make_request(url, method, headers, body, timeout)
            if not response["success"]:
                break
            
            items, metadata = self._get_items_from_response(response["body"], config)
            all_items.extend(items)
            
            has_more = metadata.get(config.has_more_field, False)
            
            if not items:
                break
            
            page_number += 1
            
            if config.max_total and len(all_items) >= config.max_total:
                all_items = all_items[:config.max_total]
                break
        
        return {
            "items": all_items,
            "total_items": len(all_items),
            "total_pages": page_number + 1,
            "pagination_type": config.pagination_type.value
        }
    
    def _get_nested_field(self, obj: Any, path: str) -> Any:
        """Get nested field from object using dot notation."""
        if not path:
            return None
        parts = path.split(".")
        value = obj
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value
    
    def _make_request(
        self, url: str, method: str, headers: Dict, body: Any, timeout: float
    ) -> Dict[str, Any]:
        """Make an HTTP request."""
        try:
            body_bytes = None
            if body is not None:
                if isinstance(body, dict):
                    body_bytes = json.dumps(body).encode("utf-8")
                elif isinstance(body, str):
                    body_bytes = body.encode("utf-8")
                else:
                    body_bytes = str(body).encode("utf-8")
            
            req_headers = dict(headers)
            if body_bytes and "Content-Type" not in req_headers:
                req_headers["Content-Type"] = "application/json"
            
            req = Request(url, data=body_bytes, headers=req_headers, method=method.upper())
            
            with urlopen(req, timeout=timeout) as response:
                response_body = response.read()
                try:
                    parsed = json.loads(response_body)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    parsed = response_body.decode("utf-8", errors="replace")
                
                return {
                    "success": True,
                    "body": parsed,
                    "status_code": response.status,
                    "headers": dict(response.headers)
                }
                
        except HTTPError as e:
            return {
                "success": False,
                "error": f"HTTP {e.code}: {str(e)}",
                "status_code": e.code
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate pagination parameters."""
        if "base_url" not in params:
            return False, "Missing required parameter: base_url"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["base_url"]
