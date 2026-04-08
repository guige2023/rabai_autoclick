"""REST client action module for RabAI AutoClick.

Provides advanced REST API operations including pagination, retry logic,
rate limiting, request batching, and response caching.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""
    capacity: int = 10
    refill_rate: float = 10.0
    tokens: float = field(default=10.0)
    last_refill: float = field(default_factory=time.time)
    
    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    def wait_time(self) -> float:
        """Seconds to wait before next token available."""
        self._refill()
        return max(0, (1 - self.tokens) / self.refill_rate) if self.tokens < 1 else 0


class RestClientAction(BaseAction):
    """Advanced REST API client with retry, pagination, and rate limiting.
    
    Supports automatic retry with exponential backoff, cursor/page-based
    pagination, token bucket rate limiting, and response caching.
    """
    action_type = "rest_client"
    display_name = "REST客户端"
    description = "高级REST API客户端，支持重试、分页、限流"
    
    def __init__(self) -> None:
        super().__init__()
        self._rate_limiters: Dict[str, RateLimiter] = {}
        self._cache: Dict[str, tuple] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute REST API request with advanced features.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - url: Request URL (required)
                - method: HTTP method (default GET)
                - headers: Request headers dict
                - body: Request body (dict or string)
                - auth_type: 'bearer' or 'basic' (default none)
                - auth_token: Token for bearer auth
                - auth_username: Username for basic auth
                - auth_password: Password for basic auth
                - timeout: Request timeout in seconds (default 30)
                - retry_count: Number of retries on failure (default 3)
                - retry_delay: Base delay between retries in seconds (default 1)
                - retry_backoff: Backoff multiplier (default 2)
                - retry_on: List of HTTP status codes to retry on (default [429, 500, 502, 503, 504])
                - rate_limit: Max requests per second (default 10, 0 to disable)
                - pagination_type: 'cursor', 'page', or 'none' (default 'none')
                - pagination_field: Response field containing next page info
                - pagination_param: Query param name for pagination (default 'page')
                - max_pages: Maximum pages to fetch (default 10)
                - cache_ttl: Cache TTL in seconds (default 0, disabled)
                - expected_status: Expected status code(s), default 200
        
        Returns:
            ActionResult with response data and metadata.
        """
        url = params.get('url', '')
        if not url:
            return ActionResult(success=False, message="url is required")
        
        method = params.get('method', 'GET').upper()
        headers = {str(k): str(v) for k, v in params.get('headers', {}).items()}
        body = params.get('body')
        timeout = params.get('timeout', 30)
        retry_count = params.get('retry_count', 3)
        retry_delay = params.get('retry_delay', 1)
        retry_backoff = params.get('retry_backoff', 2)
        retry_on = params.get('retry_on', [429, 500, 502, 503, 504])
        rate_limit = params.get('rate_limit', 10)
        pagination_type = params.get('pagination_type', 'none')
        pagination_field = params.get('pagination_field', '')
        pagination_param = params.get('pagination_param', 'page')
        max_pages = params.get('max_pages', 10)
        cache_ttl = params.get('cache_ttl', 0)
        expected_status = params.get('expected_status', 200)
        
        # Auth
        auth_type = params.get('auth_type', '')
        auth_token = params.get('auth_token', '')
        auth_username = params.get('auth_username', '')
        auth_password = params.get('auth_password', '')
        
        # Prepare body
        request_body = None
        if body:
            if isinstance(body, dict):
                request_body = json.dumps(body).encode('utf-8')
                if 'Content-Type' not in headers:
                    headers['Content-Type'] = 'application/json'
            elif isinstance(body, str):
                request_body = body.encode('utf-8')
            elif isinstance(body, bytes):
                request_body = body
        
        # Cache check (only for GET)
        if method == 'GET' and cache_ttl > 0:
            cache_key = f"{method}:{url}:{json.dumps(headers, sort_keys=True)}"
            if cache_key in self._cache:
                cached_time, cached_data = self._cache[cache_key]
                if time.time() - cached_time < cache_ttl:
                    return ActionResult(
                        success=True,
                        message="Retrieved from cache",
                        data=cached_data
                    )
        
        # Rate limiter
        if rate_limit > 0:
            limiter_key = url.split('/')[2] if '://' in url else url
            if limiter_key not in self._rate_limiters:
                self._rate_limiters[limiter_key] = RateLimiter(
                    capacity=rate_limit,
                    refill_rate=rate_limit
                )
            limiter = self._rate_limiters[limiter_key]
            wait_time = limiter.wait_time()
            if wait_time > 0:
                time.sleep(wait_time)
        
        def do_request(req_url: str, page_token: str = None) -> tuple:
            """Execute a single request."""
            final_url = req_url
            if page_token and pagination_param:
                sep = '&' if '?' in req_url else '?'
                final_url = f"{req_url}{sep}{pagination_param}={page_token}"
            
            request = Request(final_url, data=request_body, headers=headers, method=method)
            
            # Auth
            if auth_type == 'bearer' and auth_token:
                request.add_header('Authorization', f'Bearer {auth_token}')
            elif auth_type == 'basic' and auth_username and auth_password:
                import base64
                creds = f"{auth_username}:{auth_password}"
                encoded = base64.b64encode(creds.encode()).decode()
                request.add_header('Authorization', f'Basic {encoded}')
            
            with urlopen(request, timeout=timeout) as response:
                return response.status, response.read().decode('utf-8'), dict(response.headers)
        
        # Execute with retry
        all_results: List[Any] = []
        current_page: Optional[str] = None
        pages_fetched = 0
        last_error = None
        
        for attempt in range(retry_count + 1):
            try:
                status, response_body, resp_headers = do_request(url, current_page)
                
                # Parse response
                content_type = resp_headers.get('Content-Type', '')
                parsed = response_body
                if 'application/json' in content_type:
                    try:
                        parsed = json.loads(response_body)
                    except json.JSONDecodeError:
                        pass
                
                # Pagination
                if pagination_type != 'none' and parsed:
                    if pagination_type == 'cursor' and pagination_field:
                        items = parsed.get(pagination_field, parsed.get('data', []))
                        all_results.extend(items if isinstance(items, list) else [items])
                        current_page = parsed.get('next_cursor') or parsed.get('cursor')
                    elif pagination_type == 'page':
                        items = parsed.get(pagination_field, parsed.get('data', []))
                        all_results.extend(items if isinstance(items, list) else [items])
                        current_page = str(parsed.get('page', 0) + 1) if isinstance(parsed.get('page'), int) else None
                    else:
                        all_results.append(parsed)
                        break
                    
                    pages_fetched += 1
                    if not current_page or pages_fetched >= max_pages:
                        break
                    continue
                else:
                    all_results.append(parsed)
                    break
                    
            except HTTPError as e:
                last_error = f"HTTP {e.code}: {e.reason}"
                if e.code in retry_on and attempt < retry_count:
                    delay = retry_delay * (retry_backoff ** attempt)
                    time.sleep(delay)
                    continue
                return ActionResult(
                    success=e.code == expected_status or expected_status in ([429, 500, 502, 503, 504] if e.code in retry_on else []),
                    message=last_error,
                    data={'status_code': e.code, 'error': str(e)}
                )
            except URLError as e:
                last_error = f"URL Error: {e.reason}"
                if attempt < retry_count:
                    delay = retry_delay * (retry_backoff ** attempt)
                    time.sleep(delay)
                    continue
                return ActionResult(success=False, message=last_error, data={'error': str(e)})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {e}", data={'error': str(e)})
        
        # Cache store
        if method == 'GET' and cache_ttl > 0:
            cache_key = f"{method}:{url}:{json.dumps(headers, sort_keys=True)}"
            self._cache[cache_key] = (time.time(), all_results if len(all_results) > 1 else all_results[0] if all_results else None)
        
        return ActionResult(
            success=True,
            message=f"Completed {pages_fetched + 1} page(s)",
            data={
                'pages': all_results,
                'total_pages': pages_fetched + 1,
                'has_more': current_page is not None and pages_fetched < max_pages
            }
        )


class BatchRequestAction(BaseAction):
    """Execute multiple API requests in a batch.
    
    Supports parallel execution with concurrency limit and
    aggregated results collection.
    """
    action_type = "batch_request"
    display_name = "批量请求"
    description = "批量执行多个API请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch requests.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - requests: List of request configs [{url, method, headers, body}, ...]
                - concurrency: Max parallel requests (default 5)
                - continue_on_error: Continue if one request fails (default True)
        
        Returns:
            ActionResult with aggregated results.
        """
        requests = params.get('requests', [])
        concurrency = params.get('concurrency', 5)
        continue_on_error = params.get('continue_on_error', True)
        
        if not isinstance(requests, list) or len(requests) == 0:
            return ActionResult(success=False, message="requests must be a non-empty list")
        
        results: List[Dict[str, Any]] = []
        succeeded = 0
        failed = 0
        
        import concurrent.futures
        
        def single_request(req: Dict[str, Any]) -> Dict[str, Any]:
            """Execute single request."""
            req_url = req.get('url', '')
            req_method = req.get('method', 'GET').upper()
            req_headers = {str(k): str(v) for k, v in req.get('headers', {}).items()}
            req_body = req.get('body')
            req_timeout = req.get('timeout', 30)
            
            body_bytes = None
            if req_body:
                if isinstance(req_body, dict):
                    body_bytes = json.dumps(req_body).encode('utf-8')
                elif isinstance(req_body, str):
                    body_bytes = req_body.encode('utf-8')
                elif isinstance(req_body, bytes):
                    body_bytes = req_body
            
            try:
                request = Request(req_url, data=body_bytes, headers=req_headers, method=req_method)
                with urlopen(request, timeout=req_timeout) as response:
                    body = response.read().decode('utf-8')
                    try:
                        body = json.loads(body)
                    except json.JSONDecodeError:
                        pass
                    return {'success': True, 'status': response.status, 'body': body}
            except Exception as e:
                return {'success': False, 'error': str(e)}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(single_request, r): i for i, r in enumerate(requests)}
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results.append({'index': idx, **result})
                    if result.get('success'):
                        succeeded += 1
                    else:
                        failed += 1
                        if not continue_on_error:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                except Exception as e:
                    results.append({'index': idx, 'success': False, 'error': str(e)})
                    failed += 1
        
        results.sort(key=lambda x: x['index'])
        
        return ActionResult(
            success=failed == 0,
            message=f"Batch: {succeeded} succeeded, {failed} failed",
            data={'results': results, 'succeeded': succeeded, 'failed': failed}
        )
