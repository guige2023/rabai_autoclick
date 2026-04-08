"""HTTP V2 action module for RabAI AutoClick.

Provides advanced HTTP operations including
conditional headers, response caching, and request retry logic.
"""

import json
import time
import sys
import os
import threading
import hashlib
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class HTTPResponse:
    """Represents an HTTP response.
    
    Attributes:
        status: HTTP status code.
        headers: Response headers.
        body: Response body.
        url: Final URL (after redirects).
        elapsed_ms: Request elapsed time.
    """
    status: int
    headers: Dict[str, str]
    body: str
    url: str
    elapsed_ms: float


class ResponseCache:
    """Thread-safe in-memory response cache for HTTP requests."""
    
    def __init__(self, max_size: int = 100, ttl: int = 300):
        """Initialize cache.
        
        Args:
            max_size: Maximum cached entries.
            ttl: Default TTL in seconds.
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def _make_key(self, method: str, url: str, headers: Dict, body: Any) -> str:
        """Generate cache key."""
        content = f"{method}:{url}:{json.dumps(headers, sort_keys=True)}:{str(body)}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, method: str, url: str, headers: Dict, body: Any) -> Optional[HTTPResponse]:
        """Get cached response.
        
        Args:
            method: HTTP method.
            url: Request URL.
            headers: Request headers.
            body: Request body.
        
        Returns:
            Cached HTTPResponse or None.
        """
        key = self._make_key(method, url, headers, body)
        
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            if time.time() - entry['timestamp'] > entry['ttl']:
                del self._cache[key]
                return None
            
            return entry['response']
    
    def set(self, method: str, url: str, headers: Dict, body: Any, response: HTTPResponse, ttl: int = None) -> None:
        """Cache a response.
        
        Args:
            method: HTTP method.
            url: Request URL.
            headers: Request headers.
            body: Request body.
            response: Response to cache.
            ttl: TTL override.
        """
        if ttl is None:
            ttl = self.ttl
        
        key = self._make_key(method, url, headers, body)
        
        with self._lock:
            if len(self._cache) >= self.max_size:
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k]['timestamp'])
                del self._cache[oldest_key]
            
            self._cache[key] = {
                'response': response,
                'timestamp': time.time(),
                'ttl': ttl
            }
    
    def clear(self) -> int:
        """Clear all cached entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def size(self) -> int:
        """Get number of cached entries."""
        with self._lock:
            return len(self._cache)


# Global response cache
_response_cache = ResponseCache()


class HTTPRequestV2Action(BaseAction):
    """Advanced HTTP request with caching and conditional headers."""
    action_type = "http_request_v2"
    display_name = "HTTP请求V2"
    description = "高级HTTP请求(缓存/重试/条件)"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP request.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   use_cache, cache_ttl, expected_status.
        
        Returns:
            ActionResult with response data.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET').upper()
        headers = params.get('headers', {})
        body = params.get('body', None)
        use_cache = params.get('use_cache', False)
        cache_ttl = params.get('cache_ttl', 300)
        expected_status = params.get('expected_status', 200)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        if use_cache:
            cached = _response_cache.get(method, url, headers, body)
            if cached:
                return ActionResult(
                    success=True,
                    message=f"Cache hit for {url}",
                    data={
                        "status": cached.status,
                        "body": cached.body,
                        "headers": cached.headers,
                        "from_cache": True,
                        "url": cached.url
                    }
                )
        
        start_time = time.time()
        
        try:
            body_bytes = None
            if body is not None:
                if isinstance(body, dict):
                    body_bytes = json.dumps(body).encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')
                elif isinstance(body, str):
                    body_bytes = body.encode('utf-8')
                else:
                    body_bytes = body
            
            req = Request(url, data=body_bytes, method=method)
            
            for key, value in headers.items():
                req.add_header(key, value)
            
            with urlopen(req, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                response_status = response.getcode()
                
                response_headers = {}
                for key, value in response.headers.items():
                    response_headers[key.lower()] = value
                
                elapsed_ms = (time.time() - start_time) * 1000
                
                http_response = HTTPResponse(
                    status=response_status,
                    headers=response_headers,
                    body=response_body,
                    url=url,
                    elapsed_ms=elapsed_ms
                )
                
                if use_cache:
                    _response_cache.set(method, url, headers, body, http_response, cache_ttl)
                
                return ActionResult(
                    success=response_status == expected_status,
                    message=f"HTTP {response_status} from {url}",
                    data={
                        "status": response_status,
                        "body": response_body,
                        "headers": response_headers,
                        "elapsed_ms": round(elapsed_ms, 2),
                        "url": url,
                        "from_cache": False
                    }
                )
        
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {str(e)}",
                data={
                    "status": e.code,
                    "error": str(e),
                    "url": url
                }
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"URL error: {str(e)}",
                data={
                    "error": str(e),
                    "url": url
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Request failed: {str(e)}",
                data={"error": str(e), "url": url}
            )


class HTTPPostJSONAction(BaseAction):
    """POST JSON data to URL."""
    action_type = "http_post_json"
    display_name = "POST JSON"
    description = "发送JSON POST请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """POST JSON data.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, data, headers.
        
        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        data = params.get('data', {})
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dict")
        
        headers.setdefault('Content-Type', 'application/json')
        headers.setdefault('Accept', 'application/json')
        
        try:
            body = json.dumps(data, ensure_ascii=False).encode('utf-8')
            req = Request(url, data=body, method='POST')
            
            for key, value in headers.items():
                req.add_header(key, value)
            
            start_time = time.time()
            
            with urlopen(req, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                elapsed_ms = (time.time() - start_time) * 1000
                
                try:
                    response_json = json.loads(response_body)
                except:
                    response_json = response_body
                
                return ActionResult(
                    success=True,
                    message=f"POST to {url} succeeded",
                    data={
                        "status": response.getcode(),
                        "body": response_json,
                        "elapsed_ms": round(elapsed_ms, 2)
                    }
                )
        
        except HTTPError as e:
            return ActionResult(success=False, message=f"HTTP {e.code}: {str(e)}", data={"status": e.code, "error": str(e)})
        except Exception as e:
            return ActionResult(success=False, message=f"POST failed: {str(e)}")


class HTTPGetJSONAction(BaseAction):
    """GET JSON data from URL."""
    action_type = "http_get_json"
    display_name = "GET JSON"
    description = "获取JSON数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """GET JSON data.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, params, headers.
        
        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        query_params = params.get('params', {})
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        if query_params:
            encoded = urlencode(query_params)
            url = f"{url}{'&' if '?' in url else '?'}{encoded}"
        
        headers.setdefault('Accept', 'application/json')
        
        try:
            req = Request(url, method='GET')
            
            for key, value in headers.items():
                req.add_header(key, value)
            
            start_time = time.time()
            
            with urlopen(req, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                elapsed_ms = (time.time() - start_time) * 1000
                
                try:
                    response_json = json.loads(response_body)
                except:
                    response_json = response_body
                
                return ActionResult(
                    success=True,
                    message=f"GET from {url} succeeded",
                    data={
                        "status": response.getcode(),
                        "body": response_json,
                        "elapsed_ms": round(elapsed_ms, 2)
                    }
                )
        
        except HTTPError as e:
            return ActionResult(success=False, message=f"HTTP {e.code}: {str(e)}", data={"status": e.code, "error": str(e)})
        except Exception as e:
            return ActionResult(success=False, message=f"GET failed: {str(e)}")


class HTTPBatchAction(BaseAction):
    """Execute multiple HTTP requests in batch."""
    action_type = "http_batch"
    display_name = "批量HTTP"
    description = "批量执行HTTP请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch requests.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests (list of request configs).
        
        Returns:
            ActionResult with all responses.
        """
        requests = params.get('requests', [])
        max_concurrent = params.get('max_concurrent', 5)
        
        if not requests:
            return ActionResult(success=False, message="requests list is required")
        
        import concurrent.futures
        
        def execute_single(req_config: Dict) -> Dict[str, Any]:
            url = req_config.get('url', '')
            method = req_config.get('method', 'GET').upper()
            headers = req_config.get('headers', {})
            body = req_config.get('body', None)
            
            start_time = time.time()
            
            try:
                body_bytes = None
                if body is not None:
                    if isinstance(body, dict):
                        body_bytes = json.dumps(body).encode('utf-8')
                    else:
                        body_bytes = body.encode('utf-8') if isinstance(body, str) else body
                
                req = Request(url, data=body_bytes, method=method)
                
                for key, value in headers.items():
                    req.add_header(key, value)
                
                with urlopen(req, timeout=30) as response:
                    response_body = response.read().decode('utf-8')
                    elapsed_ms = (time.time() - start_time) * 1000
                    
                    return {
                        "success": True,
                        "url": url,
                        "status": response.getcode(),
                        "body": response_body,
                        "elapsed_ms": round(elapsed_ms, 2)
                    }
            
            except HTTPError as e:
                return {"success": False, "url": url, "status": e.code, "error": str(e)}
            except Exception as e:
                return {"success": False, "url": url, "error": str(e)}
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(execute_single, req) for req in requests]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        total_duration = time.time() - start_time
        successful = sum(1 for r in results if r.get('success'))
        
        return ActionResult(
            success=True,
            message=f"Batch completed: {successful}/{len(requests)} successful",
            data={
                "results": results,
                "total": len(requests),
                "successful": successful,
                "failed": len(requests) - successful,
                "duration_ms": round(total_duration * 1000, 2)
            }
        )


class CacheClearAction(BaseAction):
    """Clear HTTP response cache."""
    action_type = "http_cache_clear"
    display_name = "清除HTTP缓存"
    description = "清空HTTP响应缓存"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear cache.
        
        Args:
            context: Execution context.
            params: Dict (unused).
        
        Returns:
            ActionResult with cleared count.
        """
        cleared = _response_cache.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared {cleared} cached responses",
            data={"cleared": cleared}
        )
