"""
API Proxy Action.

Provides API proxy functionality.
Supports:
- Forward proxy
- Reverse proxy
- Caching
- Authentication passthrough
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import logging
import hashlib
import json
import time

logger = logging.getLogger(__name__)


@dataclass
class ProxyRequest:
    """Proxy request context."""
    method: str
    url: str
    headers: Dict[str, str]
    body: Optional[bytes] = None
    client_ip: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ProxyResponse:
    """Proxy response context."""
    status_code: int
    headers: Dict[str, str]
    body: Optional[bytes] = None
    cached: bool = False
    duration_ms: float = 0.0


@dataclass
class CacheEntry:
    """Cache entry."""
    response: ProxyResponse
    expires_at: datetime
    etag: Optional[str] = None


class ApiProxyAction:
    """
    API Proxy Action.
    
    Provides proxy functionality with support for:
    - Request/response caching
    - Header manipulation
    - Authentication passthrough
    - Load balancing
    """
    
    def __init__(
        self,
        cache_ttl: int = 300,
        cache_size: int = 1000,
        timeout: float = 30.0
    ):
        """
        Initialize the API Proxy Action.
        
        Args:
            cache_ttl: Cache TTL in seconds
            cache_size: Maximum cache size
            timeout: Request timeout in seconds
        """
        self.cache_ttl = cache_ttl
        self.cache_size = cache_size
        self.timeout = timeout
        
        self._cache: Dict[str, CacheEntry] = {}
        self._cache_lock = threading.RLock()
        self._stats: Dict[str, int] = {
            "requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0
        }
    
    async def forward(
        self,
        request: ProxyRequest,
        upstream_url: str,
        headers_modify: Optional[Callable[[Dict], Dict]] = None
    ) -> ProxyResponse:
        """Forward a request to the upstream."""
        start_time = time.time()
        
        # Check cache for GET requests
        cache_key = self._get_cache_key(request)
        if request.method == "GET":
            cached = self._get_from_cache(cache_key)
            if cached:
                cached.cached = True
                return cached
        
        try:
            # In production, would use httpx/aiohttp to forward
            response = ProxyResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=b'{"proxied": true}',
                cached=False,
                duration_ms=(time.time() - start_time) * 1000
            )
            
            # Cache response
            if request.method == "GET" and response.status_code == 200:
                self._put_in_cache(cache_key, response)
            
            return response
        
        except Exception as e:
            logger.error(f"Proxy error: {e}")
            return ProxyResponse(
                status_code=502,
                headers={},
                body=json.dumps({"error": str(e)}).encode(),
                duration_ms=(time.time() - start_time) * 1000
            )
    
    def _get_cache_key(self, request: ProxyRequest) -> str:
        """Generate cache key."""
        data = f"{request.method}:{request.url}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def _get_from_cache(self, key: str) -> Optional[ProxyResponse]:
        """Get response from cache."""
        with self._cache_lock:
            if key in self._cache:
                entry = self._cache[key]
                if datetime.utcnow() < entry.expires_at:
                    return entry.response
                del self._cache[key]
        return None
    
    def _put_in_cache(self, key: str, response: ProxyResponse) -> None:
        """Put response in cache."""
        with self._cache_lock:
            if len(self._cache) >= self.cache_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].expires_at)
                del self._cache[oldest[0]]
            
            self._cache[key] = CacheEntry(
                response=response,
                expires_at=datetime.utcnow() + timedelta(seconds=self.cache_ttl)
            )
    
    def invalidate_cache(self, pattern: Optional[str] = None) -> int:
        """Invalidate cache entries."""
        with self._cache_lock:
            if pattern:
                count = 0
                for key in list(self._cache.keys()):
                    if pattern in key:
                        del self._cache[key]
                        count += 1
                return count
            else:
                count = len(self._cache)
                self._cache = {}
                return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get proxy statistics."""
        total = self._stats["requests"]
        return {
            "total_requests": total,
            "cache_hits": self._stats["cache_hits"],
            "cache_misses": self._stats["cache_misses"],
            "cache_hit_rate": self._stats["cache_hits"] / total if total > 0 else 0,
            "errors": self._stats["errors"],
            "cached_entries": len(self._cache)
        }


if __name__ == "__main__":
    import asyncio
    
    async def main():
        proxy = ApiProxyAction(cache_ttl=60)
        
        request = ProxyRequest(
            method="GET",
            url="http://api.example.com/users",
            headers={"accept": "application/json"},
            client_ip="192.168.1.1"
        )
        
        response = await proxy.forward(request, "http://api.example.com")
        print(f"Status: {response.status_code}, Cached: {response.cached}")
        print(f"Stats: {json.dumps(proxy.get_stats(), indent=2)}")
    
    asyncio.run(main())
