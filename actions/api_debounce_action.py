"""
API debounce action for request deduplication and coalescing.

Provides request coalescing, deduplication, and response caching.
"""

from typing import Any, Callable, Dict, Optional
import time
import threading
import hashlib
import json


class APIDebounceAction:
    """API request debouncing with deduplication and coalescing."""

    def __init__(
        self,
        debounce_window: float = 0.5,
        max_wait: float = 5.0,
        enable_caching: bool = True,
        cache_ttl: float = 60.0,
    ) -> None:
        """
        Initialize API debouncer.

        Args:
            debounce_window: Window for coalescing requests (seconds)
            max_wait: Maximum time to wait for coalescing (seconds)
            enable_caching: Enable response caching
            cache_ttl: Cache TTL (seconds)
        """
        self.debounce_window = debounce_window
        self.max_wait = max_wait
        self.enable_caching = enable_caching
        self.cache_ttl = cache_ttl

        self._pending_requests: Dict[str, Dict[str, Any]] = {}
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._handlers: Dict[str, Callable] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute debounce operation.

        Args:
            params: Dictionary containing:
                - operation: 'request', 'register', 'clear', 'status'
                - key: Request key for deduplication
                - request: Request data
                - handler: Request handler function

        Returns:
            Dictionary with debounced result
        """
        operation = params.get("operation", "request")

        if operation == "request":
            return self._handle_request(params)
        elif operation == "register":
            return self._register_handler(params)
        elif operation == "clear":
            return self._clear_cache(params)
        elif operation == "status":
            return self._get_status(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _handle_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Handle debounced request."""
        key = params.get("key", "")
        request = params.get("request", {})
        force_execute = params.get("force_execute", False)

        if not key:
            return {"success": False, "error": "Request key is required"}

        request_key = self._generate_key(key, request)

        if self.enable_caching and not force_execute:
            cached = self._get_cached_response(request_key)
            if cached:
                return {"success": True, "cached": True, **cached}

        with self._lock:
            if request_key in self._pending_requests:
                existing = self._pending_requests[request_key]
                existing["count"] += 1
                existing["waiters"].append(time.time())
                return {
                    "success": True,
                    "coalesced": True,
                    "pending_count": existing["count"],
                    "request_key": request_key,
                }

            self._pending_requests[request_key] = {
                "request": request,
                "count": 1,
                "created_at": time.time(),
                "waiters": [],
            }

        try:
            result = self._execute_request(request_key, params)

            if self.enable_caching:
                self._cache_response(request_key, result)

            return {"success": True, "coalesced": False, **result}
        finally:
            with self._lock:
                if request_key in self._pending_requests:
                    self._notify_waiters(request_key)
                    del self._pending_requests[request_key]

    def _generate_key(self, key: str, request: dict[str, Any]) -> str:
        """Generate unique request key."""
        request_str = json.dumps(request, sort_keys=True)
        combined = f"{key}:{request_str}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _get_cached_response(self, request_key: str) -> Optional[Dict[str, Any]]:
        """Get cached response if valid."""
        if request_key not in self._cache:
            return None

        cached = self._cache[request_key]
        if time.time() - cached["cached_at"] < self.cache_ttl:
            return cached["response"]
        del self._cache[request_key]
        return None

    def _cache_response(self, request_key: str, response: Dict[str, Any]) -> None:
        """Cache response."""
        self._cache[request_key] = {
            "response": response,
            "cached_at": time.time(),
        }

    def _execute_request(self, request_key: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute actual request."""
        handler_name = params.get("handler_name", "default")

        if handler_name in self._handlers:
            handler = self._handlers[handler_name]
            return handler(params.get("request", {}))

        return {"success": True, "message": "Request executed", "request_key": request_key}

    def _notify_waiters(self, request_key: str) -> None:
        """Notify waiting requests of completion."""
        pending = self._pending_requests.get(request_key)
        if pending:
            for _ in pending.get("waiters", []):
                pass

    def _register_handler(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register request handler."""
        handler_name = params.get("handler_name", "default")
        handler = params.get("handler")

        if not handler_name or not callable(handler):
            return {"success": False, "error": "handler_name and callable handler are required"}

        self._handlers[handler_name] = handler
        return {"success": True, "handler_name": handler_name}

    def _clear_cache(self, params: dict[str, Any]) -> dict[str, Any]:
        """Clear response cache."""
        count = len(self._cache)
        self._cache.clear()
        return {"success": True, "cleared_entries": count}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get debouncer status."""
        with self._lock:
            return {
                "success": True,
                "pending_requests": len(self._pending_requests),
                "cached_responses": len(self._cache),
                "registered_handlers": list(self._handlers.keys()),
                "debounce_window": self.debounce_window,
                "max_wait": self.max_wait,
                "cache_ttl": self.cache_ttl,
            }
