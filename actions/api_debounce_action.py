"""API debounce action module for RabAI AutoClick.

Provides debouncing for API request operations:
- ApiRequestDebouncer: Debounce rapid API requests
- ApiRequestCoalescer: Coalesce multiple API requests
- ApiRequestBatcher: Batch similar API requests together
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Set
import time
import threading
import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiDebounceMode(Enum):
    """API debounce modes."""
    REQUEST = "request"
    RESPONSE = "response"
    COMBINED = "combined"
    CACHING = "caching"


@dataclass
class ApiDebounceConfig:
    """Configuration for API debouncing."""
    mode: ApiDebounceMode = ApiDebounceMode.REQUEST
    delay: float = 0.5
    max_pending: int = 100
    cache_ttl: float = 30.0
    dedup_enabled: bool = True
    dedup_key_fields: List[str] = field(default_factory=lambda: ["url", "method"])
    coalesce_by: Optional[str] = None
    batch_size: int = 5
    batch_timeout: float = 1.0


class RequestDeduplicator:
    """Deduplicate identical API requests."""
    
    def __init__(self):
        self._pending: Dict[str, Tuple[Any, threading.Event]] = {}
        self._results: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"dedup_hits": 0, "dedup_misses": 0}
    
    def _make_key(self, request: Dict[str, Any], fields: List[str]) -> str:
        """Generate dedup key from request."""
        parts = []
        for field in fields:
            val = request.get(field, "")
            parts.append(f"{field}={val}")
        return "|".join(parts)
    
    def register(self, request: Dict[str, Any]) -> Tuple[Optional[Any], bool]:
        """Register a request. Returns (existing_result, was_deduped)."""
        key = self._make_key(request, ["url", "method", "params", "data"])
        
        with self._lock:
            if key in self._results:
                age = time.time() - self._results[key][1]
                if age < 30.0:
                    self._stats["dedup_hits"] += 1
                    return self._results[key][0], True
            
            if key in self._pending:
                event = threading.Event()
                self._pending[key] = (self._pending[key][0], event)
                self._stats["dedup_hits"] += 1
                return None, True
            
            event = threading.Event()
            self._pending[key] = (None, event)
            self._stats["dedup_misses"] += 1
            return None, False
    
    def complete(self, request: Dict[str, Any], result: Any):
        """Mark request as complete and notify waiters."""
        key = self._make_key(request, ["url", "method", "params", "data"])
        
        with self._lock:
            self._results[key] = (result, time.time())
            
            if key in self._pending:
                _, event = self._pending.pop(key)
                event.set()
    
    def wait(self, request: Dict[str, Any], timeout: float = 5.0) -> Optional[Any]:
        """Wait for existing request to complete."""
        key = self._make_key(request, ["url", "method", "params", "data"])
        
        with self._lock:
            if key not in self._pending:
                return None
            _, event = self._pending[key]
        
        if event.wait(timeout):
            with self._lock:
                if key in self._results:
                    return self._results[key][0]
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        with self._lock:
            return dict(self._stats)


class ApiRequestDebouncer:
    """Debounce API requests."""
    
    def __init__(self, config: Optional[ApiDebounceConfig] = None):
        self.config = config or ApiDebounceConfig()
        self._timers: Dict[str, Any] = {}
        self._pending_requests: Dict[str, Tuple[Any, ...]] = {}
        self._deduplicator = RequestDeduplicator()
        self._lock = threading.RLock()
        self._stats = {"total_requests": 0, "debounced": 0, "batched": 0, "executed": 0}
    
    def _make_request_key(self, request: Dict[str, Any]) -> str:
        """Generate key for request grouping."""
        if self.config.coalesce_by:
            return f"{request.get(self.config.coalesce_by, 'default')}"
        
        url = request.get("url", "")
        method = request.get("method", "GET")
        params_hash = hashlib.md5(str(request.get("params", "")).encode()).hexdigest()[:8]
        return f"{method}:{url}:{params_hash}"
    
    def schedule(self, request: Dict[str, Any], operation: Callable, *args, **kwargs) -> bool:
        """Schedule a debounced API request."""
        with self._lock:
            self._stats["total_requests"] += 1
            key = self._make_request_key(request)
            
            existing_timer = self._timers.get(key)
            if existing_timer:
                existing_timer.cancel()
            
            self._pending_requests[key] = (operation, args, kwargs)
            
            timer = threading.Timer(self.config.delay, self._execute_pending, args=(key,))
            self._timers[key] = timer
            timer.start()
            
            self._stats["debounced"] += 1
            return True
    
    def _execute_pending(self, key: str):
        """Execute pending requests for key."""
        with self._lock:
            pending = self._pending_requests.pop(key, None)
            timer = self._timers.pop(key, None)
        
        if pending:
            operation, args, kwargs = pending
            try:
                operation(*args, **kwargs)
                self._stats["executed"] += 1
            except Exception as e:
                logging.error(f"ApiRequestDebouncer execution error: {e}")
    
    def flush(self, key: Optional[str] = None):
        """Flush pending requests."""
        with self._lock:
            if key:
                timer = self._timers.pop(key, None)
                if timer:
                    timer.cancel()
                self._pending_requests.pop(key, None)
            else:
                for timer in self._timers.values():
                    timer.cancel()
                self._timers.clear()
                self._pending_requests.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get debounce statistics."""
        with self._lock:
            dedup_stats = self._deduplicator.get_stats()
            return {
                **dict(self._stats),
                "deduplicator": dedup_stats,
            }


class ApiDebounceAction(BaseAction):
    """API debounce action."""
    action_type = "api_debounce"
    display_name = "API防抖"
    description = "API请求防抖与去重"
    
    def __init__(self):
        super().__init__()
        self._debouncers: Dict[str, ApiRequestDebouncer] = {}
        self._lock = threading.Lock()
    
    def _get_debouncer(self, name: str, config: Optional[ApiDebounceConfig] = None) -> ApiRequestDebouncer:
        """Get or create debouncer."""
        with self._lock:
            if name not in self._debouncers:
                self._debouncers[name] = ApiRequestDebouncer(config)
            return self._debouncers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute debounced API operation."""
        try:
            name = params.get("name", "default")
            operation = params.get("operation")
            request = params.get("request", {})
            command = params.get("command", "schedule")
            
            config = ApiDebounceConfig(
                mode=ApiDebounceMode[params.get("mode", "request").upper()],
                delay=params.get("delay", 0.5),
                max_pending=params.get("max_pending", 100),
                cache_ttl=params.get("cache_ttl", 30.0),
                coalesce_by=params.get("coalesce_by"),
            )
            
            debouncer = self._get_debouncer(name, config)
            
            if command == "schedule" and operation:
                success = debouncer.schedule(request, operation)
                return ActionResult(success=success)
            
            elif command == "flush":
                debouncer.flush()
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = debouncer.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiDebounceAction error: {str(e)}")
