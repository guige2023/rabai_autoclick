"""Automation idempotency action module for RabAI AutoClick.

Provides idempotency guarantees for automation workflows:
- IdempotencyStore: Store and check idempotency keys
- IdempotencyGuard: Guard against duplicate executions
- RequestDeduplicator: Deduplicate requests
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IdempotencyScope(Enum):
    """Idempotency scopes."""
    REQUEST = "request"
    WORKFLOW = "workflow"
    SESSION = "session"


@dataclass
class IdempotencyConfig:
    """Configuration for idempotency."""
    scope: IdempotencyScope = IdempotencyScope.REQUEST
    ttl: float = 3600.0
    max_keys: int = 10000
    store_results: bool = True


class IdempotencyEntry:
    """Idempotency entry."""
    def __init__(self, key: str, result: Any, created_at: float):
        self.key = key
        self.result = result
        self.created_at = created_at
        self.access_count = 0


class IdempotencyStore:
    """Store for idempotency keys and results."""
    
    def __init__(self, config: IdempotencyConfig):
        self.config = config
        self._entries: Dict[str, IdempotencyEntry] = {}
        self._access_order: deque = deque(maxlen=config.max_keys)
        self._lock = threading.RLock()
        self._stats = {"total_checks": 0, "hits": 0, "misses": 0, "stored": 0, "evictions": 0}
    
    def _make_key(self, key: str, scope: str) -> str:
        """Create scope-prefixed key."""
        return f"{scope}:{key}"
    
    def _cleanup_expired(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, v in self._entries.items() if (now - v.created_at) > self.config.ttl]
        for k in expired:
            self._entries.pop(k, None)
            self._stats["evictions"] += 1
    
    def check(self, key: str, scope: str = "default") -> Tuple[bool, Optional[Any]]:
        """Check if key exists, return (is_duplicate, cached_result)."""
        with self._lock:
            self._stats["total_checks"] += 1
            self._cleanup_expired()
            
            full_key = self._make_key(key, scope)
            entry = self._entries.get(full_key)
            
            if entry:
                age = time.time() - entry.created_at
                if age <= self.config.ttl:
                    entry.access_count += 1
                    self._stats["hits"] += 1
                    return True, entry.result if self.config.store_results else None
                else:
                    del self._entries[full_key]
            
            self._stats["misses"] += 1
            return False, None
    
    def store(self, key: str, result: Any, scope: str = "default"):
        """Store result for key."""
        with self._lock:
            full_key = self._make_key(key, scope)
            
            if len(self._entries) >= self.config.max_keys:
                oldest_key = self._access_order.popleft()
                self._entries.pop(oldest_key, None)
                self._stats["evictions"] += 1
            
            entry = IdempotencyEntry(key=full_key, result=result, created_at=time.time())
            self._entries[full_key] = entry
            self._access_order.append(full_key)
            self._stats["stored"] += 1
    
    def generate_key(self, *parts: Any) -> str:
        """Generate idempotency key from parts."""
        combined = ":".join(str(p) for p in parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:32]
    
    def clear(self, scope: Optional[str] = None):
        """Clear entries."""
        with self._lock:
            if scope:
                to_remove = [k for k in self._entries if k.startswith(f"{scope}:")]
                for k in to_remove:
                    self._entries.pop(k, None)
            else:
                self._entries.clear()
                self._access_order.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        with self._lock:
            return {
                "stored_entries": len(self._entries),
                "max_entries": self.config.max_keys,
                **{k: v for k, v in self._stats.items()},
            }


class AutomationIdempotencyAction(BaseAction):
    """Automation idempotency action."""
    action_type = "automation_idempotency"
    display_name = "自动化幂等性"
    description = "自动化幂等性保证"
    
    def __init__(self):
        super().__init__()
        self._store: Optional[IdempotencyStore] = None
        self._lock = threading.Lock()
    
    def _get_store(self, params: Dict[str, Any]) -> IdempotencyStore:
        """Get or create idempotency store."""
        with self._lock:
            if self._store is None:
                config = IdempotencyConfig(
                    scope=IdempotencyScope[params.get("scope", "request").upper()],
                    ttl=params.get("ttl", 3600.0),
                    max_keys=params.get("max_keys", 10000),
                    store_results=params.get("store_results", True),
                )
                self._store = IdempotencyStore(config)
            return self._store
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute idempotency operation."""
        try:
            store = self._get_store(params)
            command = params.get("command", "check")
            scope = params.get("scope", "default")
            
            if command == "check":
                key = params.get("key")
                if not key:
                    return ActionResult(success=False, message="key required")
                
                is_duplicate, cached = store.check(key, scope)
                return ActionResult(success=True, data={"duplicate": is_duplicate, "cached_result": cached})
            
            elif command == "store":
                key = params.get("key")
                result = params.get("result")
                if not key:
                    return ActionResult(success=False, message="key required")
                
                store.store(key, result, scope)
                return ActionResult(success=True)
            
            elif command == "generate_key":
                parts = params.get("parts", [])
                key = store.generate_key(*parts)
                return ActionResult(success=True, data={"key": key})
            
            elif command == "clear":
                store.clear(scope)
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = store.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationIdempotencyAction error: {str(e)}")
