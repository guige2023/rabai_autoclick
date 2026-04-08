"""Cache Aside Action Module.

Provides cache-aside pattern for
data caching.
"""

import time
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    ttl_seconds: float = 0


class CacheAsideManager:
    """Manages cache-aside pattern."""

    def __init__(self):
        self._cache: Dict[str, CacheEntry] = {}

    def get_or_load(
        self,
        key: str,
        loader: Callable,
        ttl_seconds: float = 300
    ) -> Any:
        """Get from cache or load."""
        entry = self._cache.get(key)

        if entry:
            if entry.ttl_seconds > 0:
                if time.time() - entry.created_at < entry.ttl_seconds:
                    return entry.value
            else:
                return entry.value

        value = loader()
        self.set(key, value, ttl_seconds)
        return value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float = 0
    ) -> None:
        """Set cache entry."""
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            ttl_seconds=ttl_seconds
        )

    def get(self, key: str) -> Optional[Any]:
        """Get cache entry."""
        entry = self._cache.get(key)
        if not entry:
            return None

        if entry.ttl_seconds > 0:
            if time.time() - entry.created_at >= entry.ttl_seconds:
                del self._cache[key]
                return None

        return entry.value

    def invalidate(self, key: str) -> bool:
        """Invalidate cache entry."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all cache."""
        self._cache.clear()


class CacheAsideAction(BaseAction):
    """Action for cache-aside operations."""

    def __init__(self):
        super().__init__("cache_aside")
        self._manager = CacheAsideManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute cache-aside action."""
        try:
            operation = params.get("operation", "get_or_load")

            if operation == "get_or_load":
                return self._get_or_load(params)
            elif operation == "set":
                return self._set(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "invalidate":
                return self._invalidate(params)
            elif operation == "clear":
                return self._clear(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get_or_load(self, params: Dict) -> ActionResult:
        """Get or load."""
        def default_loader():
            return None

        result = self._manager.get_or_load(
            key=params.get("key", ""),
            loader=params.get("loader") or default_loader,
            ttl_seconds=params.get("ttl_seconds", 300)
        )
        return ActionResult(success=True, data={"value": result})

    def _set(self, params: Dict) -> ActionResult:
        """Set cache."""
        self._manager.set(
            key=params.get("key", ""),
            value=params.get("value"),
            ttl_seconds=params.get("ttl_seconds", 0)
        )
        return ActionResult(success=True)

    def _get(self, params: Dict) -> ActionResult:
        """Get cache."""
        value = self._manager.get(params.get("key", ""))
        return ActionResult(success=value is not None, data={"value": value})

    def _invalidate(self, params: Dict) -> ActionResult:
        """Invalidate."""
        success = self._manager.invalidate(params.get("key", ""))
        return ActionResult(success=success)

    def _clear(self, params: Dict) -> ActionResult:
        """Clear cache."""
        self._manager.clear()
        return ActionResult(success=True)
