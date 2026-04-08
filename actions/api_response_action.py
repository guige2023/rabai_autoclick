"""
API Response Action - Transforms and shapes API responses.

This module provides response transformation, pagination handling,
field selection, and response caching for API interactions.
"""

from __future__ import annotations

import time
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum


class CacheMode(Enum):
    """Response caching behavior."""
    NO_CACHE = "no_cache"
    MEMORY = "memory"
    DISK = "disk"


@dataclass
class FieldSelector:
    """Specification for selecting and transforming fields."""
    include: list[str] | None = None
    exclude: list[str] | None = None
    rename: dict[str, str] = field(default_factory=dict)
    transform: dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    default: Any = None


@dataclass
class ResponseConfig:
    """Configuration for API response handling."""
    cache_mode: CacheMode = CacheMode.NO_CACHE
    cache_ttl: float = 300.0
    cache_prefix: str = "api_response"
    field_selector: FieldSelector | None = None
    max_depth: int = 10
    timeout: float = 30.0


@dataclass
class TransformedResponse:
    """A transformed API response."""
    data: Any
    status_code: int
    headers: dict[str, str]
    cached: bool = False
    cached_at: float | None = None
    transformed_fields: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class APIResponseCache:
    """In-memory cache for API responses."""
    
    def __init__(self, ttl: float = 300.0) -> None:
        self.ttl = ttl
        self._cache: dict[str, tuple[Any, float]] = {}
    
    def _make_key(self, prefix: str, data: Any) -> str:
        """Generate cache key."""
        content = json.dumps(data, sort_keys=True, default=str)
        return f"{prefix}:{hashlib.sha256(content.encode()).hexdigest()}"
    
    def get(self, key: str) -> Any | None:
        """Get cached value if not expired."""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if (time.time() - timestamp) < self.ttl:
                return value
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        """Set cached value."""
        self._cache[key] = (value, time.time())
    
    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()


class ResponseTransformer:
    """
    Transforms API responses based on field selection and renaming.
    
    Example:
        transformer = ResponseTransformer()
        result = transformer.transform(
            {"users": [{"id": 1, "name": "John"}]},
            FieldSelector(include=["users.id", "users.name"])
        )
    """
    
    def __init__(self, max_depth: int = 10) -> None:
        self.max_depth = max_depth
    
    def transform(
        self,
        data: Any,
        selector: FieldSelector | None = None,
    ) -> Any:
        """Transform data based on field selector."""
        if selector is None:
            return data
        
        if selector.include:
            return self._select_fields(data, selector.include, selector)
        elif selector.exclude:
            return self._exclude_fields(data, selector.exclude)
        
        return data
    
    def _select_fields(
        self,
        data: Any,
        fields: list[str],
        selector: FieldSelector,
    ) -> Any:
        """Select only the specified fields."""
        result = {}
        for field_path in fields:
            value = self._get_nested(data, field_path)
            if value is not None or selector.default is not None:
                renamed = selector.rename.get(field_path, field_path)
                result[renamed] = value if value is not None else selector.default
        
        return result
    
    def _exclude_fields(
        self,
        data: Any,
        fields: list[str],
    ) -> Any:
        """Exclude the specified fields."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key not in fields:
                    result[key] = self._exclude_fields(value, fields)
            return result
        elif isinstance(data, list):
            return [self._exclude_fields(item, fields) for item in data]
        return data
    
    def _get_nested(self, data: Any, path: str) -> Any:
        """Get value from nested dict/list using dot notation."""
        keys = path.split(".")
        current = data
        
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list):
                try:
                    index = int(key)
                    current = current[index] if 0 <= index < len(current) else None
                except ValueError:
                    return None
            else:
                return None
            
            if current is None:
                return None
        
        return current


class APIResponseAction:
    """
    Handles API response transformation, caching, and field selection.
    
    Example:
        action = APIResponseAction(
            ResponseConfig(cache_mode=CacheMode.MEMORY)
        )
        result = await action.handle_response(
            api_response,
            field_selector=FieldSelector(include=["data.id", "data.name"])
        )
    """
    
    def __init__(self, config: ResponseConfig | None = None) -> None:
        self.config = config or ResponseConfig()
        self._cache = APIResponseCache(self.config.cache_ttl)
        self._transformer = ResponseTransformer(self.config.max_depth)
    
    async def handle_response(
        self,
        response: tuple[int, Any, dict[str, str]],
        selector: FieldSelector | None = None,
        use_cache: bool = True,
        cache_key: str | None = None,
    ) -> TransformedResponse:
        """
        Handle and transform an API response.
        
        Args:
            response: Tuple of (status_code, data, headers)
            selector: Field selection and transformation config
            use_cache: Whether to check/use cache
            cache_key: Custom cache key
            
        Returns:
            TransformedResponse with processed data
        """
        status_code, data, headers = response
        
        cache_key = cache_key or self._cache.make_key(
            self.config.cache_prefix,
            {"status": status_code, "data": data}
        ) if hasattr(self._cache, 'make_key') else str(hash(str(data)))
        
        if use_cache and self.config.cache_mode != CacheMode.NO_CACHE:
            cached = self._cache.get(cache_key)
            if cached:
                return TransformedResponse(
                    data=cached,
                    status_code=status_code,
                    headers=headers,
                    cached=True,
                    cached_at=time.time(),
                )
        
        transformed = self._transformer.transform(data, selector or self.config.field_selector)
        
        transformed_fields = []
        if selector:
            if selector.include:
                transformed_fields = selector.include
            elif selector.exclude:
                transformed_fields = list(set(self._flatten_keys(data)) - set(selector.exclude))
        
        if use_cache and self.config.cache_mode != CacheMode.NO_CACHE:
            self._cache.set(cache_key, transformed)
        
        return TransformedResponse(
            data=transformed,
            status_code=status_code,
            headers=headers,
            cached=False,
            transformed_fields=transformed_fields,
        )
    
    def _flatten_keys(self, data: Any, prefix: str = "") -> list[str]:
        """Flatten all keys from nested structure."""
        keys = []
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                keys.append(full_key)
                keys.extend(self._flatten_keys(value, full_key))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                full_key = f"{prefix}[{i}]"
                keys.extend(self._flatten_keys(item, full_key))
        return keys
    
    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()
    
    def get_cache_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        return {"entries": len(self._cache._cache)}


# Export public API
__all__ = [
    "CacheMode",
    "FieldSelector",
    "ResponseConfig",
    "TransformedResponse",
    "APIResponseCache",
    "ResponseTransformer",
    "APIResponseAction",
]
