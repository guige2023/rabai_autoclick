"""Data Cache Invalidation Action Module for RabAI AutoClick.

Manages cache invalidation patterns including TTL-based expiration,
tag-based invalidation, and dependency graph-based purging.
"""

import time
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class InvalidationStrategy:
    """Cache invalidation strategies."""
    TTL = "ttl"
    LRU = "lru"
    TAG_BASED = "tag_based"
    DEPENDENCY = "dependency"
    MANUAL = "manual"


class DataCacheInvalidationAction(BaseAction):
    """Cache invalidation with multiple strategies.

    Handles cache invalidation using TTL, LRU, tag-based, and
    dependency-graph strategies. Supports cascading invalidation
    and selective tag removal.
    """
    action_type = "data_cache_invalidation"
    display_name = "缓存失效管理"
    description = "TTL/标签/依赖图缓存失效策略"

    _cache_store: Dict[str, Dict[str, Any]] = {}
    _tag_index: Dict[str, Set[str]] = defaultdict(set)
    _dependency_graph: Dict[str, Set[str]] = defaultdict(set)
    _access_order: List[str] = []

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cache invalidation operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'invalidate', 'invalidate_tag', 'invalidate_pattern',
                               'clear', 'register_tags', 'register_dependency', 'stats'
                - key: str (optional) - cache key
                - strategy: str (optional) - invalidation strategy
                - ttl: float (optional) - TTL in seconds
                - tags: list (optional) - tags for tag-based invalidation
                - pattern: str (optional) - key pattern to match
                - cascade: bool (optional) - cascade to dependents

        Returns:
            ActionResult with invalidation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'invalidate')

            if operation == 'invalidate':
                return self._invalidate_key(params, start_time)
            elif operation == 'invalidate_tag':
                return self._invalidate_tag(params, start_time)
            elif operation == 'invalidate_pattern':
                return self._invalidate_pattern(params, start_time)
            elif operation == 'clear':
                return self._clear_all(params, start_time)
            elif operation == 'register_tags':
                return self._register_tags(params, start_time)
            elif operation == 'register_dependency':
                return self._register_dependency(params, start_time)
            elif operation == 'stats':
                return self._get_stats(start_time)
            elif operation == 'evict_lru':
                return self._evict_lru(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cache invalidation failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _invalidate_key(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Invalidate a specific cache key."""
        key = params.get('key', '')
        cascade = params.get('cascade', False)

        if not key:
            return ActionResult(
                success=False,
                message="key is required",
                duration=time.time() - start_time
            )

        removed = 0
        if key in self._cache_store:
            del self._cache_store[key]
            removed = 1

        for tag, keys in self._tag_index.items():
            keys.discard(key)

        if cascade:
            removed += self._cascade_invalidate(key)

        return ActionResult(
            success=True,
            message=f"Invalidated key: {key}",
            data={
                'key': key,
                'removed_count': removed,
                'cascade': cascade
            },
            duration=time.time() - start_time
        )

    def _invalidate_tag(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Invalidate all keys with a specific tag."""
        tag = params.get('tag', '')
        tags = params.get('tags', [tag] if tag else [])

        if not tags:
            return ActionResult(
                success=False,
                message="tag or tags is required",
                duration=time.time() - start_time
            )

        total_removed = 0
        invalidated_tags = []

        for t in tags:
            keys_to_remove = list(self._tag_index.get(t, set()))
            for key in keys_to_remove:
                if key in self._cache_store:
                    del self._cache_store[key]
                    total_removed += 1
            self._tag_index.pop(t, None)
            invalidated_tags.append(t)

        return ActionResult(
            success=True,
            message=f"Invalidated {total_removed} keys with tags",
            data={
                'tags': invalidated_tags,
                'removed_count': total_removed
            },
            duration=time.time() - start_time
        )

    def _invalidate_pattern(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Invalidate keys matching a pattern."""
        pattern = params.get('pattern', '')

        if not pattern:
            return ActionResult(
                success=False,
                message="pattern is required",
                duration=time.time() - start_time
            )

        import re
        regex = pattern.replace('*', '.*').replace('?', '.')
        keys_to_remove = [
            k for k in self._cache_store.keys()
            if re.match(f'^{regex}$', k)
        ]

        for key in keys_to_remove:
            del self._cache_store[key]
            for tag_keys in self._tag_index.values():
                tag_keys.discard(key)

        return ActionResult(
            success=True,
            message=f"Pattern invalidated: {len(keys_to_remove)} keys",
            data={
                'pattern': pattern,
                'removed_count': len(keys_to_remove)
            },
            duration=time.time() - start_time
        )

    def _clear_all(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear all cache entries."""
        cleared = len(self._cache_store)
        self._cache_store.clear()
        self._tag_index.clear()
        self._access_order.clear()

        return ActionResult(
            success=True,
            message=f"Cache cleared: {cleared} entries",
            data={'cleared_count': cleared},
            duration=time.time() - start_time
        )

    def _register_tags(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register tags for a cache key."""
        key = params.get('key', '')
        tags = params.get('tags', [])

        if not key or not tags:
            return ActionResult(
                success=False,
                message="key and tags are required",
                duration=time.time() - start_time
            )

        for tag in tags:
            self._tag_index[tag].add(key)

        return ActionResult(
            success=True,
            message=f"Registered tags for {key}",
            data={
                'key': key,
                'tags': tags,
                'total_tags': len(self._tag_index)
            },
            duration=time.time() - start_time
        )

    def _register_dependency(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register a dependency relationship between keys."""
        parent_key = params.get('parent_key', '')
        child_key = params.get('child_key', '')

        if not parent_key or not child_key:
            return ActionResult(
                success=False,
                message="parent_key and child_key are required",
                duration=time.time() - start_time
            )

        self._dependency_graph[parent_key].add(child_key)

        return ActionResult(
            success=True,
            message=f"Registered dependency: {child_key} -> {parent_key}",
            data={
                'parent_key': parent_key,
                'child_key': child_key
            },
            duration=time.time() - start_time
        )

    def _evict_lru(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Evict least recently used entries."""
        max_entries = params.get('max_entries', 10)

        evicted = 0
        while len(self._cache_store) > max_entries and self._access_order:
            lru_key = self._access_order.pop(0)
            if lru_key in self._cache_store:
                del self._cache_store[lru_key]
                evicted += 1

        return ActionResult(
            success=True,
            message=f"LRU evicted: {evicted} entries",
            data={
                'evicted': evicted,
                'remaining': len(self._cache_store)
            },
            duration=time.time() - start_time
        )

    def _get_stats(self, start_time: float) -> ActionResult:
        """Get cache statistics."""
        expired = sum(
            1 for entry in self._cache_store.values()
            if entry.get('expires_at', float('inf')) < time.time()
        )

        return ActionResult(
            success=True,
            message="Cache statistics",
            data={
                'total_entries': len(self._cache_store),
                'expired_entries': expired,
                'total_tags': len(self._tag_index),
                'dependency_count': len(self._dependency_graph)
            },
            duration=time.time() - start_time
        )

    def _cascade_invalidate(self, key: str) -> int:
        """Cascade invalidation to dependent keys."""
        removed = 0
        if key in self._dependency_graph:
            for dependent in self._dependency_graph[key]:
                if dependent in self._cache_store:
                    del self._cache_store[dependent]
                    removed += 1

        return removed
