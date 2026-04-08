"""
Cache invalidation module for managing distributed cache invalidation strategies.

Supports tag-based, pattern-based, and dependency-based cache invalidation.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class InvalidationStrategy(Enum):
    """Cache invalidation strategies."""
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    BATCHED = "batched"
    CASCADING = "cascading"


@dataclass
class CacheKey:
    """A cache key with metadata."""
    key: str
    hash: str
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    ttl_seconds: Optional[float] = None


@dataclass
class InvalidationRule:
    """Rule for cache invalidation."""
    id: str
    pattern: str
    strategy: InvalidationStrategy
    tags: list[str] = field(default_factory=list)
    delay_seconds: float = 0
    batch_size: int = 100
    enabled: bool = True


@dataclass
class InvalidationEvent:
    """An invalidation event."""
    id: str
    keys: list[str]
    reason: str
    triggered_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


class CacheInvalidator:
    """
    Cache invalidation service.

    Supports tag-based, pattern-based, and dependency-based
    cache invalidation strategies.
    """

    def __init__(self):
        self._keys: dict[str, CacheKey] = {}
        self._keys_by_tag: dict[str, set[str]] = defaultdict(set)
        self._keys_by_dependency: dict[str, set[str]] = defaultdict(set)
        self._rules: dict[str, InvalidationRule] = {}
        self._events: list[InvalidationEvent] = []
        self._pending_invalidations: dict[str, float] = {}

    def register_key(
        self,
        key: str,
        tags: Optional[list[str]] = None,
        dependencies: Optional[list[str]] = None,
        ttl_seconds: Optional[float] = None,
    ) -> CacheKey:
        """Register a cache key with metadata."""
        key_hash = hashlib.md5(key.encode()).hexdigest()

        cache_key = CacheKey(
            key=key,
            hash=key_hash,
            tags=tags or [],
            dependencies=dependencies or [],
            ttl_seconds=ttl_seconds,
        )

        self._keys[key] = cache_key

        for tag in cache_key.tags:
            self._keys_by_tag[tag].add(key)

        for dep in cache_key.dependencies:
            self._keys_by_dependency[dep].add(key)

        return cache_key

    def invalidate_key(self, key: str) -> bool:
        """Invalidate a single cache key."""
        if key in self._keys:
            self._keys.pop(key)
            return True
        return False

    def invalidate_keys(self, keys: list[str]) -> int:
        """Invalidate multiple cache keys."""
        count = 0
        for key in keys:
            if self.invalidate_key(key):
                count += 1
        return count

    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern."""
        import re

        count = 0
        keys_to_remove = []

        for key in self._keys:
            if re.match(pattern, key):
                keys_to_remove.append(key)

        for key in keys_to_remove:
            self.invalidate_key(key)
            count += 1

        return count

    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all keys with a specific tag."""
        keys = list(self._keys_by_tag.get(tag, set()))
        count = self.invalidate_keys(keys)

        for key in keys:
            cache_key = self._keys.get(key)
            if cache_key and tag in cache_key.tags:
                cache_key.tags.remove(tag)

        return count

    def invalidate_by_dependency(self, dependency: str) -> int:
        """Invalidate all keys that depend on a given key."""
        keys = list(self._keys_by_dependency.get(dependency, set()))
        count = self.invalidate_keys(keys)
        return count

    def add_rule(
        self,
        pattern: str,
        strategy: InvalidationStrategy,
        tags: Optional[list[str]] = None,
        delay_seconds: float = 0,
        batch_size: int = 100,
    ) -> InvalidationRule:
        """Add an invalidation rule."""
        rule = InvalidationRule(
            id=str(uuid.uuid4())[:8],
            pattern=pattern,
            strategy=strategy,
            tags=tags or [],
            delay_seconds=delay_seconds,
            batch_size=batch_size,
        )
        self._rules[rule.id] = rule
        return rule

    def trigger_rule(self, rule_id: str) -> InvalidationEvent:
        """Trigger an invalidation rule."""
        rule = self._rules.get(rule_id)
        if not rule:
            raise ValueError(f"Rule not found: {rule_id}")

        event = InvalidationEvent(
            id=str(uuid.uuid4())[:8],
            keys=[],
            reason=f"Rule triggered: {rule.pattern}",
        )

        keys = []
        for key in self._keys:
            import re
            if re.match(rule.pattern, key):
                keys.append(key)

        event.keys = keys
        event.triggered_at = time.time()

        if rule.strategy == InvalidationStrategy.IMMEDIATE:
            self.invalidate_keys(keys)
            event.completed_at = time.time()

        elif rule.strategy == InvalidationStrategy.DELAYED:
            for key in keys:
                self._pending_invalidations[key] = time.time() + rule.delay_seconds

        elif rule.strategy == InvalidationStrategy.BATCHED:
            for i in range(0, len(keys), rule.batch_size):
                batch = keys[i:i + rule.batch_size]
                self._pending_invalidations[f"batch_{i}"] = (time.time() + rule.delay_seconds + i)

        self._events.append(event)
        return event

    def process_pending_invalidations(self) -> int:
        """Process pending delayed invalidations."""
        now = time.time()
        count = 0

        keys_to_invalidate = []

        for key, trigger_time in list(self._pending_invalidations.items()):
            if now >= trigger_time:
                keys_to_invalidate.append(key)
                del self._pending_invalidations[key]

        for key in keys_to_invalidate:
            if key.startswith("batch_"):
                continue
            self.invalidate_key(key)
            count += 1

        return count

    def get_dependent_keys(self, key: str) -> list[str]:
        """Get all keys that depend on a given key."""
        return list(self._keys_by_dependency.get(key, set()))

    def get_keys_by_tags(self, tags: list[str]) -> list[str]:
        """Get all keys that have any of the given tags."""
        result = set()
        for tag in tags:
            result.update(self._keys_by_tag.get(tag, set()))
        return list(result)

    def list_keys(
        self,
        tag: Optional[str] = None,
        pattern: Optional[str] = None,
        limit: int = 100,
    ) -> list[CacheKey]:
        """List cache keys with optional filters."""
        keys = list(self._keys.values())

        if tag:
            keys = [k for k in keys if tag in k.tags]

        if pattern:
            import re
            keys = [k for k in keys if re.match(pattern, k.key)]

        return keys[:limit]

    def get_stats(self) -> dict:
        """Get invalidation statistics."""
        return {
            "total_keys": len(self._keys),
            "total_tags": len(self._keys_by_tag),
            "total_dependencies": len(self._keys_by_dependency),
            "total_rules": len(self._rules),
            "pending_invalidations": len(self._pending_invalidations),
            "total_events": len(self._events),
        }
