"""
API Cache Invalidation Action Module.

Provides intelligent cache invalidation strategies including
tag-based, pattern-based, and cascading invalidation.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class InvalidationStrategy(Enum):
    """Cache invalidation strategy types."""

    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    TTL_BASED = "ttl_based"
    TAG_BASED = "tag_based"
    PATTERN_BASED = "pattern_based"
    CASCADING = "cascading"
    LRU = "lru"


@dataclass
class CacheTag:
    """Represents a cache tag for group invalidation."""

    name: str
    keys: set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)


@dataclass
class InvalidationRule:
    """Rule for cache key invalidation."""

    pattern: str
    strategy: InvalidationStrategy
    tags: list[str] = field(default_factory=list)
    delay_seconds: float = 0.0


class APICacheInvalidationAction:
    """
    Manages cache invalidation with multiple strategies.

    Supports immediate, delayed, tag-based, pattern-based,
    and cascading invalidation approaches.

    Example:
        action = APICacheInvalidationAction()
        action.invalidate_tag("user_profile")
        action.invalidate_pattern("user:*")
        action.invalidate_cascading("product:123")
    """

    def __init__(
        self,
        default_strategy: InvalidationStrategy = InvalidationStrategy.TAG_BASED,
        enable_logging: bool = True,
    ) -> None:
        """
        Initialize cache invalidation action.

        Args:
            default_strategy: Default invalidation strategy to use.
            enable_logging: Whether to log invalidation events.
        """
        self.default_strategy = default_strategy
        self.enable_logging = enable_logging
        self._cache: dict[str, Any] = {}
        self._tags: dict[str, CacheTag] = {}
        self._rules: list[InvalidationRule] = []
        self._invalidation_history: list[dict[str, Any]] = []
        self._pending_invalidations: dict[str, float] = {}

    def set(self, key: str, value: Any, tags: Optional[list[str]] = None) -> None:
        """
        Set a cache entry with optional tags.

        Args:
            key: Cache key.
            value: Value to cache.
            tags: Optional list of tags for this entry.
        """
        self._cache[key] = value
        if tags:
            for tag_name in tags:
                if tag_name not in self._tags:
                    self._tags[tag_name] = CacheTag(name=tag_name)
                self._tags[tag_name].keys.add(key)

        if self.enable_logging:
            logger.debug(f"Cache set: key={key}, tags={tags}")

    def get(self, key: str) -> Optional[Any]:
        """
        Get a cache entry.

        Args:
            key: Cache key to retrieve.

        Returns:
            Cached value or None if not found.
        """
        return self._cache.get(key)

    def invalidate_tag(self, tag: str) -> int:
        """
        Invalidate all cache entries with a given tag.

        Args:
            tag: Tag name to invalidate.

        Returns:
            Number of entries invalidated.
        """
        if tag not in self._tags:
            return 0

        cache_tag = self._tags[tag]
        invalidated = len(cache_tag.keys)

        for key in list(cache_tag.keys):
            if key in self._cache:
                del self._cache[key]
            for other_tag in self._tags.values():
                other_tag.keys.discard(key)

        cache_tag.keys.clear()

        self._record_invalidation("tag", tag, invalidated)
        return invalidated

    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all cache entries matching a pattern.

        Args:
            pattern: Glob-style pattern (e.g., "user:*").

        Returns:
            Number of entries invalidated.
        """
        regex_pattern = self._glob_to_regex(pattern)
        regex = re.compile(regex_pattern)

        invalidated = 0
        keys_to_remove = [k for k in self._cache.keys() if regex.match(k)]

        for key in keys_to_remove:
            del self._cache[key]
            for cache_tag in self._tags.values():
                cache_tag.keys.discard(key)
            invalidated += 1

        self._record_invalidation("pattern", pattern, invalidated)
        return invalidated

    def invalidate_cascading(self, key: str) -> int:
        """
        Invalidate a key and all its dependent keys.

        Args:
            key: Primary cache key.

        Returns:
            Total number of keys invalidated.
        """
        invalidated = 0

        if key in self._cache:
            del self._cache[key]
            invalidated += 1

        cascade_patterns = [f"{key}:*", f"*:{key}", f"{key}.*"]
        for pattern in cascade_patterns:
            invalidated += self.invalidate_pattern(pattern)

        self._record_invalidation("cascading", key, invalidated)
        return invalidated

    def invalidate_immediate(self, key: str) -> bool:
        """
        Immediately invalidate a specific key.

        Args:
            key: Cache key to invalidate.

        Returns:
            True if key was found and removed.
        """
        if key in self._cache:
            del self._cache[key]
            for cache_tag in self._tags.values():
                cache_tag.keys.discard(key)
            self._record_invalidation("immediate", key, 1)
            return True
        return False

    def schedule_invalidation(self, key: str, delay_seconds: float) -> None:
        """
        Schedule a key for delayed invalidation.

        Args:
            key: Cache key to invalidate.
            delay_seconds: Delay before invalidation.
        """
        self._pending_invalidations[key] = time.time() + delay_seconds
        if self.enable_logging:
            logger.debug(f"Scheduled invalidation: key={key}, delay={delay_seconds}s")

    def process_pending_invalidations(self) -> int:
        """
        Process all pending delayed invalidations.

        Returns:
            Number of keys invalidated.
        """
        now = time.time()
        invalidated = 0
        keys_to_remove = []

        for key, invalidate_at in self._pending_invalidations.items():
            if now >= invalidate_at:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._pending_invalidations[key]
            if self.invalidate_immediate(key):
                invalidated += 1

        return invalidated

    def add_rule(self, rule: InvalidationRule) -> None:
        """
        Add an invalidation rule.

        Args:
            rule: InvalidationRule to add.
        """
        self._rules.append(rule)
        logger.info(f"Added invalidation rule: pattern={rule.pattern}, strategy={rule.strategy}")

    def apply_rules(self, key: str) -> bool:
        """
        Apply all matching rules to a cache key.

        Args:
            key: Cache key to evaluate.

        Returns:
            True if any rule was applied.
        """
        applied = False
        for rule in self._rules:
            if re.match(self._glob_to_regex(rule.pattern), key):
                if rule.strategy == InvalidationStrategy.TAG_BASED:
                    for tag in rule.tags:
                        self.invalidate_tag(tag)
                    applied = True
                elif rule.strategy == InvalidationStrategy.PATTERN_BASED:
                    self.invalidate_pattern(key)
                    applied = True

        return applied

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache invalidation statistics.

        Returns:
            Dictionary with stats including invalidation history.
        """
        return {
            "total_entries": len(self._cache),
            "total_tags": len(self._tags),
            "total_rules": len(self._rules),
            "pending_invalidations": len(self._pending_invalidations),
            "history_count": len(self._invalidation_history),
            "recent_invalidations": self._invalidation_history[-10:],
        }

    def clear(self) -> None:
        """Clear all cache entries and tags."""
        self._cache.clear()
        self._tags.clear()
        self._pending_invalidations.clear()
        logger.info("Cache cleared")

    def _glob_to_regex(self, pattern: str) -> str:
        """Convert glob pattern to regex pattern."""
        regex = pattern.replace(".", r"\.")
        regex = regex.replace("*", ".*")
        regex = regex.replace("?", ".")
        return f"^{regex}$"

    def _record_invalidation(self, strategy: str, key: str, count: int) -> None:
        """Record invalidation event in history."""
        self._invalidation_history.append({
            "strategy": strategy,
            "key": key,
            "count": count,
            "timestamp": time.time(),
        })
        if len(self._invalidation_history) > 1000:
            self._invalidation_history = self._invalidation_history[-500:]

        if self.enable_logging:
            logger.info(f"Invalidated: strategy={strategy}, key={key}, count={count}")
