"""Element locator cache for UI automation.

Caches element locators (CSS selectors, XPath, etc.) with versioning
to handle dynamic UI element changes.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class LocatorType(Enum):
    """Types of element locators."""
    ID = auto()
    CSS = auto()
    XPATH = auto()
    NAME = auto()
    CLASS_NAME = auto()
    TAG_NAME = auto()
    LINK_TEXT = auto()
    PARTIAL_LINK_TEXT = auto()
    ACCESSIBILITY = auto()
    IMAGE = auto()


@dataclass
class ElementLocator:
    """A locator for finding a UI element.

    Attributes:
        locator_type: Type of locator.
        value: The locator value string.
        index: Index for multiple matches (0 = first).
        timeout: Search timeout in seconds.
        description: Human-readable description.
    """
    locator_type: LocatorType
    value: str
    index: int = 0
    timeout: float = 10.0
    description: str = ""

    def to_string(self) -> str:
        """Return a string representation."""
        prefix_map = {
            LocatorType.ID: "#",
            LocatorType.CSS: "",
            LocatorType.XPATH: "xpath=",
            LocatorType.NAME: "name=",
            LocatorType.CLASS_NAME: ".",
            LocatorType.TAG_NAME: "",
            LocatorType.LINK_TEXT: "text=",
            LocatorType.PARTIAL_LINK_TEXT: "partial=",
            LocatorType.ACCESSIBILITY: "ax=",
            LocatorType.IMAGE: "image=",
        }
        prefix = prefix_map.get(self.locator_type, "")
        suffix = f"[{self.index}]" if self.index > 0 else ""
        return f"{prefix}{self.value}{suffix}"

    def hash_key(self) -> str:
        """Return a unique hash key for this locator."""
        raw = f"{self.locator_type.name}:{self.value}:{self.index}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


@dataclass
class CachedLocator:
    """A cached locator with metadata."""
    locator: ElementLocator
    element_id: str = ""
    resolved: bool = False
    version: int = 1
    last_validated: float = 0.0
    hit_count: int = 0
    failure_count: int = 0


class ElementLocatorCache:
    """Caches element locators for faster lookup.

    Tracks locator validity, handles version increments when
    elements change, and provides fallback locator chains.
    """

    def __init__(self) -> None:
        """Initialize empty cache."""
        self._cache: dict[str, CachedLocator] = {}
        self._fallbacks: dict[str, list[ElementLocator]] = {}
        self._on_miss_callbacks: list[Callable[[str], Optional[ElementLocator]]] = []

    def register(
        self,
        key: str,
        locator: ElementLocator,
        element_id: str = "",
    ) -> None:
        """Register a locator in the cache."""
        cached = CachedLocator(
            locator=locator,
            element_id=element_id,
            last_validated=self._now(),
        )
        self._cache[key] = cached

    def get(self, key: str) -> Optional[ElementLocator]:
        """Get a cached locator."""
        cached = self._cache.get(key)
        if cached:
            cached.hit_count += 1
            return cached.locator
        return None

    def invalidate(self, key: str) -> bool:
        """Invalidate a cached locator."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def bump_version(self, key: str) -> bool:
        """Increment version and mark invalid when element changes."""
        cached = self._cache.get(key)
        if cached:
            cached.version += 1
            cached.resolved = False
            return True
        return False

    def set_fallbacks(self, key: str, locators: list[ElementLocator]) -> None:
        """Set fallback locators for a key."""
        self._fallbacks[key] = locators

    def get_with_fallbacks(self, key: str) -> Optional[ElementLocator]:
        """Get primary or fallback locator."""
        locator = self.get(key)
        if locator:
            return locator

        fallbacks = self._fallbacks.get(key, [])
        for fallback in fallbacks:
            fb_key = f"{key}:{fallback.hash_key()}"
            cached = self._cache.get(fb_key)
            if cached and cached.resolved:
                return fallback

        return None

    def on_miss(self, callback: Callable[[str], Optional[ElementLocator]]) -> None:
        """Register callback for cache misses."""
        self._on_miss_callbacks.append(callback)

    def handle_miss(self, key: str) -> Optional[ElementLocator]:
        """Handle a cache miss by calling registered callbacks."""
        for callback in self._on_miss_callbacks:
            result = callback(key)
            if result:
                self.register(key, result)
                return result
        return None

    def record_success(self, key: str) -> None:
        """Record successful resolution."""
        cached = self._cache.get(key)
        if cached:
            cached.resolved = True
            cached.last_validated = self._now()
            cached.failure_count = 0

    def record_failure(self, key: str) -> bool:
        """Record failed resolution. Returns True if should invalidate."""
        cached = self._cache.get(key)
        if cached:
            cached.failure_count += 1
            if cached.failure_count >= 3:
                self.invalidate(key)
                return True
        return False

    def clear(self) -> None:
        """Clear all cached locators."""
        self._cache.clear()

    @property
    def size(self) -> int:
        """Return number of cached locators."""
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """Return cache hit rate."""
        total = sum(c.hit_count for c in self._cache.values())
        resolved = sum(c.hit_count for c in self._cache.values() if c.resolved)
        if total == 0:
            return 0.0
        return resolved / total

    def _now(self) -> float:
        """Return current time."""
        import time
        return time.time()
