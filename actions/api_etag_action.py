"""
API ETag Action Module.

Implements ETag generation and validation for API caching,
providing optimistic concurrency control and cache invalidation.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class ETagStrength(Enum):
    """ETag strength types."""
    STRONG = auto()
    WEAK = auto()


@dataclass
class ETag:
    """Represents an ETag."""
    value: str
    strength: ETagStrength
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "value": self.value,
            "strength": self.strength.name,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class ETagValidationResult:
    """Result of ETag validation."""
    is_valid: bool
    matched: bool
    etag: Optional[ETag] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "matched": self.matched,
            "etag": self.etag.to_dict() if self.etag else None,
            "error": self.error,
        }


@dataclass
class CacheEntry:
    """A cache entry with ETag."""
    key: str
    value: Any
    etag: ETag
    created_at: datetime
    expires_at: Optional[datetime] = None
    hit_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.now)

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


class ApiETagAction:
    """
    Handles ETag generation, validation, and caching.

    This action implements ETag-based caching and conditional request
    handling for API responses, enabling efficient cache validation
    and optimistic concurrency control.

    Example:
        >>> action = ApiETagAction()
        >>> etag = action.generate_etag({"user": {"id": 123, "name": "Alice"}})
        >>> result = action.validate_etag(etag, "W/etag123")
        >>> print(result.matched)
        True
    """

    def __init__(
        self,
        strong_etag_enabled: bool = True,
        weak_etag_prefix: str = "W/",
        default_ttl_seconds: Optional[float] = 3600,
    ):
        """
        Initialize the API ETag Action.

        Args:
            strong_etag_enabled: Whether to generate strong ETags.
            weak_etag_prefix: Prefix for weak ETags.
            default_ttl_seconds: Default cache TTL.
        """
        self.strong_etag_enabled = strong_etag_enabled
        self.weak_etag_prefix = weak_etag_prefix
        self.default_ttl = default_ttl_seconds
        self._cache: Dict[str, CacheEntry] = {}
        self._etag_store: Dict[str, ETag] = {}

    def generate_etag(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        weak: bool = False,
    ) -> ETag:
        """
        Generate an ETag for data.

        Args:
            data: Data to generate ETag for.
            metadata: Optional metadata to include.
            weak: Whether to generate a weak ETag.

        Returns:
            Generated ETag.
        """
        if isinstance(data, (dict, list)):
            content = json.dumps(data, sort_keys=True, default=str)
        else:
            content = str(data)

        content_bytes = content.encode("utf-8")

        if not self.strong_etag_enabled or weak:
            hash_value = hashlib.md5(content_bytes).hexdigest()
            value = f'{self.weak_etag_prefix}"{hash_value}"'
            strength = ETagStrength.WEAK
        else:
            hash_value = hashlib.sha256(content_bytes).hexdigest()
            value = f'"{hash_value}"'
            strength = ETagStrength.STRONG

        etag = ETag(
            value=value,
            strength=strength,
            metadata=metadata or {},
        )

        self._etag_store[value] = etag
        return etag

    def generate_etag_for_file(
        self,
        content: bytes,
        weak: bool = False,
    ) -> ETag:
        """
        Generate an ETag for file content.

        Args:
            content: File content bytes.
            weak: Whether to generate weak ETag.

        Returns:
            Generated ETag.
        """
        if weak:
            hash_value = hashlib.md5(content).hexdigest()
            value = f'{self.weak_etag_prefix}"{hash_value}"'
            strength = ETagStrength.WEAK
        else:
            hash_value = hashlib.sha256(content).hexdigest()
            value = f'"{hash_value}"'
            strength = ETagStrength.STRONG

        etag = ETag(value=value, strength=strength)
        self._etag_store[value] = etag
        return etag

    def parse_etag(self, etag_str: str) -> Optional[ETag]:
        """
        Parse an ETag from a string.

        Args:
            etag_str: ETag string.

        Returns:
            Parsed ETag or None.
        """
        if etag_str in self._etag_store:
            return self._etag_store[etag_str]

        is_weak = etag_str.startswith(self.weak_etag_prefix)
        strength = ETagStrength.WEAK if is_weak else ETagStrength.STRONG

        return ETag(value=etag_str, strength=strength)

    def validate_etag(
        self,
        etag: ETag,
        if_match: str,
    ) -> ETagValidationResult:
        """
        Validate ETag against If-Match header.

        Args:
            etag: Current ETag.
            if_match: If-Match header value.

        Returns:
            ETagValidationResult.
        """
        if if_match == "*":
            return ETagValidationResult(is_valid=True, matched=True, etag=etag)

        etags = self._parse_etag_list(if_match)

        for etag_str in etags:
            parsed = self.parse_etag(etag_str)
            if parsed and self._etag_match(etag, parsed):
                return ETagValidationResult(is_valid=True, matched=True, etag=etag)

        return ETagValidationResult(
            is_valid=True,
            matched=False,
            etag=etag,
            error="ETag does not match",
        )

    def validate_if_none_match(
        self,
        etag: ETag,
        if_none_match: str,
    ) -> ETagValidationResult:
        """
        Validate ETag against If-None-Match header.

        Args:
            etag: Current ETag.
            if_none_match: If-None-Match header value.

        Returns:
            ETagValidationResult.
        """
        if if_none_match == "*":
            return ETagValidationResult(is_valid=True, matched=True, etag=etag)

        etags = self._parse_etag_list(if_none_match)

        for etag_str in etags:
            parsed = self.parse_etag(etag_str)
            if parsed and self._etag_match(etag, parsed):
                return ETagValidationResult(
                    is_valid=True,
                    matched=False,
                    etag=etag,
                    error="ETag matches - return 304",
                )

        return ETagValidationResult(is_valid=True, matched=True, etag=etag)

    def _parse_etag_list(self, etag_list: str) -> List[str]:
        """Parse comma-separated ETag list."""
        return [e.strip() for e in etag_list.split(",")]

    def _etag_match(self, etag1: ETag, etag2: ETag) -> bool:
        """Check if two ETags match."""
        if etag1.value == etag2.value:
            return True

        if etag1.strength == ETagStrength.WEAK and etag2.strength == ETagStrength.WEAK:
            return etag1.value == etag2.value

        return False

    def cache_put(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """
        Put a value in cache with ETag.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Optional TTL in seconds.
            metadata: Optional metadata.

        Returns:
            Created CacheEntry.
        """
        data_copy = json.loads(json.dumps(value, default=str))

        etag = self.generate_etag(data_copy, metadata=metadata)

        entry = CacheEntry(
            key=key,
            value=value,
            etag=etag,
            created_at=datetime.now(timezone.utc),
            expires_at=(
                datetime.now(timezone.utc).timestamp() + ttl
                if ttl else None
            ) if ttl else None,
        )

        self._cache[key] = entry
        return entry

    def cache_get(
        self,
        key: str,
        update_hit_count: bool = True,
    ) -> Optional[CacheEntry]:
        """
        Get a cached value.

        Args:
            key: Cache key.
            update_hit_count: Whether to update hit count.

        Returns:
            CacheEntry if found and not expired.
        """
        entry = self._cache.get(key)

        if entry is None:
            return None

        if entry.is_expired():
            del self._cache[key]
            return None

        if update_hit_count:
            entry.hit_count += 1
            entry.last_accessed = datetime.now(timezone.utc)

        return entry

    def cache_get_with_validation(
        self,
        key: str,
        if_none_match: Optional[str] = None,
    ) -> Tuple[Optional[Any], Optional[ETag], bool]:
        """
        Get cached value with ETag validation.

        Args:
            key: Cache key.
            if_none_match: Optional If-None-Match header.

        Returns:
            Tuple of (value, etag, should_return_304).
        """
        entry = self.cache_get(key)

        if entry is None:
            return None, None, False

        if if_none_match:
            result = self.validate_if_none_match(entry.etag, if_none_match)
            if not result.matched:
                return entry.value, entry.etag, True

        return entry.value, entry.etag, False

    def cache_invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def cache_clear(self) -> int:
        """Clear all cache entries."""
        count = len(self._cache)
        self._cache.clear()
        return count

    def cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(e.hit_count for e in self._cache.values())
        expired = sum(1 for e in self._cache.values() if e.is_expired())

        return {
            "entry_count": len(self._cache),
            "total_hits": total_hits,
            "expired_entries": expired,
            "etag_count": len(self._etag_store),
        }

    def get_weak_etag_hash(self, data: Any) -> str:
        """Get weak ETag hash for data without creating full ETag."""
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(content.encode()).hexdigest()

    def strong_etag_from_weak(self, weak_etag: ETag) -> ETag:
        """
        Convert a weak ETag to strong by recalculating hash.

        Args:
            weak_etag: Weak ETag to convert.

        Returns:
            Strong ETag.
        """
        if weak_etag.strength == ETagStrength.STRONG:
            return weak_etag

        value = weak_etag.value
        if value.startswith(self.weak_etag_prefix):
            value = value[len(self.weak_etag_prefix):]

        return ETag(
            value=f'"{value}"',
            strength=ETagStrength.STRONG,
            metadata=weak_etag.metadata,
        )


def create_etag_action(**kwargs) -> ApiETagAction:
    """Factory function to create an ApiETagAction."""
    return ApiETagAction(**kwargs)
