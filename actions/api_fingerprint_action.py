"""API Fingerprint Action Module.

Provides API fingerprinting, request/response hashing,
and deduplication for caching and comparison.
"""

from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import time
from datetime import datetime


class HashAlgorithm(Enum):
    """Supported hashing algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    BLAKE2B = "blake2b"


@dataclass
class Fingerprint:
    """Request/response fingerprint."""
    hash_value: str
    algorithm: HashAlgorithm
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheEntry:
    """Cached response entry."""
    fingerprint: Fingerprint
    response: Any
    created_at: datetime
    accessed_at: datetime
    hit_count: int = 0
    ttl_seconds: Optional[int] = None

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl_seconds


@dataclass
class FingerprintConfig:
    """Configuration for fingerprint generation."""
    algorithm: HashAlgorithm = HashAlgorithm.SHA256
    include_headers: bool = False
    header_whitelist: List[str] = field(default_factory=list)
    include_body: bool = True
    include_method: bool = True
    include_path: bool = True
    include_query: bool = True
    normalize_values: bool = False


class RequestFingerprinter:
    """Generates fingerprints for requests."""

    def __init__(self, config: Optional[FingerprintConfig] = None):
        self.config = config or FingerprintConfig()

    def fingerprint(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, Any]] = None,
    ) -> Fingerprint:
        """Generate fingerprint for a request."""
        components = []

        if self.config.include_method:
            components.append(method.upper())

        if self.config.include_path:
            components.append(path)

        if self.config.include_query and query_params:
            normalized_query = self._normalize_query(query_params)
            components.append(normalized_query)

        if self.config.include_body and body:
            if self.config.normalize_values:
                body = self._normalize_body(body)
            components.append(body.decode('utf-8', errors='replace'))

        if self.config.include_headers and headers:
            filtered_headers = self._filter_headers(headers)
            components.append(json.dumps(filtered_headers, sort_keys=True))

        content = "|".join(str(c) for c in components)
        hash_value = self._hash(content.encode('utf-8'))

        return Fingerprint(
            hash_value=hash_value,
            algorithm=self.config.algorithm,
            metadata={
                "method": method,
                "path": path,
            }
        )

    def _hash(self, data: bytes) -> str:
        """Hash data using configured algorithm."""
        algo = self.config.algorithm
        if algo == HashAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif algo == HashAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif algo == HashAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        elif algo == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b(data).hexdigest()
        return hashlib.sha256(data).hexdigest()

    def _normalize_query(self, query: Dict[str, Any]) -> str:
        """Normalize query parameters for consistent hashing."""
        normalized = {}
        for key, value in sorted(query.items()):
            if self.config.normalize_values:
                if isinstance(value, str):
                    value = value.lower().strip()
            normalized[key] = value
        return json.dumps(normalized, sort_keys=True)

    def _normalize_body(self, body: bytes) -> bytes:
        """Normalize request body."""
        try:
            data = json.loads(body)
            normalized = self._normalize_dict(data)
            return json.dumps(normalized, sort_keys=True).encode('utf-8')
        except json.JSONDecodeError:
            return body

    def _normalize_dict(self, data: Any) -> Any:
        """Recursively normalize dictionary values."""
        if isinstance(data, dict):
            return {k: self._normalize_dict(v) for k, v in sorted(data.items())}
        elif isinstance(data, list):
            return [self._normalize_dict(item) for item in data]
        elif isinstance(data, str):
            return data.lower().strip()
        return data

    def _filter_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Filter headers based on whitelist."""
        if not self.config.header_whitelist:
            return headers
        return {
            k: v for k, v in headers.items()
            if k.lower() in [h.lower() for h in self.config.header_whitelist]
        }


class ResponseFingerprinter:
    """Generates fingerprints for responses."""

    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.algorithm = algorithm

    def fingerprint(
        self,
        body: bytes,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
    ) -> Fingerprint:
        """Generate fingerprint for a response."""
        components = [str(status_code)]

        if headers:
            content_type = headers.get('Content-Type', '')
            components.append(content_type)

        components.append(body.decode('utf-8', errors='replace'))

        content = "|".join(components)
        hash_value = self._hash(content.encode('utf-8'))

        return Fingerprint(
            hash_value=hash_value,
            algorithm=self.algorithm,
            metadata={
                "status_code": status_code,
            }
        )

    def _hash(self, data: bytes) -> str:
        """Hash data."""
        if self.algorithm == HashAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b(data).hexdigest()
        return hashlib.sha256(data).hexdigest()


class FingerprintCache:
    """Cache for storing response fingerprints."""

    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size

    def get(self, fingerprint: Fingerprint) -> Optional[Any]:
        """Get cached response by fingerprint."""
        entry = self._cache.get(fingerprint.hash_value)
        if not entry:
            return None

        if entry.is_expired():
            del self._cache[fingerprint.hash_value]
            return None

        entry.accessed_at = datetime.now()
        entry.hit_count += 1
        return entry.response

    def set(
        self,
        fingerprint: Fingerprint,
        response: Any,
        ttl_seconds: Optional[int] = None,
    ):
        """Store response with fingerprint."""
        if len(self._cache) >= self._max_size:
            self._evict_oldest()

        self._cache[fingerprint.hash_value] = CacheEntry(
            fingerprint=fingerprint,
            response=response,
            created_at=datetime.now(),
            accessed_at=datetime.now(),
            ttl_seconds=ttl_seconds,
        )

    def invalidate(self, fingerprint: Fingerprint):
        """Invalidate a cache entry."""
        if fingerprint.hash_value in self._cache:
            del self._cache[fingerprint.hash_value]

    def invalidate_pattern(self, path_pattern: str):
        """Invalidate entries matching path pattern."""
        keys_to_delete = [
            k for k, v in self._cache.items()
            if path_pattern in v.fingerprint.metadata.get('path', '')
        ]
        for key in keys_to_delete:
            del self._cache[key]

    def _evict_oldest(self):
        """Evict least recently accessed entry."""
        if not self._cache:
            return
        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].accessed_at
        )
        del self._cache[oldest_key]

    def clear(self):
        """Clear all cache entries."""
        self._cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(e.hit_count for e in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "total_hits": total_hits,
            "utilization": len(self._cache) / self._max_size * 100,
        }


class Deduplicator:
    """Deduplicates requests based on fingerprints."""

    def __init__(self, cache: Optional[FingerprintCache] = None):
        self._cache = cache or FingerprintCache()
        self._pending: Dict[str, datetime] = {}
        self._dedup_window_seconds: int = 60

    def check_duplicate(
        self,
        fingerprint: Fingerprint,
    ) -> Tuple[bool, Optional[Any]]:
        """Check if request is a duplicate."""
        cached_response = self._cache.get(fingerprint)
        if cached_response is not None:
            return True, cached_response

        if fingerprint.hash_value in self._pending:
            pending_since = self._pending[fingerprint.hash_value]
            elapsed = (datetime.now() - pending_since).total_seconds()
            if elapsed < self._dedup_window_seconds:
                return True, None

        return False, None

    def mark_pending(self, fingerprint: Fingerprint):
        """Mark a request as pending."""
        self._pending[fingerprint.hash_value] = datetime.now()

    def mark_complete(
        self,
        fingerprint: Fingerprint,
        response: Any,
        ttl_seconds: Optional[int] = None,
    ):
        """Mark request as complete and cache response."""
        if fingerprint.hash_value in self._pending:
            del self._pending[fingerprint.hash_value]
        self._cache.set(fingerprint, response, ttl_seconds)

    def clear_pending(self, fingerprint: Fingerprint):
        """Clear pending status."""
        if fingerprint.hash_value in self._pending:
            del self._pending[fingerprint.hash_value]


class APIFingerprintAction:
    """High-level API fingerprinting action."""

    def __init__(
        self,
        request_fingerprinter: Optional[RequestFingerprinter] = None,
        response_fingerprinter: Optional[ResponseFingerprinter] = None,
        cache: Optional[FingerprintCache] = None,
        deduplicator: Optional[Deduplicator] = None,
    ):
        self.request_fingerprinter = request_fingerprinter or RequestFingerprinter()
        self.response_fingerprinter = response_fingerprinter or ResponseFingerprinter()
        self.cache = cache or FingerprintCache()
        self.deduplicator = deduplicator or Deduplicator(self.cache)

    def fingerprint_request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, Any]] = None,
    ) -> Fingerprint:
        """Generate fingerprint for a request."""
        return self.request_fingerprinter.fingerprint(
            method, path, body, headers, query_params
        )

    def fingerprint_response(
        self,
        body: bytes,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
    ) -> Fingerprint:
        """Generate fingerprint for a response."""
        return self.response_fingerprinter.fingerprint(body, status_code, headers)

    def check_duplicate(self, fingerprint: Fingerprint) -> bool:
        """Check if request is a duplicate."""
        is_dup, _ = self.deduplicator.check_duplicate(fingerprint)
        return is_dup

    def get_cached_response(self, fingerprint: Fingerprint) -> Optional[Any]:
        """Get cached response for fingerprint."""
        return self.cache.get(fingerprint)

    def cache_response(
        self,
        fingerprint: Fingerprint,
        response: Any,
        ttl_seconds: Optional[int] = None,
    ):
        """Cache a response."""
        self.cache.set(fingerprint, response, ttl_seconds)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self.cache.get_stats()


# Module exports
__all__ = [
    "APIFingerprintAction",
    "RequestFingerprinter",
    "ResponseFingerprinter",
    "FingerprintCache",
    "Deduplicator",
    "Fingerprint",
    "CacheEntry",
    "FingerprintConfig",
    "HashAlgorithm",
]
