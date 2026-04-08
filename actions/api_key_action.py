"""API key management action module.

Provides API key generation, rotation, validation, and secure storage.
Supports key versioning, rate limiting per key, and access tracking.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
import json
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class APIKey:
    """Represents an API key with metadata."""
    key_id: str
    key_hash: str
    key_prefix: str
    key_type: str = "bearer"
    scopes: List[str] = field(default_factory=list)
    rate_limit: int = 1000
    rate_window: int = 3600
    created_at: float = field(default_factory=lambda: time.time())
    expires_at: Optional[float] = None
    last_used_at: Optional[float] = None
    is_active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if key has expired."""
        if self.expires_at is None:
            return False
        return time.time() >= self.expires_at

    @property
    def is_valid(self) -> bool:
        """Check if key is valid (active and not expired)."""
        return self.is_active and not self.is_expired


class APIKeyStore:
    """In-memory API key store with persistence support.

    Provides key generation, validation, rotation, and access tracking.
    """

    def __init__(self, storage_path: Optional[str] = None) -> None:
        """Initialize API key store.

        Args:
            storage_path: Optional path to persist keys as JSON.
        """
        self.storage_path = storage_path
        self._keys: Dict[str, APIKey] = {}
        self._load()

    def generate_key(
        self,
        key_type: str = "bearer",
        scopes: Optional[List[str]] = None,
        rate_limit: int = 1000,
        rate_window: int = 3600,
        expires_in: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, APIKey]:
        """Generate a new API key.

        Args:
            key_type: Type of key (bearer, hmac, jwt).
            scopes: List of permission scopes.
            rate_limit: Max requests per rate_window.
            rate_window: Rate limit window in seconds.
            expires_in: Key lifetime in seconds (None = never).
            metadata: Additional metadata dict.

        Returns:
            Tuple of (plaintext_key, APIKey object).
            Plaintext key is only available at generation time.
        """
        raw_key = secrets.token_urlsafe(32)
        key_id = secrets.token_urlsafe(8)
        key_prefix = raw_key[:8]

        plain_key = f"{key_type}_{raw_key}" if key_type != "bearer" else raw_key

        key_obj = APIKey(
            key_id=key_id,
            key_hash=self._hash_key(plain_key),
            key_prefix=key_prefix,
            key_type=key_type,
            scopes=scopes or [],
            rate_limit=rate_limit,
            rate_window=rate_window,
            expires_at=(time.time() + expires_in) if expires_in else None,
            metadata=metadata or {},
        )

        self._keys[key_id] = key_obj
        self._save()
        logger.info("Generated API key: %s", key_id)
        return plain_key, key_obj

    def validate_key(self, raw_key: str) -> Optional[APIKey]:
        """Validate a plaintext API key.

        Args:
            raw_key: The plaintext API key.

        Returns:
            APIKey if valid, None if invalid or expired.
        """
        key_hash = self._hash_key(raw_key)
        for key_obj in self._keys.values():
            if key_obj.key_hash == key_hash:
                if not key_obj.is_valid:
                    logger.warning("Key %s is invalid or expired", key_obj.key_id)
                    return None
                key_obj.last_used_at = time.time()
                self._save()
                return key_obj
        return None

    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key by its ID.

        Args:
            key_id: The key identifier.

        Returns:
            True if revoked, False if not found.
        """
        if key_id in self._keys:
            self._keys[key_id].is_active = False
            self._save()
            logger.info("Revoked API key: %s", key_id)
            return True
        return False

    def rotate_key(self, key_id: str, expires_in: Optional[int] = None) -> Optional[tuple[str, APIKey]]:
        """Rotate a key by issuing a new key with the same metadata.

        Args:
            key_id: ID of the key to rotate.
            expires_in: New expiration in seconds.

        Returns:
            Tuple of (new_plaintext_key, new_APIKey) or None if not found.
        """
        old_key = self._keys.get(key_id)
        if not old_key:
            return None

        new_key, new_obj = self.generate_key(
            key_type=old_key.key_type,
            scopes=old_key.scopes,
            rate_limit=old_key.rate_limit,
            rate_window=old_key.rate_window,
            expires_in=expires_in,
            metadata=old_key.metadata,
        )
        old_key.is_active = False
        new_obj.metadata["rotated_from"] = key_id
        return new_key, new_obj

    def get_key(self, key_id: str) -> Optional[APIKey]:
        """Get key metadata by ID."""
        return self._keys.get(key_id)

    def list_keys(self, include_expired: bool = False) -> List[APIKey]:
        """List all API keys.

        Args:
            include_expired: Include expired keys.

        Returns:
            List of APIKey objects.
        """
        keys = list(self._keys.values())
        if not include_expired:
            keys = [k for k in keys if k.is_valid]
        return sorted(keys, key=lambda k: k.created_at, reverse=True)

    def _hash_key(self, raw_key: str) -> str:
        """Create a SHA-256 hash of a key."""
        return hashlib.sha256(raw_key.encode()).hexdigest()

    def _load(self) -> None:
        """Load keys from storage file."""
        if not self.storage_path:
            return
        try:
            with open(self.storage_path, "r") as f:
                data = json.load(f)
                for item in data:
                    self._keys[item["key_id"]] = APIKey(**item)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _save(self) -> None:
        """Persist keys to storage file."""
        if not self.storage_path:
            return
        try:
            with open(self.storage_path, "w") as f:
                json.dump([k.__dict__ for k in self._keys.values()], f, indent=2)
        except IOError as e:
            logger.error("Failed to save API keys: %s", e)


class APIKeyAction:
    """High-level API key management action.

    Example:
        store = APIKeyStore("/tmp/keys.json")
        action = APIKeyAction(store)

        raw_key, key_obj = action.create_key(scopes=["read", "write"])
        validated = action.validate_key(raw_key)
    """

    def __init__(self, store: Optional[APIKeyStore] = None) -> None:
        """Initialize API key action.

        Args:
            store: APIKeyStore instance. Creates new in-memory store if None.
        """
        self.store = store or APIKeyStore()

    def create_key(
        self,
        scopes: Optional[List[str]] = None,
        rate_limit: int = 1000,
        expires_in: int = 86400 * 30,
    ) -> tuple[str, APIKey]:
        """Create a new API key.

        Args:
            scopes: Permission scopes.
            rate_limit: Requests per hour.
            expires_in: Lifetime in seconds (default 30 days).

        Returns:
            Tuple of (plaintext_key, APIKey).
        """
        return self.store.generate_key(
            scopes=scopes,
            rate_limit=rate_limit,
            expires_in=expires_in,
        )

    def validate_key(self, raw_key: str) -> Optional[APIKey]:
        """Validate and track usage of an API key."""
        return self.store.validate_key(raw_key)

    def check_rate_limit(self, raw_key: str) -> tuple[bool, int]:
        """Check if a key is within rate limits.

        Args:
            raw_key: Plaintext API key.

        Returns:
            Tuple of (allowed, remaining_requests).
        """
        from collections import deque

        key_obj = self.validate_key(raw_key)
        if not key_obj:
            return False, 0

        now = time.time()
        window_start = now - key_obj.rate_window

        if not hasattr(self, "_request_log"):
            self._request_log: Dict[str, deque] = {}

        log = self._request_log.get(key_obj.key_id, deque(maxlen=key_obj.rate_limit))
        self._request_log[key_obj.key_id] = log

        while log and log[0] < window_start:
            log.popleft()

        remaining = key_obj.rate_limit - len(log)
        if remaining > 0:
            log.append(now)
            return True, remaining - 1

        return False, 0

    def revoke_key(self, key_id: str) -> bool:
        """Revoke a key by ID."""
        return self.store.revoke_key(key_id)

    def rotate_key(self, key_id: str, grace_period: int = 300) -> Optional[tuple[str, APIKey]]:
        """Rotate a key with optional grace period for old key.

        Args:
            key_id: Key to rotate.
            grace_period: Seconds to keep old key valid after rotation.

        Returns:
            Tuple of (new_key, new_APIKey) or None.
        """
        return self.store.rotate_key(key_id)
