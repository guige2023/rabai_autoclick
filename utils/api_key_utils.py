"""API key management utilities: rotation, validation, hashing, and storage."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "APIKey",
    "APIKeyManager",
    "hash_api_key",
    "generate_api_key",
    "validate_api_key",
]


@dataclass
class APIKey:
    """Represents an API key with metadata."""

    key_id: str
    key_hash: str
    prefix: str
    created_at: float
    expires_at: float | None = None
    last_used_at: float | None = None
    scopes: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    is_active: bool = True

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def check_scope(self, scope: str) -> bool:
        if not self.scopes:
            return True
        return scope in self.scopes


def generate_api_key(
    prefix: str = "sk",
    length: int = 32,
    separator: str = "_",
) -> tuple[str, str]:
    """Generate a new API key. Returns (raw_key, key_id)."""
    raw = secrets.token_urlsafe(length)
    key_id = f"{prefix}{separator}{raw[:8]}"
    return raw, key_id


def hash_api_key(raw_key: str, algorithm: str = "sha256") -> str:
    """Hash an API key for storage. Never store the raw key."""
    if algorithm == "sha256":
        return hashlib.sha256(raw_key.encode()).hexdigest()
    elif algorithm == "blake2b":
        return hashlib.blake2b(raw_key.encode()).hexdigest()
    elif algorithm == "argon2":
        import argon2

        return argon2.low_level.hash_secret_raw(raw_key.encode())
    raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def validate_api_key(
    raw_key: str,
    stored_hash: str,
    algorithm: str = "sha256",
) -> bool:
    """Validate a raw key against a stored hash."""
    computed = hash_api_key(raw_key, algorithm)
    return hmac.compare_digest(computed, stored_hash)


class APIKeyManager:
    """In-memory API key manager with rotation and scope support."""

    def __init__(self, hash_algorithm: str = "sha256") -> None:
        self._keys: dict[str, APIKey] = {}
        self._hash_index: dict[str, str] = {}
        self._algorithm = hash_algorithm

    def create_key(
        self,
        prefix: str = "sk",
        scopes: tuple[str, ...] = (),
        expires_in: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[str, APIKey]:
        """Create and store a new API key. Returns (raw_key, APIKey object)."""
        raw, key_id = generate_api_key(prefix)
        key_hash = hash_api_key(raw, self._algorithm)
        created = time.time()
        expires = created + expires_in if expires_in else None

        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            prefix=prefix,
            created_at=created,
            expires_at=expires,
            scopes=scopes,
            metadata=metadata or {},
        )
        self._keys[key_id] = api_key
        self._hash_index[key_hash] = key_id
        return raw, api_key

    def get(self, key_id: str) -> APIKey | None:
        return self._keys.get(key_id)

    def verify(self, raw_key: str) -> APIKey | None:
        """Verify a raw key and return its metadata if valid."""
        key_hash = hash_api_key(raw_key, self._algorithm)
        key_id = self._hash_index.get(key_hash)
        if not key_id:
            return None
        key = self._keys.get(key_id)
        if not key or not key.is_active or key.is_expired:
            return None
        key.last_used_at = time.time()
        return key

    def revoke(self, key_id: str) -> bool:
        """Revoke an API key."""
        key = self._keys.get(key_id)
        if not key:
            return False
        key.is_active = False
        del self._hash_index[key.key_hash]
        return True

    def rotate(
        self,
        key_id: str,
        expires_in: float | None = None,
    ) -> tuple[str, APIKey] | None:
        """Rotate a key: revoke old and create new with same scopes."""
        old = self._keys.get(key_id)
        if not old:
            return None
        raw, new_key = self.create_key(
            prefix=old.prefix,
            scopes=old.scopes,
            expires_in=expires_in,
            metadata=old.metadata,
        )
        self.revoke(key_id)
        return raw, new_key

    def list_keys(self) -> list[APIKey]:
        return list(self._keys.values())

    def prune_expired(self) -> int:
        """Remove expired keys. Returns count of removed keys."""
        expired = [k for k in self._keys.values() if k.is_expired]
        for k in expired:
            self.revoke(k.key_id)
        return len(expired)
