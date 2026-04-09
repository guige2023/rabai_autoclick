"""API Key Store Action Module.

Secure API key storage and validation.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .id_generator_action import IDGenerator


class KeyType(Enum):
    """API key types."""
    FULL_ACCESS = "full_access"
    READ_ONLY = "read_only"
    CUSTOM = "custom"


class KeyStatus(Enum):
    """API key status."""
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"


@dataclass
class APIKey:
    """API key record."""
    key_id: str
    key_hash: str
    key_prefix: str
    name: str
    key_type: KeyType
    status: KeyStatus
    created_at: float
    expires_at: float | None = None
    last_used: float | None = None
    scopes: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class APIKeyStore:
    """Store and validate API keys."""

    def __init__(self, hash_algorithm: str = "sha256") -> None:
        self.hash_algorithm = hash_algorithm
        self._keys: dict[str, APIKey] = {}
        self._keys_by_hash: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._id_gen = IDGenerator()

    async def create_key(
        self,
        name: str,
        key_type: KeyType = KeyType.FULL_ACCESS,
        scopes: list[str] | None = None,
        expires_in_seconds: float | None = None
    ) -> tuple[str, APIKey]:
        """Create a new API key. Returns (raw_key, key_record)."""
        raw_key = f"sk_{secrets.token_urlsafe(32)}"
        key_hash = self._hash_key(raw_key)
        key_id = self._id_gen.generate_custom(prefix="key")
        prefix = raw_key[:8]
        expires_at = time.time() + expires_in_seconds if expires_in_seconds else None
        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            key_prefix=prefix,
            name=name,
            key_type=key_type,
            status=KeyStatus.ACTIVE,
            created_at=time.time(),
            expires_at=expires_at,
            scopes=scopes or []
        )
        async with self._lock:
            self._keys[key_id] = api_key
            self._keys_by_hash[key_hash] = key_id
        return raw_key, api_key

    async def validate_key(self, raw_key: str) -> tuple[bool, APIKey | None, str]:
        """Validate an API key. Returns (valid, key_record, error)."""
        if not raw_key or not raw_key.startswith("sk_"):
            return False, None, "Invalid key format"
        key_hash = self._hash_key(raw_key)
        async with self._lock:
            key_id = self._keys_by_hash.get(key_hash)
            if not key_id:
                return False, None, "Key not found"
            api_key = self._keys.get(key_id)
            if not api_key:
                return False, None, "Key not found"
            if api_key.status == KeyStatus.REVOKED:
                return False, api_key, "Key has been revoked"
            if api_key.expires_at and time.time() > api_key.expires_at:
                api_key.status = KeyStatus.EXPIRED
                return False, api_key, "Key has expired"
            api_key.last_used = time.time()
            return True, api_key, ""

    async def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key."""
        async with self._lock:
            api_key = self._keys.get(key_id)
            if api_key:
                api_key.status = KeyStatus.REVOKED
                return True
            return False

    def _hash_key(self, raw_key: str) -> str:
        """Hash an API key."""
        return hashlib.sha256(raw_key.encode()).hexdigest()
