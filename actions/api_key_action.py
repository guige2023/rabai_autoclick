"""
API Key Action Module.

API key management with generation, validation,
rotation, and access control.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import secrets
import hashlib
import time
from enum import Enum

logger = logging.getLogger(__name__)


class KeyPermission(Enum):
    """API key permission levels."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


@dataclass
class APIKey:
    """API key metadata."""
    key_id: str
    key_hash: str
    name: str
    permissions: list[KeyPermission]
    created_at: float
    expires_at: Optional[float] = None
    last_used: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class APIKeyAction:
    """
    API key generation and management.

    Generates secure API keys, manages permissions,
    handles rotation and revocation.

    Example:
        key_mgr = APIKeyAction()
        key, key_id = key_mgr.generate_key("MyApp", [KeyPermission.READ])
        is_valid = key_mgr.validate_key(key)
    """

    def __init__(
        self,
        key_prefix: str = "sk",
        key_length: int = 32,
        hash_algorithm: str = "sha256",
    ) -> None:
        self.key_prefix = key_prefix
        self.key_length = key_length
        self.hash_algorithm = hash_algorithm
        self._keys: dict[str, APIKey] = {}
        self._key_lookup: dict[str, str] = {}

    def generate_key(
        self,
        name: str,
        permissions: Optional[list[KeyPermission]] = None,
        expires_in: Optional[float] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> tuple[str, str]:
        """Generate a new API key."""
        raw_key = secrets.token_urlsafe(self.key_length)
        key = f"{self.key_prefix}_{raw_key}"

        key_id = self._generate_key_id(raw_key)
        key_hash = self._hash_key(raw_key)

        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            name=name,
            permissions=permissions or [KeyPermission.READ],
            created_at=time.time(),
            expires_at=time.time() + expires_in if expires_in else None,
            metadata=metadata or {},
        )

        self._keys[key_id] = api_key
        self._key_lookup[key_hash] = key_id

        logger.info("Generated API key for %s", name)

        return key, key_id

    def validate_key(
        self,
        key: str,
        required_permission: Optional[KeyPermission] = None,
    ) -> tuple[bool, Optional[str]]:
        """Validate an API key."""
        if not key or "_" not in key:
            return False, None

        parts = key.split("_", 1)
        if len(parts) != 2 or parts[0] != self.key_prefix:
            return False, None

        raw_key = parts[1]
        key_hash = self._hash_key(raw_key)

        key_id = self._key_lookup.get(key_hash)
        if not key_id:
            return False, None

        api_key = self._keys.get(key_id)
        if not api_key:
            return False, None

        if api_key.expires_at and time.time() > api_key.expires_at:
            return False, "Key expired"

        if required_permission and required_permission not in api_key.permissions:
            return False, "Insufficient permissions"

        api_key.last_used = time.time()

        return True, key_id

    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key."""
        if key_id not in self._keys:
            return False

        api_key = self._keys[key_id]
        del self._key_lookup[api_key.key_hash]
        del self._keys[key_id]

        logger.info("Revoked API key %s", key_id)

        return True

    def get_key_info(self, key_id: str) -> Optional[dict[str, Any]]:
        """Get API key metadata."""
        api_key = self._keys.get(key_id)
        if not api_key:
            return None

        return {
            "key_id": api_key.key_id,
            "name": api_key.name,
            "permissions": [p.value for p in api_key.permissions],
            "created_at": api_key.created_at,
            "expires_at": api_key.expires_at,
            "last_used": api_key.last_used,
            "metadata": api_key.metadata,
        }

    def rotate_key(
        self,
        key_id: str,
        expires_in: Optional[float] = None,
    ) -> Optional[str]:
        """Rotate an API key."""
        api_key = self._keys.get(key_id)
        if not api_key:
            return None

        new_key, _ = self.generate_key(
            name=api_key.name,
            permissions=[p for p in api_key.permissions],
            expires_in=expires_in,
            metadata=api_key.metadata,
        )

        self.revoke_key(key_id)

        return new_key

    def list_keys(self) -> list[dict[str, Any]]:
        """List all API keys."""
        return [
            self.get_key_info(key_id)
            for key_id in self._keys
        ]

    def _generate_key_id(self, raw_key: str) -> str:
        """Generate a short key ID."""
        return hashlib.sha256(raw_key.encode()).hexdigest()[:16]

    def _hash_key(self, raw_key: str) -> str:
        """Hash a key for storage."""
        return hashlib.sha256(raw_key.encode()).hexdigest()

    def cleanup_expired(self) -> int:
        """Remove expired keys."""
        now = time.time()
        expired = [
            key_id for key_id, key in self._keys.items()
            if key.expires_at and key.expires_at < now
        ]

        for key_id in expired:
            self.revoke_key(key_id)

        return len(expired)
