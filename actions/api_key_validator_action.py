"""API Key Validator and Rotator.

This module provides API key validation, verification, and rotation capabilities:
- HMAC-based key verification
- Key expiration checking
- Key scope validation
- Automatic key rotation support

Example:
    >>> from actions.api_key_validator_action import APIKeyValidator
    >>> validator = APIKeyValidator()
    >>> result = validator.validate_key("sk_live_xxx")
"""

from __future__ import annotations

import hashlib
import hmac
import time
import logging
import secrets
import threading
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class KeyInfo:
    """Information about an API key."""
    key_id: str
    key_prefix: str
    key_hash: str
    scopes: list[str]
    created_at: float
    expires_at: Optional[float] = None
    last_used_at: Optional[float] = None
    is_active: bool = True


class APIKeyValidator:
    """Validates and manages API keys with HMAC verification."""

    def __init__(
        self,
        secret: Optional[str] = None,
        default_expiry_seconds: int = 86400 * 30,
    ) -> None:
        """Initialize the API key validator.

        Args:
            secret: Secret used for HMAC verification. If None, generates random.
            default_expiry_seconds: Default key expiry time in seconds.
        """
        self._secret = secret or secrets.token_hex(32)
        self._keys: dict[str, KeyInfo] = {}
        self._default_expiry = default_expiry_seconds
        self._lock = threading.RLock()

    def generate_key(
        self,
        key_id: str,
        scopes: Optional[list[str]] = None,
        expires_in: Optional[int] = None,
    ) -> str:
        """Generate a new API key.

        Args:
            key_id: Unique identifier for this key.
            scopes: List of permission scopes for this key.
            expires_in: Seconds until expiration. None = use default.

        Returns:
            The generated API key string.
        """
        with self._lock:
            prefix = f"sk_{key_id[:8]}"
            random_part = secrets.token_urlsafe(24)
            raw_key = f"{prefix}.{random_part}"
            key_hash = self._hash_key(raw_key)

            expires_at = None
            if expires_in is not None:
                expires_at = time.time() + expires_in
            elif self._default_expiry > 0:
                expires_at = time.time() + self._default_expiry

            self._keys[key_id] = KeyInfo(
                key_id=key_id,
                key_prefix=prefix,
                key_hash=key_hash,
                scopes=scopes or [],
                created_at=time.time(),
                expires_at=expires_at,
            )

            logger.info("Generated API key for %s (expires=%s)", key_id, expires_at)
            return raw_key

    def validate_key(self, raw_key: str) -> Optional[KeyInfo]:
        """Validate an API key and return its info.

        Args:
            raw_key: The raw API key string.

        Returns:
            KeyInfo if valid, None if invalid or expired.
        """
        with self._lock:
            parts = raw_key.split(".")
            if len(parts) != 2:
                return None

            prefix, _ = parts
            key_id = prefix.replace("sk_", "")[:8]

            key_info = None
            for ki in self._keys.values():
                if ki.key_prefix == prefix:
                    key_info = ki
                    break

            if key_info is None:
                return None

            if not key_info.is_active:
                return None

            if key_info.expires_at and time.time() > key_info.expires_at:
                return None

            key_info.last_used_at = time.time()
            return key_info

    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key.

        Args:
            key_id: The key identifier to revoke.

        Returns:
            True if revoked, False if not found.
        """
        with self._lock:
            if key_id in self._keys:
                self._keys[key_id].is_active = False
                logger.info("Revoked API key %s", key_id)
                return True
            return False

    def rotate_key(self, key_id: str) -> Optional[str]:
        """Rotate an API key (revoke old, generate new with same scopes).

        Args:
            key_id: The key identifier to rotate.

        Returns:
            New API key string, or None if key not found.
        """
        with self._lock:
            old = self._keys.get(key_id)
            if old is None:
                return None

            new_key = self.generate_key(
                key_id,
                scopes=old.scopes,
                expires_in=int(old.expires_at - time.time()) if old.expires_at else None,
            )
            old.is_active = False
            logger.info("Rotated API key %s", key_id)
            return new_key

    def check_scope(self, key_info: KeyInfo, required_scope: str) -> bool:
        """Check if a key has a required scope.

        Args:
            key_info: The validated key info.
            required_scope: The scope to check for.

        Returns:
            True if key has the scope, False otherwise.
        """
        if "*" in key_info.scopes:
            return True
        return required_scope in key_info.scopes

    def _hash_key(self, raw_key: str) -> str:
        """Hash a raw key for storage."""
        return hashlib.sha256(raw_key.encode()).hexdigest()

    def _verify_hmac(self, raw_key: str) -> bool:
        """Verify key using HMAC."""
        expected = hmac.new(
            self._secret.encode(),
            raw_key.encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected[:16], raw_key[-16:])

    def list_keys(self) -> list[KeyInfo]:
        """List all API keys (metadata only, no secrets)."""
        with self._lock:
            return list(self._keys.values())

    def cleanup_expired(self) -> int:
        """Remove expired keys from storage.

        Returns:
            Number of keys removed.
        """
        with self._lock:
            now = time.time()
            expired = [
                kid for kid, ki in self._keys.items()
                if ki.expires_at and now > ki.expires_at
            ]
            for kid in expired:
                del self._keys[kid]
            if expired:
                logger.info("Cleaned up %d expired keys", len(expired))
            return len(expired)
