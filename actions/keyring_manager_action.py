"""Keyring manager action for secure credential storage.

Provides encrypted storage for passwords, API keys, and
other sensitive data with access control.
"""

import hashlib
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class Credential:
    key: str
    value: str
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class KeyringManagerAction:
    """Manage secure credential storage with encryption.

    Args:
        master_password: Master password for encryption.
        max_entries: Maximum number of stored credentials.
        enable_audit: Enable access audit logging.
    """

    def __init__(
        self,
        master_password: Optional[str] = None,
        max_entries: int = 1000,
        enable_audit: bool = True,
    ) -> None:
        self._credentials: dict[str, Credential] = {}
        self._max_entries = max_entries
        self._enable_audit = enable_audit
        self._audit_log: list[dict[str, Any]] = []
        self._master_key = self._derive_key(master_password or secrets.token_hex(32))
        self._access_hooks: list[callable] = []

    def _derive_key(self, password: str) -> bytes:
        """Derive encryption key from password.

        Args:
            password: Password string.

        Returns:
            Derived key bytes.
        """
        return hashlib.sha256(password.encode()).digest()

    def _encrypt(self, data: str) -> str:
        """Encrypt data using master key.

        Args:
            data: Data to encrypt.

        Returns:
            Encrypted data string.
        """
        key_bytes = self._master_key
        encrypted = bytes(
            ord(c) ^ key_bytes[i % len(key_bytes)]
            for i, c in enumerate(data)
        )
        return encrypted.hex()

    def _decrypt(self, encrypted_data: str) -> str:
        """Decrypt data using master key.

        Args:
            encrypted_data: Encrypted data string.

        Returns:
            Decrypted data string.
        """
        key_bytes = self._master_key
        encrypted_bytes = bytes.fromhex(encrypted_data)
        decrypted = bytes(
            encrypted_bytes[i] ^ key_bytes[i % len(key_bytes)]
            for i in range(len(encrypted_bytes))
        )
        return decrypted.decode()

    def store(
        self,
        key: str,
        value: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Store a credential.

        Args:
            key: Credential key/name.
            value: Credential value (password, token, etc.).
            metadata: Optional metadata.

        Returns:
            True if stored successfully.
        """
        if len(self._credentials) >= self._max_entries and key not in self._credentials:
            oldest = min(
                self._credentials.items(),
                key=lambda x: x[1].created_at
            )
            del self._credentials[oldest[0]]

        encrypted_value = self._encrypt(value)

        credential = Credential(
            key=key,
            value=encrypted_value,
            metadata=metadata or {},
        )

        self._credentials[key] = credential
        self._audit("store", key)
        logger.debug(f"Stored credential: {key}")
        return True

    def retrieve(self, key: str) -> Optional[str]:
        """Retrieve a credential value.

        Args:
            key: Credential key.

        Returns:
            Decrypted credential value or None.
        """
        credential = self._credentials.get(key)
        if not credential:
            return None

        credential.last_accessed = time.time()
        credential.access_count += 1

        for hook in self._access_hooks:
            try:
                hook(key)
            except Exception as e:
                logger.error(f"Access hook error: {e}")

        self._audit("retrieve", key)
        return self._decrypt(credential.value)

    def delete(self, key: str) -> bool:
        """Delete a credential.

        Args:
            key: Credential key to delete.

        Returns:
            True if deleted.
        """
        if key in self._credentials:
            del self._credentials[key]
            self._audit("delete", key)
            logger.debug(f"Deleted credential: {key}")
            return True
        return False

    def exists(self, key: str) -> bool:
        """Check if a credential exists.

        Args:
            key: Credential key.

        Returns:
            True if credential exists.
        """
        return key in self._credentials

    def list_keys(self) -> list[str]:
        """List all credential keys.

        Returns:
            List of credential keys.
        """
        return list(self._credentials.keys())

    def get_info(self, key: str) -> Optional[dict[str, Any]]:
        """Get credential metadata without revealing value.

        Args:
            key: Credential key.

        Returns:
            Credential info or None.
        """
        credential = self._credentials.get(key)
        if not credential:
            return None

        return {
            "key": credential.key,
            "created_at": credential.created_at,
            "last_accessed": credential.last_accessed,
            "access_count": credential.access_count,
            "metadata": credential.metadata,
        }

    def register_access_hook(self, hook: callable) -> None:
        """Register a hook for credential access events.

        Args:
            hook: Callback function(key).
        """
        self._access_hooks.append(hook)

    def _audit(self, action: str, key: str) -> None:
        """Log an access event.

        Args:
            action: Action type.
            key: Credential key.
        """
        if not self._enable_audit:
            return

        self._audit_log.append({
            "action": action,
            "key": key,
            "timestamp": time.time(),
        })

        if len(self._audit_log) > 10000:
            self._audit_log.pop(0)

    def get_audit_log(
        self,
        key_filter: Optional[str] = None,
        action_filter: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get audit log entries.

        Args:
            key_filter: Filter by credential key.
            action_filter: Filter by action type.
            limit: Maximum entries.

        Returns:
            List of audit entries (newest first).
        """
        entries = self._audit_log
        if key_filter:
            entries = [e for e in entries if e["key"] == key_filter]
        if action_filter:
            entries = [e for e in entries if e["action"] == action_filter]
        return entries[-limit:][::-1]

    def clear_all(self) -> int:
        """Clear all stored credentials.

        Returns:
            Number of credentials cleared.
        """
        count = len(self._credentials)
        self._credentials.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get keyring statistics.

        Returns:
            Dictionary with stats.
        """
        total_accesses = sum(c.access_count for c in self._credentials.values())
        return {
            "total_credentials": len(self._credentials),
            "max_entries": self._max_entries,
            "total_accesses": total_accesses,
            "audit_entries": len(self._audit_log),
            "storage_encrypted": True,
        }
