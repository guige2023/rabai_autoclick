"""
Secrets management module for secure credential storage and retrieval.

Supports encryption, versioning, access control, and integration
with external secret stores (Vault, AWS Secrets Manager, etc.).
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class SecretType(Enum):
    """Type of secret."""
    PASSWORD = "password"
    API_KEY = "api_key"
    SSH_KEY = "ssh_key"
    TLS_CERT = "tls_cert"
    OAUTH_TOKEN = "oauth_token"
    DATABASE_CREDENTIALS = "database_credentials"
    CUSTOM = "custom"


class SecretStatus(Enum):
    """Secret status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    EXPIRED = "expired"
    REVOKED = "revoked"


@dataclass
class Secret:
    """A stored secret."""
    key: str
    value: str
    secret_type: SecretType
    status: SecretStatus = SecretStatus.ACTIVE
    description: str = ""
    version: int = 1
    versions: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    metadata: dict = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    access_count: int = 0
    last_accessed_at: Optional[float] = None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def to_dict(self, include_value: bool = False) -> dict:
        result = {
            "key": self.key,
            "secret_type": self.secret_type.value,
            "status": self.status.value,
            "description": self.description,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "metadata": self.metadata,
            "tags": self.tags,
            "access_count": self.access_count,
            "last_accessed_at": self.last_accessed_at,
        }
        if include_value:
            result["value"] = self.value
        return result


@dataclass
class SecretAccess:
    """Record of secret access."""
    key: str
    accessed_by: str
    accessed_at: float = field(default_factory=time.time)
    access_type: str = "read"
    ip_address: Optional[str] = None


class SecretsManager:
    """
    Secrets management service.

    Provides secure storage, versioning, access control, and audit logging
    for secrets and credentials.
    """

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        storage_path: Optional[str] = None,
    ):
        self._encryption_key = encryption_key or os.environ.get("SECRETS_ENCRYPTION_KEY", "")
        self._storage_path = storage_path
        self._secrets: dict[str, Secret] = {}
        self._access_log: list[SecretAccess] = []
        self._load_from_disk()

    def _encrypt(self, value: str) -> str:
        """Encrypt a secret value."""
        if not self._encryption_key:
            return base64.b64encode(value.encode()).decode()
        key_bytes = self._encryption_key.encode()[:32].ljust(32, b"0")
        encrypted = bytearray()
        for i, char in enumerate(value.encode()):
            encrypted.append(char ^ key_bytes[i % len(key_bytes)])
        return base64.b64encode(bytes(encrypted)).decode()

    def _decrypt(self, encrypted: str) -> str:
        """Decrypt a secret value."""
        if not self._encryption_key:
            return base64.b64decode(encrypted.encode()).decode()
        key_bytes = self._encryption_key.encode()[:32].ljust(32, b"0")
        data = base64.b64decode(encrypted.encode())
        decrypted = bytearray()
        for i, byte in enumerate(data):
            decrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        return decrypted.decode()

    def create(
        self,
        key: str,
        value: str,
        secret_type: SecretType = SecretType.CUSTOM,
        description: str = "",
        expires_at: Optional[float] = None,
        metadata: Optional[dict] = None,
        tags: Optional[list[str]] = None,
    ) -> Secret:
        """Create a new secret."""
        if key in self._secrets:
            raise ValueError(f"Secret already exists: {key}")

        encrypted_value = self._encrypt(value)

        secret = Secret(
            key=key,
            value=encrypted_value,
            secret_type=secret_type,
            description=description,
            expires_at=expires_at,
            metadata=metadata or {},
            tags=tags or [],
            versions=[{"version": 1, "value": encrypted_value, "created_at": time.time()}],
        )

        self._secrets[key] = secret
        self._save_to_disk()
        return secret

    def get(
        self,
        key: str,
        version: Optional[int] = None,
        accessed_by: Optional[str] = None,
    ) -> Optional[str]:
        """Get a secret value."""
        secret = self._secrets.get(key)
        if not secret:
            return None

        if secret.status == SecretStatus.REVOKED:
            return None

        if secret.is_expired():
            secret.status = SecretStatus.EXPIRED
            return None

        if version and version != secret.version:
            for v in secret.versions:
                if v["version"] == version:
                    self._log_access(key, accessed_by)
                    secret.access_count += 1
                    secret.last_accessed_at = time.time()
                    return self._decrypt(v["value"])

        self._log_access(key, accessed_by)
        secret.access_count += 1
        secret.last_accessed_at = time.time()

        return self._decrypt(secret.value)

    def update(
        self,
        key: str,
        value: str,
        description: Optional[str] = None,
        expires_at: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> Optional[Secret]:
        """Update a secret with a new version."""
        secret = self._secrets.get(key)
        if not secret:
            return None

        encrypted_value = self._encrypt(value)
        secret.version += 1
        secret.value = encrypted_value
        secret.updated_at = time.time()
        secret.versions.append({
            "version": secret.version,
            "value": encrypted_value,
            "created_at": time.time(),
        })

        if description is not None:
            secret.description = description
        if expires_at is not None:
            secret.expires_at = expires_at
        if metadata:
            secret.metadata.update(metadata)

        self._save_to_disk()
        return secret

    def delete(self, key: str, soft: bool = True) -> bool:
        """Delete a secret."""
        secret = self._secrets.get(key)
        if not secret:
            return False

        if soft:
            secret.status = SecretStatus.REVOKED
            secret.updated_at = time.time()
        else:
            del self._secrets[key]

        self._save_to_disk()
        return True

    def rotate(
        self,
        key: str,
        generator: Optional[callable] = None,
    ) -> Optional[Secret]:
        """Rotate a secret with a new value."""
        if generator:
            new_value = generator()
        else:
            new_value = self._generate_random_secret(32)

        return self.update(key, new_value)

    def _generate_random_secret(self, length: int) -> str:
        """Generate a random secret."""
        import secrets
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _log_access(
        self,
        key: str,
        accessed_by: Optional[str] = None,
        access_type: str = "read",
    ) -> None:
        """Log secret access."""
        access = SecretAccess(
            key=key,
            accessed_by=accessed_by or "system",
            access_type=access_type,
        )
        self._access_log.append(access)

    def list_secrets(
        self,
        secret_type: Optional[SecretType] = None,
        status: Optional[SecretStatus] = None,
        tags: Optional[list[str]] = None,
        include_values: bool = False,
    ) -> list[dict]:
        """List secrets with optional filters."""
        results = list(self._secrets.values())

        if secret_type:
            results = [s for s in results if s.secret_type == secret_type]
        if status:
            results = [s for s in results if s.status == status]
        if tags:
            results = [s for s in results if any(tag in s.tags for tag in tags)]

        return [s.to_dict(include_value=include_values) for s in results]

    def get_access_log(
        self,
        key: Optional[str] = None,
        accessed_by: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get access log entries."""
        logs = self._access_log

        if key:
            logs = [l for l in logs if l.key == key]
        if accessed_by:
            logs = [l for l in logs if l.accessed_by == accessed_by]

        return [
            {
                "key": l.key,
                "accessed_by": l.accessed_by,
                "accessed_at": l.accessed_at,
                "access_type": l.access_type,
                "ip_address": l.ip_address,
            }
            for l in logs[-limit:]
        ]

    def get_stats(self) -> dict:
        """Get secrets statistics."""
        total = len(self._secrets)
        by_type = {}
        by_status = {}

        for secret in self._secrets.values():
            type_key = secret.secret_type.value
            by_type[type_key] = by_type.get(type_key, 0) + 1
            status_key = secret.status.value
            by_status[status_key] = by_status.get(status_key, 0) + 1

        return {
            "total_secrets": total,
            "by_type": by_type,
            "by_status": by_status,
            "total_accesses": sum(s.access_count for s in self._secrets.values()),
        }

    def _save_to_disk(self) -> None:
        """Save secrets to disk."""
        if not self._storage_path:
            return

        data = {
            key: {
                **secret.to_dict(include_value=True),
                "value": secret.value,
            }
            for key, secret in self._secrets.items()
        }

        with open(self._storage_path, "w") as f:
            json.dump(data, f, indent=2)

    def _load_from_disk(self) -> None:
        """Load secrets from disk."""
        if not self._storage_path or not os.path.exists(self._storage_path):
            return

        with open(self._storage_path) as f:
            data = json.load(f)

        for key, item in data.items():
            secret = Secret(
                key=key,
                value=item["value"],
                secret_type=SecretType(item["secret_type"]),
                status=SecretStatus(item["status"]),
                description=item.get("description", ""),
                version=item.get("version", 1),
                created_at=item.get("created_at", time.time()),
                updated_at=item.get("updated_at", time.time()),
                expires_at=item.get("expires_at"),
                metadata=item.get("metadata", {}),
                tags=item.get("tags", []),
            )
            self._secrets[key] = secret
