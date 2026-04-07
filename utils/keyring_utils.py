"""
Keyring and Secrets Management Utilities

Provides secure storage and retrieval of secrets,
API keys, passwords, and sensitive configuration.
"""

from __future__ import annotations

import copy
import hashlib
import os
import secrets
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generator

try:
    import keyring
    import keyring.errors
    HAS_KEYRING = True
except ImportError:
    HAS_KEYRING = False


@dataclass
class Secret:
    """A stored secret."""
    key: str
    value: str
    service: str = "rabai"
    created_at: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class SecretStore(ABC):
    """Abstract secret store interface."""

    @abstractmethod
    def set(self, key: str, value: str, service: str = "rabai") -> None:
        """Store a secret."""
        pass

    @abstractmethod
    def get(self, key: str, service: str = "rabai") -> str | None:
        """Retrieve a secret."""
        pass

    @abstractmethod
    def delete(self, key: str, service: str = "rabai") -> bool:
        """Delete a secret."""
        pass

    @abstractmethod
    def exists(self, key: str, service: str = "rabai") -> bool:
        """Check if a secret exists."""
        pass

    @abstractmethod
    def list_keys(self, service: str = "rabai") -> list[str]:
        """List all keys for a service."""
        pass


class InMemorySecretStore(SecretStore):
    """
    Simple in-memory secret store.
    Not persistent, but thread-safe.
    """

    def __init__(self):
        self._secrets: dict[str, dict[str, Secret]] = {}  # service -> key -> Secret
        self._lock = threading.RLock()

    def set(self, key: str, value: str, service: str = "rabai") -> None:
        """Store a secret."""
        with self._lock:
            if service not in self._secrets:
                self._secrets[service] = {}

            import time
            self._secrets[service][key] = Secret(
                key=key,
                value=value,
                service=service,
                created_at=time.time(),
            )

    def get(self, key: str, service: str = "rabai") -> str | None:
        """Retrieve a secret."""
        with self._lock:
            if service in self._secrets and key in self._secrets[service]:
                return self._secrets[service][key].value
            return None

    def delete(self, key: str, service: str = "rabai") -> bool:
        """Delete a secret."""
        with self._lock:
            if service in self._secrets and key in self._secrets[service]:
                del self._secrets[service][key]
                return True
            return False

    def exists(self, key: str, service: str = "rabai") -> bool:
        """Check if a secret exists."""
        with self._lock:
            return service in self._secrets and key in self._secrets[service]

    def list_keys(self, service: str = "rabai") -> list[str]:
        """List all keys for a service."""
        with self._lock:
            if service in self._secrets:
                return list(self._secrets[service].keys())
            return []


class KeyringSecretStore(SecretStore):
    """
    Secret store backed by the system keyring.
    """

    def __init__(self, app_name: str = "rabai"):
        self._app_name = app_name

    def _make_key(self, key: str, service: str) -> str:
        """Create a full keyring key."""
        return f"{service}.{key}"

    def set(self, key: str, value: str, service: str = "rabai") -> None:
        """Store a secret."""
        if not HAS_KEYRING:
            raise RuntimeError("keyring package not installed")

        keyring.set_password(self._app_name, self._make_key(key, service), value)

    def get(self, key: str, service: str = "rabai") -> str | None:
        """Retrieve a secret."""
        if not HAS_KEYRING:
            raise RuntimeError("keyring package not installed")

        try:
            value = keyring.get_password(self._app_name, self._make_key(key, service))
            return value
        except keyring.errors.KeyringError:
            return None

    def delete(self, key: str, service: str = "rabai") -> bool:
        """Delete a secret."""
        if not HAS_KEYRING:
            raise RuntimeError("keyring package not installed")

        try:
            keyring.delete_password(self._app_name, self._make_key(key, service))
            return True
        except keyring.errors.PasswordDeleteError:
            return False

    def exists(self, key: str, service: str = "rabai") -> bool:
        """Check if a secret exists."""
        return self.get(key, service) is not None

    def list_keys(self, service: str = "rabai") -> list[str]:
        """List all keys for a service."""
        # Keyring doesn't support listing, so we track this separately
        return []


class EnvironmentSecretStore(SecretStore):
    """
    Secret store backed by environment variables.
    Useful for development and testing.
    """

    def __init__(self, prefix: str = "SECRET_"):
        self._prefix = prefix

    def _make_env_key(self, key: str, service: str) -> str:
        """Create an environment variable name."""
        service_part = service.upper().replace(".", "_")
        key_part = key.upper().replace(".", "_")
        return f"{self._prefix}{service_part}_{key_part}"

    def set(self, key: str, value: str, service: str = "rabai") -> None:
        """Store a secret."""
        os.environ[self._make_env_key(key, service)] = value

    def get(self, key: str, service: str = "rabai") -> str | None:
        """Retrieve a secret."""
        return os.environ.get(self._make_env_key(key, service))

    def delete(self, key: str, service: str = "rabai") -> bool:
        """Delete a secret."""
        env_key = self._make_env_key(key, service)
        if env_key in os.environ:
            del os.environ[env_key]
            return True
        return False

    def exists(self, key: str, service: str = "rabai") -> bool:
        """Check if a secret exists."""
        return self._make_env_key(key, service) in os.environ

    def list_keys(self, service: str = "rabai") -> list[str]:
        """List all keys for a service."""
        prefix = f"{self._prefix}{service.upper()}_"
        return [
            k[len(prefix):]
            for k in os.environ.keys()
            if k.startswith(prefix)
        ]


class SecretManager:
    """
    High-level secret management with caching and fallback.
    """

    def __init__(
        self,
        stores: list[SecretStore] | None = None,
        use_cache: bool = True,
    ):
        self._stores = stores or [InMemorySecretStore()]
        self._cache: dict[str, str] = {}
        self._use_cache = use_cache
        self._lock = threading.RLock()

    def set(
        self,
        key: str,
        value: str,
        service: str = "rabai",
        persist: bool = True,
    ) -> None:
        """Store a secret."""
        full_key = f"{service}.{key}"

        # Cache it
        if self._use_cache:
            with self._lock:
                self._cache[full_key] = value

        # Store in all stores
        if persist:
            for store in self._stores:
                store.set(key, value, service)

    def get(
        self,
        key: str,
        service: str = "rabai",
        default: str | None = None,
    ) -> str | None:
        """Retrieve a secret."""
        full_key = f"{service}.{key}"

        # Check cache first
        if self._use_cache:
            with self._lock:
                if full_key in self._cache:
                    return self._cache[full_key]

        # Try stores in order
        for store in self._stores:
            value = store.get(key, service)
            if value is not None:
                if self._use_cache:
                    with self._lock:
                        self._cache[full_key] = value
                return value

        return default

    def delete(self, key: str, service: str = "rabai") -> bool:
        """Delete a secret."""
        full_key = f"{service}.{key}"

        # Remove from cache
        if self._use_cache:
            with self._lock:
                self._cache.pop(full_key, None)

        # Delete from all stores
        deleted = False
        for store in self._stores:
            if store.delete(key, service):
                deleted = True

        return deleted

    def generate_key(self, length: int = 32) -> str:
        """Generate a cryptographically secure random key."""
        return secrets.token_urlsafe(length)

    def hash_secret(self, secret: str, algorithm: str = "sha256") -> str:
        """Hash a secret for comparison."""
        if algorithm == "sha256":
            return hashlib.sha256(secret.encode()).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(secret.encode()).hexdigest()
        elif algorithm == "md5":
            return hashlib.md5(secret.encode()).hexdigest()
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")

    def clear_cache(self) -> None:
        """Clear the secret cache."""
        with self._lock:
            self._cache.clear()


# Global default secret manager
_default_manager: SecretManager | None = None


def get_secret_manager() -> SecretManager:
    """Get the default secret manager."""
    global _default_manager
    if _default_manager is None:
        stores = []
        if HAS_KEYRING:
            stores.append(KeyringSecretStore())
        stores.append(InMemorySecretStore())
        _default_manager = SecretManager(stores=stores)
    return _default_manager
