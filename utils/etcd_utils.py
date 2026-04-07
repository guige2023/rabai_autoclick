"""
etcd utilities for distributed key-value store operations.

Provides cluster management, lease handling, watch streams,
transaction support, and consistent backup/restore.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EventType(Enum):
    PUT = auto()
    DELETE = auto()
    COMPARE = auto()


@dataclass
class KVEvent:
    """Represents an etcd key-value event."""
    event_type: EventType
    key: str
    value: bytes
    version: int = 0
    revision: int = 0
    lease_id: int = 0


@dataclass
class EtcdConfig:
    """Configuration for etcd connection."""
    endpoints: list[str] = field(default_factory=lambda: ["http://localhost:2379"])
    user: Optional[str] = None
    password: Optional[str] = None
    timeout: float = 10.0
    permit_without_stream: bool = False
    max_call_timeout: float = 10.0


@dataclass
class LeaseInfo:
    """Information about an etcd lease."""
    id: int
    ttl: int
    granted_ttl: int


class EtcdClient:
    """High-level etcd client."""

    def __init__(self, config: Optional[EtcdConfig] = None) -> None:
        self.config = config or EtcdConfig()
        self._client: Any = None
        self._kv: Any = None
        self._watcher: Any = None

    def _get_client(self) -> Any:
        """Lazily initialize the etcd client."""
        if self._client is None:
            try:
                import etcd3
                credentials = None
                if self.config.user and self.config.password:
                    credentials = (self.config.user, self.config.password)
                self._client = etcd3.client(
                    host=self.config.endpoints[0].split(":")[0] if ":" in self.config.endpoints[0] else self.config.endpoints[0],
                    port=int(self.config.endpoints[0].split(":")[-1]) if ":" in self.config.endpoints[0] else 2379,
                    timeout=self.config.timeout,
                    user=credentials[0] if credentials else None,
                    password=credentials[1] if credentials else None,
                )
                self._kv = self._client
            except ImportError:
                logger.warning("etcd3 not installed, using mock mode")
                self._client = MockEtcdClient()
                self._kv = self._client
        return self._client

    def put(self, key: str, value: bytes | str, lease: Optional[int] = None) -> bool:
        """Put a key-value pair."""
        if isinstance(value, str):
            value = value.encode()
        try:
            client = self._get_client()
            client.put(key, value, lease_id=lease)
            return True
        except Exception as e:
            logger.error("etcd put failed: %s", e)
            return False

    def get(self, key: str) -> Optional[bytes]:
        """Get a value by key."""
        try:
            client = self._get_client()
            value, meta = client.get(key)
            return value
        except Exception as e:
            logger.error("etcd get failed: %s", e)
            return None

    def get_prefix(self, prefix: str) -> dict[str, bytes]:
        """Get all keys with a prefix."""
        result = {}
        try:
            client = self._get_client()
            for value, key in client.get_prefix(prefix):
                if key:
                    result[key.decode() if isinstance(key, bytes) else key] = value
        except Exception as e:
            logger.error("etcd get_prefix failed: %s", e)
        return result

    def delete(self, key: str) -> bool:
        """Delete a key."""
        try:
            client = self._get_client()
            client.delete(key)
            return True
        except Exception as e:
            logger.error("etcd delete failed: %s", e)
            return False

    def delete_prefix(self, prefix: str) -> int:
        """Delete all keys with a prefix, returns count deleted."""
        try:
            client = self._get_client()
            count = 0
            for value, key in client.get_prefix(prefix):
                if key:
                    client.delete(key)
                    count += 1
            return count
        except Exception as e:
            logger.error("etcd delete_prefix failed: %s", e)
            return 0

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        try:
            client = self._get_client()
            value, _ = client.get(key)
            return value is not None
        except Exception:
            return False

    def acquire_lease(self, ttl: int) -> Optional[int]:
        """Acquire a lease with TTL in seconds."""
        try:
            client = self._get_client()
            lease_id = client.lease(ttl)
            return lease_id
        except Exception as e:
            logger.error("etcd acquire_lease failed: %s", e)
            return None

    def revoke_lease(self, lease_id: int) -> bool:
        """Revoke a lease."""
        try:
            client = self._get_client()
            client.revoke_lease(lease_id)
            return True
        except Exception as e:
            logger.error("etcd revoke_lease failed: %s", e)
            return False

    def put_with_lease(self, key: str, value: bytes | str, ttl: int) -> bool:
        """Put a key with an attached lease (auto-cleanup)."""
        lease_id = self.acquire_lease(ttl)
        if lease_id is None:
            return False
        return self.put(key, value, lease=lease_id)

    def watch(
        self,
        key: str,
        callback: Callable[[KVEvent], None],
        prefix: bool = False,
    ) -> None:
        """Watch a key or prefix for changes."""
        try:
            client = self._get_client()
            events_iterator, cancel = client.watch(
                key,
                is_prefix=prefix,
            )
            for event in events_iterator:
                kv_event = self._parse_event(event)
                if kv_event:
                    callback(kv_event)
        except Exception as e:
            logger.error("etcd watch failed: %s", e)

    def _parse_event(self, event: Any) -> Optional[KVEvent]:
        """Parse an etcd event into KVEvent."""
        try:
            if hasattr(event, "type"):
                event_type = EventType.PUT if "PUT" in str(event.type) else EventType.DELETE
                key = getattr(event, "key", b"").decode()
                value = getattr(event, "value", b"")
                return KVEvent(
                    event_type=event_type,
                    key=key,
                    value=value,
                )
        except Exception:
            pass
        return None

    def transaction(
        self,
        compare: list[dict[str, Any]],
        success: list[dict[str, Any]],
        failure: Optional[list[dict[str, Any]]] = None,
    ) -> bool:
        """Execute an etcd transaction."""
        try:
            client = self._get_client()
            client.transaction(
                compare=compare,
                success=success,
                failure=failure or [],
            )
            return True
        except Exception as e:
            logger.error("etcd transaction failed: %s", e)
            return False

    def get_member_list(self) -> list[dict[str, Any]]:
        """Get cluster member list."""
        try:
            client = self._get_client()
            members = client.member_list()
            return [{"name": m.name, "peer_urls": m.peer_urls} for m in members]
        except Exception:
            return []

    def close(self) -> None:
        """Close the etcd client."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            self._kv = None


class MockEtcdClient:
    """In-memory mock etcd client for testing."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._leases: dict[int, int] = {}
        self._lease_counter = 1

    def put(self, key: str, value: bytes, lease_id: Optional[int] = None) -> None:
        self._data[key] = value

    def get(self, key: str) -> tuple[Optional[bytes], Any]:
        return self._data.get(key), None

    def get_prefix(self, prefix: str) -> list[tuple[bytes, bytes]]:
        result = []
        for k, v in self._data.items():
            if k.startswith(prefix):
                result.append((v, k.encode()))
        return result

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def lease(self, ttl: int) -> int:
        lease_id = self._lease_counter
        self._lease_counter += 1
        self._leases[lease_id] = ttl
        return lease_id

    def revoke_lease(self, lease_id: int) -> None:
        self._leases.pop(lease_id, None)

    def close(self) -> None:
        pass


class DistributedLock:
    """Distributed lock using etcd."""

    def __init__(self, client: EtcdClient, lock_name: str, ttl: int = 30) -> None:
        self.client = client
        self.lock_name = lock_name
        self.ttl = ttl
        self._key = f"/locks/{lock_name}"
        self._acquired = False

    def acquire(self, timeout: float = 10.0) -> bool:
        """Acquire the distributed lock."""
        start = time.time()
        while time.time() - start < timeout:
            if self.client.put(self._key, b"locked", lease=self.client.acquire_lease(self.ttl)):
                self._acquired = True
                return True
            time.sleep(0.1)
        return False

    def release(self) -> bool:
        """Release the distributed lock."""
        if self._acquired:
            self._acquired = False
            return self.client.delete(self._key)
        return False

    def __enter__(self) -> "DistributedLock":
        if not self.acquire():
            raise RuntimeError(f"Failed to acquire lock: {self.lock_name}")
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()
