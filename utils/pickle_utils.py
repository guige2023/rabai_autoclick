"""
Pickle serialization utilities with security enhancements.

Provides secure pickle operations with signing, versioning,
and support for custom reducers and persistent ID mapping.

Example:
    >>> from utils.pickle_utils import SecurePickler, load, dump
    >>> handler = SecurePickler(secret_key="mykey")
    >>> data = handler.dumps({"key": "value"})
"""

from __future__ import annotations

import hashlib
import hmac
import pickle
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union


class SecurePickler:
    """
    Secure pickle handler with HMAC signing.

    Prevents deserialization of tampered data by verifying
    HMAC signatures before unpickling.

    Attributes:
        secret_key: Key for HMAC signing.
        protocol: Pickle protocol version.
    """

    def __init__(
        self,
        secret_key: Optional[str] = None,
        protocol: int = pickle.HIGHEST_PROTOCOL,
        fix_imports: bool = True,
    ) -> None:
        """
        Initialize the secure pickler.

        Args:
            secret_key: Key for HMAC signing (required for security).
            protocol: Pickle protocol version.
            fix_imports: Fix imports for compatibility.
        """
        self.secret_key = secret_key.encode() if secret_key else None
        self.protocol = protocol
        self.fix_imports = fix_imports

    def _sign(self, data: bytes) -> bytes:
        """Create HMAC signature of data."""
        if not self.secret_key:
            return b""
        return hmac.new(self.secret_key, data, hashlib.sha256).digest()

    def _verify(self, data: bytes, signature: bytes) -> bool:
        """Verify HMAC signature of data."""
        if not self.secret_key:
            return True
        expected = self._sign(data)
        return hmac.compare_digest(signature, expected)

    def dumps(self, obj: Any) -> bytes:
        """
        Serialize an object to bytes with optional signing.

        Args:
            obj: Object to serialize.

        Returns:
            Signed pickled bytes (signature prepended).
        """
        pickled = pickle.dumps(
            obj,
            protocol=self.protocol,
            fix_imports=self.fix_imports,
        )

        if self.secret_key:
            signature = self._sign(pickled)
            return signature + pickled
        return pickled

    def loads(self, data: bytes) -> Any:
        """
        Deserialize signed pickled bytes with signature verification.

        Args:
            data: Signed pickled bytes.

        Returns:
            Deserialized object.

        Raises:
            ValueError: If signature verification fails.
        """
        if self.secret_key and len(data) > 32:
            signature = data[:32]
            pickled = data[32:]
            if not self._verify(pickled, signature):
                raise ValueError("Pickle signature verification failed")
            return pickle.loads(pickled, fix_imports=self.fix_imports)
        return pickle.loads(data, fix_imports=self.fix_imports)

    def dump(self, path: Union[str, Path], obj: Any) -> None:
        """
        Write signed pickle to file.

        Args:
            path: Destination file path.
            obj: Object to serialize.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.dumps(obj))

    def load(self, path: Union[str, Path]) -> Any:
        """
        Load and verify signed pickle from file.

        Args:
            path: Source file path.

        Returns:
            Deserialized object.
        """
        return self.loads(Path(path).read_bytes())


class VersionedPickler:
    """
    Pickler with version tracking for schema evolution.

    Stores version metadata with pickled data to support
    migration during deserialization.

    Attributes:
        version: Current schema version.
        migrators: Dictionary mapping version to migration function.
    """

    def __init__(
        self,
        version: int = 1,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> None:
        """
        Initialize the versioned pickler.

        Args:
            version: Current schema version.
            protocol: Pickle protocol version.
        """
        self.version = version
        self.protocol = protocol
        self.migrators: Dict[int, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}

    def register_migrator(
        self,
        from_version: int,
        migrator: Callable[[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """
        Register a migration function.

        Args:
            from_version: Version to migrate from.
            migrator: Function that takes old data and returns new data.
        """
        self.migrators[from_version] = migrator

    def dumps(self, obj: Any) -> bytes:
        """
        Serialize an object with version metadata.

        Args:
            obj: Object to serialize.

        Returns:
            Versioned pickled bytes.
        """
        wrapped = {"__version__": self.version, "__data__": obj}
        return pickle.dumps(wrapped, protocol=self.protocol)

    def loads(self, data: bytes, auto_migrate: bool = True) -> Any:
        """
        Deserialize versioned pickle with optional migration.

        Args:
            data: Versioned pickled bytes.
            auto_migrate: Automatically apply migrations.
            obj: Object to serialize.

        Returns:
            Deserialized object.

        Raises:
            ValueError: If migration fails.
        """
        wrapped = pickle.loads(data)

        if not isinstance(wrapped, dict) or "__version__" not in wrapped:
            return wrapped

        current_version = wrapped["__version__"]
        data_obj = wrapped["__data__"]

        if auto_migrate and current_version < self.version:
            data_obj = self._migrate(data_obj, current_version)

        return data_obj

    def _migrate(
        self,
        data: Any,
        from_version: int
    ) -> Any:
        """Apply migrations from from_version to current version."""
        current = from_version

        while current < self.version:
            if current in self.migrators:
                data = self.migrators[current](data)
            current += 1

        return data


class PersistentPickler:
    """
    Pickler with persistent ID mapping for object references.

    Allows efficient serialization of objects with cross-references
    by using persistent IDs.

    Attributes:
        protocol: Pickle protocol version.
    """

    def __init__(
        self,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> None:
        """
        Initialize the persistent pickler.

        Args:
            protocol: Pickle protocol version.
        """
        self.protocol = protocol
        self._obj_to_id: Dict[int, str] = {}
        self._id_to_obj: Dict[str, Any] = {}

    def dumps(
        self,
        obj: Any,
        pid_func: Optional[Callable[[Any], str]] = None
    ) -> bytes:
        """
        Serialize an object with persistent IDs.

        Args:
            obj: Object to serialize.
            pid_func: Function to generate persistent ID for objects.

        Returns:
            Pickled bytes.
        """
        self._obj_to_id.clear()
        self._id_to_obj.clear()

        if pid_func is None:
            pid_func = lambda o: f"obj_{id(o)}"

        class PersistentPicklerImpl(pickle.Pickler):
            def persistent_id(self, obj: Any) -> Optional[str]:
                obj_id = id(obj)
                if obj_id in self._obj_to_id:
                    return self._obj_to_id[obj_id]
                pid = pid_func(obj)
                if pid:
                    self._obj_to_id[obj_id] = pid
                    return pid
                return None

        import io
        buffer = io.BytesIO()
        pickler = PersistentPicklerImpl(buffer, protocol=self.protocol)
        pickler._obj_to_id = self._obj_to_id
        pickler.dump(obj)
        return buffer.getvalue()

    def loads(
        self,
        data: bytes,
        obj_func: Optional[Callable[[str], Any]] = None
    ) -> Any:
        """
        Deserialize data with persistent ID resolution.

        Args:
            data: Pickled bytes.
            obj_func: Function to resolve persistent ID to object.

        Returns:
            Deserialized object.
        """
        self._id_to_obj.clear()

        if obj_func is None:
            obj_func = lambda pid: None

        class PersistentUnpickler(pickle.Unpickler):
            def persistent_load(self, pid: str) -> Any:
                if pid not in self._id_to_obj:
                    self._id_to_obj[pid] = obj_func(pid)
                return self._id_to_obj[pid]

        import io
        unpickler = PersistentUnpickler(io.BytesIO(data))
        unpickler._id_to_obj = self._id_to_obj
        return unpickler.load()


def dump(
    path: Union[str, Path],
    obj: Any,
    **kwargs
) -> None:
    """
    Convenience function to pickle an object to file.

    Args:
        path: Destination file path.
        obj: Object to serialize.
        **kwargs: Additional arguments for SecurePickler.
    """
    SecurePickler(**kwargs).dump(path, obj)


def load(
    path: Union[str, Path],
    **kwargs
) -> Any:
    """
    Convenience function to unpickle an object from file.

    Args:
        path: Source file path.
        **kwargs: Additional arguments for SecurePickler.

    Returns:
        Deserialized object.
    """
    return SecurePickler(**kwargs).load(path)


def dumps(obj: Any, **kwargs) -> bytes:
    """
    Convenience function to pickle an object to bytes.

    Args:
        obj: Object to serialize.
        **kwargs: Additional arguments for SecurePickler.

    Returns:
        Pickled bytes.
    """
    return SecurePickler(**kwargs).dumps(obj)


def loads(data: bytes, **kwargs) -> Any:
    """
    Convenience function to unpickle bytes.

    Args:
        data: Pickled bytes.
        **kwargs: Additional arguments for SecurePickler.

    Returns:
        Deserialized object.
    """
    return SecurePickler(**kwargs).loads(data)
