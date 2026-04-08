"""
Data Hashing Action Module.

Provides data hashing capabilities for integrity
verification, deduplication, and checksums.
"""

from typing import Any, BinaryIO, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import hashlib
import hmac
import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Hash algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE3 = "blake3"


@dataclass
class HashResult:
    """Result of hashing operation."""
    algorithm: HashAlgorithm
    hash_value: str
    original_size: int
    computation_time: float = 0.0


@dataclass
class HMACResult:
    """Result of HMAC operation."""
    algorithm: HashAlgorithm
    signature: str
    valid: bool = True


class DataHasher:
    """Hashes data using various algorithms."""

    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.algorithm = algorithm

    def _get_hash_func(self) -> Callable:
        """Get hash function for algorithm."""
        if self.algorithm == HashAlgorithm.MD5:
            return hashlib.md5
        elif self.algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1
        elif self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256
        elif self.algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b
        elif self.algorithm == HashAlgorithm.BLAKE3:
            import hashlib
            return hashlib.blake2b
        return hashlib.sha256

    def hash_bytes(self, data: bytes) -> HashResult:
        """Hash bytes data."""
        import time
        start = time.time()

        hash_func = self._get_hash_func()
        hasher = hash_func()
        hasher.update(data)

        computation_time = time.time() - start

        return HashResult(
            algorithm=self.algorithm,
            hash_value=hasher.hexdigest(),
            original_size=len(data),
            computation_time=computation_time
        )

    def hash_string(self, data: str, encoding: str = "utf-8") -> HashResult:
        """Hash string data."""
        return self.hash_bytes(data.encode(encoding))

    def hash_file(self, file_path: str, chunk_size: int = 8192) -> HashResult:
        """Hash file contents."""
        import time
        start = time.time()

        hash_func = self._get_hash_func()
        hasher = hash_func()

        file_size = 0
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                file_size += len(chunk)

        computation_time = time.time() - start

        return HashResult(
            algorithm=self.algorithm,
            hash_value=hasher.hexdigest(),
            original_size=file_size,
            computation_time=computation_time
        )

    def hash_dict(self, data: Dict[str, Any]) -> HashResult:
        """Hash dictionary data."""
        import json
        json_str = json.dumps(data, sort_keys=True)
        return self.hash_string(json_str)


class HMACGenerator:
    """Generates HMAC signatures."""

    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.algorithm = algorithm

    def _get_hash_func(self) -> Callable:
        """Get hash function for algorithm."""
        if self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256
        elif self.algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b
        return hashlib.sha256

    def generate(
        self,
        data: bytes,
        key: bytes
    ) -> HMACResult:
        """Generate HMAC signature."""
        hash_func = self._get_hash_func()
        signature = hmac.new(key, data, hash_func).hexdigest()

        return HMACResult(
            algorithm=self.algorithm,
            signature=signature,
            valid=True
        )

    def verify(
        self,
        data: bytes,
        key: bytes,
        signature: str
    ) -> HMACResult:
        """Verify HMAC signature."""
        expected = self.generate(data, key)

        return HMACResult(
            algorithm=self.algorithm,
            signature=signature,
            valid=hmac.compare_digest(expected.signature, signature)
        )


class ChunkedHasher:
    """Hashes data in chunks for large files."""

    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.algorithm = algorithm

    async def hash_stream(
        self,
        stream: BinaryIO,
        chunk_size: int = 8192
    ) -> HashResult:
        """Hash data from stream."""
        import time
        start = time.time()

        hash_func = self._get_hash_func()
        hasher = hash_func()

        total_size = 0
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
            total_size += len(chunk)

        computation_time = time.time() - start

        return HashResult(
            algorithm=self.algorithm,
            hash_value=hasher.hexdigest(),
            original_size=total_size,
            computation_time=computation_time
        )

    def _get_hash_func(self) -> Callable:
        """Get hash function."""
        if self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256
        elif self.algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b
        return hashlib.sha256


class HashVerifier:
    """Verifies data integrity using hashes."""

    def __init__(self):
        self.tracked_hashes: Dict[str, HashResult] = {}

    def track(self, identifier: str, hash_result: HashResult):
        """Track a hash."""
        self.tracked_hashes[identifier] = hash_result

    def verify(
        self,
        identifier: str,
        data: bytes,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256
    ) -> bool:
        """Verify data against tracked hash."""
        if identifier not in self.tracked_hashes:
            return False

        stored = self.tracked_hashes[identifier]
        hasher = DataHasher(algorithm)
        computed = hasher.hash_bytes(data)

        return stored.hash_value == computed.hash_value


def main():
    """Demonstrate data hashing."""
    hasher = DataHasher(HashAlgorithm.SHA256)

    result = hasher.hash_string("Hello, World!")
    print(f"SHA256: {result.hash_value}")
    print(f"Size: {result.original_size} bytes")

    hmac_gen = HMACGenerator(HashAlgorithm.SHA256)
    hmac_result = hmac_gen.generate(b"Hello, World!", b"secret_key")
    print(f"HMAC: {hmac_result.signature}")


if __name__ == "__main__":
    main()
