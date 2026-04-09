"""Data Fingerprint Action Module.

Generates cryptographic fingerprints for data integrity verification,
duplicate detection, and content identification.
"""

from __future__ import annotations

import hashlib
import logging
import mmh3
import xxhash
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Supported hash algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    XXHASH64 = "xxhash64"
    XXHASH128 = "xxhash128"
    MURMUR3 = "murmur3"


@dataclass
class Fingerprint:
    """Represents a data fingerprint."""
    algorithm: HashAlgorithm
    value: str
    bits: int
    created_at: float = field(default_factory=lambda: __import__("time").time())


@dataclass
class MultiFingerprint:
    """Multiple fingerprints for the same data."""
    fingerprints: Dict[HashAlgorithm, Fingerprint] = field(default_factory=dict)
    combined_hash: Optional[str] = None


class FingerprintGenerator:
    """Generates fingerprints using various hash algorithms."""

    @staticmethod
    def generate_md5(data: Union[str, bytes]) -> str:
        """Generate MD5 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return hashlib.md5(data).hexdigest()

    @staticmethod
    def generate_sha1(data: Union[str, bytes]) -> str:
        """Generate SHA1 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return hashlib.sha1(data).hexdigest()

    @staticmethod
    def generate_sha256(data: Union[str, bytes]) -> str:
        """Generate SHA256 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def generate_sha512(data: Union[str, bytes]) -> str:
        """Generate SHA512 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return hashlib.sha512(data).hexdigest()

    @staticmethod
    def generate_xxhash64(data: Union[str, bytes]) -> str:
        """Generate xxHash64 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return xxhash.xxh64(data).hexstring()

    @staticmethod
    def generate_xxhash128(data: Union[str, bytes]) -> str:
        """Generate xxHash128 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return xxhash.xxh128(data).hexstring()

    @staticmethod
    def generate_murmur3(data: Union[str, bytes], seed: int = 0) -> str:
        """Generate MurmurHash3 fingerprint."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return str(mmh3.hash64(data, seed))

    @classmethod
    def generate(
        cls,
        data: Union[str, bytes],
        algorithm: HashAlgorithm
    ) -> str:
        """Generate fingerprint using specified algorithm."""
        generators = {
            HashAlgorithm.MD5: cls.generate_md5,
            HashAlgorithm.SHA1: cls.generate_sha1,
            HashAlgorithm.SHA256: cls.generate_sha256,
            HashAlgorithm.SHA512: cls.generate_sha512,
            HashAlgorithm.XXHASH64: cls.generate_xxhash64,
            HashAlgorithm.XXHASH128: cls.generate_xxhash128,
            HashAlgorithm.MURMUR3: cls.generate_murmur3,
        }
        generator = generators.get(algorithm)
        if not generator:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        return generator(data)

    @classmethod
    def generate_multi(
        cls,
        data: Union[str, bytes],
        algorithms: Optional[List[HashAlgorithm]] = None
    ) -> MultiFingerprint:
        """Generate multiple fingerprints for the same data."""
        if algorithms is None:
            algorithms = [HashAlgorithm.SHA256, HashAlgorithm.XXHASH64]

        result = MultiFingerprint()
        for algo in algorithms:
            fp = Fingerprint(
                algorithm=algo,
                value=cls.generate(data, algo),
                bits=cls._get_bit_size(algo)
            )
            result.fingerprints[algo] = fp

        # Create combined hash from all fingerprints
        combined = "".join(fp.value for fp in sorted(
            result.fingerprints.values(),
            key=lambda f: f.algorithm.value
        ))
        result.combined_hash = hashlib.sha256(combined.encode()).hexdigest()

        return result

    @staticmethod
    def _get_bit_size(algorithm: HashAlgorithm) -> int:
        """Return bit size for each algorithm."""
        sizes = {
            HashAlgorithm.MD5: 128,
            HashAlgorithm.SHA1: 160,
            HashAlgorithm.SHA256: 256,
            HashAlgorithm.SHA512: 512,
            HashAlgorithm.XXHASH64: 64,
            HashAlgorithm.XXHASH128: 128,
            HashAlgorithm.MURMUR3: 64,
        }
        return sizes.get(algorithm, 0)


class DataFingerprintCache:
    """Caches fingerprints for deduplication."""

    def __init__(self, max_entries: int = 10000):
        self._cache: Dict[str, Fingerprint] = {}
        self._max_entries = max_entries
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Fingerprint]:
        """Get cached fingerprint."""
        fp = self._cache.get(key)
        if fp:
            self._hits += 1
        else:
            self._misses += 1
        return fp

    def set(self, key: str, fingerprint: Fingerprint) -> None:
        """Cache a fingerprint."""
        if len(self._cache) >= self._max_entries:
            # Simple eviction: remove first item
            first_key = next(iter(self._cache))
            self._cache.pop(first_key)
        self._cache[key] = fingerprint

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def stats(self) -> Dict[str, int]:
        """Return cache statistics."""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 2),
            "size": len(self._cache),
            "max_size": self._max_entries
        }


class DataFingerprintAction:
    """Main action class for data fingerprinting."""

    def __init__(self):
        self._generator = FingerprintGenerator()
        self._cache = DataFingerprintCache()
        self._default_algorithm = HashAlgorithm.SHA256

    def set_default_algorithm(self, algorithm: HashAlgorithm) -> None:
        """Set the default hash algorithm."""
        self._default_algorithm = algorithm

    def generate(
        self,
        data: Union[str, bytes],
        algorithm: Optional[HashAlgorithm] = None
    ) -> Fingerprint:
        """Generate a single fingerprint."""
        algo = algorithm or self._default_algorithm
        return Fingerprint(
            algorithm=algo,
            value=self._generator.generate(data, algo),
            bits=self._generator._get_bit_size(algo)
        )

    def generate_multi(
        self,
        data: Union[str, bytes],
        algorithms: Optional[List[HashAlgorithm]] = None
    ) -> MultiFingerprint:
        """Generate multiple fingerprints."""
        return self._generator.generate_multi(data, algorithms)

    def fingerprint_file(self, file_path: str, algorithm: Optional[HashAlgorithm] = None) -> Fingerprint:
        """Generate fingerprint for a file."""
        with open(file_path, "rb") as f:
            data = f.read()
        return self.generate(data, algorithm)

    def check_duplicate(
        self,
        data: Union[str, bytes],
        algorithm: Optional[HashAlgorithm] = None
    ) -> tuple[bool, Optional[Fingerprint]]:
        """Check if data is duplicate using cache."""
        # Create cache key from data hash
        cache_key = self._generator.generate(data, HashAlgorithm.SHA256)

        cached = self._cache.get(cache_key)
        if cached:
            return True, cached

        fp = self.generate(data, algorithm)
        self._cache.set(cache_key, fp)
        return False, fp

    def get_cache_stats(self) -> Dict[str, int]:
        """Return cache statistics."""
        return self._cache.stats()

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data fingerprint action.

        Args:
            context: Dictionary containing:
                - data: Data to fingerprint
                - algorithm: Hash algorithm to use
                - operation: Operation type (generate, multi, duplicate, stats)

        Returns:
            Dictionary with fingerprint results.
        """
        operation = context.get("operation", "generate")

        if operation == "generate":
            algo_str = context.get("algorithm", "sha256")
            try:
                algo = HashAlgorithm(algo_str)
            except ValueError:
                algo = self._default_algorithm

            fp = self.generate(context.get("data", ""), algo)
            return {
                "success": True,
                "fingerprint": {
                    "algorithm": fp.algorithm.value,
                    "value": fp.value,
                    "bits": fp.bits
                }
            }

        elif operation == "multi":
            algorithms_str = context.get("algorithms", ["sha256", "xxhash64"])
            algorithms = [HashAlgorithm(a) for a in algorithms_str]
            mfp = self.generate_multi(context.get("data", ""), algorithms)
            return {
                "success": True,
                "fingerprints": {
                    algo.value: fp.value
                    for algo, fp in mfp.fingerprints.items()
                },
                "combined_hash": mfp.combined_hash
            }

        elif operation == "duplicate":
            is_dup, fp = self.check_duplicate(context.get("data", ""))
            return {
                "success": True,
                "is_duplicate": is_dup,
                "fingerprint": {
                    "algorithm": fp.algorithm.value,
                    "value": fp.value
                } if fp else None
            }

        elif operation == "stats":
            return {
                "success": True,
                "cache_stats": self.get_cache_stats()
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
