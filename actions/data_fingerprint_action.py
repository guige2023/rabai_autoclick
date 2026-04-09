"""
Data Fingerprint Action Module.

Generate and verify data fingerprints/hashes for deduplication.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class FingerprintConfig:
    """Configuration for fingerprinting."""
    algorithm: str = "xxh64"
    prefix: str = "fp"
    truncate: int = 16


@dataclass
class Fingerprint:
    """A data fingerprint."""
    value: str
    algorithm: str
    length: int


class DataFingerprintAction:
    """
    Generate unique fingerprints for data.

    Supports multiple hash algorithms and formats.
    """

    ALGORITHMS = {
        "md5": hashlib.md5,
        "sha1": hashlib.sha1,
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
        "xxh64": lambda: hashlib.blake2b(digest_size=8),
    }

    def __init__(
        self,
        config: Optional[FingerprintConfig] = None,
    ) -> None:
        self.config = config or FingerprintConfig()

    def fingerprint(
        self,
        data: Union[str, bytes, Dict[str, Any], List[Any]],
    ) -> Fingerprint:
        """
        Generate a fingerprint for data.

        Args:
            data: Data to fingerprint

        Returns:
            Fingerprint object
        """
        if isinstance(data, dict):
            content = json.dumps(data, sort_keys=True)
        elif isinstance(data, list):
            content = json.dumps(data, sort_keys=True)
        elif isinstance(data, str):
            content = data
        else:
            content = str(data)

        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = content

        return self._hash_bytes(content_bytes)

    def _hash_bytes(self, data: bytes) -> Fingerprint:
        """Hash bytes using configured algorithm."""
        algo_name = self.config.algorithm.lower()

        if algo_name == "xxh64":
            h = hashlib.blake2b(data, digest_size=8)
            value = h.hexdigest()
        elif algo_name in self.ALGORITHMS:
            h = self.ALGORITHMS[algo_name](data)
            value = h.hexdigest()
        else:
            h = hashlib.sha256(data)
            value = h.hexdigest()

        if self.config.truncate and len(value) > self.config.truncate:
            value = value[: self.config.truncate]

        full_fingerprint = f"{self.config.prefix}:{value}"

        return Fingerprint(
            value=full_fingerprint,
            algorithm=algo_name,
            length=len(full_fingerprint),
        )

    def fingerprint_file(self, file_path: str) -> Fingerprint:
        """
        Generate fingerprint for a file.

        Args:
            file_path: Path to file

        Returns:
            Fingerprint object
        """
        hasher = hashlib.blake2b(digest_size=8)

        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)

        value = hasher.hexdigest()[: self.config.truncate or 16]
        full_fingerprint = f"{self.config.prefix}:{value}"

        return Fingerprint(
            value=full_fingerprint,
            algorithm="xxh64",
            length=len(full_fingerprint),
        )

    def verify(
        self,
        data: Union[str, bytes, Dict[str, Any]],
        fingerprint: str,
    ) -> bool:
        """
        Verify data matches a fingerprint.

        Args:
            data: Data to verify
            fingerprint: Expected fingerprint

        Returns:
            True if matches
        """
        computed = self.fingerprint(data)
        return computed.value == fingerprint

    def dedupe(
        self,
        items: List[Any],
        key: Optional[Union[str, int]] = None,
    ) -> List[Any]:
        """
        Deduplicate items by fingerprint.

        Args:
            items: List of items
            key: Optional field to use for fingerprinting

        Returns:
            Deduplicated list preserving order
        """
        seen: set[str] = set()
        result: List[Any] = []

        for item in items:
            if key is not None:
                data = item[key] if isinstance(key, str) else item[key]
            else:
                data = item

            fp = self.fingerprint(data)
            if fp.value not in seen:
                seen.add(fp.value)
                result.append(item)

        return result

    def similarity(
        self,
        data1: Union[str, bytes],
        data2: Union[str, bytes],
    ) -> float:
        """
        Calculate similarity between two data items.

        Uses Jaccard similarity on character n-grams.

        Args:
            data1: First data
            data2: Second data

        Returns:
            Similarity score 0.0 to 1.0
        """
        if isinstance(data1, str):
            data1 = data1.encode("utf-8")
        if isinstance(data2, str):
            data2 = data2.encode("utf-8")

        def get_ngrams(data: bytes, n: int = 3) -> set[bytes]:
            return set(data[i : i + n] for i in range(len(data) - n + 1))

        ngrams1 = get_ngrams(data1)
        ngrams2 = get_ngrams(data2)

        if not ngrams1 or not ngrams2:
            return 0.0

        intersection = len(ngrams1 & ngrams2)
        union = len(ngrams1 | ngrams2)

        return intersection / union if union > 0 else 0.0
