"""
Data Fingerprint Action Module.

Generates fingerprints/hashes for data integrity verification,
change detection, and deduplication using MD5, SHA-256, or custom hash functions.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class FingerprintResult:
    """Result of fingerprint computation."""
    hash: str
    algorithm: str
    size_bytes: int
    checksum: str


class DataFingerprintAction(BaseAction):
    """Generate data fingerprints for integrity and deduplication."""

    def __init__(self) -> None:
        super().__init__("data_fingerprint")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Generate fingerprint for data.

        Args:
            context: Execution context
            params: Parameters:
                - data: Data to fingerprint (str, bytes, or list/dict)
                - algorithm: hashlib algorithm (md5, sha1, sha256, sha512)
                - as_hex: Return hex string (default: True)
                - include_size: Include size in fingerprint (default: True)

        Returns:
            dict with FingerprintResult and metadata
        """
        import hashlib
        import json

        data = params.get("data")
        algorithm = params.get("algorithm", "sha256")
        as_hex = params.get("as_hex", True)
        include_size = params.get("include_size", True)

        if data is None:
            return {"error": "Data is required"}

        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        elif isinstance(data, (dict, list)):
            data_bytes = json.dumps(data, sort_keys=True).encode("utf-8")
        else:
            data_bytes = str(data).encode("utf-8")

        size_bytes = len(data_bytes)

        hash_func = getattr(hashlib, algorithm, None)
        if not hash_func:
            return {"error": f"Unsupported algorithm: {algorithm}"}

        hasher = hash_func()
        hasher.update(data_bytes)
        hash_value = hasher.hexdigest() if as_hex else hasher.digest()

        fp = FingerprintResult(
            hash=hash_value,
            algorithm=algorithm,
            size_bytes=size_bytes,
            checksum=self._compute_checksum(data_bytes)
        )

        return {
            "fingerprint": fp,
            "fingerprint_string": self._format_fingerprint(fp) if include_size else hash_value
        }

    def _compute_checksum(self, data: bytes) -> str:
        """Compute simple checksum (XOR of bytes)."""
        if not data:
            return "0"
        checksum = 0
        for b in data:
            checksum ^= b
        return str(checksum % 256)

    def _format_fingerprint(self, fp: FingerprintResult) -> str:
        """Format fingerprint for display."""
        return f"{fp.algorithm}:{fp.hash} ({fp.size_bytes} bytes)"

    def verify_fingerprint(self, data: Any, expected_hash: str, algorithm: str = "sha256") -> bool:
        """Verify data matches expected fingerprint."""
        result = self.execute({}, {"data": data, "algorithm": algorithm})
        if "error" in result:
            return False
        return result.get("fingerprint", {}).hash == expected_hash
