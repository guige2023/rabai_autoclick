"""
Data fingerprint action for data integrity verification.

Provides hash-based fingerprinting and change detection.
"""

from typing import Any, Dict, List, Optional
import hashlib
import json
import time


class DataFingerprintAction:
    """Data fingerprinting for integrity and change detection."""

    def __init__(
        self,
        algorithm: str = "sha256",
        chunk_size: int = 8192,
    ) -> None:
        """
        Initialize data fingerprint action.

        Args:
            algorithm: Hash algorithm to use
            chunk_size: Chunk size for streaming fingerprints
        """
        self.algorithm = algorithm
        self.chunk_size = chunk_size
        self._fingerprints: Dict[str, Dict[str, Any]] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute fingerprint operation.

        Args:
            params: Dictionary containing:
                - operation: 'compute', 'verify', 'track', 'compare'
                - data: Data to fingerprint
                - key: Identifier for the fingerprint
                - previous_fp: Previous fingerprint for comparison

        Returns:
            Dictionary with fingerprint result
        """
        operation = params.get("operation", "compute")

        if operation == "compute":
            return self._compute_fingerprint(params)
        elif operation == "verify":
            return self._verify_fingerprint(params)
        elif operation == "track":
            return self._track_fingerprint(params)
        elif operation == "compare":
            return self._compare_fingerprints(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _compute_fingerprint(self, params: dict[str, Any]) -> dict[str, Any]:
        """Compute fingerprint of data."""
        data = params.get("data", "")
        include_timestamp = params.get("include_timestamp", False)

        if not data:
            return {"success": False, "error": "Data is required"}

        if isinstance(data, dict):
            data_str = json.dumps(data, sort_keys=True)
        else:
            data_str = str(data)

        hash_func = getattr(hashlib, self.algorithm)()
        hash_func.update(data_str.encode())

        fingerprint = hash_func.hexdigest()

        result = {
            "success": True,
            "fingerprint": fingerprint,
            "algorithm": self.algorithm,
            "length": len(data_str),
        }

        if include_timestamp:
            result["computed_at"] = time.time()

        return result

    def _verify_fingerprint(self, params: dict[str, Any]) -> dict[str, Any]:
        """Verify data against stored fingerprint."""
        data = params.get("data", "")
        expected_fingerprint = params.get("fingerprint", "")

        if not data or not expected_fingerprint:
            return {"success": False, "error": "Data and fingerprint are required"}

        compute_result = self._compute_fingerprint({"data": data})

        matches = compute_result.get("fingerprint") == expected_fingerprint

        return {
            "success": True,
            "matches": matches,
            "expected": expected_fingerprint,
            "actual": compute_result.get("fingerprint"),
        }

    def _track_fingerprint(self, params: dict[str, Any]) -> dict[str, Any]:
        """Track fingerprint for data over time."""
        key = params.get("key", "")
        data = params.get("data", "")

        if not key:
            return {"success": False, "error": "Key is required"}

        compute_result = self._compute_fingerprint({"data": data})
        fingerprint = compute_result.get("fingerprint")

        if key not in self._fingerprints:
            self._fingerprints[key] = {
                "history": [],
                "current": None,
                "first_seen": time.time(),
            }

        entry = self._fingerprints[key]
        previous_fp = entry.get("current")

        entry["history"].append({
            "fingerprint": fingerprint,
            "timestamp": time.time(),
        })

        if len(entry["history"]) > 100:
            entry["history"] = entry["history"][-100:]

        entry["current"] = fingerprint

        changed = previous_fp is not None and previous_fp != fingerprint

        return {
            "success": True,
            "key": key,
            "fingerprint": fingerprint,
            "changed": changed,
            "previous": previous_fp,
            "version": len(entry["history"]),
        }

    def _compare_fingerprints(self, params: dict[str, Any]) -> dict[str, Any]:
        """Compare two fingerprints to determine similarity."""
        fp1 = params.get("fingerprint1", "")
        fp2 = params.get("fingerprint2", "")

        if not fp1 or not fp2:
            return {"success": False, "error": "Both fingerprints are required"}

        return {
            "success": True,
            "identical": fp1 == fp2,
            "fingerprint1": fp1,
            "fingerprint2": fp2,
        }
