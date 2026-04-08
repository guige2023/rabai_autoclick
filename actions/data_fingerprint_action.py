"""
Data Fingerprint Action - Generates fingerprints for data records.

This module provides fingerprinting capabilities for data
profiling, uniqueness, and change detection.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FingerprintResult:
    """Result of fingerprinting."""
    fingerprint: str
    algorithm: str
    record_count: int


class DataFingerprinter:
    """Generates fingerprints for data."""
    
    def __init__(self, algorithm: str = "sha256") -> None:
        self.algorithm = algorithm
    
    def fingerprint_record(self, record: dict[str, Any]) -> str:
        """Generate fingerprint for a record."""
        normalized = self._normalize_record(record)
        content = json.dumps(normalized, sort_keys=True, default=str)
        return self._hash_string(content)
    
    def fingerprint_dataset(self, records: list[dict[str, Any]]) -> str:
        """Generate fingerprint for entire dataset."""
        fingerprints = [self.fingerprint_record(r) for r in records]
        combined = "".join(sorted(fingerprints))
        return self._hash_string(combined)
    
    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize record for consistent fingerprinting."""
        result = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                result[key] = json.dumps(value, sort_keys=True)
            elif value is not None:
                result[key] = value
        return result
    
    def _hash_string(self, content: str) -> str:
        """Hash a string."""
        hasher = getattr(hashlib, self.algorithm)()
        hasher.update(content.encode("utf-8"))
        return hasher.hexdigest()


class DataFingerprintAction:
    """Data fingerprint action for automation workflows."""
    
    def __init__(self, algorithm: str = "sha256") -> None:
        self.fingerprinter = DataFingerprinter(algorithm)
    
    def fingerprint(self, record: dict[str, Any]) -> str:
        """Generate fingerprint for a record."""
        return self.fingerprinter.fingerprint_record(record)
    
    async def fingerprint_dataset(self, records: list[dict[str, Any]]) -> FingerprintResult:
        """Generate fingerprint for dataset."""
        fingerprint = self.fingerprinter.fingerprint_dataset(records)
        return FingerprintResult(
            fingerprint=fingerprint,
            algorithm=self.fingerprinter.algorithm,
            record_count=len(records),
        )


__all__ = ["FingerprintResult", "DataFingerprinter", "DataFingerprintAction"]
