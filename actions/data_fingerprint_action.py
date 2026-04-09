"""
Data Fingerprinting and Change Detection Module.

Generates unique fingerprints for datasets and data structures,
enabling change detection, deduplication, and integrity verification
for data quality monitoring.
"""

from typing import (
    Dict, List, Optional, Any, Tuple, Set, Union,
    Callable, Hashable
)
from dataclasses import dataclass, field
from enum import Enum, auto
import hashlib
import json
import zlib
from collections import Counter
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class FingerprintType(Enum):
    """Types of fingerprint algorithms."""
    MD5 = auto()
    SHA1 = auto()
    SHA256 = auto()
    XXHASH = auto()
    CRC32 = auto()
    SIMHASH = auto()
    MINHASH = auto()


@dataclass
class DataFingerprint:
    """Represents a computed data fingerprint."""
    value: str
    algorithm: FingerprintType
    size_bytes: int
    computed_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        return self.value
    
    def __len__(self) -> int:
        return len(self.value)


@dataclass
class ChangeRecord:
    """Record of detected data change."""
    timestamp: datetime
    change_type: str  # added, removed, modified
    path: str  # JSON path to changed element
    old_value: Any = None
    new_value: Any = None
    fingerprint_before: Optional[DataFingerprint] = None
    fingerprint_after: Optional[DataFingerprint] = None


class ContentHasher:
    """Handles content hashing operations."""
    
    HASH_FUNCTIONS = {
        FingerprintType.MD5: hashlib.md5,
        FingerprintType.SHA1: hashlib.sha1,
        FingerprintType.SHA256: hashlib.sha256,
        FingerprintType.CRC32: lambda: zlib.crc32(b""),
    }
    
    @classmethod
    def hash_bytes(
        cls,
        data: bytes,
        algorithm: FingerprintType = FingerprintType.SHA256
    ) -> str:
        """Hash raw bytes using specified algorithm."""
        if algorithm == FingerprintType.CRC32:
            return str(zlib.crc32(data) & 0xFFFFFFFF)
        
        hasher = cls.HASH_FUNCTIONS.get(algorithm, hashlib.sha256)()
        hasher.update(data)
        return hasher.hexdigest()
    
    @classmethod
    def hash_string(
        cls,
        data: str,
        algorithm: FingerprintType = FingerprintType.SHA256
    ) -> str:
        """Hash string data."""
        return cls.hash_bytes(data.encode("utf-8"), algorithm)
    
    @classmethod
    def hash_json(
        cls,
        data: Any,
        algorithm: FingerprintType = FingerprintType.SHA256,
        sort_keys: bool = True
    ) -> str:
        """Hash JSON-serializable data."""
        try:
            json_str = json.dumps(data, sort_keys=sort_keys, default=str)
            return cls.hash_string(json_str, algorithm)
        except (TypeError, ValueError) as e:
            logger.error(f"JSON serialization failed: {e}")
            return cls.hash_string(str(data), algorithm)


class DataFingerprinter:
    """
    Generates fingerprints for data structures and datasets.
    
    Supports multiple algorithms for different use cases
    including quick checksums and similarity detection.
    """
    
    def __init__(self, default_algorithm: FingerprintType = FingerprintType.SHA256):
        self.default_algorithm = default_algorithm
        self.hasher = ContentHasher()
    
    def fingerprint(
        self,
        data: Any,
        algorithm: Optional[FingerprintType] = None
    ) -> DataFingerprint:
        """
        Generate fingerprint for data.
        
        Args:
            data: Data to fingerprint
            algorithm: Hash algorithm to use
            
        Returns:
            DataFingerprint object
        """
        algo = algorithm or self.default_algorithm
        
        # Serialize data to bytes
        if isinstance(data, bytes):
            serialized = data
        elif isinstance(data, str):
            serialized = data.encode("utf-8")
        else:
            serialized = json.dumps(data, sort_keys=True, default=str).encode("utf-8")
        
        hash_value = self.hasher.hash_bytes(serialized, algo)
        
        return DataFingerprint(
            value=hash_value,
            algorithm=algo,
            size_bytes=len(serialized)
        )
    
    def fingerprint_file(self, file_path: str) -> DataFingerprint:
        """Generate fingerprint for file."""
        with open(file_path, "rb") as f:
            content = f.read()
        
        return self.fingerprint(content)
    
    def fingerprint_schema(
        self,
        schema: Dict[str, Any]
    ) -> DataFingerprint:
        """Generate fingerprint for data schema."""
        # Sort keys and normalize schema representation
        schema_repr = self._normalize_schema(schema)
        return self.fingerprint(schema_repr, FingerprintType.SHA256)
    
    def _normalize_schema(self, schema: Dict[str, Any]) -> str:
        """Normalize schema for consistent fingerprinting."""
        normalized = {}
        for key, value in sorted(schema.items()):
            if isinstance(value, dict):
                normalized[key] = self._normalize_schema(value)
            elif isinstance(value, list):
                normalized[key] = tuple(value) if value else ()
            else:
                normalized[key] = value
        return json.dumps(normalized, sort_keys=True)


class SimHashCalculator:
    """SimHash implementation for near-duplicate detection."""
    
    def __init__(self, hash_size: int = 64) -> None:
        self.hash_size = hash_size
    
    def calculate(self, features: List[str]) -> int:
        """
        Calculate SimHash for feature set.
        
        Args:
            features: List of feature strings
            
        Returns:
            SimHash as 64-bit integer
        """
        v = [0] * self.hash_size
        
        for feature in features:
            hash_val = int(hashlib.md5(feature.encode()).hexdigest(), 16)
            
            for i in range(self.hash_size):
                bit = (hash_val >> i) & 1
                v[i] += 1 if bit else -1
        
        result = 0
        for i in range(self.hash_size):
            if v[i] > 0:
                result |= (1 << i)
        
        return result
    
    def hamming_distance(self, hash1: int, hash2: int) -> int:
        """Calculate Hamming distance between two hashes."""
        xor = hash1 ^ hash2
        return bin(xor).count("1")
    
    def are_similar(
        self,
        hash1: int,
        hash2: int,
        threshold: int = 3
    ) -> bool:
        """Check if two hashes are similar within threshold."""
        return self.hamming_distance(hash1, hash2) <= threshold


class MinHashCalculator:
    """MinHash for set similarity estimation."""
    
    def __init__(self, num_hashes: int = 128) -> None:
        self.num_hashes = num_hashes
    
    def calculate(self, items: Set[str]) -> List[int]:
        """
        Calculate MinHash signature for item set.
        
        Args:
            items: Set of items
            
        Returns:
            List of minhash values
        """
        if not items:
            return [float("inf")] * self.num_hashes
        
        signatures = []
        for i in range(self.num_hashes):
            min_hash = float("inf")
            for item in items:
                combined = f"{item}:{i}".encode()
                hash_val = int(hashlib.sha1(combined).hexdigest(), 16)
                min_hash = min(min_hash, hash_val)
            signatures.append(min_hash)
        
        return signatures
    
    def estimate_similarity(
        self,
        sig1: List[int],
        sig2: List[int]
    ) -> float:
        """Estimate Jaccard similarity from MinHash signatures."""
        if len(sig1) != len(sig2):
            raise ValueError("Signatures must have same length")
        
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        return matches / len(sig1)


class ChangeDetector:
    """
    Detects changes between data versions using fingerprints.
    
    Provides detailed change records for data auditing
    and synchronization.
    """
    
    def __init__(self) -> None:
        self.fingerprinter = DataFingerprinter()
    
    def detect_changes(
        self,
        old_data: Any,
        new_data: Any,
        path: str = "$"
    ) -> List[ChangeRecord]:
        """
        Detect changes between two data structures.
        
        Args:
            old_data: Previous data version
            new_data: New data version
            path: Current JSON path
            
        Returns:
            List of change records
        """
        changes = []
        
        old_type = type(old_data)
        new_type = type(new_data)
        
        if old_type != new_type:
            changes.append(ChangeRecord(
                timestamp=datetime.now(),
                change_type="modified",
                path=path,
                old_value=old_data,
                new_value=new_data
            ))
            return changes
        
        if isinstance(old_data, dict):
            all_keys = set(old_data.keys()) | set(new_data.keys())
            for key in all_keys:
                child_path = f"{path}.{key}"
                if key not in old_data:
                    changes.append(ChangeRecord(
                        timestamp=datetime.now(),
                        change_type="added",
                        path=child_path,
                        new_value=new_data[key]
                    ))
                elif key not in new_data:
                    changes.append(ChangeRecord(
                        timestamp=datetime.now(),
                        change_type="removed",
                        path=child_path,
                        old_value=old_data[key]
                    ))
                else:
                    changes.extend(self.detect_changes(
                        old_data[key], new_data[key], child_path
                    ))
        
        elif isinstance(old_data, (list, tuple)):
            old_len = len(old_data)
            new_len = len(new_data)
            
            for i in range(max(old_len, new_len)):
                child_path = f"{path}[{i}]"
                if i >= old_len:
                    changes.append(ChangeRecord(
                        timestamp=datetime.now(),
                        change_type="added",
                        path=child_path,
                        new_value=new_data[i]
                    ))
                elif i >= new_len:
                    changes.append(ChangeRecord(
                        timestamp=datetime.now(),
                        change_type="removed",
                        path=child_path,
                        old_value=old_data[i]
                    ))
                else:
                    changes.extend(self.detect_changes(
                        old_data[i], new_data[i], child_path
                    ))
        
        else:
            if old_data != new_data:
                changes.append(ChangeRecord(
                    timestamp=datetime.now(),
                    change_type="modified",
                    path=path,
                    old_value=old_data,
                    new_value=new_data
                ))
        
        return changes
    
    def has_changes(
        self,
        old_data: Any,
        new_data: Any
    ) -> bool:
        """Quick check if data has changed."""
        old_fp = self.fingerprinter.fingerprint(old_data)
        new_fp = self.fingerprinter.fingerprint(new_data)
        return old_fp.value != new_fp.value


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test fingerprinting
    fingerprinter = DataFingerprinter()
    
    data1 = {"name": "Alice", "age": 30, "scores": [85, 90, 78]}
    data2 = {"name": "Alice", "age": 31, "scores": [85, 90, 78]}
    
    fp1 = fingerprinter.fingerprint(data1)
    fp2 = fingerprinter.fingerprint(data2)
    
    print(f"Data1 fingerprint: {fp1.value[:16]}...")
    print(f"Data2 fingerprint: {fp2.value[:16]}...")
    print(f"Changed: {fp1.value != fp2.value}")
    
    # Test change detection
    detector = ChangeDetector()
    changes = detector.detect_changes(data1, data2)
    
    print(f"\nDetected {len(changes)} change(s):")
    for change in changes:
        print(f"  {change.change_type}: {change.path}")
        if change.old_value is not None:
            print(f"    old: {change.old_value}")
        if change.new_value is not None:
            print(f"    new: {change.new_value}")
