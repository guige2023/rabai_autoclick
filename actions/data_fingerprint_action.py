"""Data Fingerprint Action Module.

Provides data fingerprinting, deduplication, and
content-addressable storage for datasets.
"""

from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import csv
import io
from datetime import datetime


class ContentType(Enum):
    """Supported content types for fingerprinting."""
    JSON = "json"
    CSV = "csv"
    TEXT = "text"
    BINARY = "binary"
    STRUCTURED = "structured"


@dataclass
class DataFingerprint:
    """Fingerprint for a data record."""
    content_hash: str
    algorithm: str
    content_type: ContentType
    size_bytes: int
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Record:
    """A data record with fingerprint."""
    id: str
    fingerprint: DataFingerprint
    content: Any
    created_at: datetime = field(default_factory=datetime.now)
    tags: List[str] = field(default_factory=list)


@dataclass
class DatasetFingerprint:
    """Fingerprint for an entire dataset."""
    record_count: int
    total_size_bytes: int
    algorithm: str
    dataset_hash: str
    record_hashes: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ContentHasher:
    """Hashes various content types."""

    def __init__(self, algorithm: str = "sha256"):
        self.algorithm = algorithm

    def hash_bytes(self, data: bytes) -> str:
        """Hash bytes directly."""
        if self.algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif self.algorithm == "sha1":
            return hashlib.sha1(data).hexdigest()
        elif self.algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif self.algorithm == "blake2b":
            return hashlib.blake2b(data).hexdigest()
        return hashlib.sha256(data).hexdigest()

    def hash_json(self, data: Any, sort_keys: bool = True) -> str:
        """Hash JSON-serializable data."""
        json_str = json.dumps(data, sort_keys=sort_keys, default=str)
        return self.hash_bytes(json_str.encode('utf-8'))

    def hash_csv(self, data: List[Dict[str, Any]]) -> str:
        """Hash CSV data."""
        if not data:
            return self.hash_bytes(b"")

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)

        return self.hash_bytes(output.getvalue().encode('utf-8'))

    def hash_text(self, text: str) -> str:
        """Hash text content."""
        return self.hash_bytes(text.encode('utf-8'))

    def hash_record(self, record: Dict[str, Any]) -> str:
        """Hash a structured record."""
        return self.hash_json(record)


class DataFingerprinter:
    """Generates fingerprints for data records."""

    def __init__(self, hasher: Optional[ContentHasher] = None):
        self.hasher = hasher or ContentHasher()

    def fingerprint_json(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataFingerprint:
        """Fingerprint JSON data."""
        content_bytes = json.dumps(data, sort_keys=True, default=str).encode('utf-8')
        return DataFingerprint(
            content_hash=self.hasher.hash_bytes(content_bytes),
            algorithm=self.hasher.algorithm,
            content_type=ContentType.JSON,
            size_bytes=len(content_bytes),
            metadata=metadata or {},
        )

    def fingerprint_csv(
        self,
        data: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataFingerprint:
        """Fingerprint CSV data."""
        content_bytes = json.dumps(data, sort_keys=True, default=str).encode('utf-8')
        return DataFingerprint(
            content_hash=self.hasher.hash_bytes(content_bytes),
            algorithm=self.hasher.algorithm,
            content_type=ContentType.CSV,
            size_bytes=len(content_bytes),
            metadata=metadata or {},
        )

    def fingerprint_text(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataFingerprint:
        """Fingerprint text content."""
        content_bytes = text.encode('utf-8')
        return DataFingerprint(
            content_hash=self.hasher.hash_bytes(content_bytes),
            algorithm=self.hasher.algorithm,
            content_type=ContentType.TEXT,
            size_bytes=len(content_bytes),
            metadata=metadata or {},
        )

    def fingerprint_record(
        self,
        record: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataFingerprint:
        """Fingerprint a structured record."""
        content_hash = self.hasher.hash_record(record)
        content_bytes = json.dumps(record, sort_keys=True, default=str).encode('utf-8')
        return DataFingerprint(
            content_hash=content_hash,
            algorithm=self.hasher.algorithm,
            content_type=ContentType.STRUCTURED,
            size_bytes=len(content_bytes),
            metadata=metadata or {},
        )


class DataDeduplicator:
    """Deduplicates data records based on fingerprints."""

    def __init__(self):
        self._seen_hashes: Dict[str, List[str]] = {}
        self._duplicate_count: int = 0

    def is_duplicate(
        self,
        fingerprint: DataFingerprint,
        record_id: Optional[str] = None,
    ) -> bool:
        """Check if fingerprint has been seen."""
        content_hash = fingerprint.content_hash

        if content_hash not in self._seen_hashes:
            self._seen_hashes[content_hash] = []
            return False

        if record_id and record_id in self._seen_hashes[content_hash]:
            return True

        self._duplicate_count += 1
        return True

    def add(
        self,
        fingerprint: DataFingerprint,
        record_id: Optional[str] = None,
    ):
        """Record a fingerprint as seen."""
        content_hash = fingerprint.content_hash
        if content_hash not in self._seen_hashes:
            self._seen_hashes[content_hash] = []
        if record_id:
            self._seen_hashes[content_hash].append(record_id)

    def get_duplicate_groups(
        self,
    ) -> Dict[str, List[str]]:
        """Get all duplicate groups."""
        return {
            hash_val: ids
            for hash_val, ids in self._seen_hashes.items()
            if len(ids) > 1
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        total_unique = len(self._seen_hashes)
        total_duplicates = self._duplicate_count
        return {
            "unique_hashes": total_unique,
            "duplicate_count": total_duplicates,
            "deduplication_rate": (
                total_duplicates / (total_unique + total_duplicates)
                if total_unique + total_duplicates > 0 else 0
            ),
        }

    def reset(self):
        """Reset deduplication state."""
        self._seen_hashes.clear()
        self._duplicate_count = 0


class ContentAddressableStore:
    """Content-addressable storage for data records."""

    def __init__(self, max_size: int = 10000):
        self._storage: Dict[str, Record] = {}
        self._max_size = max_size
        self._access_times: Dict[str, datetime] = {}

    def put(
        self,
        record_id: str,
        fingerprint: DataFingerprint,
        content: Any,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Store a record."""
        if fingerprint.content_hash in self._storage:
            return False

        if len(self._storage) >= self._max_size:
            self._evict_oldest()

        record = Record(
            id=record_id,
            fingerprint=fingerprint,
            content=content,
            tags=tags or [],
        )
        self._storage[fingerprint.content_hash] = record
        self._access_times[fingerprint.content_hash] = datetime.now()
        return True

    def get(self, fingerprint: DataFingerprint) -> Optional[Record]:
        """Retrieve a record by fingerprint."""
        record = self._storage.get(fingerprint.content_hash)
        if record:
            self._access_times[fingerprint.content_hash] = datetime.now()
        return record

    def get_by_id(self, record_id: str) -> Optional[Record]:
        """Retrieve a record by ID."""
        for record in self._storage.values():
            if record.id == record_id:
                self._access_times[record.fingerprint.content_hash] = datetime.now()
                return record
        return None

    def contains(self, fingerprint: DataFingerprint) -> bool:
        """Check if fingerprint exists."""
        return fingerprint.content_hash in self._storage

    def delete(self, fingerprint: DataFingerprint) -> bool:
        """Delete a record."""
        if fingerprint.content_hash in self._storage:
            del self._storage[fingerprint.content_hash]
            del self._access_times[fingerprint.content_hash]
            return True
        return False

    def _evict_oldest(self):
        """Evict least recently accessed record."""
        if not self._storage:
            return
        oldest_hash = min(
            self._access_times.keys(),
            key=lambda h: self._access_times[h]
        )
        del self._storage[oldest_hash]
        del self._access_times[oldest_hash]

    def clear(self):
        """Clear all records."""
        self._storage.clear()
        self._access_times.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        total_size = sum(
            r.fingerprint.size_bytes for r in self._storage.values()
        )
        return {
            "record_count": len(self._storage),
            "max_size": self._max_size,
            "total_bytes": total_size,
            "utilization": len(self._storage) / self._max_size * 100,
        }


class DatasetFingerprinter:
    """Fingerprints entire datasets."""

    def __init__(self, hasher: Optional[ContentHasher] = None):
        self.hasher = hasher or ContentHasher()
        self.data_fingerprinter = DataFingerprinter(hasher)

    def fingerprint_dataset(
        self,
        records: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DatasetFingerprint:
        """Generate fingerprint for entire dataset."""
        record_hashes = []
        total_size = 0

        for record in records:
            fp = self.data_fingerprinter.fingerprint_record(record)
            record_hashes.append(fp.content_hash)
            total_size += fp.size_bytes

        dataset_content = "|".join(sorted(record_hashes))
        dataset_hash = self.hasher.hash_bytes(dataset_content.encode('utf-8'))

        return DatasetFingerprint(
            record_count=len(records),
            total_size_bytes=total_size,
            algorithm=self.hasher.algorithm,
            dataset_hash=dataset_hash,
            record_hashes=record_hashes,
            metadata=metadata or {},
        )

    def detect_dataset_changes(
        self,
        old_dataset: DatasetFingerprint,
        new_records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Detect changes between dataset fingerprints."""
        new_fp = self.fingerprint_dataset(new_records)

        added = []
        removed = []
        unchanged = []

        old_hashes = set(old_dataset.record_hashes)
        new_hashes = set(new_fp.record_hashes)

        for hash_val in old_dataset.record_hashes:
            if hash_val not in new_hashes:
                removed.append(hash_val)

        for hash_val in new_fp.record_hashes:
            if hash_val not in old_hashes:
                added.append(hash_val)
            else:
                unchanged.append(hash_val)

        return {
            "changed": old_dataset.dataset_hash != new_fp.dataset_hash,
            "added_count": len(added),
            "removed_count": len(removed),
            "unchanged_count": len(unchanged),
            "added": added[:10],
            "removed": removed[:10],
        }


class DataFingerprintAction:
    """High-level data fingerprinting action."""

    def __init__(
        self,
        fingerprinter: Optional[DataFingerprinter] = None,
        deduplicator: Optional[DataDeduplicator] = None,
        cas: Optional[ContentAddressableStore] = None,
        dataset_fingerprinter: Optional[DatasetFingerprinter] = None,
    ):
        self.fingerprinter = fingerprinter or DataFingerprinter()
        self.deduplicator = deduplicator or DataDeduplicator()
        self.cas = cas or ContentAddressableStore()
        self.dataset_fingerprinter = dataset_fingerprinter or DatasetFingerprinter()

    def fingerprint(
        self,
        data: Any,
        content_type: str = "json",
    ) -> DataFingerprint:
        """Fingerprint data based on content type."""
        if content_type == "json":
            return self.fingerprinter.fingerprint_json(data)
        elif content_type == "csv":
            return self.fingerprinter.fingerprint_csv(data)
        elif content_type == "text":
            return self.fingerprinter.fingerprint_text(data)
        else:
            return self.fingerprinter.fingerprint_json(data)

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Deduplicate records, returning unique and duplicate lists."""
        unique = []
        duplicates = []

        for record in records:
            fp = self.fingerprinter.fingerprint_record(record)
            if not self.deduplicator.is_duplicate(fp):
                unique.append(record)
                self.deduplicator.add(fp)
            else:
                duplicates.append(record)

        return unique, duplicates

    def store(
        self,
        record_id: str,
        data: Any,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Store data in CAS."""
        fp = self.fingerprinter.fingerprint_json(data)
        return self.cas.put(record_id, fp, data, tags)

    def get(self, fingerprint: DataFingerprint) -> Optional[Any]:
        """Retrieve data from CAS."""
        record = self.cas.get(fingerprint)
        return record.content if record else None

    def fingerprint_dataset(
        self,
        records: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DatasetFingerprint:
        """Fingerprint entire dataset."""
        return self.dataset_fingerprinter.fingerprint_dataset(records, metadata)

    def get_dedup_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        return self.deduplicator.get_stats()

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return self.cas.get_stats()


# Module exports
__all__ = [
    "DataFingerprintAction",
    "DataFingerprinter",
    "DatasetFingerprinter",
    "DataDeduplicator",
    "ContentAddressableStore",
    "ContentHasher",
    "DataFingerprint",
    "DatasetFingerprint",
    "Record",
    "ContentType",
]
