"""
Data Indexing Action Module.

Provides data indexing capabilities for fast lookups
including primary, secondary, composite, and full-text indexing.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class IndexType(Enum):
    """Index types."""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    COMPOSITE = "composite"
    FULL_TEXT = "full_text"
    HASH = "hash"
    BTREE = "btree"


@dataclass
class IndexDefinition:
    """Index definition."""
    index_id: str
    name: str
    index_type: IndexType
    fields: List[str]
    unique: bool = False
    sparse: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IndexedRecord:
    """Indexed record."""
    record_id: str
    data: Dict[str, Any]
    indexed_at: datetime = field(default_factory=datetime.now)


class IndexEntry:
    """Single index entry."""

    def __init__(self, key: Any, record_ids: Set[str]):
        self.key = key
        self.record_ids = record_ids


class DataIndexer:
    """Manages data indexes."""

    def __init__(self):
        self.indexes: Dict[str, Dict[Any, IndexEntry]] = {}
        self.definitions: Dict[str, IndexDefinition] = {}
        self.records: Dict[str, IndexedRecord] = {}

    def create_index(self, definition: IndexDefinition):
        """Create an index."""
        self.definitions[definition.index_id] = definition
        self.indexes[definition.index_id] = {}

    def drop_index(self, index_id: str) -> bool:
        """Drop an index."""
        if index_id in self.indexes:
            del self.indexes[index_id]
            del self.definitions[index_id]
            return True
        return False

    def _extract_key(self, data: Dict[str, Any], fields: List[str]) -> Any:
        """Extract key from data."""
        if len(fields) == 1:
            return data.get(fields[0])
        return tuple(data.get(f) for f in fields)

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text for full-text search."""
        return re.findall(r'\w+', text.lower())

    def index_record(self, record_id: str, data: Dict[str, Any]):
        """Index a record."""
        indexed_record = IndexedRecord(record_id=record_id, data=data)
        self.records[record_id] = indexed_record

        for index_id, definition in self.definitions.items():
            key = self._extract_key(data, definition.fields)

            if key is None and definition.sparse:
                continue

            if definition.index_type == IndexType.FULL_TEXT:
                tokens = self._tokenize(str(key))
                for token in tokens:
                    if token not in self.indexes[index_id]:
                        self.indexes[index_id][token] = IndexEntry(token, set())
                    self.indexes[index_id][token].record_ids.add(record_id)
            else:
                if key not in self.indexes[index_id]:
                    self.indexes[index_id][key] = IndexEntry(key, set())
                self.indexes[index_id][key].record_ids.add(record_id)

    def remove_record(self, record_id: str):
        """Remove record from indexes."""
        if record_id not in self.records:
            return

        record = self.records[record_id]

        for index_id in self.definitions.keys():
            key = self._extract_key(record.data, self.definitions[index_id].fields)

            if self.definitions[index_id].index_type == IndexType.FULL_TEXT:
                tokens = self._tokenize(str(key))
                for token in tokens:
                    if token in self.indexes[index_id]:
                        self.indexes[index_id][token].record_ids.discard(record_id)
            else:
                if key in self.indexes[index_id]:
                    self.indexes[index_id][key].record_ids.discard(record_id)

        del self.records[record_id]

    def search(
        self,
        index_id: str,
        key: Any,
        operator: str = "eq"
    ) -> Set[str]:
        """Search index for record IDs."""
        if index_id not in self.indexes:
            return set()

        definition = self.definitions[index_id]
        results: Set[str] = set()

        if definition.index_type == IndexType.FULL_TEXT:
            if operator == "eq":
                tokens = self._tokenize(str(key))
                for token in tokens:
                    if token in self.indexes[index_id]:
                        if not results:
                            results = self.indexes[index_id][token].record_ids.copy()
                        else:
                            results &= self.indexes[index_id][token].record_ids
        else:
            if operator == "eq":
                if key in self.indexes[index_id]:
                    results = self.indexes[index_id][key].record_ids.copy()

            elif operator == "gt":
                for idx_key, entry in self.indexes[index_id].items():
                    if idx_key > key:
                        results |= entry.record_ids

            elif operator == "gte":
                for idx_key, entry in self.indexes[index_id].items():
                    if idx_key >= key:
                        results |= entry.record_ids

            elif operator == "lt":
                for idx_key, entry in self.indexes[index_id].items():
                    if idx_key < key:
                        results |= entry.record_ids

            elif operator == "lte":
                for idx_key, entry in self.indexes[index_id].items():
                    if idx_key <= key:
                        results |= entry.record_ids

            elif operator == "in":
                for k in key:
                    if k in self.indexes[index_id]:
                        results |= self.indexes[index_id][k].record_ids

        return results

    def get_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Get record by ID."""
        record = self.records.get(record_id)
        return record.data if record else None

    def get_records(self, record_ids: Set[str]) -> List[Dict[str, Any]]:
        """Get multiple records by IDs."""
        return [
            self.records[rid].data
            for rid in record_ids
            if rid in self.records
        ]


import re


def main():
    """Demonstrate data indexing."""
    indexer = DataIndexer()

    indexer.create_index(IndexDefinition(
        index_id="idx_email",
        name="Email Index",
        index_type=IndexType.HASH,
        fields=["email"],
        unique=True
    ))

    indexer.create_index(IndexDefinition(
        index_id="idx_status",
        name="Status Index",
        index_type=IndexType.BTREE,
        fields=["status"]
    ))

    indexer.index_record("1", {"email": "alice@example.com", "status": "active"})
    indexer.index_record("2", {"email": "bob@example.com", "status": "inactive"})
    indexer.index_record("3", {"email": "charlie@example.com", "status": "active"})

    results = indexer.search("idx_status", "active", "eq")
    print(f"Active users: {len(results)} records")

    results = indexer.search("idx_status", "active", operator="in")
    print(f"Results: {results}")


if __name__ == "__main__":
    main()
