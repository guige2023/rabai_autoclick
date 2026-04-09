"""Data Indexer Action Module.

Provides multi-dimensional indexing, search, and retrieval
for structured data with filtering and ranking capabilities.
"""

from __future__ import annotations

import sys
import os
import time
import threading
import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IndexType(Enum):
    """Types of indexes."""
    HASH = "hash"
    BTREE = "btree"
    INVERTED = "inverted"
    COMPOSITE = "composite"
    FULLTEXT = "fulltext"


@dataclass
class IndexConfig:
    """Configuration for an index."""
    index_id: str
    index_type: IndexType
    fields: List[str]
    unique: bool = False
    sparse: bool = True
    case_sensitive: bool = True


@dataclass
class IndexedRecord:
    """A record stored in the index."""
    record_id: str
    data: Dict[str, Any]
    indexed_at: float = field(default_factory=time.time)


class InMemoryIndex:
    """In-memory index implementation with multiple index types."""

    def __init__(self, config: IndexConfig):
        self.config = config
        self._hash_index: Dict[str, str] = {}
        self._inverted_index: Dict[str, Set[str]] = defaultdict(set)
        self._composite_index: Dict[Tuple, str] = {}
        self._fulltext_index: Dict[str, Set[str]] = defaultdict(set)
        self._records: Dict[str, IndexedRecord] = {}
        self._lock = threading.RLock()

    def insert(self, record_id: str, data: Dict[str, Any]) -> bool:
        """Insert or update a record in the index."""
        with self._lock:
            if self.config.unique and record_id in self._records:
                return False

            for field_name in self.config.fields:
                value = self._get_nested_value(data, field_name)
                if value is None and self.config.sparse:
                    continue

                str_value = str(value) if not self.config.case_sensitive else str(value).lower()

                if self.config.index_type == IndexType.HASH:
                    self._hash_index[str_value] = record_id

                elif self.config.index_type == IndexType.INVERTED:
                    self._inverted_index[str_value].add(record_id)

                elif self.config.index_type == IndexType.COMPOSITE:
                    values = []
                    for fn in self.config.fields:
                        v = self._get_nested_value(data, fn)
                        values.append(str(v).lower() if not self.config.case_sensitive else str(v))
                    key = tuple(values)
                    self._composite_index[key] = record_id

                elif self.config.index_type == IndexType.FULLTEXT:
                    words = str_value.split()
                    for word in words:
                        if len(word) > 2:
                            self._fulltext_index[word].add(record_id)

            self._records[record_id] = IndexedRecord(record_id=record_id, data=data)
            return True

    def search(self, query: Dict[str, Any]) -> List[str]:
        """Search for records matching the query."""
        with self._lock:
            result_sets = []
            negate = query.get("_not", False)

            for field_name, value in query.items():
                if field_name.startswith("_"):
                    continue

                str_value = str(value) if not self.config.case_sensitive else str(value).lower()

                if self.config.index_type == IndexType.HASH:
                    matched = {v for k, v in self._hash_index.items() if k == str_value}

                elif self.config.index_type == IndexType.INVERTED:
                    matched = set()
                    if str_value in self._inverted_index:
                        matched = self._inverted_index[str_value]
                    if "*" in str_value or "?" in str_value:
                        pattern = str_value.replace("*", "").replace("?", "")
                        matched = {
                            rid for k, rids in self._inverted_index.items()
                            for rid in rids if pattern in k
                        }

                elif self.config.index_type == IndexType.FULLTEXT:
                    words = str_value.split()
                    matched = None
                    for word in words:
                        word = word.lower() if not self.config.case_sensitive else word
                        word_set = self._fulltext_index.get(word, set())
                        if matched is None:
                            matched = word_set
                        else:
                            matched = matched & word_set

                elif self.config.index_type == IndexType.COMPOSITE:
                    matched = set()
                    for key, rid in self._composite_index.items():
                        if str_value in key:
                            matched.add(rid)
                else:
                    matched = set()

                if matched:
                    result_sets.append(matched)

            if not result_sets:
                return list(self._records.keys()) if not negate else []

            result = set.intersection(*result_sets) if result_sets else set()
            if negate:
                return [rid for rid in self._records.keys() if rid not in result]
            return list(result)

    def delete(self, record_id: str) -> bool:
        """Delete a record from the index."""
        with self._lock:
            if record_id not in self._records:
                return False

            record = self._records[record_id]

            for field_name in self.config.fields:
                value = self._get_nested_value(record.data, field_name)
                if value is None:
                    continue
                str_value = str(value) if not self.config.case_sensitive else str(value).lower()

                if self.config.index_type == IndexType.HASH:
                    self._hash_index.pop(str_value, None)

                elif self.config.index_type == IndexType.INVERTED:
                    self._inverted_index[str_value].discard(record_id)

                elif self.config.index_type == IndexType.COMPOSITE:
                    values = []
                    for fn in self.config.fields:
                        v = self._get_nested_value(record.data, fn)
                        values.append(str(v).lower() if not self.config.case_sensitive else str(v))
                    key = tuple(values)
                    self._composite_index.pop(key, None)

                elif self.config.index_type == IndexType.FULLTEXT:
                    words = str_value.split()
                    for word in words:
                        if len(word) > 2:
                            self._fulltext_index[word].discard(record_id)

            del self._records[record_id]
            return True

    def _get_nested_value(self, data: Dict, field_path: str) -> Any:
        """Get a value from nested dict using dot notation."""
        keys = field_path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class DataIndexerAction(BaseAction):
    """Index and search structured data with multiple index types.

    Provides high-performance in-memory indexing with support for
    hash, inverted, composite, and full-text indexes.
    """
    action_type = "data_indexer"
    display_name = "数据索引器"
    description = "多维度数据索引和搜索，支持哈希、倒排和全文索引"

    def __init__(self):
        super().__init__()
        self._indexes: Dict[str, InMemoryIndex] = {}
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute indexing operation.

        Args:
            context: Execution context.
            params: Dict with keys: action, index_id, data, query, etc.

        Returns:
            ActionResult with indexing result.
        """
        action = params.get("action", "create")

        if action == "create":
            return self._create_index(params)
        elif action == "insert":
            return self._insert_records(context, params)
        elif action == "search":
            return self._search(params)
        elif action == "delete":
            return self._delete_record(params)
        elif action == "rebuild":
            return self._rebuild_index(params)
        elif action == "stats":
            return self._get_index_stats(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _create_index(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new index."""
        index_id = params.get("index_id", "")
        index_type_str = params.get("index_type", "hash").lower()
        fields = params.get("fields", [])
        unique = params.get("unique", False)
        sparse = params.get("sparse", True)
        case_sensitive = params.get("case_sensitive", True)

        if not index_id:
            return ActionResult(success=False, message="index_id is required")
        if not fields:
            return ActionResult(success=False, message="fields list is required")

        try:
            index_type = IndexType[index_type_str.upper()]
        except KeyError:
            return ActionResult(
                success=False,
                message=f"Unknown index type: {index_type_str}"
            )

        config = IndexConfig(
            index_id=index_id,
            index_type=index_type,
            fields=fields,
            unique=unique,
            sparse=sparse,
            case_sensitive=case_sensitive
        )

        with self._lock:
            self._indexes[index_id] = InMemoryIndex(config)

        return ActionResult(
            success=True,
            message=f"Index '{index_id}' created: {index_type.value} on {fields}",
            data={"index_id": index_id, "type": index_type.value, "fields": fields}
        )

    def _insert_records(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Insert records into an index."""
        index_id = params.get("index_id", "")
        records = params.get("records", [])
        save_to_var = params.get("save_to_var", None)

        if not index_id:
            return ActionResult(success=False, message="index_id is required")
        if not isinstance(records, list):
            return ActionResult(
                success=False,
                message="records must be a list"
            )

        with self._lock:
            index = self._indexes.get(index_id)
            if not index:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_id}' not found"
                )

            inserted = 0
            failed = 0
            for rec in records:
                record_id = rec.get("id") or rec.get("_id") or hashlib.md5(
                    json.dumps(rec, sort_keys=True).encode()
                ).hexdigest()[:12]
                if index.insert(record_id, rec):
                    inserted += 1
                else:
                    failed += 1

        result_data = {
            "index_id": index_id,
            "inserted": inserted,
            "failed": failed,
            "total_records": len(index._records)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=failed == 0,
            message=f"Indexed {inserted} records, {failed} failed",
            data=result_data
        )

    def _search(self, params: Dict[str, Any]) -> ActionResult:
        """Search for records in an index."""
        index_id = params.get("index_id", "")
        query = params.get("query", {})
        limit = int(params.get("limit", 100))
        offset = int(params.get("offset", 0))
        sort_by = params.get("sort_by", None)
        save_to_var = params.get("save_to_var", None)

        if not index_id:
            return ActionResult(success=False, message="index_id is required")

        with self._lock:
            index = self._indexes.get(index_id)
            if not index:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_id}' not found"
                )

            record_ids = index.search(query)

            records = []
            for rid in record_ids:
                rec = index._records.get(rid)
                if rec:
                    records.append({"id": rid, **rec.data})

        if sort_by:
            reverse = params.get("sort_desc", False)
            records.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

        total = len(records)
        paginated = records[offset:offset + limit]

        result_data = {
            "index_id": index_id,
            "query": query,
            "total": total,
            "returned": len(paginated),
            "offset": offset,
            "limit": limit,
            "records": paginated
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Search returned {len(paginated)}/{total} records",
            data=result_data
        )

    def _delete_record(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a record from an index."""
        index_id = params.get("index_id", "")
        record_id = params.get("record_id", "")

        if not index_id or not record_id:
            return ActionResult(
                success=False,
                message="index_id and record_id are required"
            )

        with self._lock:
            index = self._indexes.get(index_id)
            if not index:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_id}' not found"
                )

            deleted = index.delete(record_id)

        return ActionResult(
            success=deleted,
            message=f"Record '{record_id}' {'deleted' if deleted else 'not found'}"
        )

    def _rebuild_index(self, params: Dict[str, Any]) -> ActionResult:
        """Rebuild an index from scratch."""
        index_id = params.get("index_id", "")

        if not index_id:
            return ActionResult(success=False, message="index_id is required")

        with self._lock:
            index = self._indexes.get(index_id)
            if not index:
                return ActionResult(
                    success=False,
                    message=f"Index '{index_id}' not found"
                )

            records = list(index._records.values())
            old_config = index.config

            new_index = InMemoryIndex(old_config)
            for rec in records:
                new_index.insert(rec.record_id, rec.data)

            self._indexes[index_id] = new_index

        return ActionResult(
            success=True,
            message=f"Index '{index_id}' rebuilt with {len(records)} records"
        )

    def _get_index_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get statistics for an index or all indexes."""
        index_id = params.get("index_id", None)
        save_to_var = params.get("save_to_var", None)

        with self._lock:
            if index_id:
                index = self._indexes.get(index_id)
                if not index:
                    return ActionResult(
                        success=False,
                        message=f"Index '{index_id}' not found"
                    )
                data = {
                    "index_id": index_id,
                    "type": index.config.index_type.value,
                    "fields": index.config.fields,
                    "unique": index.config.unique,
                    "record_count": len(index._records),
                    "index_size": {
                        "hash_entries": len(index._hash_index),
                        "inverted_keys": len(index._inverted_index),
                        "composite_keys": len(index._composite_index),
                        "fulltext_keys": len(index._fulltext_index)
                    }
                }
            else:
                data = {
                    "indexes": {
                        iid: {
                            "type": idx.config.index_type.value,
                            "fields": idx.config.fields,
                            "records": len(idx._records)
                        }
                        for iid, idx in self._indexes.items()
                    },
                    "total_indexes": len(self._indexes)
                }

        if save_to_var:
            context.variables[save_to_var] = data

        return ActionResult(success=True, message="Stats retrieved", data=data)

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "index_id": None,
            "index_type": "hash",
            "fields": [],
            "records": [],
            "query": {},
            "record_id": None,
            "unique": False,
            "sparse": True,
            "case_sensitive": True,
            "limit": 100,
            "offset": 0,
            "sort_by": None,
            "sort_desc": False,
            "save_to_var": None
        }


import json
