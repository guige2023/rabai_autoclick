"""Data Cascade Action Module.

Implements cascade delete and update operations with referential
integrity checking and rollback support for related data structures.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class CascadeRule:
    target_collection: str
    relation_field: str
    on_delete: str = "cascade"
    on_update: str = "restrict"
    filter_fn: Optional[Callable[[Any], bool]] = None


@dataclass
class CascadeResult:
    collection: str
    affected_count: int
    duration_ms: float
    success: bool
    error: Optional[str] = None


class DataCascadeAction:
    """Handles cascade operations across related data collections."""

    def __init__(self) -> None:
        self._rules: Dict[str, List[CascadeRule]] = {}
        self._collections: Dict[str, List[Dict]] = {}
        self._snapshots: Dict[str, Dict] = {}
        self._results: List[CascadeResult] = []

    def register_collection(
        self,
        collection_name: str,
        data: Optional[List[Dict]] = None,
    ) -> None:
        self._collections[collection_name] = data or []

    def add_rule(
        self,
        source_collection: str,
        target_collection: str,
        relation_field: str,
        on_delete: str = "cascade",
        on_update: str = "restrict",
        filter_fn: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        if source_collection not in self._rules:
            self._rules[source_collection] = []
        self._rules[source_collection].append(
            CascadeRule(
                target_collection=target_collection,
                relation_field=relation_field,
                on_delete=on_delete,
                on_update=on_update,
                filter_fn=filter_fn,
            )
        )

    def take_snapshot(self, collection_name: str) -> bool:
        if collection_name not in self._collections:
            return False
        self._snapshots[collection_name] = list(self._collections[collection_name])
        logger.info(f"Snapshot taken for {collection_name}")
        return True

    def take_all_snapshots(self) -> int:
        count = 0
        for name in self._collections:
            if self.take_snapshot(name):
                count += 1
        return count

    def rollback(self, collection_name: Optional[str] = None) -> int:
        if collection_name:
            if collection_name in self._snapshots:
                self._collections[collection_name] = list(self._snapshots[collection_name])
                del self._snapshots[collection_name]
                return 1
            return 0
        restored = 0
        for name in list(self._snapshots.keys()):
            self._collections[name] = list(self._snapshots[name])
            del self._snapshots[name]
            restored += 1
        return restored

    def cascade_delete(
        self,
        collection_name: str,
        item_id: str,
        id_field: str = "id",
    ) -> List[CascadeResult]:
        self._results.clear()
        start = time.time()
        try:
            self._cascade_delete_impl(collection_name, item_id, id_field)
        except Exception as e:
            logger.error(f"Cascade delete failed: {e}")
            self._results.append(
                CascadeResult(
                    collection=collection_name,
                    affected_count=0,
                    duration_ms=(time.time() - start) * 1000,
                    success=False,
                    error=str(e),
                )
            )
        return self._results

    def _cascade_delete_impl(
        self,
        collection_name: str,
        item_id: str,
        id_field: str,
        visited: Optional[Set[str]] = None,
    ) -> None:
        if visited is None:
            visited = set()
        if collection_name in visited:
            return
        visited.add(collection_name)
        collection = self._collections.get(collection_name, [])
        original_count = len(collection)
        deleted_ids = set()
        new_collection = []
        for item in collection:
            if item.get(id_field) == item_id:
                deleted_ids.add(item_id)
            else:
                new_collection.append(item)
        self._collections[collection_name] = new_collection
        self._results.append(
            CascadeResult(
                collection=collection_name,
                affected_count=original_count - len(new_collection),
                duration_ms=0,
                success=True,
            )
        )
        rules = self._rules.get(collection_name, [])
        for rule in rules:
            if rule.on_delete != "cascade":
                continue
            target_collection = rule.target_collection
            if target_collection not in self._collections:
                continue
            target = self._collections[target_collection]
            new_target = []
            affected = 0
            for item in target:
                rel_value = item.get(rule.relation_field)
                if rel_value in deleted_ids or (rule.filter_fn and rule.filter_fn(item)):
                    affected += 1
                else:
                    new_target.append(item)
            self._collections[target_collection] = new_target
            self._results.append(
                CascadeResult(
                    collection=target_collection,
                    affected_count=affected,
                    duration_ms=0,
                    success=True,
                )
            )
            self._cascade_delete_impl(target_collection, item_id, id_field, visited)

    def cascade_update(
        self,
        collection_name: str,
        item_id: str,
        updates: Dict[str, Any],
        id_field: str = "id",
    ) -> List[CascadeResult]:
        self._results.clear()
        start = time.time()
        try:
            collection = self._collections.get(collection_name, [])
            for item in collection:
                if item.get(id_field) == item_id:
                    item.update(updates)
            self._results.append(
                CascadeResult(
                    collection=collection_name,
                    affected_count=1,
                    duration_ms=(time.time() - start) * 1000,
                    success=True,
                )
            )
        except Exception as e:
            logger.error(f"Cascade update failed: {e}")
            self._results.append(
                CascadeResult(
                    collection=collection_name,
                    affected_count=0,
                    duration_ms=(time.time() - start) * 1000,
                    success=False,
                    error=str(e),
                )
            )
        return self._results

    def get_results(self) -> List[CascadeResult]:
        return list(self._results)

    def get_collection(self, name: str) -> List[Dict]:
        return list(self._collections.get(name, []))
