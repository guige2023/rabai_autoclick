# Copyright (c) 2024. coded by claude
"""Data Merger Action Module.

Merges multiple data sources into unified API responses with support for
conflict resolution, field mapping, and data deduplication.
"""
from typing import Optional, Dict, Any, List, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    OVERWRITE = "overwrite"
    PRESERVE = "preserve"
    CONFLICT_RESOLVE = "conflict_resolve"
    UNION = "union"


@dataclass
class MergeConfig:
    strategy: MergeStrategy = MergeStrategy.OVERWRITE
    conflict_resolver: Optional[Callable[[Any, Any], Any]] = None
    key_field: Optional[str] = None
    skip_nulls: bool = False


@dataclass
class MergeResult:
    success: bool
    merged_data: Dict[str, Any]
    conflicts: List[Tuple[str, Any, Any]] = field(default_factory=list)
    error: Optional[str] = None


class DataMerger:
    def __init__(self, config: Optional[MergeConfig] = None):
        self.config = config or MergeConfig()

    def merge(self, *sources: Dict[str, Any]) -> MergeResult:
        if not sources:
            return MergeResult(success=True, merged_data={})
        merged: Dict[str, Any] = {}
        conflicts: List[Tuple[str, Any, Any]] = []
        for source in sources:
            for key, value in source.items():
                if self.config.skip_nulls and value is None:
                    continue
                if key not in merged:
                    merged[key] = value
                else:
                    resolved, conflict = self._resolve_conflict(key, merged[key], value)
                    if conflict:
                        conflicts.append((key, merged[key], value))
                    merged[key] = resolved
        return MergeResult(success=True, merged_data=merged, conflicts=conflicts)

    def merge_list(self, items: List[Dict[str, Any]], merge_config: Optional[MergeConfig] = None) -> List[Dict[str, Any]]:
        config = merge_config or self.config
        if config.strategy == MergeStrategy.UNION:
            return self._merge_union(items, config)
        elif config.strategy == MergeStrategy.CONFLICT_RESOLVE and config.key_field:
            return self._merge_dedup(items, config)
        return items

    def _resolve_conflict(self, key: str, old_value: Any, new_value: Any) -> Tuple[Any, bool]:
        if self.config.strategy == MergeStrategy.PRESERVE:
            return old_value, False
        elif self.config.strategy == MergeStrategy.OVERWRITE:
            return new_value, old_value != new_value
        elif self.config.strategy == MergeStrategy.CONFLICT_RESOLVE:
            if self.config.conflict_resolver:
                return self.config.conflict_resolver(old_value, new_value), True
            return new_value, old_value != new_value
        return new_value, False

    def _merge_union(self, items: List[Dict[str, Any]], config: MergeConfig) -> List[Dict[str, Any]]:
        all_keys: set = set()
        for item in items:
            all_keys.update(item.keys())
        result = {}
        for key in all_keys:
            values = [item.get(key) for item in items if key in item]
            if values:
                result[key] = values[0] if len(values) == 1 else values
        return [result] if result else []

    def _merge_dedup(self, items: List[Dict[str, Any]], config: MergeConfig) -> List[Dict[str, Any]]:
        if not config.key_field:
            return items
        seen: Dict[str, Dict[str, Any]] = {}
        for item in items:
            key_value = item.get(config.key_field)
            if key_value is None:
                continue
            if key_value not in seen:
                seen[key_value] = dict(item)
            else:
                merge_result = self.merge(seen[key_value], item)
                seen[key_value] = merge_result.merged_data
        return list(seen.values())
