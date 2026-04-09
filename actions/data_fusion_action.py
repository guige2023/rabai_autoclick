"""
Data Fusion Action Module.

Combines data from multiple sources with conflict resolution,
schema matching, and deduplication capabilities.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ConflictResolution(Enum):
    """Conflict resolution strategies."""

    LAST_WRITE_WINS = "last_write_wins"
    SOURCE_PRIORITY = "source_priority"
    MERGE_PREFER_NEW = "merge_prefer_new"
    MERGE_PREFER_OLD = "merge_prefer_old"
    CUSTOM = "custom"


@dataclass
class DataSource:
    """Represents a data source for fusion."""

    name: str
    priority: int = 1
    last_updated: float = field(default_factory=time.time)
    schema: Optional[dict[str, str]] = None


@dataclass
class FusionConfig:
    """Configuration for data fusion."""

    conflict_resolution: ConflictResolution = ConflictResolution.LAST_WRITE_WINS
    enable_deduplication: bool = True
    similarity_threshold: float = 0.85
    custom_resolver: Optional[Callable] = None


@dataclass
class FusionResult:
    """Result of a fusion operation."""

    fused_data: dict[str, Any]
    conflicts_resolved: int = 0
    duplicates_merged: int = 0
    sources_used: int = 0


class DataFusionAction:
    """
    Fuses data from multiple sources with intelligent merging.

    Features:
    - Schema matching across sources
    - Configurable conflict resolution
    - Automatic deduplication
    - Field-level and record-level fusion

    Example:
        fusion = DataFusionAction()
        fusion.add_source("api", api_data)
        fusion.add_source("db", db_data)
        result = fusion.fuse()
    """

    def __init__(self, config: Optional[FusionConfig] = None) -> None:
        """
        Initialize data fusion.

        Args:
            config: Fusion configuration.
        """
        self.config = config or FusionConfig()
        self._sources: dict[str, DataSource] = {}
        self._data: dict[str, list[dict[str, Any]]] = {}
        self._fusion_log: list[dict[str, Any]] = []

    def add_source(
        self,
        name: str,
        data: list[dict[str, Any]],
        priority: int = 1,
        schema: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Add a data source.

        Args:
            name: Source name.
            data: List of data records.
            priority: Source priority (higher = more trusted).
            schema: Optional field name mapping.
        """
        source = DataSource(name=name, priority=priority, schema=schema)
        self._sources[name] = source
        self._data[name] = data
        logger.info(f"Added fusion source: {name} with {len(data)} records")

    def fuse(
        self,
        key_field: str = "id",
        field_mappings: Optional[dict[str, str]] = None,
    ) -> FusionResult:
        """
        Fuse data from all sources.

        Args:
            key_field: Field used for record matching.
            field_mappings: Optional field name aliases.

        Returns:
            FusionResult with fused data.
        """
        all_records: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        conflicts_resolved = 0
        duplicates_merged = 0

        for source_name, records in self._data.items():
            source = self._sources[source_name]

            for record in records:
                key = record.get(key_field)
                if not key:
                    key = str(id(record))

                if key not in all_records:
                    all_records[key] = []

                all_records[key].append((source_name, record))

        fused_records: dict[str, dict[str, Any]] = {}

        for key, source_records in all_records.items():
            if len(source_records) == 1:
                fused_records[key] = source_records[0][1].copy()
            else:
                fused_record, conflicts = self._merge_records(
                    key, source_records, field_mappings
                )
                fused_records[key] = fused_record
                conflicts_resolved += conflicts

                if self.config.enable_deduplication and len(source_records) > 1:
                    duplicates_merged += len(source_records) - 1

        total_sources = len(self._sources)
        result = FusionResult(
            fused_data=fused_records,
            conflicts_resolved=conflicts_resolved,
            duplicates_merged=duplicates_merged,
            sources_used=total_sources,
        )

        self._fusion_log.append({
            "timestamp": time.time(),
            "result": result,
        })

        logger.info(
            f"Fusion complete: {len(fused_records)} records, "
            f"{conflicts_resolved} conflicts resolved"
        )

        return result

    def _merge_records(
        self,
        key: str,
        source_records: list[tuple[str, dict[str, Any]]],
        field_mappings: Optional[dict[str, str]],
    ) -> tuple[dict[str, Any], int]:
        """Merge records from multiple sources."""
        merged: dict[str, Any] = {}
        conflicts = 0

        field_mappings = field_mappings or {}

        all_fields: set[str] = set()
        for _, record in source_records:
            all_fields.update(record.keys())

        for source_name, record in sorted(source_records, key=lambda x: -self._sources[x[0]].priority):
            for field_name in all_fields:
                actual_field = field_mappings.get(field_name, field_name)
                value = record.get(field_name)

                if actual_field in merged:
                    if merged[actual_field] != value:
                        conflicts += 1
                        merged[actual_field] = self._resolve_conflict(
                            field_name, merged[actual_field], value, source_name
                        )
                else:
                    merged[actual_field] = value

        merged["_fusion_sources"] = [s[0] for s in source_records]
        merged["_fusion_key"] = key

        return merged, conflicts

    def _resolve_conflict(
        self,
        field_name: str,
        old_value: Any,
        new_value: Any,
        source_name: str,
    ) -> Any:
        """Resolve a field conflict between sources."""
        if self.config.conflict_resolution == ConflictResolution.LAST_WRITE_WINS:
            return new_value

        elif self.config.conflict_resolution == ConflictResolution.SOURCE_PRIORITY:
            return new_value

        elif self.config.conflict_resolution == ConflictResolution.MERGE_PREFER_NEW:
            return new_value if new_value else old_value

        elif self.config.conflict_resolution == ConflictResolution.CUSTOM:
            if self.config.custom_resolver:
                return self.config.custom_resolver(field_name, old_value, new_value)

        return new_value

    def fuse_with_join(
        self,
        join_key: str,
        join_type: str = "left",
    ) -> list[dict[str, Any]]:
        """
        Fuse data using a join operation.

        Args:
            join_key: Key field for joining.
            join_type: Type of join ('left', 'right', 'inner', 'outer').

        Returns:
            List of fused records.
        """
        if len(self._sources) < 2:
            return list(self._data.get(list(self._sources.keys())[0], []))

        source_names = list(self._sources.keys())
        left_data = self._data.get(source_names[0], [])
        right_data = self._data.get(source_names[1], [])

        results = []

        for left_record in left_data:
            matched = False
            for right_record in right_data:
                if left_record.get(join_key) == right_record.get(join_key):
                    merged = {**right_record, **left_record}
                    merged["_joined_from"] = source_names
                    results.append(merged)
                    matched = True

            if not matched and join_type in ("left", "outer"):
                results.append({**left_record, "_joined_from": [source_names[0]]})

        return results

    def get_stats(self) -> dict[str, Any]:
        """
        Get fusion statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            "sources": len(self._sources),
            "total_records": sum(len(r) for r in self._data.values()),
            "fusion_operations": len(self._fusion_log),
            "last_fusion": self._fusion_log[-1] if self._fusion_log else None,
        }
