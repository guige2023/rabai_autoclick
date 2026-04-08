"""
Data catalog module for organizing and discovering data assets.

Provides metadata management, tagging, lineage tracking,
search, and data discovery capabilities.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class DataAssetType(Enum):
    """Type of data asset."""
    TABLE = "table"
    VIEW = "view"
    STREAM = "stream"
    FILE = "file"
    PIPELINE = "pipeline"
    MODEL = "model"
    DASHBOARD = "dashboard"
    REPORT = "report"
    DATASET = "dataset"


class DataFormat(Enum):
    """Data format types."""
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"
    DELTA = "delta"
    ICEBERG = "iceberg"


@dataclass
class Column:
    """A data column schema."""
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""
    partition_key: bool = False
    sort_key: bool = False


@dataclass
class DataAsset:
    """A data asset in the catalog."""
    id: str
    name: str
    asset_type: DataAssetType
    database: str
    schema: str = ""
    physical_name: str = ""
    description: str = ""
    columns: list[Column] = field(default_factory=list)
    format: Optional[DataFormat] = None
    location: Optional[str] = None
    owner: str = ""
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    statistics: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    last_analyzed_at: Optional[float] = None
    certified: bool = False
    certification_status: str = ""

    def full_name(self) -> str:
        parts = [p for p in [self.database, self.schema, self.physical_name or self.name] if p]
        return ".".join(parts)


@dataclass
class LineageNode:
    """A node in the data lineage graph."""
    asset_id: str
    asset_name: str
    operation: str
    columns: list[str] = field(default_factory=list)


@dataclass
class LineageEdge:
    """An edge in the data lineage graph."""
    source: str
    target: str
    edge_type: str = "direct"
    columns: list[str] = field(default_factory=list)


@dataclass
class Tag:
    """A catalog tag."""
    name: str
    description: str = ""
    color: str = "#808080"
    created_at: float = field(default_factory=time.time)
    created_by: str = ""


@dataclass
class DataQualityRule:
    """A data quality rule."""
    id: str
    name: str
    asset_id: str
    column: str
    rule_type: str
    threshold: Any
    enabled: bool = True


class DataCatalog:
    """
    Data catalog service for organizing and discovering data assets.

    Provides metadata management, lineage tracking, search,
    and data discovery capabilities.
    """

    def __init__(self):
        self._assets: dict[str, DataAsset] = {}
        self._assets_by_name: dict[str, str] = {}
        self._lineage_edges: list[LineageEdge] = []
        self._tags: dict[str, Tag] = {}
        self._quality_rules: dict[str, DataQualityRule] = {}

    def register_asset(
        self,
        name: str,
        asset_type: DataAssetType,
        database: str,
        schema: str = "",
        physical_name: str = "",
        description: str = "",
        columns: Optional[list[Column]] = None,
        format: Optional[DataFormat] = None,
        location: Optional[str] = None,
        owner: str = "",
        tags: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> DataAsset:
        """Register a new data asset."""
        asset_id = str(uuid.uuid4())[:12]

        asset = DataAsset(
            id=asset_id,
            name=name,
            asset_type=asset_type,
            database=database,
            schema=schema,
            physical_name=physical_name,
            description=description,
            columns=columns or [],
            format=format,
            location=location,
            owner=owner,
            tags=tags or [],
            metadata=metadata or {},
        )

        self._assets[asset_id] = asset
        full_name = asset.full_name()
        self._assets_by_name[full_name] = asset_id

        return asset

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get an asset by ID."""
        return self._assets.get(asset_id)

    def get_asset_by_name(
        self,
        database: str,
        schema: str = "",
        name: str = "",
    ) -> Optional[DataAsset]:
        """Get an asset by its full name."""
        full_name = f"{database}.{schema}.{name}" if schema else f"{database}.{name}"
        asset_id = self._assets_by_name.get(full_name)
        return self._assets.get(asset_id) if asset_id else None

    def update_asset(
        self,
        asset_id: str,
        **kwargs,
    ) -> Optional[DataAsset]:
        """Update an asset's metadata."""
        asset = self._assets.get(asset_id)
        if not asset:
            return None

        for key, value in kwargs.items():
            if hasattr(asset, key):
                setattr(asset, key, value)
        asset.updated_at = time.time()

        return asset

    def delete_asset(self, asset_id: str) -> bool:
        """Delete an asset from the catalog."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        full_name = asset.full_name()
        self._assets_by_name.pop(full_name, None)
        self._assets.pop(asset_id, None)

        self._lineage_edges = [
            e for e in self._lineage_edges
            if e.source != asset_id and e.target != asset_id
        ]

        return True

    def search(
        self,
        query: Optional[str] = None,
        asset_type: Optional[DataAssetType] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        tags: Optional[list[str]] = None,
        owner: Optional[str] = None,
        certified: Optional[bool] = None,
        limit: int = 100,
    ) -> list[DataAsset]:
        """Search for assets in the catalog."""
        results = list(self._assets.values())

        if query:
            query_lower = query.lower()
            results = [
                a for a in results
                if query_lower in a.name.lower()
                or query_lower in a.description.lower()
                or query_lower in a.physical_name.lower()
            ]

        if asset_type:
            results = [a for a in results if a.asset_type == asset_type]
        if database:
            results = [a for a in results if a.database == database]
        if schema:
            results = [a for a in results if a.schema == schema]
        if owner:
            results = [a for a in results if a.owner == owner]
        if certified is not None:
            results = [a for a in results if a.certified == certified]

        if tags:
            results = [
                a for a in results
                if any(tag in a.tags for tag in tags)
            ]

        return results[:limit]

    def add_lineage(
        self,
        source_id: str,
        target_id: str,
        edge_type: str = "direct",
        columns: Optional[list[str]] = None,
    ) -> Optional[LineageEdge]:
        """Add a lineage edge between two assets."""
        if source_id not in self._assets or target_id not in self._assets:
            return None

        edge = LineageEdge(
            source=source_id,
            target=target_id,
            edge_type=edge_type,
            columns=columns or [],
        )
        self._lineage_edges.append(edge)
        return edge

    def get_lineage(
        self,
        asset_id: str,
        direction: str = "downstream",
        depth: int = 10,
    ) -> list[DataAsset]:
        """Get lineage for an asset (upstream or downstream)."""
        visited = set()
        result = []
        queue = deque([asset_id])

        for _ in range(depth):
            if not queue:
                break

            current_id = queue.popleft()
            if current_id in visited:
                continue
            visited.add(current_id)

            for edge in self._lineage_edges:
                next_id = None
                if direction == "downstream" and edge.source == current_id:
                    next_id = edge.target
                elif direction == "upstream" and edge.target == current_id:
                    next_id = edge.source

                if next_id and next_id not in visited:
                    asset = self._assets.get(next_id)
                    if asset:
                        result.append(asset)
                        queue.append(next_id)

        return result

    def create_tag(
        self,
        name: str,
        description: str = "",
        color: str = "#808080",
        created_by: str = "",
    ) -> Tag:
        """Create a new tag."""
        tag = Tag(
            name=name,
            description=description,
            color=color,
            created_by=created_by,
        )
        self._tags[name] = tag
        return tag

    def add_tag_to_asset(self, asset_id: str, tag_name: str) -> bool:
        """Add a tag to an asset."""
        asset = self._assets.get(asset_id)
        if not asset or tag_name not in self._tags:
            return False

        if tag_name not in asset.tags:
            asset.tags.append(tag_name)
            asset.updated_at = time.time()

        return True

    def remove_tag_from_asset(self, asset_id: str, tag_name: str) -> bool:
        """Remove a tag from an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        if tag_name in asset.tags:
            asset.tags.remove(tag_name)
            asset.updated_at = time.time()

        return True

    def list_tags(self) -> list[Tag]:
        """List all tags."""
        return list(self._tags.values())

    def add_quality_rule(
        self,
        name: str,
        asset_id: str,
        column: str,
        rule_type: str,
        threshold: Any,
    ) -> Optional[DataQualityRule]:
        """Add a data quality rule to an asset."""
        if asset_id not in self._assets:
            return None

        rule = DataQualityRule(
            id=str(uuid.uuid4())[:8],
            name=name,
            asset_id=asset_id,
            column=column,
            rule_type=rule_type,
            threshold=threshold,
        )
        self._quality_rules[rule.id] = rule
        return rule

    def get_quality_rules(self, asset_id: str) -> list[DataQualityRule]:
        """Get all quality rules for an asset."""
        return [
            r for r in self._quality_rules.values()
            if r.asset_id == asset_id
        ]

    def analyze_asset(self, asset_id: str, statistics: dict) -> bool:
        """Update statistics for an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        asset.statistics = statistics
        asset.last_analyzed_at = time.time()
        asset.updated_at = time.time()
        return True

    def certify_asset(
        self,
        asset_id: str,
        status: str = "certified",
    ) -> bool:
        """Certify or recertify an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        asset.certified = True
        asset.certification_status = status
        asset.updated_at = time.time()
        return True

    def list_assets(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        asset_type: Optional[DataAssetType] = None,
        limit: int = 1000,
    ) -> list[DataAsset]:
        """List all assets with optional filters."""
        results = list(self._assets.values())

        if database:
            results = [a for a in results if a.database == database]
        if schema:
            results = [a for a in results if a.schema == schema]
        if asset_type:
            results = [a for a in results if a.asset_type == asset_type]

        return results[:limit]

    def get_catalog_stats(self) -> dict:
        """Get catalog statistics."""
        by_type = {}
        by_database = {}
        total_columns = 0

        for asset in self._assets.values():
            type_key = asset.asset_type.value
            by_type[type_key] = by_type.get(type_key, 0) + 1

            db_key = asset.database
            by_database[db_key] = by_database.get(db_key, 0) + 1

            total_columns += len(asset.columns)

        return {
            "total_assets": len(self._assets),
            "by_type": by_type,
            "by_database": by_database,
            "total_columns": total_columns,
            "total_tags": len(self._tags),
            "total_lineage_edges": len(self._lineage_edges),
            "certified_assets": sum(1 for a in self._assets.values() if a.certified),
        }
