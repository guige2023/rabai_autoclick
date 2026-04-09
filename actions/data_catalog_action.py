"""
Data Catalog Action Module.

Manages a data catalog with schema registry,
lineage tracking, and metadata management.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Supported data types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"
    UNKNOWN = "unknown"


@dataclass
class SchemaField:
    """Represents a schema field."""

    name: str
    data_type: DataType
    nullable: bool = True
    description: str = ""
    default_value: Any = None


@dataclass
class DataSchema:
    """Represents a data schema."""

    name: str
    version: str = "1.0"
    fields: list[SchemaField] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class DataAsset:
    """Represents a data asset in the catalog."""

    name: str
    asset_type: str
    schema: Optional[DataSchema] = None
    location: str = ""
    owner: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class LineageNode:
    """Represents a lineage node."""

    asset_name: str
    operation: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class DataCatalogAction:
    """
    Manages data catalog with schema and lineage tracking.

    Features:
    - Schema registration and versioning
    - Asset metadata management
    - Data lineage tracking
    - Tag-based classification
    - Search and discovery

    Example:
        catalog = DataCatalogAction()
        catalog.register_schema("user_events", user_schema)
        catalog.register_asset("events.parquet", asset)
        catalog.add_lineage("process", inputs=["raw"], outputs=["processed"])
    """

    def __init__(self) -> None:
        """Initialize data catalog action."""
        self._schemas: dict[str, DataSchema] = {}
        self._assets: dict[str, DataAsset] = {}
        self._lineage: list[LineageNode] = []
        self._stats = {
            "total_schemas": 0,
            "total_assets": 0,
            "total_lineage_nodes": 0,
        }

    def register_schema(
        self,
        name: str,
        fields: list[SchemaField],
        version: str = "1.0",
    ) -> DataSchema:
        """
        Register a data schema.

        Args:
            name: Schema name.
            fields: List of schema fields.
            version: Schema version.

        Returns:
            Created DataSchema.
        """
        schema = DataSchema(name=name, version=version, fields=fields)
        self._schemas[name] = schema
        self._stats["total_schemas"] += 1

        logger.info(f"Registered schema: {name} v{version}")
        return schema

    def get_schema(self, name: str) -> Optional[DataSchema]:
        """
        Get a schema by name.

        Args:
            name: Schema name.

        Returns:
            DataSchema or None.
        """
        return self._schemas.get(name)

    def register_asset(
        self,
        name: str,
        asset_type: str,
        schema: Optional[DataSchema] = None,
        location: str = "",
        owner: str = "",
        description: str = "",
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> DataAsset:
        """
        Register a data asset.

        Args:
            name: Asset name.
            asset_type: Type of asset (table, file, stream, etc.).
            schema: Associated schema.
            location: Storage location.
            owner: Asset owner.
            description: Asset description.
            tags: Classification tags.
            metadata: Additional metadata.

        Returns:
            Created DataAsset.
        """
        asset = DataAsset(
            name=name,
            asset_type=asset_type,
            schema=schema,
            location=location,
            owner=owner,
            description=description,
            tags=tags or [],
            metadata=metadata or {},
        )
        self._assets[name] = asset
        self._stats["total_assets"] += 1

        logger.info(f"Registered asset: {name}")
        return asset

    def get_asset(self, name: str) -> Optional[DataAsset]:
        """
        Get an asset by name.

        Args:
            name: Asset name.

        Returns:
            DataAsset or None.
        """
        return self._assets.get(name)

    def update_asset(
        self,
        name: str,
        **updates: Any,
    ) -> Optional[DataAsset]:
        """
        Update an asset's metadata.

        Args:
            name: Asset name.
            **updates: Fields to update.

        Returns:
            Updated DataAsset or None.
        """
        if name not in self._assets:
            return None

        asset = self._assets[name]
        for key, value in updates.items():
            if hasattr(asset, key):
                setattr(asset, key, value)
        asset.updated_at = time.time()

        return asset

    def add_lineage(
        self,
        operation: str,
        inputs: Optional[list[str]] = None,
        outputs: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> LineageNode:
        """
        Add a lineage node.

        Args:
            operation: Operation name.
            inputs: Input asset names.
            outputs: Output asset names.
            metadata: Additional metadata.

        Returns:
            Created LineageNode.
        """
        node = LineageNode(
            asset_name="",
            operation=operation,
            inputs=inputs or [],
            outputs=outputs or [],
            metadata=metadata or {},
        )
        self._lineage.append(node)
        self._stats["total_lineage_nodes"] += 1

        logger.info(f"Added lineage: {operation}")
        return node

    def get_lineage(
        self,
        asset_name: Optional[str] = None,
        depth: int = 10,
    ) -> list[LineageNode]:
        """
        Get lineage for an asset.

        Args:
            asset_name: Optional asset name filter.
            depth: Maximum lineage depth.

        Returns:
            List of lineage nodes.
        """
        if asset_name:
            return [
                n for n in self._lineage
                if asset_name in n.inputs or asset_name in n.outputs
            ][:depth]
        return self._lineage[:depth]

    def search_assets(
        self,
        query: str,
        tags: Optional[list[str]] = None,
        asset_type: Optional[str] = None,
    ) -> list[DataAsset]:
        """
        Search for assets.

        Args:
            query: Search query.
            tags: Filter by tags.
            asset_type: Filter by asset type.

        Returns:
            List of matching assets.
        """
        results = []

        for asset in self._assets.values():
            if asset_type and asset.asset_type != asset_type:
                continue

            if tags and not any(t in asset.tags for t in tags):
                continue

            if query.lower() in asset.name.lower() or query.lower() in asset.description.lower():
                results.append(asset)

        return results

    def get_stats(self) -> dict[str, Any]:
        """
        Get catalog statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "schemas": list(self._schemas.keys()),
            "asset_types": list(set(a.asset_type for a in self._assets.values())),
            "all_tags": list(set(tag for a in self._assets.values() for tag in a.tags)),
        }
