"""
Data Catalog Action Module.

Provides data catalog management for organizing, discovering, and documenting
data assets with search, tagging, lineage linkage, and schema evolution tracking.

Author: RabAi Team
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class DataAssetType(Enum):
    """Types of data assets in catalog."""
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    MODEL = "model"
    DASHBOARD = "dashboard"
    REPORT = "report"
    PIPELINE = "pipeline"
    DATASET = "dataset"


class DataFormat(Enum):
    """Data storage formats."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"
    DELTA = "delta"
    ICEBERG = "iceberg"
    SQL = "sql"
    XML = "xml"


@dataclass
class SchemaField:
    """A field/column in a schema."""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    default_value: Any = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    examples: List[Any] = field(default_factory=list)


@dataclass
class DataAsset:
    """A cataloged data asset."""
    id: str
    name: str
    asset_type: DataAssetType
    format: Optional[DataFormat] = None
    owner: Optional[str] = None
    description: str = ""
    tags: Set[str] = field(default_factory=set)
    schema: List[SchemaField] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    location: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    columns: int = 0
    rows: Optional[int] = None
    size_bytes: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    last_accessed: datetime = field(default_factory=datetime.now)
    certification_status: str = "uncertified"
    sensitivity_level: str = "internal"
    related_assets: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "asset_type": self.asset_type.value,
            "format": self.format.value if self.format else None,
            "owner": self.owner,
            "description": self.description,
            "tags": list(self.tags),
            "schema": [
                {
                    "name": f.name,
                    "data_type": f.data_type,
                    "nullable": f.nullable,
                    "description": f.description,
                    "tags": list(f.tags),
                }
                for f in self.schema
            ],
            "metadata": self.metadata,
            "location": self.location,
            "database": self.database,
            "schema_name": self.schema_name,
            "columns": self.columns,
            "rows": self.rows,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "certification_status": self.certification_status,
            "sensitivity_level": self.sensitivity_level,
            "related_assets": self.related_assets,
        }


@dataclass
class Tag:
    """A catalog tag."""
    name: str
    description: str = ""
    color: str = "#808080"
    category: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SearchResult:
    """Search result with relevance scoring."""
    asset: DataAsset
    relevance_score: float
    matched_fields: List[str] = field(default_factory=list)
    highlight: Optional[str] = None


class DataCatalog:
    """
    Data catalog for organizing and discovering data assets.

    Provides asset registration, search, tagging, schema management,
    and certification workflows for data governance.

    Example:
        >>> catalog = DataCatalog()
        >>> catalog.register_asset(name="users", asset_type=DataAssetType.TABLE)
        >>> results = catalog.search("user data", tags=["pii"])
    """

    def __init__(self):
        self._assets: Dict[str, DataAsset] = {}
        self._tags: Dict[str, Tag] = {}
        self._name_index: Dict[str, List[str]] = defaultdict(list)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._type_index: Dict[DataAssetType, Set[str]] = defaultdict(set)
        self._owner_index: Dict[str, List[str]] = defaultdict(list)
        self._full_text_index: Dict[str, Set[str]] = defaultdict(set)

    def register_asset(
        self,
        name: str,
        asset_type: DataAssetType,
        format: Optional[DataFormat] = None,
        owner: Optional[str] = None,
        description: str = "",
        tags: Optional[List[str]] = None,
        schema: Optional[List[SchemaField]] = None,
        location: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Register a new data asset in the catalog."""
        asset_id = str(uuid.uuid4())
        asset = DataAsset(
            id=asset_id,
            name=name,
            asset_type=asset_type,
            format=format,
            owner=owner,
            description=description,
            tags=set(tags) if tags else set(),
            schema=schema or [],
            location=location,
            database=database,
            schema_name=schema_name,
            **kwargs,
        )
        asset.columns = len(asset.schema)

        self._assets[asset_id] = asset
        self._index_asset(asset)
        return asset_id

    def update_asset(self, asset_id: str, **updates) -> bool:
        """Update asset properties."""
        if asset_id not in self._assets:
            return False
        asset = self._assets[asset_id]
        for key, value in updates.items():
            if hasattr(asset, key):
                setattr(asset, key, value)
        asset.updated_at = datetime.now()
        return True

    def add_tag(self, asset_id: str, tag_name: str) -> bool:
        """Add a tag to an asset."""
        if asset_id not in self._assets:
            return False
        asset = self._assets[asset_id]
        asset.tags.add(tag_name)
        self._tag_index[tag_name].add(asset_id)
        self._rebuild_full_text_index(asset)
        return True

    def remove_tag(self, asset_id: str, tag_name: str) -> bool:
        """Remove a tag from an asset."""
        if asset_id not in self._assets:
            return False
        asset = self._assets[asset_id]
        asset.tags.discard(tag_name)
        self._tag_index[tag_name].discard(asset_id)
        return True

    def create_tag(
        self,
        name: str,
        description: str = "",
        color: str = "#808080",
        category: Optional[str] = None,
    ) -> None:
        """Create a catalog tag."""
        self._tags[name] = Tag(
            name=name,
            description=description,
            color=color,
            category=category,
        )

    def search(
        self,
        query: str,
        asset_types: Optional[List[DataAssetType]] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        limit: int = 20,
    ) -> List[SearchResult]:
        """Search for data assets."""
        query_lower = query.lower()
        results = []

        candidate_ids = set(self._assets.keys())

        if tags:
            tag_ids = set()
            for tag in tags:
                tag_ids.update(self._tag_index.get(tag, set()))
            candidate_ids &= tag_ids

        if asset_types:
            type_ids = set()
            for at in asset_types:
                type_ids.update(self._type_index.get(at, set()))
            candidate_ids &= type_ids

        if owner:
            owner_ids = set(self._owner_index.get(owner, []))
            candidate_ids &= owner_ids

        for asset_id in candidate_ids:
            asset = self._assets[asset_id]
            score = 0.0
            matched_fields = []

            if query_lower in asset.name.lower():
                score += 10.0
                matched_fields.append("name")

            if query_lower in asset.description.lower():
                score += 5.0
                matched_fields.append("description")

            if query_lower in asset.tags:
                score += 8.0
                matched_fields.append("tags")

            for field in asset.schema:
                if query_lower in field.name.lower():
                    score += 6.0
                    matched_fields.append("schema")

            query_words = query_lower.split()
            for word in query_words:
                if word in self._full_text_index.get(asset_id, set()):
                    score += 2.0
                    matched_fields.append("full_text")

            if score > 0:
                results.append(SearchResult(
                    asset=asset,
                    relevance_score=score,
                    matched_fields=list(set(matched_fields)),
                    highlight=self._create_highlight(asset, query_lower),
                ))

        results.sort(key=lambda r: r.relevance_score, reverse=True)
        return results[:limit]

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get asset by ID."""
        return self._assets.get(asset_id)

    def get_asset_by_name(self, name: str) -> Optional[DataAsset]:
        """Get asset by name."""
        asset_ids = self._name_index.get(name.lower(), [])
        if asset_ids:
            return self._assets.get(asset_ids[0])
        return None

    def get_assets_by_tag(self, tag: str) -> List[DataAsset]:
        """Get all assets with a specific tag."""
        asset_ids = self._tag_index.get(tag, set())
        return [self._assets[aid] for aid in asset_ids if aid in self._assets]

    def get_assets_by_owner(self, owner: str) -> List[DataAsset]:
        """Get all assets owned by a user."""
        asset_ids = self._owner_index.get(owner, [])
        return [self._assets[aid] for aid in asset_ids if aid in self._assets]

    def get_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics."""
        assets = list(self._assets.values())
        return {
            "total_assets": len(assets),
            "by_type": {
                at.value: len(self._type_index.get(at, set()))
                for at in DataAssetType
            },
            "by_tag": {tag: len(ids) for tag, ids in self._tag_index.items()},
            "total_tags": len(self._tags),
            "certified": sum(1 for a in assets if a.certification_status == "certified"),
            "total_size_bytes": sum(a.size_bytes or 0 for a in assets),
        }

    def export_catalog(self) -> Dict[str, Any]:
        """Export full catalog as JSON."""
        return {
            "assets": [a.to_dict() for a in self._assets.values()],
            "tags": {t.name: {"description": t.description, "color": t.color} for t in self._tags.values()},
            "exported_at": datetime.now().isoformat(),
        }

    def import_catalog(self, data: Dict[str, Any]) -> int:
        """Import catalog from JSON."""
        count = 0
        for asset_data in data.get("assets", []):
            asset_type = DataAssetType(asset_data["asset_type"])
            format_val = None
            if asset_data.get("format"):
                format_val = DataFormat(asset_data["format"])
            schema_fields = [
                SchemaField(
                    name=f["name"],
                    data_type=f["data_type"],
                    nullable=f.get("nullable", True),
                    description=f.get("description"),
                    tags=set(f.get("tags", [])),
                )
                for f in asset_data.get("schema", [])
            ]
            self.register_asset(
                name=asset_data["name"],
                asset_type=asset_type,
                format=format_val,
                owner=asset_data.get("owner"),
                description=asset_data.get("description", ""),
                tags=asset_data.get("tags", []),
                schema=schema_fields,
                location=asset_data.get("location"),
                database=asset_data.get("database"),
                schema_name=asset_data.get("schema_name"),
            )
            count += 1
        return count

    def _index_asset(self, asset: DataAsset) -> None:
        """Index an asset for search."""
        self._name_index[asset.name.lower()].append(asset.id)
        for tag in asset.tags:
            self._tag_index[tag].add(asset.id)
        self._type_index[asset.asset_type].add(asset.id)
        if asset.owner:
            self._owner_index[asset.owner].append(asset.id)
        self._rebuild_full_text_index(asset)

    def _rebuild_full_text_index(self, asset: DataAsset) -> None:
        """Rebuild full-text index for an asset."""
        terms = set()
        terms.update(asset.name.lower().split())
        terms.update(asset.description.lower().split())
        terms.update(asset.tags)
        for field in asset.schema:
            terms.add(field.name.lower())
            if field.description:
                terms.update(field.description.lower().split())
        self._full_text_index[asset.id] = terms

    def _create_highlight(self, asset: DataAsset, query: str) -> str:
        """Create a highlight snippet for search results."""
        if query in asset.description.lower():
            idx = asset.description.lower().find(query)
            start = max(0, idx - 30)
            end = min(len(asset.description), idx + len(query) + 30)
            return f"...{asset.description[start:end]}..."
        return asset.description[:100] + "..." if len(asset.description) > 100 else asset.description


def create_data_catalog() -> DataCatalog:
    """Factory to create a data catalog."""
    return DataCatalog()
