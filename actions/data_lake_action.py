"""
Data Lake Action Module

Provides data lake management capabilities including zone organization,
data ingestion, catalog management, and schema evolution tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class LakeZone(Enum):
    """Data lake zones."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    RAW = "raw"
    ARCHIVE = "archive"


class DataFormat(Enum):
    """Data storage formats."""

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    ORC = "orc"
    AVRO = "avro"


@dataclass
class DataAsset:
    """A data asset in the lake."""

    asset_id: str
    name: str
    zone: LakeZone
    format: DataFormat
    path: str
    size_bytes: int = 0
    row_count: int = 0
    schema: Optional[Dict[str, Any]] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    checksum: Optional[str] = None


@dataclass
class LakeCatalogEntry:
    """Entry in the data lake catalog."""

    entry_id: str
    asset_id: str
    zone: LakeZone
    partition_keys: List[str] = field(default_factory=list)
    statistics: Dict[str, float] = field(default_factory=dict)


@dataclass
class DataLakeConfig:
    """Configuration for data lake."""

    base_path: str = "/data/lake"
    enable_catalog: bool = True
    enable_versioning: bool = True
    enable_caching: bool = True
    default_format: DataFormat = DataFormat.PARQUET


class DataLakeAction:
    """
    Data lake management action.

    Features:
    - Multi-zone organization (bronze, silver, gold)
    - Data asset cataloging
    - Schema management and evolution tracking
    - Data ingestion from multiple sources
    - Partition management
    - Data versioning
    - Asset statistics

    Usage:
        lake = DataLakeAction(config)
        
        asset = await lake.ingest("sales-data", LakeZone.BRONZE, data)
        
        refined = await lake.transform("sales-data", "refined-sales", LakeZone.SILVER)
        
        gold = await lake.aggregate("refined-sales", "summary", LakeZone.GOLD)
    """

    def __init__(self, config: Optional[DataLakeConfig] = None):
        self.config = config or DataLakeConfig()
        self._assets: Dict[str, DataAsset] = {}
        self._catalog: Dict[str, LakeCatalogEntry] = {}
        self._zone_assets: Dict[LakeZone, List[str]] = {zone: [] for zone in LakeZone}
        self._stats = {
            "assets_created": 0,
            "assets_read": 0,
            "assets_deleted": 0,
            "bytes_written": 0,
        }

    async def ingest(
        self,
        name: str,
        zone: LakeZone,
        data: Any,
        format: Optional[DataFormat] = None,
        schema: Optional[Dict[str, Any]] = None,
        partition_keys: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataAsset:
        """Ingest data into the lake."""
        asset_id = f"asset_{uuid.uuid4().hex[:12]}"
        fmt = format or self.config.default_format

        data_str = str(data)
        checksum = hashlib.md5(data_str.encode()).hexdigest()

        asset = DataAsset(
            asset_id=asset_id,
            name=name,
            zone=zone,
            format=fmt,
            path=f"{self.config.base_path}/{zone.value}/{name}",
            size_bytes=len(data_str),
            row_count=getattr(data, "__len__", lambda: 0)(),
            schema=schema,
            metadata=metadata or {},
            checksum=checksum,
        )

        self._assets[asset_id] = asset
        self._zone_assets[zone].append(asset_id)

        if self.config.enable_catalog and partition_keys:
            entry = LakeCatalogEntry(
                entry_id=f"cat_{uuid.uuid4().hex[:8]}",
                asset_id=asset_id,
                zone=zone,
                partition_keys=partition_keys,
            )
            self._catalog[asset_id] = entry

        self._stats["assets_created"] += 1
        self._stats["bytes_written"] += asset.size_bytes

        return asset

    async def read(
        self,
        asset_id: str,
    ) -> Optional[DataAsset]:
        """Read a data asset from the lake."""
        asset = self._assets.get(asset_id)
        if asset:
            self._stats["assets_read"] += 1
        return asset

    async def move(
        self,
        asset_id: str,
        target_zone: LakeZone,
    ) -> Optional[DataAsset]:
        """Move an asset to a different zone."""
        asset = self._assets.get(asset_id)
        if asset is None:
            return None

        old_zone = asset.zone
        asset.zone = target_zone
        asset.path = f"{self.config.base_path}/{target_zone.value}/{asset.name}"
        asset.updated_at = time.time()

        if old_zone in self._zone_assets:
            self._zone_assets[old_zone] = [
                a for a in self._zone_assets[old_zone] if a != asset_id
            ]
        self._zone_assets[target_zone].append(asset_id)

        return asset

    async def transform(
        self,
        source_asset_id: str,
        target_name: str,
        target_zone: LakeZone,
        transformer: Callable[[DataAsset], Any],
        schema: Optional[Dict[str, Any]] = None,
    ) -> Optional[DataAsset]:
        """Transform data and write to a new asset."""
        source = await self.read(source_asset_id)
        if source is None:
            return None

        transformed = transformer(source)

        return await self.ingest(
            target_name,
            target_zone,
            transformed,
            schema=schema,
        )

    async def delete(
        self,
        asset_id: str,
        move_to_archive: bool = True,
    ) -> bool:
        """Delete a data asset."""
        asset = self._assets.get(asset_id)
        if asset is None:
            return False

        if move_to_archive:
            await self.move(asset_id, LakeZone.ARCHIVE)
        else:
            zone = asset.zone
            if zone in self._zone_assets:
                self._zone_assets[zone] = [
                    a for a in self._zone_assets[zone] if a != asset_id
                ]
            del self._assets[asset_id]
            if asset_id in self._catalog:
                del self._catalog[asset_id]

        self._stats["assets_deleted"] += 1
        return True

    def get_assets_by_zone(self, zone: LakeZone) -> List[DataAsset]:
        """Get all assets in a zone."""
        asset_ids = self._zone_assets.get(zone, [])
        return [self._assets[aid] for aid in asset_ids if aid in self._assets]

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get an asset by ID."""
        return self._assets.get(asset_id)

    def get_catalog_entry(self, asset_id: str) -> Optional[LakeCatalogEntry]:
        """Get catalog entry for an asset."""
        return self._catalog.get(asset_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get data lake statistics."""
        total_size = sum(a.size_bytes for a in self._assets.values())
        total_rows = sum(a.row_count for a in self._assets.values())

        return {
            **self._stats.copy(),
            "total_assets": len(self._assets),
            "total_size_bytes": total_size,
            "total_rows": total_rows,
            "assets_by_zone": {
                zone.value: len(assets)
                for zone, assets in self._zone_assets.items()
            },
        }


async def demo_data_lake():
    """Demonstrate data lake management."""
    config = DataLakeConfig()
    lake = DataLakeAction(config)

    asset = await lake.ingest(
        "sales-2024",
        LakeZone.BRONZE,
        [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}],
    )

    print(f"Asset created: {asset.asset_id}")
    print(f"Zone: {asset.zone.value}")

    silver_asset = await lake.move(asset.asset_id, LakeZone.SILVER)
    print(f"Moved to: {silver_asset.zone.value}")

    print(f"Stats: {lake.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_data_lake())
