"""
Data SCD2 Action Module

Implements Slowly Changing Dimension Type 2 (SCD2) for
tracking historical changes in dimension data.

Author: RabAi Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

import logging

logger = logging.getLogger(__name__)


class SCD2VersionStatus(Enum):
    """Status of an SCD2 version."""

    ACTIVE = auto()
    EXPIRED = auto()


@dataclass
class SCD2Version:
    """A versioned record in SCD2."""

    version_id: str
    business_key: str
    version_start: float
    version_end: Optional[float]
    is_current: bool
    status: SCD2VersionStatus
    version_number: int
    attributes: Dict[str, Any]

    def is_expired(self) -> bool:
        return self.status == SCD2VersionStatus.EXPIRED


@dataclass
class SCD2Config:
    """Configuration for SCD2 handling."""

    tracking_columns: List[str] = field(default_factory=list)
    compare_all_columns: bool = True
    precision_seconds: bool = True
    expire_on_change: bool = True


class SCD2DimensionTable:
    """Manages a slowly changing dimension table."""

    def __init__(self, config: Optional[SCD2Config] = None) -> None:
        self.config = config or SCD2Config()
        self._versions: Dict[str, List[SCD2Version]] = {}
        self._current: Dict[str, SCD2Version] = {}

    def get_business_key(self, record: Dict[str, Any]) -> str:
        """Extract business key from record."""
        if self.config.tracking_columns:
            key_parts = [str(record.get(c, "")) for c in self.config.tracking_columns]
            return "|".join(key_parts)
        key_field = self.config.tracking_columns[0] if self.config.tracking_columns else "id"
        return str(record.get(key_field, ""))

    def _compare_records(
        self,
        old_attrs: Dict[str, Any],
        new_attrs: Dict[str, Any],
    ) -> bool:
        """Compare two records to detect changes."""
        if self.config.compare_all_columns:
            tracking = set(old_attrs.keys())
        else:
            tracking = set(self.config.tracking_columns)

        for key in tracking:
            if old_attrs.get(key) != new_attrs.get(key):
                return True
        return False

    def upsert(
        self,
        business_key: str,
        new_attributes: Dict[str, Any],
        effective_date: Optional[float] = None,
    ) -> List[SCD2Version]:
        """Insert or update a record, creating version history if changed."""
        effective = effective_date or time.time()
        versions = self._versions.get(business_key, [])

        current = self._current.get(business_key)
        should_expire = False
        new_version_number = 1

        if current is not None:
            if self._compare_records(current.attributes, new_attributes):
                should_expire = True
                new_version_number = current.version_number + 1

        if should_expire and current:
            current.version_end = effective
            current.is_current = False
            current.status = SCD2VersionStatus.EXPIRED

            new_version = SCD2Version(
                version_id=f"{business_key}_v{new_version_number}",
                business_key=business_key,
                version_start=effective,
                version_end=None,
                is_current=True,
                status=SCD2VersionStatus.ACTIVE,
                version_number=new_version_number,
                attributes=new_attributes.copy(),
            )
            versions.append(new_version)
            self._current[business_key] = new_version
        elif current is None:
            new_version = SCD2Version(
                version_id=f"{business_key}_v{new_version_number}",
                business_key=business_key,
                version_start=effective,
                version_end=None,
                is_current=True,
                status=SCD2VersionStatus.ACTIVE,
                version_number=new_version_number,
                attributes=new_attributes.copy(),
            )
            versions.append(new_version)
            self._versions[business_key] = versions
            self._current[business_key] = new_version
        else:
            for k, v in new_attributes.items():
                current.attributes[k] = v

        return versions

    def get_current(self, business_key: str) -> Optional[SCD2Version]:
        """Get current version of a record."""
        return self._current.get(business_key)

    def get_history(self, business_key: str) -> List[SCD2Version]:
        """Get full version history for a record."""
        return self._versions.get(business_key, []).copy()

    def get_version_at(
        self,
        business_key: str,
        timestamp: float,
    ) -> Optional[SCD2Version]:
        """Get version of a record as of a specific timestamp."""
        versions = self._versions.get(business_key, [])
        for v in versions:
            if v.version_start <= timestamp:
                if v.version_end is None or v.version_end > timestamp:
                    return v
        return None

    def get_all_current(self) -> Dict[str, SCD2Version]:
        """Get all current versions."""
        return self._current.copy()

    def expire_version(
        self,
        business_key: str,
        version_number: int,
        effective_date: Optional[float] = None,
    ) -> bool:
        """Manually expire a specific version."""
        versions = self._versions.get(business_key, [])
        for v in versions:
            if v.version_number == version_number and v.is_current:
                v.version_end = effective_date or time.time()
                v.is_current = False
                v.status = SCD2VersionStatus.EXPIRED
                return True
        return False


class SCD2Action:
    """Action class for SCD2 dimension operations."""

    def __init__(self, config: Optional[SCD2Config] = None) -> None:
        self.dimension = SCD2DimensionTable(config)

    def process_batch(
        self,
        records: List[Dict[str, Any]],
        business_key_field: str = "id",
        effective_date: Optional[float] = None,
    ) -> Dict[str, List[SCD2Version]]:
        """Process a batch of records."""
        if business_key_field != "id" and not self.config.tracking_columns:
            self.dimension.config.tracking_columns = [business_key_field]

        results = {}
        for record in records:
            bk = str(record.get(business_key_field, ""))
            if bk:
                results[bk] = self.dimension.upsert(bk, record, effective_date)
        return results

    def get_dimension_snapshot(
        self,
        as_of_date: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Get a point-in-time snapshot of the dimension."""
        timestamp = as_of_date or time.time()
        snapshot = []

        for bk, versions in self.dimension._versions.items():
            for v in versions:
                if v.version_start <= timestamp:
                    if v.version_end is None or v.version_end > timestamp:
                        row = {"business_key": bk, **v.attributes, "_version_id": v.version_id}
                        snapshot.append(row)
                        break
        return snapshot
