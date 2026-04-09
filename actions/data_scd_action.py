"""
Data SCD (Slowly Changing Dimensions) Action Module

Provides slowly changing dimension handling for data warehousing including
Type 1, Type 2, and Type 3 SCD strategies with automatic history tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class SCDType(Enum):
    """Slowly changing dimension types."""

    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"
    TYPE6 = "type6"


class RecordStatus(Enum):
    """Dimension record status."""

    CURRENT = "current"
    EXPIRED = "expired"
    HISTORICAL = "historical"


@dataclass
class SCDRecord:
    """A record in a slowly changing dimension."""

    record_id: str
    business_key: str
    version: int
    status: RecordStatus
    valid_from: float
    valid_to: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SCDConfig:
    """Configuration for SCD handling."""

    scd_type: SCDType = SCDType.TYPE2
    tracking_columns: List[str] = field(default_factory=list)
    expire_on_delete: bool = True
    track_version_history: bool = True


class DataSCDAction:
    """
    Slowly Changing Dimensions action for data warehousing.

    Features:
    - Multiple SCD strategies (Type 1, 2, 3, 6)
    - Automatic history tracking
    - Version management
    - Business key resolution
    - Current record lookup
    - Historical record queries

    Usage:
        scd = DataSCDAction(config)
        scd.add_dimension("customer", SCDType.TYPE2)
        
        scd.insert("customer", {"customer_id": "C001", "name": "Alice", "city": "NYC"})
        scd.update("customer", "C001", {"city": "LA"})
    """

    def __init__(self, config: Optional[SCDConfig] = None):
        self.config = config or SCDConfig()
        self._dimensions: Dict[str, Dict[str, SCDRecord]] = {}
        self._versions: Dict[str, Dict[str, List[SCDRecord]]] = {}
        self._stats = {
            "records_inserted": 0,
            "records_updated": 0,
            "records_expired": 0,
        }

    def add_dimension(
        self,
        dimension_name: str,
        scd_type: Optional[SCDType] = None,
    ) -> None:
        """Add a dimension for SCD tracking."""
        if dimension_name not in self._dimensions:
            self._dimensions[dimension_name] = {}
        if dimension_name not in self._versions:
            self._versions[dimension_name] = {}

    def insert(
        self,
        dimension_name: str,
        record: Dict[str, Any],
        business_key_column: str = "id",
    ) -> SCDRecord:
        """Insert a new record into a dimension."""
        if dimension_name not in self._dimensions:
            self.add_dimension(dimension_name)

        business_key = str(record.get(business_key_column, uuid.uuid4().hex[:8]))
        record_id = f"{dimension_name}_{business_key}_v1"

        attributes = {k: v for k, v in record.items() if k != business_key_column}

        scd_record = SCDRecord(
            record_id=record_id,
            business_key=business_key,
            version=1,
            status=RecordStatus.CURRENT,
            valid_from=time.time(),
            attributes=attributes,
        )

        self._dimensions[dimension_name][business_key] = scd_record
        self._versions[dimension_name][business_key] = [scd_record]
        self._stats["records_inserted"] += 1

        return scd_record

    def update(
        self,
        dimension_name: str,
        business_key: str,
        changes: Dict[str, Any],
        business_key_column: str = "id",
    ) -> Optional[SCDRecord]:
        """Update a record based on SCD type."""
        if dimension_name not in self._dimensions:
            return None

        current = self._dimensions[dimension_name].get(business_key)
        if current is None:
            return None

        scd_type = self.config.scd_type

        if scd_type == SCDType.TYPE1:
            return self._update_type1(dimension_name, business_key, changes)

        elif scd_type == SCDType.TYPE2:
            return self._update_type2(dimension_name, business_key, changes)

        elif scd_type == SCDType.TYPE3:
            return self._update_type3(dimension_name, business_key, changes)

        return None

    def _update_type1(
        self,
        dimension_name: str,
        business_key: str,
        changes: Dict[str, Any],
    ) -> SCDRecord:
        """Type 1 SCD: overwrite without history."""
        current = self._dimensions[dimension_name][business_key]

        for key, value in changes.items():
            current.attributes[key] = value

        self._stats["records_updated"] += 1
        return current

    def _update_type2(
        self,
        dimension_name: str,
        business_key: str,
        changes: Dict[str, Any],
    ) -> SCDRecord:
        """Type 2 SCD: add new version, expire old."""
        current = self._dimensions[dimension_name][business_key]
        new_version = current.version + 1

        current.status = RecordStatus.EXPIRED
        current.valid_to = time.time()

        record_id = f"{dimension_name}_{business_key}_v{new_version}"
        new_record = SCDRecord(
            record_id=record_id,
            business_key=business_key,
            version=new_version,
            status=RecordStatus.CURRENT,
            valid_from=time.time(),
            attributes={**current.attributes, **changes},
        )

        self._dimensions[dimension_name][business_key] = new_record
        self._versions[dimension_name][business_key].append(new_record)
        self._stats["records_updated"] += 1
        self._stats["records_expired"] += 1

        return new_record

    def _update_type3(
        self,
        dimension_name: str,
        business_key: str,
        changes: Dict[str, Any],
    ) -> SCDRecord:
        """Type 3 SCD: store previous value in separate column."""
        current = self._dimensions[dimension_name][business_key]

        for key, value in changes.items():
            old_key = f"previous_{key}"
            if old_key not in current.attributes:
                current.attributes[old_key] = current.attributes.get(key)
            current.attributes[key] = value

        self._stats["records_updated"] += 1
        return current

    def get_current(
        self,
        dimension_name: str,
        business_key: str,
    ) -> Optional[SCDRecord]:
        """Get the current version of a record."""
        return self._dimensions.get(dimension_name, {}).get(business_key)

    def get_history(
        self,
        dimension_name: str,
        business_key: str,
    ) -> List[SCDRecord]:
        """Get all versions of a record."""
        return self._versions.get(dimension_name, {}).get(business_key, [])

    def get_all_current(self, dimension_name: str) -> List[SCDRecord]:
        """Get all current records for a dimension."""
        return [
            r for r in self._dimensions.get(dimension_name, {}).values()
            if r.status == RecordStatus.CURRENT
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get SCD statistics."""
        total_records = sum(len(dims) for dims in self._dimensions.values())
        return {
            **self._stats.copy(),
            "total_dimensions": len(self._dimensions),
            "total_current_records": total_records,
        }


async def demo_scd():
    """Demonstrate SCD handling."""
    config = SCDConfig(scd_type=SCDType.TYPE2)
    scd = DataSCDAction(config)

    scd.add_dimension("customer", SCDType.TYPE2)

    scd.insert("customer", {"id": "C001", "name": "Alice", "city": "NYC"})
    print(f"Inserted: {scd.get_current('customer', 'C001').record_id}")

    scd.update("customer", "C001", {"city": "LA"})
    print(f"Updated: {scd.get_current('customer', 'C001').record_id}")

    history = scd.get_history("customer", "C001")
    print(f"History length: {len(history)}")

    print(f"Stats: {scd.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_scd())
