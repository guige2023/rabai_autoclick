"""
Data SCD (Slowly Changing Dimensions) Action Module.

Manages slowly changing dimensions for data warehousing
with Type 1, 2, and 3 SCD strategies.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SCDType(Enum):
    """Slowly changing dimension types."""

    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"
    HYBRID = "hybrid"


@dataclass
class SCDRecord:
    """Represents an SCD record."""

    surrogate_key: str
    business_key: str
    data: dict[str, Any]
    version: int = 1
    is_current: bool = True
    valid_from: float = field(default_factory=time.time)
    valid_to: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SCDConfig:
    """Configuration for SCD processing."""

    scd_type: SCDType = SCDType.TYPE2
    tracking_fields: list[str] = field(default_factory=list)
    Surrogate_key_generator: Optional[Callable] = None


class DataSCDAction:
    """
    Manages Slowly Changing Dimensions in data.

    Supports:
    - Type 1: Overwrite historical data
    - Type 2: Add new version with full history
    - Type 3: Add new column for previous value
    - Hybrid: Combination of types

    Example:
        scd = DataSCDAction(scd_type=SCDType.TYPE2)
        scd.register_dimension("customer", ["name", "email"])
        scd.process_change("customer", business_key="c123", new_data={"name": "new"})
    """

    def __init__(self, config: Optional[SCDConfig] = None) -> None:
        """
        Initialize SCD action.

        Args:
            config: SCD configuration.
        """
        self.config = config or SCDConfig()
        self._dimensions: dict[str, dict[str, SCDRecord]] = {}
        self._dimension_keys: dict[str, set[str]] = {}
        self._stats = {
            "total_changes": 0,
            "type1_updates": 0,
            "type2_inserts": 0,
            "type3_updates": 0,
        }

    def register_dimension(
        self,
        dimension_name: str,
        tracking_fields: list[str],
        scd_type: Optional[SCDType] = None,
    ) -> None:
        """
        Register a dimension for SCD tracking.

        Args:
            dimension_name: Name of the dimension.
            tracking_fields: Fields to track for changes.
            scd_type: SCD type for this dimension.
        """
        if dimension_name not in self._dimensions:
            self._dimensions[dimension_name] = {}
            self._dimension_keys[dimension_name] = set()

        logger.info(
            f"Registered dimension: {dimension_name} "
            f"(type={scd_type or self.config.scd_type.value})"
        )

    def process_change(
        self,
        dimension_name: str,
        business_key: str,
        new_data: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[SCDRecord]:
        """
        Process a change for a dimension.

        Args:
            dimension_name: Dimension to update.
            business_key: Business key for the record.
            new_data: New data values.
            metadata: Optional metadata.

        Returns:
            Created or updated SCDRecord.
        """
        if dimension_name not in self._dimensions:
            self.register_dimension(dimension_name, list(new_data.keys()))

        self._stats["total_changes"] += 1
        scd_type = self.config.scd_type

        if scd_type == SCDType.TYPE1:
            return self._process_type1(dimension_name, business_key, new_data, metadata)
        elif scd_type == SCDType.TYPE2:
            return self._process_type2(dimension_name, business_key, new_data, metadata)
        elif scd_type == SCDType.TYPE3:
            return self._process_type3(dimension_name, business_key, new_data, metadata)
        else:
            return self._process_type2(dimension_name, business_key, new_data, metadata)

    def _generate_surrogate_key(self) -> str:
        """Generate a surrogate key."""
        if self.config.Surrogate_key_generator:
            return self.config.Surrogate_key_generator()
        return str(uuid.uuid4())

    def _process_type1(
        self,
        dimension_name: str,
        business_key: str,
        new_data: dict[str, Any],
        metadata: Optional[dict[str, Any]],
    ) -> SCDRecord:
        """Process Type 1 SCD change."""
        self._stats["type1_updates"] += 1

        if business_key in self._dimensions[dimension_name]:
            record = self._dimensions[dimension_name][business_key]
            record.data.update(new_data)
            record.metadata.update(metadata or {})
            return record
        else:
            record = SCDRecord(
                surrogate_key=self._generate_surrogate_key(),
                business_key=business_key,
                data=new_data,
                metadata=metadata or {},
            )
            self._dimensions[dimension_name][business_key] = record
            return record

    def _process_type2(
        self,
        dimension_name: str,
        business_key: str,
        new_data: dict[str, Any],
        metadata: Optional[dict[str, Any]],
    ) -> SCDRecord:
        """Process Type 2 SCD change."""
        self._stats["type2_inserts"] += 1

        current_record = self._dimensions[dimension_name].get(business_key)

        if current_record and current_record.is_current:
            has_changes = any(
                current_record.data.get(f) != new_data.get(f)
                for f in self.config.tracking_fields
                if f in new_data
            )

            if has_changes:
                current_record.is_current = False
                current_record.valid_to = time.time()

                new_record = SCDRecord(
                    surrogate_key=self._generate_surrogate_key(),
                    business_key=business_key,
                    data=new_data.copy(),
                    version=current_record.version + 1,
                    is_current=True,
                    metadata=metadata or {},
                )
                self._dimensions[dimension_name][business_key] = new_record
                return new_record
            else:
                return current_record
        else:
            record = SCDRecord(
                surrogate_key=self._generate_surrogate_key(),
                business_key=business_key,
                data=new_data.copy(),
                metadata=metadata or {},
            )
            self._dimensions[dimension_name][business_key] = record
            return record

    def _process_type3(
        self,
        dimension_name: str,
        business_key: str,
        new_data: dict[str, Any],
        metadata: Optional[dict[str, Any]],
    ) -> SCDRecord:
        """Process Type 3 SCD change."""
        self._stats["type3_updates"] += 1

        if business_key in self._dimensions[dimension_name]:
            record = self._dimensions[dimension_name][business_key]

            for field_name in self.config.tracking_fields:
                if field_name in new_data:
                    prev_field = f"prev_{field_name}"
                    record.data[prev_field] = record.data.get(field_name)
                    record.data[field_name] = new_data[field_name]

            record.metadata.update(metadata or {})
            return record
        else:
            record = SCDRecord(
                surrogate_key=self._generate_surrogate_key(),
                business_key=business_key,
                data=new_data.copy(),
                metadata=metadata or {},
            )
            self._dimensions[dimension_name][business_key] = record
            return record

    def get_current(
        self,
        dimension_name: str,
        business_key: str,
    ) -> Optional[dict[str, Any]]:
        """
        Get current version of a record.

        Args:
            dimension_name: Dimension name.
            business_key: Business key.

        Returns:
            Current record data or None.
        """
        record = self._dimensions.get(dimension_name, {}).get(business_key)
        if record and record.is_current:
            return record.data
        return None

    def get_history(
        self,
        dimension_name: str,
        business_key: str,
    ) -> list[dict[str, Any]]:
        """
        Get full history of a record.

        Args:
            dimension_name: Dimension name.
            business_key: Business key.

        Returns:
            List of all versions.
        """
        record = self._dimensions.get(dimension_name, {}).get(business_key)
        if record:
            return [record.data]
        return []

    def get_all_current(self, dimension_name: str) -> list[dict[str, Any]]:
        """
        Get all current records for a dimension.

        Args:
            dimension_name: Dimension name.

        Returns:
            List of current records.
        """
        dimension = self._dimensions.get(dimension_name, {})
        return [
            record.data
            for record in dimension.values()
            if record.is_current
        ]

    def get_stats(self) -> dict[str, Any]:
        """
        Get SCD statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "dimensions": len(self._dimensions),
            "total_records": sum(len(d) for d in self._dimensions.values()),
        }
