"""
Data SCD (Slowly Changing Dimensions) Module.

Implements SCD Type 1, 2, 3, 4, and 6 patterns for
managing historical changes in dimension tables.
Supports current and historical record management.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class SCDType(Enum):
    """Slowly Changing Dimension types."""
    TYPE1 = 1
    TYPE2 = 2
    TYPE3 = 3
    TYPE4 = 4
    TYPE6 = 6


@dataclass
class SCDRecord:
    """Represents a dimension record with SCD metadata."""
    surrogate_key: int
    natural_key: str
    attributes: dict[str, Any]
    version: int = 1
    effective_date: float = field(default_factory=time.time)
    expiration_date: float = float("inf")
    is_current: bool = True
    checksum: Optional[str] = None


@dataclass
class SCDConfig:
    """Configuration for SCD processing."""
    scd_type: SCDType = SCDType.TYPE2
    tracking_columns: list[str] = field(default_factory=list)
    surrogate_key_name: str = "surrogate_key"
    hash_attributes: bool = True


class SCDProcessor:
    """
    Slowly Changing Dimensions processor.

    Handles historical changes in dimension tables using
    various SCD strategies.

    Example:
        processor = SCDProcessor(config)
        processor.load_existing(records)
        changes = processor.process(incoming_record)
    """

    def __init__(self, config: Optional[SCDConfig] = None) -> None:
        self._config = config or SCDConfig()
        self._records: dict[str, list[SCDRecord]] = {}
        self._next_surrogate_key: dict[str, int] = {}
        self._change_callbacks: list[Callable[[SCDRecord, SCDRecord], None]] = []

    def load_existing(
        self,
        table_name: str,
        records: list[SCDRecord]
    ) -> None:
        """Load existing dimension records."""
        self._records[table_name] = records
        if records:
            max_key = max(r.surrogate_key for r in records)
            self._next_surrogate_key[table_name] = max_key + 1
        else:
            self._next_surrogate_key[table_name] = 1

    def on_change(
        self,
        callback: Callable[[SCDRecord, SCDRecord], None]
    ) -> None:
        """Register callback for SCD changes."""
        self._change_callbacks.append(callback)

    def process(
        self,
        table_name: str,
        natural_key: str,
        attributes: dict[str, Any],
        effective_date: Optional[float] = None
    ) -> tuple[list[SCDRecord], list[str]]:
        """
        Process an incoming dimension record.

        Args:
            table_name: Dimension table name
            natural_key: Business key
            attributes: Dimension attributes
            effective_date: When the change takes effect

        Returns:
            Tuple of (records to upsert, list of action descriptions)
        """
        if table_name not in self._records:
            self._records[table_name] = []
            self._next_surrogate_key[table_name] = 1

        existing_records = self._records[table_name]
        current_record = self._get_current_record(existing_records, natural_key)

        if self._config.scd_type == SCDType.TYPE1:
            return self._process_type1(table_name, natural_key, attributes, current_record)
        elif self._config.scd_type == SCDType.TYPE2:
            return self._process_type2(
                table_name, natural_key, attributes,
                current_record, effective_date or time.time()
            )
        elif self._config.scd_type == SCDType.TYPE3:
            return self._process_type3(table_name, attributes, current_record)
        elif self._config.scd_type == SCDType.TYPE4:
            return self._process_type4(table_name, natural_key, attributes, current_record)
        elif self._config.scd_type == SCDType.TYPE6:
            return self._process_type6(
                table_name, natural_key, attributes,
                current_record, effective_date or time.time()
            )

        return [], []

    def _get_current_record(
        self,
        records: list[SCDRecord],
        natural_key: str
    ) -> Optional[SCDRecord]:
        """Get the current (latest) record for a natural key."""
        matches = [r for r in records if r.natural_key == natural_key and r.is_current]
        return matches[0] if matches else None

    def _compute_checksum(self, attributes: dict[str, Any]) -> str:
        """Compute checksum for attributes."""
        import hashlib
        content = "|".join(f"{k}={attributes.get(k)}" for k in sorted(attributes.keys()))
        return hashlib.md5(content.encode()).hexdigest()

    def _process_type1(
        self,
        table_name: str,
        natural_key: str,
        attributes: dict[str, Any],
        current: Optional[SCDRecord]
    ) -> tuple[list[SCDRecord], list[str]]:
        """SCD Type 1: Overwrite - no history."""
        actions = []

        if current:
            current.attributes = attributes
            if self._config.hash_attributes:
                current.checksum = self._compute_checksum(attributes)
            actions.append(f"TYPE1_UPDATE:{natural_key}")
        else:
            new_key = self._next_surrogate_key.get(table_name, 1)
            self._next_surrogate_key[table_name] = new_key + 1
            record = SCDRecord(
                surrogate_key=new_key,
                natural_key=natural_key,
                attributes=attributes,
                checksum=self._compute_checksum(attributes) if self._config.hash_attributes else None
            )
            self._records[table_name].append(record)
            actions.append(f"TYPE1_INSERT:{natural_key}")

        return [], actions

    def _process_type2(
        self,
        table_name: str,
        natural_key: str,
        attributes: dict[str, Any],
        current: Optional[SCDRecord],
        effective_date: float
    ) -> tuple[list[SCDRecord], list[str]]:
        """SCD Type 2: Add new row - full history."""
        actions = []
        to_upsert = []

        if current:
            new_checksum = self._compute_checksum(attributes) if self._config.hash_attributes else None

            if current.checksum == new_checksum:
                return [], []

            current.is_current = False
            current.expiration_date = effective_date
            actions.append(f"TYPE2_EXPIRE:{natural_key}")

        new_key = self._next_surrogate_key.get(table_name, 1)
        self._next_surrogate_key[table_name] = new_key + 1

        new_record = SCDRecord(
            surrogate_key=new_key,
            natural_key=natural_key,
            attributes=attributes.copy(),
            version=(current.version + 1) if current else 1,
            effective_date=effective_date,
            is_current=True,
            checksum=self._compute_checksum(attributes) if self._config.hash_attributes else None
        )

        to_upsert.append(new_record)
        self._records[table_name].append(new_record)
        actions.append(f"TYPE2_INSERT:{natural_key}")

        if current:
            for callback in self._change_callbacks:
                callback(current, new_record)

        return to_upsert, actions

    def _process_type3(
        self,
        table_name: str,
        attributes: dict[str, Any],
        current: Optional[SCDRecord]
    ) -> tuple[list[SCDRecord], list[str]]:
        """SCD Type 3: Add new column - limited history."""
        actions = []

        if current:
            for attr, value in attributes.items():
                prev_key = f"prev_{attr}"
                if prev_key in current.attributes:
                    current.attributes[f"prev_{attr}"] = current.attributes[attr]
                current.attributes[attr] = value
            actions.append(f"TYPE3_UPDATE:{current.natural_key}")
        else:
            new_key = self._next_surrogate_key.get(table_name, 1)
            self._next_surrogate_key[table_name] = new_key + 1
            record = SCDRecord(
                surrogate_key=new_key,
                natural_key=table_name,
                attributes=attributes
            )
            self._records[table_name].append(record)
            actions.append(f"TYPE3_INSERT")

        return [], actions

    def _process_type4(
        self,
        table_name: str,
        natural_key: str,
        attributes: dict[str, Any],
        current: Optional[SCDRecord]
    ) -> tuple[list[SCDRecord], list[str]]:
        """SCD Type 4: History table separate from current."""
        history_table = f"{table_name}_history"
        actions = []

        if history_table not in self._records:
            self._records[history_table] = []
            self._next_surrogate_key[history_table] = 1

        if current:
            self._records[history_table].append(current)
            actions.append(f"TYPE4_MOVE_TO_HISTORY:{natural_key}")

        new_key = self._next_surrogate_key.get(table_name, 1)
        self._next_surrogate_key[table_name] = new_key + 1

        new_record = SCDRecord(
            surrogate_key=new_key,
            natural_key=natural_key,
            attributes=attributes.copy()
        )

        self._records[table_name] = [r for r in self._records[table_name] if r.natural_key != natural_key]
        self._records[table_name].append(new_record)
        actions.append(f"TYPE4_INSERT:{natural_key}")

        return [new_record], actions

    def _process_type6(
        self,
        table_name: str,
        natural_key: str,
        attributes: dict[str, Any],
        current: Optional[SCDRecord],
        effective_date: float
    ) -> tuple[list[SCDRecord], list[str]]:
        """SCD Type 6: Hybrid (1+2+3) - current overwritten, history added."""
        actions = []

        if current:
            for attr, value in attributes.items():
                prev_key = f"prev_{attr}"
                current.attributes[prev_key] = current.attributes.get(attr)
                current.attributes[attr] = value
            current.is_current = False
            current.expiration_date = effective_date
            actions.append(f"TYPE6_EXPIRE:{natural_key}")

        new_key = self._next_surrogate_key.get(table_name, 1)
        self._next_surrogate_key[table_name] = new_key + 1

        new_record = SCDRecord(
            surrogate_key=new_key,
            natural_key=natural_key,
            attributes=attributes.copy()
        )

        self._records[table_name].append(new_record)
        actions.append(f"TYPE6_INSERT:{natural_key}")

        return [new_record], actions

    def get_history(
        self,
        table_name: str,
        natural_key: str
    ) -> list[SCDRecord]:
        """Get full history for a natural key."""
        records = self._records.get(table_name, [])
        return sorted(
            [r for r in records if r.natural_key == natural_key],
            key=lambda r: r.effective_date
        )

    def get_current(
        self,
        table_name: str,
        natural_key: str
    ) -> Optional[SCDRecord]:
        """Get current record for a natural key."""
        return self._get_current_record(
            self._records.get(table_name, []),
            natural_key
        )

    def get_all_current(self, table_name: str) -> list[SCDRecord]:
        """Get all current records for a table."""
        records = self._records.get(table_name, [])
        return [r for r in records if r.is_current]

    def get_record_count(self, table_name: str) -> dict[str, int]:
        """Get record counts for a table."""
        records = self._records.get(table_name, [])
        return {
            "total": len(records),
            "current": sum(1 for r in records if r.is_current),
            "expired": sum(1 for r in records if not r.is_current)
        }
