"""Data deduplication and uniqueness enforcement action."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class DedupeConfig:
    """Configuration for deduplication."""

    key_fields: list[str]
    strategy: str = "first"  # "first", "last", "largest", "smallest"
    compare_fields: Optional[list[str]] = None
    case_sensitive: bool = True
    normalize_whitespace: bool = True


@dataclass
class DedupeResult:
    """Result of deduplication."""

    original_count: int
    deduplicated_count: int
    removed_count: int
    removed_ids: list[int]
    kept_ids: list[int]


class DataDeduplicatorAction:
    """Removes duplicate records from datasets."""

    def __init__(
        self,
        default_config: Optional[DedupeConfig] = None,
    ):
        """Initialize deduplicator.

        Args:
            default_config: Default deduplication configuration.
        """
        self._default_config = default_config
        self._seen_keys: dict[str, Any] = {}

    def _normalize_key(self, value: str, case_sensitive: bool, normalize_ws: bool) -> str:
        """Normalize a string key."""
        if not case_sensitive:
            value = value.lower()
        if normalize_ws:
            value = " ".join(value.split())
        return value

    def _extract_key(self, record: dict[str, Any], config: DedupeConfig) -> str:
        """Extract deduplication key from record."""
        key_parts = []
        for field_name in config.key_fields:
            value = record.get(field_name)
            if value is None:
                key_parts.append("__null__")
            else:
                str_value = str(value)
                key_parts.append(
                    self._normalize_key(
                        str_value,
                        case_sensitive=config.case_sensitive,
                        normalize_ws=config.normalize_whitespace,
                    )
                )
        return "||".join(key_parts)

    def _compare_records(
        self,
        existing: dict[str, Any],
        new: dict[str, Any],
        config: DedupeConfig,
    ) -> bool:
        """Compare two records for equality."""
        compare_fields = config.compare_fields or config.key_fields
        for field_name in compare_fields:
            v1 = existing.get(field_name)
            v2 = new.get(field_name)
            if v1 != v2:
                return False
        return True

    def _should_keep_record(
        self,
        record: dict[str, Any],
        key: str,
        config: DedupeConfig,
    ) -> bool:
        """Determine if a record should be kept."""
        if key not in self._seen_keys:
            return True

        existing = self._seen_keys[key]

        if config.strategy == "first":
            return False
        elif config.strategy == "last":
            self._seen_keys[key] = record
            return False
        elif config.strategy == "largest":
            primary_field = config.key_fields[0] if config.key_fields else None
            if primary_field:
                existing_val = existing.get(primary_field, 0)
                new_val = record.get(primary_field, 0)
                if new_val > existing_val:
                    self._seen_keys[key] = record
                return False
        elif config.strategy == "smallest":
            primary_field = config.key_fields[0] if config.key_fields else None
            if primary_field:
                existing_val = existing.get(primary_field, float("inf"))
                new_val = record.get(primary_field, float("inf"))
                if new_val < existing_val:
                    self._seen_keys[key] = record
                return False

        return False

    def deduplicate(
        self,
        records: Sequence[dict[str, Any]],
        config: Optional[DedupeConfig] = None,
    ) -> DedupeResult:
        """Deduplicate a sequence of records.

        Args:
            records: Input records.
            config: Deduplication configuration.

        Returns:
            DedupeResult with statistics.
        """
        config = config or self._default_config
        if not config:
            raise ValueError("No deduplication config provided")

        self._seen_keys.clear()
        kept_ids = []
        removed_ids = []

        for idx, record in enumerate(records):
            key = self._extract_key(record, config)
            should_keep = self._should_keep_record(record, key, config)

            if should_keep:
                if key not in self._seen_keys:
                    self._seen_keys[key] = record
                kept_ids.append(idx)
            else:
                removed_ids.append(idx)

        return DedupeResult(
            original_count=len(records),
            deduplicated_count=len(kept_ids),
            removed_count=len(removed_ids),
            removed_ids=removed_ids,
            kept_ids=kept_ids,
        )

    def deduplicate_with_custom_key(
        self,
        records: Sequence[dict[str, Any]],
        key_func: Callable[[dict[str, Any]], Any],
        strategy: str = "first",
    ) -> DedupeResult:
        """Deduplicate using a custom key function."""
        self._seen_keys.clear()
        kept_ids = []
        removed_ids = []

        for idx, record in enumerate(records):
            key = str(key_func(record))
            if key not in self._seen_keys:
                self._seen_keys[key] = record
                kept_ids.append(idx)
            else:
                if strategy == "last":
                    self._seen_keys[key] = record
                removed_ids.append(idx)

        return DedupeResult(
            original_count=len(records),
            deduplicated_count=len(kept_ids),
            removed_count=len(removed_ids),
            removed_ids=removed_ids,
            kept_ids=kept_ids,
        )

    def find_duplicates(
        self,
        records: Sequence[dict[str, Any]],
        config: Optional[DedupeConfig] = None,
    ) -> list[tuple[int, int]]:
        """Find duplicate pairs in records.

        Returns:
            List of (idx1, idx2) tuples for duplicate pairs.
        """
        config = config or self._default_config
        if not config:
            raise ValueError("No deduplication config provided")

        key_to_indices: dict[str, list[int]] = {}

        for idx, record in enumerate(records):
            key = self._extract_key(record, config)
            if key not in key_to_indices:
                key_to_indices[key] = []
            key_to_indices[key].append(idx)

        duplicates = []
        for indices in key_to_indices.values():
            if len(indices) > 1:
                for i in range(len(indices) - 1):
                    duplicates.append((indices[i], indices[i + 1]))

        return duplicates

    def reset(self) -> None:
        """Reset internal state."""
        self._seen_keys.clear()
