"""
Data Coalesce Action Module.

Returns the first non-null value from a list of fields for each record,
similar to SQL COALESCE. Supports nested field access and default values.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class CoalesceResult:
    """Result of coalesce operation."""
    records: list[dict[str, Any]]
    fields_coalesced: int
    defaults_used: int


class DataCoalesceAction(BaseAction):
    """Coalesce multiple fields to find first non-null value."""

    def __init__(self) -> None:
        super().__init__("data_coalesce")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Coalesce fields in records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - target_field: Output field name
                - source_fields: List of fields to coalesce (in order)
                - default_value: Default if all fields are null
                - nested_access: Support dot-notation for nested fields

        Returns:
            CoalesceResult with coalesced records
        """
        records = params.get("records", [])
        target_field = params.get("target_field", "coalesced")
        source_fields = params.get("source_fields", [])
        default_value = params.get("default_value", None)
        nested_access = params.get("nested_access", False)

        if not records or not source_fields:
            return CoalesceResult(records, 0, 0)

        fields_coalesced = 0
        defaults_used = 0

        for r in records:
            if not isinstance(r, dict):
                continue

            value = default_value
            for field in source_fields:
                extracted = self._extract_field(r, field, nested_access)
                if extracted is not None:
                    value = extracted
                    fields_coalesced += 1
                    break
            else:
                defaults_used += 1

            r[target_field] = value

        return CoalesceResult(
            records=records,
            fields_coalesced=fields_coalesced,
            defaults_used=defaults_used
        )

    def _extract_field(self, record: dict, field: str, nested: bool) -> Any:
        """Extract field value, optionally with nested access."""
        if not nested:
            return record.get(field)

        parts = field.split(".")
        current = record
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current
