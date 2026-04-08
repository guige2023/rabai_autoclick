"""
Data Enrichment Action Module.

Provides data enrichment capabilities with lookup tables,
external data joining, and computed field generation.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


@dataclass
class LookupTable:
    """Lookup table for data enrichment."""
    table_id: str
    name: str
    key_field: str
    data: Dict[Any, Dict[str, Any]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EnrichmentRule:
    """Enrichment rule definition."""
    rule_id: str
    name: str
    source_field: str
    target_field: str
    enrichment_type: str
    lookup_table_id: Optional[str] = None
    transform_func: Optional[Callable] = None
    default_value: Any = None
    required: bool = False


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    success: bool
    enriched_count: int
    failed_count: int
    errors: List[str] = field(default_factory=list)


class LookupManager:
    """Manages lookup tables."""

    def __init__(self):
        self.tables: Dict[str, LookupTable] = {}

    def add_table(self, table: LookupTable):
        """Add a lookup table."""
        self.tables[table.table_id] = table

    def remove_table(self, table_id: str) -> bool:
        """Remove a lookup table."""
        if table_id in self.tables:
            del self.tables[table_id]
            return True
        return False

    def get_table(self, table_id: str) -> Optional[LookupTable]:
        """Get lookup table by ID."""
        return self.tables.get(table_id)

    def lookup(
        self,
        table_id: str,
        key: Any,
        fields: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Look up value in table."""
        table = self.tables.get(table_id)
        if not table:
            return None

        result = table.data.get(key)
        if result and fields:
            return {k: result.get(k) for k in fields if k in result}

        return result

    def reverse_lookup(
        self,
        table_id: str,
        value: Any,
        key_field: str
    ) -> Optional[Any]:
        """Reverse lookup by value."""
        table = self.tables.get(table_id)
        if not table:
            return None

        for key, row in table.data.items():
            if row.get(key_field) == value:
                return key

        return None


class DataEnricher:
    """Enriches data with additional fields."""

    def __init__(self, lookup_manager: LookupManager):
        self.lookup_manager = lookup_manager
        self.rules: List[EnrichmentRule] = []

    def add_rule(self, rule: EnrichmentRule):
        """Add enrichment rule."""
        self.rules.append(rule)

    def remove_rule(self, rule_id: str) -> bool:
        """Remove enrichment rule."""
        for i, rule in enumerate(self.rules):
            if rule.rule_id == rule_id:
                self.rules.pop(i)
                return True
        return False

    def enrich_record(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Enrich a single record."""
        errors = []
        enriched = record.copy()

        for rule in self.rules:
            source_value = record.get(rule.source_field)

            if source_value is None and rule.required:
                errors.append(f"Missing required field: {rule.source_field}")
                continue

            try:
                if rule.enrichment_type == "lookup":
                    lookup_result = self.lookup_manager.lookup(
                        rule.lookup_table_id,
                        source_value
                    )
                    if lookup_result:
                        enriched[rule.target_field] = lookup_result
                    else:
                        enriched[rule.target_field] = rule.default_value

                elif rule.enrichment_type == "direct":
                    enriched[rule.target_field] = source_value

                elif rule.enrichment_type == "computed":
                    if rule.transform_func:
                        enriched[rule.target_field] = rule.transform_func(source_value, record)
                    else:
                        enriched[rule.target_field] = source_value

                elif rule.enrichment_type == "constant":
                    enriched[rule.target_field] = rule.default_value

            except Exception as e:
                errors.append(f"Rule {rule.rule_id} failed: {str(e)}")
                if rule.required:
                    enriched[rule.target_field] = None
                else:
                    enriched[rule.target_field] = rule.default_value

        return enriched, errors

    def enrich_batch(
        self,
        records: List[Dict[str, Any]]
    ) -> EnrichmentResult:
        """Enrich batch of records."""
        enriched_records = []
        failed_count = 0
        errors = []

        for record in records:
            enriched, record_errors = self.enrich_record(record)
            enriched_records.append(enriched)
            if record_errors:
                failed_count += 1
                errors.extend(record_errors)

        return EnrichmentResult(
            success=failed_count == 0,
            enriched_count=len(enriched_records),
            failed_count=failed_count,
            errors=errors
        )


class ComputedFieldGenerator:
    """Generates computed fields from existing data."""

    def __init__(self):
        self.generators: Dict[str, Callable] = {}

    def register(self, field_name: str, generator: Callable[[Dict[str, Any]], Any]):
        """Register a computed field generator."""
        self.generators[field_name] = generator

    def compute(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Compute all registered fields."""
        result = record.copy()

        for field_name, generator in self.generators.items():
            try:
                result[field_name] = generator(record)
            except Exception as e:
                logger.error(f"Computed field {field_name} failed: {e}")
                result[field_name] = None

        return result

    def compute_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Compute fields for batch."""
        return [self.compute(record) for record in records]


def main():
    """Demonstrate data enrichment."""
    lookup_manager = LookupManager()

    lookup_manager.add_table(LookupTable(
        table_id="status_codes",
        name="Status Code Lookup",
        key_field="code",
        data={
            200: {"label": "Success", "category": "OK"},
            400: {"label": "Bad Request", "category": "Client Error"},
            500: {"label": "Server Error", "category": "Server Error"}
        }
    ))

    enricher = DataEnricher(lookup_manager)
    enricher.add_rule(EnrichmentRule(
        rule_id="rule1",
        name="Status Label",
        source_field="status_code",
        target_field="status_label",
        enrichment_type="lookup",
        lookup_table_id="status_codes"
    ))

    record = {"id": 1, "status_code": 200, "value": 100}
    enriched, errors = enricher.enrich_record(record)

    print(f"Enriched: {enriched}")
    print(f"Errors: {errors}")


if __name__ == "__main__":
    main()
