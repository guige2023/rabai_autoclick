"""
Data Enrichment Action - Enriches data with external sources and lookups.

This module provides data enrichment capabilities including lookups,
joins, computed fields, and external data source integration.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum
import hashlib


T = TypeVar("T")


class EnrichmentType(Enum):
    """Type of data enrichment."""
    LOOKUP = "lookup"
    JOIN = "join"
    COMPUTED = "computed"
    EXTERNAL = "external"
    CACHED = "cached"


@dataclass
class LookupTable:
    """A lookup table for data enrichment."""
    name: str
    key_field: str
    value_fields: list[str]
    data: dict[Any, dict[str, Any]] = field(default_factory=dict)
    _lookup_index: dict[Any, dict[str, Any]] = field(default_factory=dict, repr=False)
    
    def __post_init__(self) -> None:
        self._build_index()
    
    def _build_index(self) -> None:
        """Build lookup index for fast access."""
        for record in self.data.values():
            key = record.get(self.key_field)
            if key is not None:
                self._lookup_index[key] = record
    
    def add(self, key: Any, record: dict[str, Any]) -> None:
        """Add a record to the lookup table."""
        self.data[key] = record
        self._lookup_index[key] = record
    
    def get(self, key: Any) -> dict[str, Any] | None:
        """Get a record by key."""
        return self._lookup_index.get(key)
    
    def lookup(self, key: Any, value_field: str) -> Any | None:
        """Lookup a specific value field."""
        record = self._lookup_index.get(key)
        return record.get(value_field) if record else None


@dataclass
class EnrichmentRule:
    """A rule defining how to enrich data."""
    rule_id: str
    name: str
    enrichment_type: EnrichmentType
    source_field: str
    target_field: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnrichmentResult:
    """Result of data enrichment operation."""
    enriched_count: int
    failed_count: int
    enriched_records: list[dict[str, Any]]
    errors: list[str] = field(default_factory=list)
    duration_ms: float = 0.0


class DataEnricher:
    """
    Enriches data using various strategies.
    
    Example:
        enricher = DataEnricher()
        enricher.add_lookup_table("countries", countries_data, "country_code")
        enriched = enricher.enrich(records, "country_code", "country_name")
    """
    
    def __init__(self) -> None:
        self._lookup_tables: dict[str, LookupTable] = {}
        self._computed_functions: dict[str, Callable[[Any], Any]] = {}
        self._external_sources: dict[str, Callable[[Any], Any]] = {}
        self._cache: dict[str, Any] = {}
        self._cache_ttl: float = 300.0
        self._cache_timestamps: dict[str, float] = {}
    
    def add_lookup_table(
        self,
        name: str,
        data: list[dict[str, Any]],
        key_field: str,
        value_fields: list[str] | None = None,
    ) -> None:
        """Add a lookup table for enrichment."""
        if value_fields is None:
            value_fields = list(data[0].keys()) if data else []
        
        lookup_data = {i: record for i, record in enumerate(data)}
        table = LookupTable(
            name=name,
            key_field=key_field,
            value_fields=value_fields,
            data=lookup_data,
        )
        for i, record in enumerate(data):
            table.add(i, record)
        
        self._lookup_tables[name] = table
    
    def add_computed_field(
        self,
        name: str,
        func: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Add a computed field function."""
        self._computed_functions[name] = func
    
    def add_external_source(
        self,
        name: str,
        fetcher: Callable[[Any], Any],
    ) -> None:
        """Add an external data source."""
        self._external_sources[name] = fetcher
    
    async def enrich(
        self,
        records: list[dict[str, Any]],
        rule: EnrichmentRule,
    ) -> EnrichmentResult:
        """Enrich records based on a rule."""
        start_time = time.time()
        enriched_records = []
        errors = []
        failed = 0
        
        for i, record in enumerate(records):
            try:
                enriched = await self._apply_rule(record, rule)
                enriched_records.append(enriched)
            except Exception as e:
                errors.append(f"Record {i}: {str(e)}")
                failed += 1
        
        return EnrichmentResult(
            enriched_count=len(enriched_records),
            failed_count=failed,
            enriched_records=enriched_records,
            errors=errors,
            duration_ms=(time.time() - start_time) * 1000,
        )
    
    async def _apply_rule(
        self,
        record: dict[str, Any],
        rule: EnrichmentRule,
    ) -> dict[str, Any]:
        """Apply a single enrichment rule to a record."""
        result = record.copy()
        
        if rule.enrichment_type == EnrichmentType.LOOKUP:
            lookup_name = rule.params.get("lookup_table")
            if lookup_name in self._lookup_tables:
                table = self._lookup_tables[lookup_name]
                key = record.get(rule.source_field)
                value = table.lookup(key, rule.params.get("value_field", rule.target_field))
                result[rule.target_field] = value
        
        elif rule.enrichment_type == EnrichmentType.COMPUTED:
            func_name = rule.params.get("function")
            if func_name in self._computed_functions:
                func = self._computed_functions[func_name]
                result[rule.target_field] = func(record)
        
        elif rule.enrichment_type == EnrichmentType.EXTERNAL:
            source_name = rule.params.get("source")
            if source_name in self._external_sources:
                fetcher = self._external_sources[source_name]
                key = record.get(rule.source_field)
                cache_key = f"{source_name}:{key}"
                
                if cache_key in self._cache:
                    if (time.time() - self._cache_timestamps.get(cache_key, 0)) < self._cache_ttl:
                        result[rule.target_field] = self._cache[cache_key]
                    else:
                        value = await fetcher(key)
                        self._cache[cache_key] = value
                        self._cache_timestamps[cache_key] = time.time()
                        result[rule.target_field] = value
                else:
                    value = await fetcher(key)
                    self._cache[cache_key] = value
                    self._cache_timestamps[cache_key] = time.time()
                    result[rule.target_field] = value
        
        elif rule.enrichment_type == EnrichmentType.JOIN:
            join_table = rule.params.get("join_table")
            join_key = rule.params.get("join_key", rule.source_field)
            if join_table in self._lookup_tables:
                table = self._lookup_tables[join_table]
                key = record.get(join_key)
                joined = table.get(key)
                if joined:
                    for field_name in rule.params.get("fields", []):
                        result[f"{rule.target_field}_{field_name}"] = joined.get(field_name)
        
        return result
    
    def lookup_sync(
        self,
        lookup_name: str,
        key: Any,
        value_field: str,
    ) -> Any | None:
        """Synchronous lookup from a table."""
        if lookup_name in self._lookup_tables:
            return self._lookup_tables[lookup_name].lookup(key, value_field)
        return None
    
    def clear_cache(self) -> None:
        """Clear the enrichment cache."""
        self._cache.clear()
        self._cache_timestamps.clear()


class DataEnrichmentAction:
    """
    Data enrichment action for automation workflows.
    
    Example:
        action = DataEnrichmentAction()
        action.add_lookup_table("geo", geo_data, "zip_code")
        
        rule = EnrichmentRule(
            rule_id="add_city",
            name="Add City",
            enrichment_type=EnrichmentType.LOOKUP,
            source_field="zip_code",
            target_field="city",
            params={"lookup_table": "geo", "value_field": "city"},
        )
        
        result = await action.enrich_records(records, [rule])
    """
    
    def __init__(self) -> None:
        self.enricher = DataEnricher()
    
    def add_lookup(
        self,
        name: str,
        data: list[dict[str, Any]],
        key_field: str,
    ) -> None:
        """Add a lookup table."""
        self.enricher.add_lookup_table(name, data, key_field)
    
    def add_computed(
        self,
        name: str,
        func: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Add a computed field."""
        self.enricher.add_computed_field(name, func)
    
    async def enrich_records(
        self,
        records: list[dict[str, Any]],
        rules: list[EnrichmentRule],
    ) -> EnrichmentResult:
        """Enrich records with multiple rules."""
        current_records = records
        total_failed = 0
        all_errors = []
        
        for rule in rules:
            result = await self.enricher.enrich(current_records, rule)
            current_records = result.enriched_records
            total_failed += result.failed_count
            all_errors.extend(result.errors)
        
        return EnrichmentResult(
            enriched_count=len(current_records),
            failed_count=total_failed,
            enriched_records=current_records,
            errors=all_errors,
        )
    
    def lookup(self, table: str, key: Any, field: str) -> Any | None:
        """Direct lookup from a table."""
        return self.enricher.lookup_sync(table, key, field)


# Export public API
__all__ = [
    "EnrichmentType",
    "LookupTable",
    "EnrichmentRule",
    "EnrichmentResult",
    "DataEnricher",
    "DataEnrichmentAction",
]
