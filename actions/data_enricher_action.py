"""
Data Enricher Action Module.

Enriches data records by joining with external sources,
 appending computed fields, and resolving references.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentSource:
    """An external source for data enrichment."""
    name: str
    lookup_func: Callable[[Any], Optional[dict[str, Any]]]
    key_field: str
    merge_strategy: str = "overwrite"


@dataclass
class ComputedField:
    """A computed field definition."""
    name: str
    func: Callable[[dict[str, Any]], Any]
    dependencies: list[str] = field(default_factory=list)


@dataclass
class EnrichmentResult:
    """Result of an enrichment operation."""
    enriched_count: int
    lookup_count: int
    hit_count: int
    computed_count: int
    errors: list[dict[str, Any]] = field(default_factory=list)


class DataEnricherAction:
    """
    Data enrichment processor.

    Enriches records with data from external sources (databases, APIs)
    and computed fields based on existing data.

    Example:
        enricher = DataEnricherAction()
        enricher.add_lookup_source("users", lookup_user_by_id, key_field="user_id")
        enricher.add_computed_field("full_name", lambda r: f"{r['first']} {r['last']}")
        result = enricher.process(records)
    """

    def __init__(
        self,
        default_strategy: str = "overwrite",
    ) -> None:
        self.default_strategy = default_strategy
        self._sources: list[EnrichmentSource] = []
        self._computed_fields: list[ComputedField] = []
        self._reference_resolvers: dict[str, Callable[[str], Optional[Any]]] = {}

    def add_lookup_source(
        self,
        name: str,
        lookup_func: Callable[[Any], Optional[dict[str, Any]]],
        key_field: str,
        merge_strategy: str = "overwrite",
    ) -> "DataEnricherAction":
        """Add a lookup source for enrichment."""
        source = EnrichmentSource(
            name=name,
            lookup_func=lookup_func,
            key_field=key_field,
            merge_strategy=merge_strategy,
        )
        self._sources.append(source)
        return self

    def add_computed_field(
        self,
        name: str,
        func: Callable[[dict[str, Any]], Any],
        dependencies: Optional[list[str]] = None,
    ) -> "DataEnricherAction":
        """Add a computed field that derives value from other fields."""
        computed = ComputedField(
            name=name,
            func=func,
            dependencies=dependencies or [],
        )
        self._computed_fields.append(computed)
        return self

    def add_reference_resolver(
        self,
        ref_type: str,
        resolver_func: Callable[[str], Optional[Any]],
    ) -> "DataEnricherAction":
        """Add a reference resolver for URI/ID references."""
        self._reference_resolvers[ref_type] = resolver_func
        return self

    def process(
        self,
        records: list[dict[str, Any]],
        stop_on_error: bool = False,
    ) -> EnrichmentResult:
        """Process records through all enrichment steps."""
        enriched_count = 0
        lookup_count = 0
        hit_count = 0
        computed_count = 0
        errors: list[dict[str, Any]] = []

        for idx, record in enumerate(records):
            try:
                for source in self._sources:
                    lookup_count += 1
                    lookup_key = record.get(source.key_field)
                    if lookup_key is None:
                        continue

                    enriched = source.lookup_func(lookup_key)
                    if enriched:
                        hit_count += 1
                        self._merge_record(record, enriched, source.merge_strategy)

                for computed in self._computed_fields:
                    try:
                        deps = computed.dependencies
                        if not deps or all(d in record for d in deps):
                            record[computed.name] = computed.func(record)
                            computed_count += 1
                    except Exception as e:
                        logger.debug(f"Computed field '{computed.name}' failed: {e}")

                for ref_type, resolver in self._reference_resolvers.items():
                    for field_name in list(record.keys()):
                        if field_name.endswith(f"_{ref_type}_ref"):
                            ref_value = record.get(field_name)
                            if ref_value:
                                resolved = resolver(ref_value)
                                if resolved is not None:
                                    result_field = field_name.replace(f"_{ref_type}_ref", "")
                                    record[result_field] = resolved

                enriched_count += 1

            except Exception as e:
                error_entry = {"index": idx, "error": str(e), "record": record.get("id")}
                errors.append(error_entry)
                if stop_on_error:
                    raise

        return EnrichmentResult(
            enriched_count=enriched_count,
            lookup_count=lookup_count,
            hit_count=hit_count,
            computed_count=computed_count,
            errors=errors,
        )

    def _merge_record(
        self,
        target: dict[str, Any],
        source: dict[str, Any],
        strategy: str,
    ) -> None:
        """Merge source data into target record."""
        if strategy == "overwrite":
            target.update(source)
        elif strategy == "preserve":
            for key, value in source.items():
                if key not in target:
                    target[key] = value
        elif strategy == "prefix":
            for key, value in source.items():
                target[f"{key}_enriched"] = value
        elif strategy == "suffix":
            for key, value in source.items():
                target[f"{key}_ext"] = value
