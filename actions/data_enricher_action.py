"""Data Enricher Action Module.

Provides data enrichment with lookup tables, external APIs,
computed fields, and cross-reference joins.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EnrichmentSource(Enum):
    """Enrichment source type."""
    LOOKUP_TABLE = "lookup_table"
    EXTERNAL_API = "external_api"
    COMPUTED = "computed"
    CROSS_REFERENCE = "cross_reference"


@dataclass
class EnrichmentConfig:
    """Enrichment configuration."""
    source_type: EnrichmentSource
    source: Any
    key_field: str
    enrichment_fields: List[str]
    cache_ttl: float = 300.0


class DataEnricherAction:
    """Data enricher with multiple source types.

    Example:
        enricher = DataEnricherAction()

        enricher.add_lookup(
            key_field="country_code",
            lookup_table={
                "US": {"country": "United States", "currency": "USD"},
                "UK": {"country": "United Kingdom", "currency": "GBP"},
            },
            enrichment_fields=["country", "currency"]
        )

        result = enricher.enrich({"country_code": "US", "name": "John"})
        # result = {"country_code": "US", "name": "John", "country": "United States", "currency": "USD"}
    """

    def __init__(self) -> None:
        self._enrichments: List[EnrichmentConfig] = []
        self._cache: Dict[str, Dict] = {}
        self._cache_times: Dict[str, float] = {}
        import time
        self._time = time.time

    def add_lookup(
        self,
        key_field: str,
        lookup_table: Dict[Any, Dict],
        enrichment_fields: List[str],
    ) -> "DataEnricherAction":
        """Add lookup table enrichment."""
        self._enrichments.append(EnrichmentConfig(
            source_type=EnrichmentSource.LOOKUP_TABLE,
            source=lookup_table,
            key_field=key_field,
            enrichment_fields=enrichment_fields,
        ))
        return self

    def add_api_enrichment(
        self,
        key_field: str,
        api_func: Callable[[str], Dict],
        enrichment_fields: List[str],
        cache_ttl: float = 300.0,
    ) -> "DataEnricherAction":
        """Add external API enrichment."""
        self._enrichments.append(EnrichmentConfig(
            source_type=EnrichmentSource.EXTERNAL_API,
            source=api_func,
            key_field=key_field,
            enrichment_fields=enrichment_fields,
            cache_ttl=cache_ttl,
        ))
        return self

    def add_computed_fields(
        self,
        field_definitions: Dict[str, Callable],
    ) -> "DataEnricherAction":
        """Add computed field enrichment."""
        self._enrichments.append(EnrichmentConfig(
            source_type=EnrichmentSource.COMPUTED,
            source=field_definitions,
            key_field="",
            enrichment_fields=list(field_definitions.keys()),
        ))
        return self

    def enrich(
        self,
        record: Dict[str, Any],
        only_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Enrich single record.

        Args:
            record: Input record
            only_fields: Optional list of specific fields to enrich

        Returns:
            Enriched record
        """
        result = dict(record)

        for config in self._enrichments:
            if only_fields and not any(f in config.enrichment_fields for f in only_fields):
                continue

            if config.source_type == EnrichmentSource.LOOKUP_TABLE:
                self._enrich_from_lookup(result, config)
            elif config.source_type == EnrichmentSource.COMPUTED:
                self._enrich_from_computed(result, config)

        return result

    def enrich_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Enrich batch of records."""
        return [self.enrich(record) for record in records]

    async def enrich_async(
        self,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich record with async API calls."""
        result = dict(record)
        import time
        now = time.time()

        for config in self._enrichments:
            if config.source_type == EnrichmentSource.EXTERNAL_API:
                key_value = record.get(config.key_field)
                if not key_value:
                    continue

                cache_key = f"{config.key_field}:{key_value}"

                if cache_key in self._cache:
                    cached_time = self._cache_times.get(cache_key, 0)
                    if now - cached_time < config.cache_ttl:
                        cached_data = self._cache[cache_key]
                        for field_name in config.enrichment_fields:
                            if field_name in cached_data:
                                result[field_name] = cached_data[field_name]
                        continue

                try:
                    api_func = config.source
                    if asyncio.iscoroutinefunction(api_func):
                        enriched_data = await api_func(key_value)
                    else:
                        enriched_data = api_func(key_value)

                    self._cache[cache_key] = enriched_data
                    self._cache_times[cache_key] = now

                    for field_name in config.enrichment_fields:
                        if field_name in enriched_data:
                            result[field_name] = enriched_data[field_name]

                except Exception as e:
                    logger.error(f"API enrichment failed for {cache_key}: {e}")

        return result

    def _enrich_from_lookup(
        self,
        record: Dict[str, Any],
        config: EnrichmentConfig,
    ) -> None:
        """Enrich from lookup table."""
        key_value = record.get(config.key_field)
        if key_value is None:
            return

        lookup_table = config.source
        if key_value in lookup_table:
            lookup_data = lookup_table[key_value]
            for field_name in config.enrichment_fields:
                if field_name in lookup_data:
                    record[field_name] = lookup_data[field_name]

    def _enrich_from_computed(
        self,
        record: Dict[str, Any],
        config: EnrichmentConfig,
    ) -> None:
        """Enrich from computed fields."""
        field_definitions = config.source
        for field_name, func in field_definitions.items():
            try:
                record[field_name] = func(record)
            except Exception as e:
                logger.error(f"Computed field '{field_name}' failed: {e}")

    def clear_cache(self) -> None:
        """Clear enrichment cache."""
        self._cache.clear()
        self._cache_times.clear()
