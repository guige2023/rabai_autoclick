"""Data Enricher Action Module.

Enrich data with external sources and computed fields.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable
import time


@dataclass
class EnrichmentSource:
    """Data enrichment source."""
    name: str
    fetch_fn: Callable[[dict], Any]
    output_field: str
    cache_ttl: float = 300.0
    timeout: float = 10.0


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    enriched: dict
    sources_used: list[str]
    fetch_time: float
    errors: dict[str, str] = field(default_factory=dict)


class DataEnricher:
    """Enrich data with external sources and computed fields."""

    def __init__(self) -> None:
        self._sources: dict[str, EnrichmentSource] = {}
        self._computed_fields: list[tuple[str, Callable[[dict], Any]]] = []
        self._cache: dict[str, tuple[Any, float]] = {}

    def add_source(self, source: EnrichmentSource) -> None:
        """Add an enrichment source."""
        self._sources[source.name] = source

    def add_computed_field(
        self,
        field_name: str,
        compute_fn: Callable[[dict], Any]
    ) -> None:
        """Add a computed field."""
        self._computed_fields.append((field_name, compute_fn))

    async def enrich(
        self,
        data: dict,
        source_names: list[str] | None = None
    ) -> EnrichmentResult:
        """Enrich data with specified sources."""
        start = time.monotonic()
        enriched = dict(data)
        sources_used = []
        errors = {}
        targets = source_names or list(self._sources.keys())
        for name in targets:
            source = self._sources.get(name)
            if not source:
                continue
            try:
                value = await self._fetch_with_cache(source, data)
                enriched[source.output_field] = value
                sources_used.append(name)
            except Exception as e:
                errors[name] = str(e)
        for field_name, compute_fn in self._computed_fields:
            try:
                enriched[field_name] = compute_fn(enriched)
            except Exception:
                pass
        return EnrichmentResult(
            enriched=enriched,
            sources_used=sources_used,
            fetch_time=time.monotonic() - start,
            errors=errors
        )

    async def enrich_batch(
        self,
        records: list[dict],
        source_names: list[str] | None = None,
        max_concurrency: int = 5
    ) -> list[EnrichmentResult]:
        """Enrich multiple records with controlled concurrency."""
        semaphore = asyncio.Semaphore(max_concurrency)
        async def enrich_with_limit(record: dict) -> EnrichmentResult:
            async with semaphore:
                return await self.enrich(record, source_names)
        return await asyncio.gather(*[enrich_with_limit(r) for r in records])

    async def _fetch_with_cache(
        self,
        source: EnrichmentSource,
        data: dict
    ) -> Any:
        """Fetch from source with caching."""
        cache_key = f"{source.name}:{str(sorted(data.items()))}"
        now = time.time()
        if cache_key in self._cache:
            value, cached_at = self._cache[cache_key]
            if now - cached_at < source.cache_ttl:
                return value
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(source.fetch_fn, data),
                timeout=source.timeout
            )
            self._cache[cache_key] = (result, now)
            return result
        except asyncio.TimeoutError:
            raise Exception(f"Source {source.name} timed out")


class LookupEnricher:
    """Enrich data using lookup tables."""

    def __init__(self) -> None:
        self._lookups: dict[str, dict] = {}

    def add_lookup(self, name: str, mapping: dict) -> None:
        """Add a lookup table."""
        self._lookups[name] = mapping

    def lookup(
        self,
        data: dict,
        lookup_field: str,
        lookup_name: str,
        output_field: str | None = None,
        default: Any = None
    ) -> dict:
        """Perform lookup enrichment on data."""
        result = dict(data)
        lookup_table = self._lookups.get(lookup_name, {})
        key = data.get(lookup_field)
        output = output_field or f"{lookup_name}_{lookup_field}"
        result[output] = lookup_table.get(key, default)
        return result
