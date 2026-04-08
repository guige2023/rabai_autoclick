"""
Data Enrichment Action Module

Provides data enrichment, augmentation, and transformation capabilities.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import asyncio


@dataclass
class EnrichmentSource:
    """An enrichment data source."""
    name: str
    lookup: Callable[[Any], Awaitable[dict[str, Any]]]
    key_field: str
    priority: int = 0
    cache_ttl_seconds: int = 3600
    timeout_seconds: float = 10.0


@dataclass
class EnrichmentConfig:
    """Configuration for enrichment."""
    sources: list[EnrichmentSource] = field(default_factory=list)
    merge_strategy: str = "override"  # override, preserve, merge
    error_strategy: str = "skip"  # skip, fail, placeholder
    parallel: bool = True
    max_enrichments_per_record: int = 10


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    original: dict[str, Any]
    enriched: dict[str, Any]
    sources_applied: list[str]
    errors: list[str]
    duration_ms: float


class DataEnrichmentAction:
    """Main data enrichment action handler."""
    
    def __init__(self, config: Optional[EnrichmentConfig] = None):
        self.config = config or EnrichmentConfig()
        self._cache: dict[str, tuple[Any, datetime]] = {}
        self._enrichment_stats: dict[str, dict] = defaultdict(lambda: {
            "hits": 0, "misses": 0, "errors": 0
        })
    
    def add_source(self, source: EnrichmentSource) -> "DataEnrichmentAction":
        """Add an enrichment source."""
        self.config.sources.append(source)
        self.config.sources.sort(key=lambda s: s.priority)
        return self
    
    async def enrich_record(
        self,
        record: dict[str, Any],
        source_names: Optional[list[str]] = None
    ) -> EnrichmentResult:
        """
        Enrich a single record with data from sources.
        
        Args:
            record: Original record to enrich
            source_names: Optional list of source names to use (None = all)
            
        Returns:
            EnrichmentResult with enriched data
        """
        start_time = datetime.now()
        errors = []
        sources_applied = []
        enriched = dict(record)
        
        sources = [
            s for s in self.config.sources
            if source_names is None or s.name in source_names
        ]
        
        if self.config.parallel:
            tasks = [self._enrich_with_source(enriched, source) for source in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for source, result in zip(sources, results):
                if isinstance(result, Exception):
                    errors.append(f"{source.name}: {str(result)}")
                    self._enrichment_stats[source.name]["errors"] += 1
                elif result:
                    self._merge_enrichment(enriched, result, source.name)
                    sources_applied.append(source.name)
        else:
            for source in sources:
                try:
                    result = await self._enrich_with_source(enriched, source)
                    if result:
                        self._merge_enrichment(enriched, result, source.name)
                        sources_applied.append(source.name)
                except Exception as e:
                    errors.append(f"{source.name}: {str(e)}")
                    self._enrichment_stats[source.name]["errors"] += 1
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return EnrichmentResult(
            original=record,
            enriched=enriched,
            sources_applied=sources_applied,
            errors=errors,
            duration_ms=duration_ms
        )
    
    async def enrich_batch(
        self,
        records: list[dict[str, Any]],
        source_names: Optional[list[str]] = None,
        max_concurrent: int = 10
    ) -> list[EnrichmentResult]:
        """
        Enrich multiple records.
        
        Args:
            records: List of records to enrich
            source_names: Optional source filter
            max_concurrent: Maximum concurrent enrichment operations
            
        Returns:
            List of EnrichmentResult
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def enrich_with_semaphore(record: dict):
            async with semaphore:
                return await self.enrich_record(record, source_names)
        
        tasks = [enrich_with_semaphore(r) for r in records]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _enrich_with_source(
        self,
        record: dict[str, Any],
        source: EnrichmentSource
    ) -> Optional[dict[str, Any]]:
        """Enrich record with a specific source."""
        # Get lookup key from record
        key = record.get(source.key_field)
        if not key:
            self._enrichment_stats[source.name]["misses"] += 1
            return None
        
        # Check cache
        cache_key = f"{source.name}:{key}"
        if cache_key in self._cache:
            cached_data, cached_at = self._cache[cache_key]
            age = (datetime.now() - cached_at).total_seconds()
            if age < source.cache_ttl_seconds:
                self._enrichment_stats[source.name]["hits"] += 1
                return cached_data
        
        # Perform lookup with timeout
        try:
            result = await asyncio.wait_for(
                source.lookup(key),
                timeout=source.timeout_seconds
            )
            
            # Cache result
            self._cache[cache_key] = (result, datetime.now())
            self._enrichment_stats[source.name]["hits"] += 1
            
            return result
            
        except asyncio.TimeoutError:
            self._enrichment_stats[source.name]["errors"] += 1
            raise Exception(f"Source {source.name} timed out after {source.timeout_seconds}s")
    
    def _merge_enrichment(
        self,
        record: dict[str, Any],
        enrichment: dict[str, Any],
        source_name: str
    ):
        """Merge enrichment data into record."""
        prefix = f"{source_name}_"
        
        for key, value in enrichment.items():
            # Prefix keys to avoid conflicts
            prefixed_key = f"{prefix}{key}"
            
            if self.config.merge_strategy == "override":
                record[prefixed_key] = value
            elif self.config.merge_strategy == "preserve":
                if key not in record:
                    record[key] = value
                record[prefixed_key] = value
            elif self.config.merge_strategy == "merge":
                # Merge at top level, prefix only if conflict
                if key not in record:
                    record[key] = value
                else:
                    record[prefixed_key] = value
    
    async def lookup_value(
        self,
        source_name: str,
        key: Any
    ) -> Optional[dict[str, Any]]:
        """Direct lookup from a specific source."""
        source = next((s for s in self.config.sources if s.name == source_name), None)
        if not source:
            return None
        
        cache_key = f"{source_name}:{key}"
        if cache_key in self._cache:
            cached_data, cached_at = self._cache[cache_key]
            age = (datetime.now() - cached_at).total_seconds()
            if age < source.cache_ttl_seconds:
                return cached_data
        
        try:
            result = await asyncio.wait_for(
                source.lookup(key),
                timeout=source.timeout_seconds
            )
            self._cache[cache_key] = (result, datetime.now())
            return result
        except Exception:
            return None
    
    async def clear_cache(self, source_name: Optional[str] = None):
        """Clear enrichment cache."""
        if source_name:
            keys_to_remove = [
                k for k in self._cache if k.startswith(f"{source_name}:")
            ]
            for key in keys_to_remove:
                del self._cache[key]
        else:
            self._cache.clear()
    
    def get_stats(self) -> dict[str, Any]:
        """Get enrichment statistics."""
        total_hits = sum(s["hits"] for s in self._enrichment_stats.values())
        total_misses = sum(s["misses"] for s in self._enrichment_stats.values())
        total_errors = sum(s["errors"] for s in self._enrichment_stats.values())
        
        return {
            "cache_size": len(self._cache),
            "total_hits": total_hits,
            "total_misses": total_misses,
            "total_errors": total_errors,
            "hit_rate": total_hits / max(1, total_hits + total_misses),
            "source_stats": dict(self._enrichment_stats)
        }


class DataAugmenter:
    """Provides data augmentation transformations."""
    
    @staticmethod
    async def add_timestamps(data: dict[str, Any]) -> dict[str, Any]:
        """Add created/updated timestamps."""
        now = datetime.now()
        result = dict(data)
        if "created_at" not in result:
            result["created_at"] = now
        result["updated_at"] = now
        return result
    
    @staticmethod
    async def add_computed_fields(
        data: dict[str, Any],
        field_definitions: dict[str, Callable]
    ) -> dict[str, Any]:
        """Add computed fields based on existing data."""
        result = dict(data)
        for field_name, compute_fn in field_definitions.items():
            try:
                result[field_name] = compute_fn(data)
            except Exception:
                result[field_name] = None
        return result
    
    @staticmethod
    async def normalize_fields(
        data: dict[str, Any],
        field_mappings: dict[str, str]
    ) -> dict[str, Any]:
        """Normalize/rename fields."""
        result = {}
        for key, value in data.items():
            new_key = field_mappings.get(key, key)
            result[new_key] = value
        return result
    
    @staticmethod
    async def add_derived_metrics(
        data: dict[str, Any],
        metrics: list[dict[str, str]]
    ) -> dict[str, Any]:
        """Add derived metrics (e.g., totals, ratios)."""
        result = dict(data)
        for metric in metrics:
            source_fields = metric["sources"]
            operation = metric.get("operation", "sum")
            target_field = metric["target"]
            
            try:
                values = [data.get(f, 0) for f in source_fields]
                if operation == "sum":
                    result[target_field] = sum(values)
                elif operation == "avg":
                    result[target_field] = sum(values) / len(values) if values else 0
                elif operation == "min":
                    result[target_field] = min(values)
                elif operation == "max":
                    result[target_field] = max(values)
                elif operation == "count":
                    result[target_field] = len([v for v in values if v])
            except Exception:
                result[target_field] = None
        
        return result
