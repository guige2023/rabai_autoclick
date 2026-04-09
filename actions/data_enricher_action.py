"""Data enrichment and lookup action."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class EnrichmentConfig:
    """Configuration for data enrichment."""

    source_field: str
    target_field: str
    lookup_table: Optional[dict[str, Any]] = None
    lookup_func: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    preserve_existing: bool = True


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""

    total_records: int
    enriched_count: int
    skipped_count: int
    error_count: int
    errors: list[str] = field(default_factory=list)


class DataEnricherAction:
    """Enriches data with lookups and computed fields."""

    def __init__(
        self,
        default_config: Optional[EnrichmentConfig] = None,
    ):
        """Initialize data enricher.

        Args:
            default_config: Default enrichment configuration.
        """
        self._default_config = default_config
        self._enrichments: list[EnrichmentConfig] = []
        self._lookup_cache: dict[str, Any] = {}

    def add_enrichment(self, config: EnrichmentConfig) -> None:
        """Add an enrichment configuration."""
        self._enrichments.append(config)

    def add_lookup_table(
        self,
        source_field: str,
        target_field: str,
        lookup_table: dict[str, Any],
        default_value: Any = None,
        preserve_existing: bool = True,
    ) -> None:
        """Add a lookup table enrichment.

        Args:
            source_field: Field to use as lookup key.
            target_field: Field to populate with lookup result.
            lookup_table: Dictionary mapping keys to values.
            default_value: Default if key not found.
            preserve_existing: Don't overwrite existing target values.
        """
        config = EnrichmentConfig(
            source_field=source_field,
            target_field=target_field,
            lookup_table=lookup_table,
            default_value=default_value,
            preserve_existing=preserve_existing,
        )
        self._enrichments.append(config)

    def add_computed_field(
        self,
        source_fields: list[str],
        target_field: str,
        compute_func: Callable[[dict[str, Any]], Any],
        preserve_existing: bool = True,
    ) -> None:
        """Add a computed field enrichment.

        Args:
            source_fields: Fields to pass to compute function.
            target_field: Field to populate with result.
            compute_func: Function to compute value from source fields.
            preserve_existing: Don't overwrite existing target values.
        """
        config = EnrichmentConfig(
            source_field="__computed__",
            target_field=target_field,
            lookup_func=lambda _: compute_func({}),
            preserve_existing=preserve_existing,
        )
        config._source_fields = source_fields
        config._compute_func = compute_func
        self._enrichments.append(config)

    def enrich_record(
        self,
        record: dict[str, Any],
    ) -> tuple[dict[str, Any], bool, Optional[str]]:
        """Enrich a single record.

        Args:
            record: Input record.

        Returns:
            Tuple of (enriched_record, success, error_message).
        """
        enriched = record.copy()

        for config in self._enrichments:
            try:
                if hasattr(config, "_compute_func"):
                    source_values = {
                        f: record.get(f) for f in config._source_fields
                    }
                    computed = config._compute_func(source_values)

                    if config.preserve_existing and config.target_field in enriched:
                        continue
                    enriched[config.target_field] = computed
                    continue

                if config.lookup_func:
                    source_value = record.get(config.source_field)
                    result = config.lookup_func(source_value)
                    if config.preserve_existing and config.target_field in enriched:
                        continue
                    enriched[config.target_field] = result
                    continue

                if config.lookup_table:
                    source_value = record.get(config.source_field)
                    key = str(source_value) if source_value is not None else "__null__"

                    if config.preserve_existing and config.target_field in enriched:
                        continue

                    enriched[config.target_field] = config.lookup_table.get(
                        key, config.default_value
                    )

            except Exception as e:
                return enriched, False, str(e)

        return enriched, True, None

    def enrich_batch(
        self,
        records: Sequence[dict[str, Any]],
    ) -> EnrichmentResult:
        """Enrich a batch of records.

        Args:
            records: Input records.

        Returns:
            EnrichmentResult with statistics.
        """
        enriched_count = 0
        skipped_count = 0
        error_count = 0
        errors = []

        for record in records:
            enriched, success, error = self.enrich_record(record)

            if success:
                enriched_count += 1
            elif error:
                error_count += 1
                errors.append(error)
            else:
                skipped_count += 1

        return EnrichmentResult(
            total_records=len(records),
            enriched_count=enriched_count,
            skipped_count=skipped_count,
            error_count=error_count,
            errors=errors[:10],
        )

    def lookup(
        self,
        key: str,
        lookup_table: dict[str, Any],
        default: Any = None,
    ) -> Any:
        """Perform a single lookup.

        Args:
            key: Lookup key.
            lookup_table: Lookup table.
            default: Default value if not found.

        Returns:
            Lookup result or default.
        """
        return lookup_table.get(key, default)

    def lookup_cached(
        self,
        key: str,
        lookup_func: Callable[[str], Any],
    ) -> Any:
        """Perform a cached lookup.

        Args:
            key: Lookup key.
            lookup_func: Function to compute value if not cached.

        Returns:
            Lookup result.
        """
        if key not in self._lookup_cache:
            self._lookup_cache[key] = lookup_func(key)
        return self._lookup_cache[key]

    def clear_cache(self) -> None:
        """Clear the lookup cache."""
        self._lookup_cache.clear()

    def get_cache_size(self) -> int:
        """Get number of cached entries."""
        return len(self._lookup_cache)
