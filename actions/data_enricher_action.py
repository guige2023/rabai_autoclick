"""
Data Enricher Action Module.

Enriches data with lookups, computations, and external data.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional


class DataEnricherAction:
    """
    Data enrichment with lookups and transformations.

    Supports field enrichment, computed fields, and lookup tables.
    """

    def __init__(self) -> None:
        self._lookup_tables: Dict[str, Dict[str, Any]] = {}
        self._enrichment_funcs: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl: float = 300.0

    def add_lookup_table(
        self,
        name: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Add a lookup table.

        Args:
            name: Table name
            data: Dict mapping keys to values
        """
        self._lookup_tables[name] = data

    def add_enrichment_func(
        self,
        field_name: str,
        func: Callable[[Dict[str, Any]], Any],
    ) -> None:
        """
        Add an enrichment function.

        Args:
            field_name: Field to add/enrich
            func: Function that takes full record and returns value
        """
        self._enrichment_funcs[field_name] = func

    def enrich(
        self,
        record: Dict[str, Any],
        fields: Optional[List[str]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Enrich a record.

        Args:
            record: Record to enrich
            fields: Specific fields to enrich (None = all)
            use_cache: Whether to use lookup cache

        Returns:
            Enriched record
        """
        result = record.copy()

        for field_name, func in self._enrichment_funcs.items():
            if fields is not None and field_name not in fields:
                continue

            try:
                result[field_name] = func(result)
            except Exception:
                pass

        return result

    def enrich_batch(
        self,
        records: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Enrich multiple records.

        Args:
            records: List of records
            fields: Specific fields to enrich

        Returns:
            List of enriched records
        """
        return [self.enrich(r, fields) for r in records]

    def lookup(
        self,
        table_name: str,
        key: str,
        field: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        """
        Lookup a value in a table.

        Args:
            table_name: Name of lookup table
            key: Key to lookup
            field: Specific field to return (None = entire record)
            default: Default if not found

        Returns:
            Lookup result or default
        """
        if table_name not in self._lookup_tables:
            return default

        table = self._lookup_tables[table_name]

        if key not in table:
            return default

        record = table[key]

        if field is not None:
            return record.get(field, default) if isinstance(record, dict) else default

        return record

    def lookup_with_cache(
        self,
        table_name: str,
        key: str,
        fetch_func: Callable[[str], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """
        Lookup with caching.

        Args:
            table_name: Cache key prefix
            key: Lookup key
            fetch_func: Function to fetch if not cached
            ttl: Cache TTL in seconds

        Returns:
            Lookup result
        """
        cache_key = f"{table_name}:{key}"
        ttl = ttl or self._cache_ttl

        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if time.time() - entry["timestamp"] < ttl:
                return entry["value"]

        value = fetch_func(key)

        self._cache[cache_key] = {
            "value": value,
            "timestamp": time.time(),
        }

        return value

    def enrich_from_lookups(
        self,
        record: Dict[str, Any],
        lookup_mappings: Dict[str, tuple[str, str]],
    ) -> Dict[str, Any]:
        """
        Enrich record using lookup tables.

        Args:
            record: Record to enrich
            lookup_mappings: Dict mapping target_field to (table_name, key_field)

        Returns:
            Enriched record
        """
        result = record.copy()

        for target_field, (table_name, key_field) in lookup_mappings.items():
            if key_field not in result:
                continue

            key = result[key_field]
            value = self.lookup(table_name, key)

            if value is not None:
                result[target_field] = value

        return result

    def clear_cache(self) -> None:
        """Clear the lookup cache."""
        self._cache.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "entries": len(self._cache),
            "tables": list(self._lookup_tables.keys()),
            "enrichment_funcs": list(self._enrichment_funcs.keys()),
        }
