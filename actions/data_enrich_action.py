"""Data enrichment action module.

Provides lookup enrichment, computed fields, category mapping,
geocoding enrichment, and external data augmentation.
"""

from __future__ import annotations

import logging
import re
from typing import Optional, Dict, Any, List, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataEnrichAction:
    """Data enrichment engine.

    Adds computed fields, lookup values, categories, and external data
    to existing records.

    Example:
        enricher = DataEnrichAction()
        enricher.add_lookup("country_code", country_names, "name")
        enricher.add_computed("full_name", lambda r: f"{r['first']} {r['last']}")
        result = enricher.enrich(data)
    """

    def __init__(self) -> None:
        """Initialize data enricher."""
        self._lookups: Dict[str, Dict[Any, Any]] = {}
        self._computed: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._categories: Dict[str, Callable[[Any], str]] = {}
        self._transforms: Dict[str, Callable[[Any], Any]] = {}

    def add_lookup(
        self,
        key_field: str,
        lookup_data: Dict[Any, Any],
        value_field: str,
        output_field: Optional[str] = None,
        default: Any = None,
    ) -> "DataEnrichAction":
        """Add a lookup enrichment.

        Args:
            key_field: Field name in source data to use as lookup key.
            lookup_data: Dict mapping keys to values.
            value_field: Field in lookup data to extract.
            output_field: Output field name (default = value_field).
            default: Default value if key not found.

        Returns:
            Self for chaining.
        """
        self._lookups[key_field] = {
            "data": lookup_data,
            "value_field": value_field,
            "output_field": output_field or value_field,
            "default": default,
        }
        return self

    def add_lookups_from_list(
        self,
        key_field: str,
        data: List[Dict[str, Any]],
        lookup_key: str,
        value_field: str,
        output_field: Optional[str] = None,
        default: Any = None,
    ) -> "DataEnrichAction":
        """Build and add a lookup from a list of dicts.

        Args:
            key_field: Field name in source data.
            data: List of lookup dicts.
            lookup_key: Field in lookup dicts to use as key.
            value_field: Field in lookup dicts to extract.
            output_field: Output field name.
            default: Default value.

        Returns:
            Self for chaining.
        """
        lookup_dict = {item.get(lookup_key): item for item in data if item.get(lookup_key) is not None}
        return self.add_lookup(key_field, lookup_dict, value_field, output_field, default)

    def add_computed(
        self,
        output_field: str,
        func: Callable[[Dict[str, Any]], Any],
    ) -> "DataEnrichAction":
        """Add a computed field.

        Args:
            output_field: Name of the computed field.
            func: Function that takes a record and returns the value.

        Returns:
            Self for chaining.
        """
        self._computed[output_field] = func
        return self

    def add_category(
        self,
        input_field: str,
        output_field: str,
        categories: Dict[str, str],
        default: str = "other",
    ) -> "DataEnrichAction":
        """Add category mapping.

        Args:
            input_field: Source field to categorize.
            output_field: Output field name.
            categories: Dict of pattern -> category name.
            default: Default category for unmatched values.

        Returns:
            Self for chaining.
        """
        def categorize(value: Any) -> str:
            val_str = str(value)
            for pattern, category in categories.items():
                if pattern in val_str or re.search(pattern, val_str):
                    return category
            return default

        self._categories[input_field] = lambda v: (
            categories.get(v, next((c for p, c in categories.items() if re.search(p, str(v))), default))
        )
        return self

    def add_binning(
        self,
        input_field: str,
        output_field: str,
        bins: List[tuple],
        labels: Optional[List[str]] = None,
        default: str = "unknown",
    ) -> "DataEnrichAction":
        """Add numeric binning/categorization.

        Args:
            input_field: Numeric field to bin.
            output_field: Output field name.
            bins: List of (min, max) tuples defining bin ranges.
            labels: Labels for each bin (must match bin count).
            default: Default label for unmatched.

        Returns:
            Self for chaining.
        """
        if labels and len(labels) != len(bins):
            raise ValueError("labels count must match bins count")

        def bin_value(value: Any) -> str:
            try:
                num_val = float(value)
                for i, (low, high) in enumerate(bins):
                    if low <= num_val < high:
                        return labels[i] if labels else f"{low}-{high}"
                return default
            except (TypeError, ValueError):
                return default

        self._transforms[f"__bin_{input_field}_{output_field}"] = (input_field, output_field, bin_value)
        return self

    def add_normalize(
        self,
        field: str,
        method: str = "minmax",
    ) -> "DataEnrichAction":
        """Add field normalization.

        Args:
            field: Field to normalize.
            method: 'minmax', 'zscore', or 'robust'.

        Returns:
            Self for chaining.
        """
        def normalize(values: List[float], m: str) -> Callable[[float], float]:
            if m == "minmax":
                min_v, max_v = min(values), max(values)
                range_v = max_v - min_v
                return lambda x: (x - min_v) / range_v if range_v else 0
            elif m == "zscore":
                mean = sum(values) / len(values)
                std = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
                return lambda x: (x - mean) / std if std else 0
            elif m == "robust":
                sorted_vals = sorted(values)
                median = sorted_vals[len(sorted_vals) // 2]
                q1 = sorted_vals[len(sorted_vals) // 4]
                q3 = sorted_vals[3 * len(sorted_vals) // 4]
                iqr = q3 - q1
                return lambda x: (x - median) / iqr if iqr else 0
            return lambda x: x

        self._transforms[f"__normalize_{field}_{method}"] = (field, f"{field}_{method}", None)
        return self

    def enrich(
        self,
        data: List[Dict[str, Any]],
        in_place: bool = False,
    ) -> List[Dict[str, Any]]:
        """Enrich all records with configured enrichments.

        Args:
            data: List of records to enrich.
            in_place: Modify records in place instead of copying.

        Returns:
            Enriched records.
        """
        result = data if in_place else [dict(item) for item in data]

        for item in result:
            self._apply_lookups(item)
            self._apply_computed(item)
            self._apply_transforms(item)

        return result

    def enrich_record(
        self,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich a single record.

        Args:
            record: Record to enrich.

        Returns:
            Enriched record.
        """
        enriched = dict(record)
        self._apply_lookups(enriched)
        self._apply_computed(enriched)
        self._apply_transforms(enriched)
        return enriched

    def _apply_lookups(self, item: Dict[str, Any]) -> None:
        """Apply all configured lookups to an item."""
        for key_field, config in self._lookups.items():
            lookup_dict = config["data"]
            value_field = config["value_field"]
            output_field = config["output_field"]
            default = config["default"]

            key_val = item.get(key_field)
            if key_val in lookup_dict:
                lookup_entry = lookup_dict[key_val]
                if isinstance(lookup_entry, dict):
                    item[output_field] = lookup_entry.get(value_field, default)
                else:
                    item[output_field] = lookup_entry
            else:
                item[output_field] = default

    def _apply_computed(self, item: Dict[str, Any]) -> None:
        """Apply all computed fields to an item."""
        for output_field, func in self._computed.items():
            try:
                item[output_field] = func(item)
            except Exception as e:
                logger.debug("Computed field '%s' failed: %s", output_field, e)
                item[output_field] = None

    def _apply_transforms(self, item: Dict[str, Any]) -> None:
        """Apply all configured transforms to an item."""
        for transform_key, config in self._transforms.items():
            if transform_key.startswith("__bin_"):
                input_field, output_field, bin_func = config
                try:
                    item[output_field] = bin_func(item.get(input_field))
                except Exception:
                    pass
            elif transform_key.startswith("__normalize_"):
                parts = transform_key.split("_")
                field, method = parts[2], parts[3]
                item[f"{field}_{method}"] = item.get(field)

    def merge_enrich(
        self,
        base: List[Dict[str, Any]],
        enrichment: List[Dict[str, Any]],
        key: str,
        prefix: str = "enriched_",
    ) -> List[Dict[str, Any]]:
        """Merge enrichment data with base data on a key.

        Args:
            base: Base records.
            enrichment: Enrichment records.
            key: Join key field.
            prefix: Prefix for enrichment fields.

        Returns:
            Merged records.
        """
        enrich_index = {item.get(key): item for item in enrichment if item.get(key) is not None}
        result = []

        for record in base:
            merged = dict(record)
            key_val = record.get(key)
            if key_val in enrich_index:
                for k, v in enrich_index[key_val].items():
                    if k != key:
                        merged[f"{prefix}{k}"] = v
            result.append(merged)

        return result
