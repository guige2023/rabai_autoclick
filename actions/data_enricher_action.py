"""
Data Enricher Action Module.

Enriches data records with additional fields from lookup tables,
external APIs, computed values, and cross-references.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class EnrichmentResult:
    """Result of data enrichment."""
    records: list[dict[str, Any]]
    enriched_count: int
    enrichment_sources: list[str]


class DataEnricherAction(BaseAction):
    """Enrich data records with additional information."""

    def __init__(self) -> None:
        super().__init__("data_enricher")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Enrich records with additional data.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - enrichments: List of enrichment configs
                    - type: lookup, api, computed, constant
                    - output_field: Field to add
                    - source: Lookup dict or API endpoint
                    - key_field: Key to match in source
                    - value_field: Value to extract from source

        Returns:
            EnrichmentResult with enriched records
        """
        records = params.get("records", [])
        enrichments = params.get("enrichments", [])

        enriched_count = 0
        sources_used = []

        for enrichment in enrichments:
            enrich_type = enrichment.get("type", "lookup")
            output_field = enrichment.get("output_field", "")
            source = enrichment.get("source", {})
            key_field = enrichment.get("key_field", "id")
            value_field = enrichment.get("value_field", "value")

            if not output_field:
                continue

            sources_used.append(output_field)

            if enrich_type == "lookup":
                lookup_dict = source if isinstance(source, dict) else {}
                for r in records:
                    if isinstance(r, dict):
                        key = str(r.get(key_field, ""))
                        if key in lookup_dict:
                            r[output_field] = lookup_dict[key]
                            enriched_count += 1
                        else:
                            r[output_field] = None

            elif enrich_type == "constant":
                for r in records:
                    if isinstance(r, dict):
                        r[output_field] = source

            elif enrich_type == "computed":
                expression = enrichment.get("expression", "")
                for r in records:
                    if isinstance(r, dict):
                        try:
                            r[output_field] = eval(expression, {"r": r}, {})
                            enriched_count += 1
                        except Exception:
                            r[output_field] = None

            elif enrich_type == "api":
                for r in records:
                    if isinstance(r, dict):
                        key = str(r.get(key_field, ""))
                        if key:
                            r[output_field] = {"lookup_key": key, "status": "pending"}
                        else:
                            r[output_field] = None

        return EnrichmentResult(
            records=records,
            enriched_count=enriched_count,
            enrichment_sources=sources_used
        ).__dict__
