"""
Data Federation Action Module.

Federates queries across multiple data sources, aggregates results,
and handles source-specific query translation.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class DataSource:
    """A federated data source."""
    name: str
    source_type: str  # sql, csv, api, json
    connection: dict[str, Any]
    query_template: Optional[str] = None


@dataclass
class FederationResult:
    """Result of federation query."""
    records: list[dict[str, Any]]
    source_counts: dict[str, int]
    total_count: int
    errors: list[str]


class DataFederationAction(BaseAction):
    """Federate queries across multiple data sources."""

    def __init__(self) -> None:
        super().__init__("data_federation")
        self._sources: dict[str, DataSource] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute federated query.

        Args:
            context: Execution context
            params: Parameters:
                - sources: List of data source configs
                - query: Query to execute across sources
                - merge_strategy: union, intersection, concat
                - deduplicate: Remove duplicate records

        Returns:
            FederationResult with aggregated results
        """
        sources_configs = params.get("sources", [])
        query = params.get("query", {})
        merge_strategy = params.get("merge_strategy", "union")
        deduplicate = params.get("deduplicate", False)

        for cfg in sources_configs:
            source = DataSource(
                name=cfg.get("name", ""),
                source_type=cfg.get("type", "csv"),
                connection=cfg.get("connection", {}),
                query_template=cfg.get("query_template")
            )
            self._sources[source.name] = source

        all_records = []
        source_counts = {}
        errors = []

        for source in self._sources.values():
            try:
                records = self._query_source(source, query)
                source_counts[source.name] = len(records)
                all_records.extend(records)
            except Exception as e:
                errors.append(f"{source.name}: {str(e)}")

        if deduplicate:
            seen = set()
            unique_records = []
            for rec in all_records:
                key = tuple(sorted(str(v) for v in rec.values()))
                if key not in seen:
                    seen.add(key)
                    unique_records.append(rec)
            all_records = unique_records

        return FederationResult(
            records=all_records,
            source_counts=source_counts,
            total_count=len(all_records),
            errors=errors
        ).__dict__

    def _query_source(self, source: DataSource, query: dict) -> list[dict]:
        """Query a single data source."""
        if source.source_type == "csv":
            import csv
            records = []
            try:
                with open(source.connection.get("path", ""), "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        records.append(dict(row))
                return records
            except Exception:
                return []

        elif source.source_type == "json":
            import json
            try:
                with open(source.connection.get("path", ""), "r") as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else [data]
            except Exception:
                return []

        elif source.source_type == "api":
            try:
                import urllib.request
                url = source.connection.get("url", "")
                with urllib.request.urlopen(url, timeout=30) as response:
                    import json
                    data = json.loads(response.read().decode())
                    return data if isinstance(data, list) else [data]
            except Exception:
                return []

        return []
