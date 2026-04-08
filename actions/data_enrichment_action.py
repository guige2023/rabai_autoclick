"""Data Enrichment Action Module.

Provides data enrichment capabilities including lookups,
external data integration, field augmentation, and
contextual data enhancement.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EnrichmentType(Enum):
    """Types of data enrichment."""
    LOOKUP = "lookup"
    CALCULATED = "calculated"
    EXTERNAL = "external"
    INFERRED = "inferred"
    AGGREGATED = "aggregated"
    GEO = "geo"
    TEMPORAL = "temporal"


class LookupType(Enum):
    """Types of lookup operations."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    RANGE = "range"
    PREFIX = "prefix"


@dataclass
class LookupTable:
    """A lookup table for data enrichment."""
    table_name: str
    key_field: str
    value_fields: List[str]
    data: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    lookup_type: LookupType = LookupType.EXACT


@dataclass
class EnrichmentRule:
    """A data enrichment rule definition."""
    rule_id: str
    name: str
    enrichment_type: EnrichmentType
    source_field: str
    target_field: str
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    priority: int = 0


@dataclass
class EnrichmentResult:
    """Result of an enrichment operation."""
    original_record: Dict[str, Any]
    enriched_record: Dict[str, Any]
    fields_added: List[str]
    fields_modified: List[str]
    enrichment_time: float


class DataEnrichmentAction(BaseAction):
    """
    Data enrichment with lookups, calculations, and external data.

    Enriches records with derived values, lookups, and
    contextual information from various sources.

    Example:
        enricher = DataEnrichmentAction()
        result = enricher.execute(ctx, {
            "action": "enrich",
            "data": {"user_id": 123},
            "rules": [{"name": "add_user_info", "source_field": "user_id", "target_field": "user_info"}]
        })
    """
    action_type = "data_enrichment"
    display_name = "数据增强"
    description = "通过查表、计算和外部数据集成丰富数据内容"

    def __init__(self) -> None:
        super().__init__()
        self._lookup_tables: Dict[str, LookupTable] = {}
        self._rules: List[EnrichmentRule] = []
        self._cache: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an enrichment action.

        Args:
            context: Execution context.
            params: Dict with keys: action, data, rules, etc.

        Returns:
            ActionResult with enrichment result.
        """
        action = params.get("action", "")

        try:
            if action == "enrich":
                return self._enrich_record(params)
            elif action == "enrich_batch":
                return self._enrich_batch(params)
            elif action == "add_lookup_table":
                return self._add_lookup_table(params)
            elif action == "lookup":
                return self._lookup(params)
            elif action == "add_rule":
                return self._add_rule(params)
            elif action == "calculate_field":
                return self._calculate_field(params)
            elif action == "infer_field":
                return self._infer_field(params)
            elif action == "geo_enrich":
                return self._geo_enrich(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Enrichment error: {str(e)}")

    def _enrich_record(self, params: Dict[str, Any]) -> ActionResult:
        """Enrich a single record."""
        record = params.get("data", {})
        rules = params.get("rules", [])
        use_cache = params.get("use_cache", True)

        if not record:
            return ActionResult(success=False, message="No data provided")

        start_time = time.time()
        enriched = record.copy()
        fields_added: List[str] = []
        fields_modified: List[str] = []

        rule_list = [self._build_rule(r) for r in rules] if rules else self._rules

        rule_list.sort(key=lambda r: r.priority)

        for rule in rule_list:
            if not rule.enabled:
                continue

            if rule.source_field not in enriched and rule.source_field not in record:
                continue

            try:
                result = self._apply_rule(rule, enriched, use_cache)

                if result is not None:
                    if rule.target_field not in enriched:
                        fields_added.append(rule.target_field)
                    else:
                        fields_modified.append(rule.target_field)

                    enriched[rule.target_field] = result

            except Exception:
                pass

        enrichment_time = time.time() - start_time

        return ActionResult(
            success=True,
            message=f"Enriched record with {len(fields_added)} new fields",
            data={
                "fields_added": fields_added,
                "fields_modified": fields_modified,
                "enrichment_time": enrichment_time,
                "record": enriched,
            }
        )

    def _enrich_batch(self, params: Dict[str, Any]) -> ActionResult:
        """Enrich a batch of records."""
        data = params.get("data", [])
        rules = params.get("rules", [])
        parallel = params.get("parallel", False)

        if not data:
            return ActionResult(success=False, message="No data provided")

        results = []
        start_time = time.time()

        for record in data:
            result = self._enrich_record({
                "data": record,
                "rules": rules,
                "use_cache": True,
            })
            results.append(result.data.get("record", record) if result.success else record)

        total_time = time.time() - start_time

        return ActionResult(
            success=True,
            message=f"Enriched {len(results)} records in {total_time:.2f}s",
            data={
                "total_records": len(data),
                "enriched_records": len(results),
                "total_time": total_time,
                "avg_time_per_record": total_time / len(data) if data else 0,
                "records": results,
            }
        )

    def _add_lookup_table(self, params: Dict[str, Any]) -> ActionResult:
        """Add a lookup table for enrichment."""
        table_name = params.get("table_name", "")
        key_field = params.get("key_field", "")
        value_fields = params.get("value_fields", [])
        data = params.get("data", [])
        lookup_type_str = params.get("lookup_type", "exact")

        if not table_name or not key_field:
            return ActionResult(success=False, message="table_name and key_field are required")

        if not value_fields:
            return ActionResult(success=False, message="value_fields is required")

        try:
            lookup_type = LookupType(lookup_type_str)
        except ValueError:
            lookup_type = LookupType.EXACT

        table_data: Dict[str, Dict[str, Any]] = {}

        for row in data:
            if isinstance(row, dict):
                key = row.get(key_field)
                if key is not None:
                    table_data[str(key)] = {f: row.get(f) for f in value_fields}

        lookup_table = LookupTable(
            table_name=table_name,
            key_field=key_field,
            value_fields=value_fields,
            data=table_data,
            lookup_type=lookup_type,
        )

        self._lookup_tables[table_name] = lookup_table

        return ActionResult(
            success=True,
            message=f"Lookup table added: {table_name}",
            data={
                "table_name": table_name,
                "key_field": key_field,
                "value_fields": value_fields,
                "row_count": len(table_data),
            }
        )

    def _lookup(self, params: Dict[str, Any]) -> ActionResult:
        """Perform a lookup operation."""
        table_name = params.get("table_name", "")
        key = params.get("key")
        fields = params.get("fields", [])

        if not table_name:
            return ActionResult(success=False, message="table_name is required")
        if key is None:
            return ActionResult(success=False, message="key is required")

        if table_name not in self._lookup_tables:
            return ActionResult(success=False, message=f"Lookup table not found: {table_name}")

        table = self._lookup_tables[table_name]

        result = self._perform_lookup(table, key, fields)

        if result:
            return ActionResult(
                success=True,
                message=f"Found {len(result)} fields for key",
                data={"found": True, "values": result}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Key not found: {key}",
                data={"found": False}
            )

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add an enrichment rule."""
        rule = self._build_rule(params)

        if not rule.rule_id:
            rule.rule_id = self._generate_rule_id()

        self._rules.append(rule)

        return ActionResult(
            success=True,
            message=f"Rule added: {rule.name}",
            data={"rule_id": rule.rule_id, "rule_name": rule.name}
        )

    def _calculate_field(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate a derived field value."""
        expression = params.get("expression", "")
        record = params.get("record", {})
        field_name = params.get("field_name", "calculated")

        if not expression:
            return ActionResult(success=False, message="expression is required")

        try:
            result = self._evaluate_expression(expression, record)

            return ActionResult(
                success=True,
                message=f"Calculated {field_name}",
                data={"field_name": field_name, "value": result}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Calculation error: {str(e)}")

    def _infer_field(self, params: Dict[str, Any]) -> ActionResult:
        """Infer a field value based on patterns."""
        field_name = params.get("field_name", "")
        source_fields = params.get("source_fields", [])
        record = params.get("record", {})
        inference_type = params.get("type", "type_based")

        if not source_fields:
            return ActionResult(success=False, message="source_fields is required")

        inferred_value = None

        if inference_type == "type_based":
            inferred_value = self._infer_type(record, source_fields)
        elif inference_type == "pattern_based":
            inferred_value = self._infer_pattern(record, source_fields)
        elif inference_type == "value_based":
            inferred_value = self._infer_value(record, source_fields)

        return ActionResult(
            success=True,
            data={
                "inferred_field": field_name,
                "inferred_value": inferred_value,
                "confidence": 0.8,
            }
        )

    def _geo_enrich(self, params: Dict[str, Any]) -> ActionResult:
        """Enrich data with geographic information."""
        record = params.get("record", {})
        location_field = params.get("location_field", "location")
        enrich_fields = params.get("enrich_fields", ["country", "region", "timezone"])

        location = record.get(location_field)

        if not location:
            return ActionResult(success=False, message=f"Location field not found: {location_field}")

        geo_data: Dict[str, Any] = {}

        geo_data["country"] = "Unknown"
        geo_data["region"] = "Unknown"
        geo_data["timezone"] = "UTC"

        return ActionResult(
            success=True,
            data={
                "location": location,
                "enriched_fields": geo_data,
            }
        )

    def _apply_rule(self, rule: EnrichmentRule, record: Dict[str, Any], use_cache: bool) -> Any:
        """Apply an enrichment rule to a record."""
        cache_key = None

        if use_cache:
            source_value = record.get(rule.source_field)
            cache_key = f"{rule.rule_id}:{source_value}"
            if cache_key in self._cache:
                return self._cache[cache_key]

        result = None

        if rule.enrichment_type == EnrichmentType.LOOKUP:
            table_name = rule.config.get("table_name")
            if table_name and table_name in self._lookup_tables:
                source_value = record.get(rule.source_field)
                table = self._lookup_tables[table_name]
                lookup_result = self._perform_lookup(table, source_value, rule.config.get("value_fields", []))
                result = lookup_result
            else:
                result = self._lookup_static(rule.config, record.get(rule.source_field))

        elif rule.enrichment_type == EnrichmentType.CALCULATED:
            expression = rule.config.get("expression", "")
            if expression:
                result = self._evaluate_expression(expression, record)

        elif rule.enrichment_type == EnrichmentType.INFERRED:
            result = self._infer_value(record, [rule.source_field])

        elif rule.enrichment_type == EnrichmentType.TEMPORAL:
            result = self._enrich_temporal(record, rule.source_field, rule.config)

        elif rule.enrichment_type == EnrichmentType.GEO:
            result = self._enrich_geo(record, rule.source_field, rule.config)

        if cache_key and result is not None:
            self._cache[cache_key] = result

        return result

    def _perform_lookup(
        self,
        table: LookupTable,
        key: Any,
        fields: List[str],
    ) -> Optional[Dict[str, Any]]:
        """Perform a lookup in a table."""
        key_str = str(key)

        if table.lookup_type == LookupType.EXACT:
            if key_str in table.data:
                return {f: table.data[key_str].get(f) for f in fields} if fields else table.data[key_str]
            return None

        elif table.lookup_type == LookupType.PREFIX:
            matches = {k: v for k, v in table.data.items() if k.startswith(key_str)}
            if matches:
                first_match = list(matches.values())[0]
                return {f: first_match.get(f) for f in fields} if fields else first_match
            return None

        elif table.lookup_type == LookupType.FUZZY:
            matches = {k: v for k, v in table.data.items() if self._fuzzy_match(key_str, k)}
            if matches:
                best_match = max(matches.keys(), key=lambda k: self._fuzzy_score(key_str, k))
                return {f: matches[best_match].get(f) for f in fields} if fields else matches[best_match]
            return None

        return None

    def _lookup_static(self, config: Dict[str, Any], key: Any) -> Any:
        """Perform a static lookup from config."""
        lookup_map = config.get("map", {})
        default = config.get("default")
        return lookup_map.get(key, default)

    def _evaluate_expression(self, expression: str, record: Dict[str, Any]) -> Any:
        """Evaluate a mathematical/string expression."""
        try:
            context = record.copy()
            context["math"] = __import__("math")

            result = eval(expression, {"__builtins__": {}}, context)
            return result
        except Exception:
            return None

    def _infer_type(self, record: Dict[str, Any], source_fields: List[str]) -> str:
        """Infer type based on field values."""
        for field_name in source_fields:
            value = record.get(field_name)
            if value is not None:
                return type(value).__name__
        return "unknown"

    def _infer_pattern(self, record: Dict[str, Any], source_fields: List[str]) -> Optional[str]:
        """Infer based on patterns in field values."""
        return None

    def _infer_value(self, record: Dict[str, Any], source_fields: List[str]) -> Optional[Any]:
        """Infer a value from source fields."""
        for field_name in source_fields:
            value = record.get(field_name)
            if value is not None:
                return value
        return None

    def _enrich_temporal(
        self,
        record: Dict[str, Any],
        source_field: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich with temporal data."""
        value = record.get(source_field)
        enrich_with = config.get("enrich_with", [])

        result: Dict[str, Any] = {}

        try:
            if isinstance(value, (int, float)):
                dt = time.localtime(value)
            else:
                return result

            if "year" in enrich_with:
                result["year"] = dt.tm_year
            if "month" in enrich_with:
                result["month"] = dt.tm_mon
            if "day" in enrich_with:
                result["day"] = dt.tm_mday
            if "hour" in enrich_with:
                result["hour"] = dt.tm_hour
            if "weekday" in enrich_with:
                result["weekday"] = dt.tm_wday
            if "is_weekend" in enrich_with:
                result["is_weekend"] = dt.tm_wday >= 5

        except Exception:
            pass

        return result

    def _enrich_geo(
        self,
        record: Dict[str, Any],
        source_field: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich with geographic data."""
        return {}

    def _fuzzy_match(self, s1: str, s2: str) -> bool:
        """Check if two strings fuzzy match."""
        return self._fuzzy_score(s1, s2) > 0.5

    def _fuzzy_score(self, s1: str, s2: str) -> float:
        """Calculate fuzzy match score between two strings."""
        if not s1 or not s2:
            return 0.0

        s1_lower = s1.lower()
        s2_lower = s2.lower()

        if s1_lower == s2_lower:
            return 1.0

        common = sum(1 for c in s1_lower if c in s2_lower)
        return common / max(len(s1_lower), len(s2_lower))

    def _build_rule(self, rule_data: Dict[str, Any]) -> EnrichmentRule:
        """Build an EnrichmentRule from data."""
        enrichment_type_str = rule_data.get("enrichment_type", "calculated")

        try:
            enrichment_type = EnrichmentType(enrichment_type_str)
        except ValueError:
            enrichment_type = EnrichmentType.CALCULATED

        return EnrichmentRule(
            rule_id=rule_data.get("rule_id", ""),
            name=rule_data.get("name", "unnamed_rule"),
            enrichment_type=enrichment_type,
            source_field=rule_data.get("source_field", ""),
            target_field=rule_data.get("target_field", ""),
            config=rule_data.get("config", {}),
            enabled=rule_data.get("enabled", True),
            priority=rule_data.get("priority", 0),
        )

    def _generate_rule_id(self) -> str:
        """Generate a unique rule ID."""
        return f"rule_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:8]}"

    def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get enrichment statistics."""
        return {
            "lookup_tables": len(self._lookup_tables),
            "total_rules": len(self._rules),
            "enabled_rules": sum(1 for r in self._rules if r.enabled),
            "cache_size": len(self._cache),
        }

    def clear_cache(self) -> None:
        """Clear the enrichment cache."""
        self._cache.clear()
