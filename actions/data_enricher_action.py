"""Data enricher action module for RabAI AutoClick.

Provides data enrichment:
- DataEnricher: Enrich data from sources
- LookupEnricher: Lookup-based enrichment
- ExternalEnricher: External API enrichment
- ComputedEnricher: Computed field enrichment
- CrossReferenceEnricher: Cross-reference enrichment
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class EnrichmentRule:
    """Enrichment rule."""
    source_field: str
    target_field: str
    enricher: Callable
    default_value: Any = None
    transform: Optional[Callable] = None


@dataclass
class EnrichmentResult:
    """Enrichment result."""
    total: int
    enriched: int
    failed: int
    duration: float
    errors: List[Dict]


class LookupEnricher:
    """Lookup-based data enricher."""

    def __init__(self):
        self._lookup_tables: Dict[str, Dict] = {}

    def add_lookup_table(self, name: str, data: Dict[str, Any]) -> bool:
        """Add lookup table."""
        self._lookup_tables[name] = data
        return True

    def get_lookup_value(self, table_name: str, key: Any, default: Any = None) -> Any:
        """Get value from lookup table."""
        table = self._lookup_tables.get(table_name, {})
        return table.get(key, default)

    def enrich_item(
        self,
        item: Dict,
        rules: List[EnrichmentRule],
    ) -> Tuple[Dict, bool]:
        """Enrich single item."""
        result = dict(item)

        for rule in rules:
            try:
                source_value = item.get(rule.source_field)

                if source_value is None:
                    source_value = rule.default_value

                enriched_value = rule.enricher(source_value, item)

                if rule.transform:
                    enriched_value = rule.transform(enriched_value)

                result[rule.target_field] = enriched_value

            except Exception:
                return result, False

        return result, True


class ComputedEnricher:
    """Computed field enricher."""

    def __init__(self):
        self._computed_fields: Dict[str, Callable] = {}

    def register_computed_field(
        self,
        field_name: str,
        compute_fn: Callable[[Dict], Any],
    ) -> bool:
        """Register computed field."""
        self._computed_fields[field_name] = compute_fn
        return True

    def compute_fields(self, item: Dict) -> Dict:
        """Compute all registered fields."""
        result = dict(item)

        for field_name, compute_fn in self._computed_fields.items():
            try:
                result[field_name] = compute_fn(item)
            except Exception:
                pass

        return result


class ExternalEnricher:
    """External API-based enricher."""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self._enrichment_apis: Dict[str, Callable] = {}

    def register_api(
        self,
        name: str,
        api_fn: Callable[[Any], Dict],
    ) -> bool:
        """Register enrichment API."""
        self._enrichment_apis[name] = api_fn
        return True

    def enrich_with_api(
        self,
        items: List[Dict],
        api_name: str,
        key_field: str,
        result_fields: List[str],
    ) -> EnrichmentResult:
        """Enrich items using external API."""
        start_time = time.time()
        enriched_count = 0
        failed_count = 0
        errors = []

        api_fn = self._enrichment_apis.get(api_name)
        if not api_fn:
            return EnrichmentResult(
                total=len(items),
                enriched=0,
                failed=len(items),
                duration=time.time() - start_time,
                errors=[{"error": f"API '{api_name}' not found"}],
            )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {}

            for item in items:
                key = item.get(key_field)
                if key:
                    future = executor.submit(api_fn, key)
                    future_to_item[future] = item

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    enriched_count += 1
                except Exception as e:
                    failed_count += 1
                    errors.append({"item": str(item.get(key_field)), "error": str(e)})

        return EnrichmentResult(
            total=len(items),
            enriched=enriched_count,
            failed=failed_count,
            duration=time.time() - start_time,
            errors=errors,
        )


class CrossReferenceEnricher:
    """Cross-reference based enricher."""

    def __init__(self):
        self._references: Dict[str, List[Dict]] = {}

    def add_reference_table(self, name: str, data: List[Dict], key_field: str) -> bool:
        """Add reference table."""
        self._references[name] = {"data": data, "key_field": key_field}
        return True

    def enrich_cross_reference(
        self,
        items: List[Dict],
        reference_name: str,
        item_key_field: str,
        mappings: Dict[str, str],
    ) -> List[Dict]:
        """Enrich items using cross-reference."""
        ref_info = self._references.get(reference_name)
        if not ref_info:
            return items

        ref_data = ref_info["data"]
        ref_key = ref_info["key_field"]

        ref_index = {item.get(ref_key): item for item in ref_data if item.get(ref_key)}

        results = []
        for item in items:
            result = dict(item)
            key = item.get(item_key_field)

            if key in ref_index:
                ref_item = ref_index[key]
                for target_field, source_field in mappings.items():
                    result[target_field] = ref_item.get(source_field)

            results.append(result)

        return results


class DataEnricherAction(BaseAction):
    """Data enricher action."""
    action_type = "data_enricher"
    display_name = "数据增强器"
    description = "数据补充和增强"

    def __init__(self):
        super().__init__()
        self._lookup_enricher = LookupEnricher()
        self._computed_enricher = ComputedEnricher()
        self._external_enricher = ExternalEnricher()
        self._cross_ref_enricher = CrossReferenceEnricher()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "enrich")

            if operation == "enrich":
                return self._enrich(params)
            elif operation == "add_lookup":
                return self._add_lookup(params)
            elif operation == "add_reference":
                return self._add_reference(params)
            elif operation == "register_computed":
                return self._register_computed(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Enrichment error: {str(e)}")

    def _enrich(self, params: Dict) -> ActionResult:
        """Enrich data."""
        data = params.get("data", [])
        rules_data = params.get("rules", [])

        rules = []
        for r in rules_data:
            rule = EnrichmentRule(
                source_field=r.get("source_field", ""),
                target_field=r.get("target_field", ""),
                enricher=lambda x, ctx: x,
                default_value=r.get("default_value"),
            )
            rules.append(rule)

        enriched = []
        failed = 0

        for item in data:
            result, success = self._lookup_enricher.enrich_item(item, rules)
            enriched.append(result)
            if not success:
                failed += 1

        return ActionResult(
            success=failed == 0,
            message=f"Enriched {len(enriched)} items, {failed} failed",
            data={
                "total": len(data),
                "enriched": len(enriched),
                "failed": failed,
            },
        )

    def _add_lookup(self, params: Dict) -> ActionResult:
        """Add lookup table."""
        name = params.get("name")
        data = params.get("data", {})

        if not name:
            return ActionResult(success=False, message="name is required")

        self._lookup_enricher.add_lookup_table(name, data)
        return ActionResult(success=True, message=f"Lookup table '{name}' added with {len(data)} entries")

    def _add_reference(self, params: Dict) -> ActionResult:
        """Add reference table."""
        name = params.get("name")
        data = params.get("data", [])
        key_field = params.get("key_field", "id")

        if not name or not data:
            return ActionResult(success=False, message="name and data are required")

        self._cross_ref_enricher.add_reference_table(name, data, key_field)
        return ActionResult(success=True, message=f"Reference table '{name}' added with {len(data)} entries")

    def _register_computed(self, params: Dict) -> ActionResult:
        """Register computed field."""
        field_name = params.get("field_name")
        expression = params.get("expression")

        if not field_name or not expression:
            return ActionResult(success=False, message="field_name and expression are required")

        try:
            compute_fn = eval(f"lambda item: {expression}")
            self._computed_enricher.register_computed_field(field_name, compute_fn)
            return ActionResult(success=True, message=f"Computed field '{field_name}' registered")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to register: {str(e)}")
