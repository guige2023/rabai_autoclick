"""Data enricher action module for RabAI AutoClick.

Provides data enrichment operations:
- EnrichLookupAction: Lookup enrichment
- EnrichComputeAction: Compute enrichment
- EnrichJoinAction: Join enrichment
- EnrichDefaultAction: Default enrichment
- EnrichBatchAction: Batch enrichment
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnrichLookupAction(BaseAction):
    """Lookup-based enrichment."""
    action_type = "enrich_lookup"
    display_name = "查找丰富"
    description = "基于查找表的数据丰富"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            lookup_table = params.get("lookup_table", {})
            key_field = params.get("key_field", "id")
            enrich_fields = params.get("enrich_fields", [])
            lookup_key = params.get("lookup_key", "id")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(lookup_table, list):
                lookup_dict = {item.get(lookup_key): item for item in lookup_table}
            else:
                lookup_dict = lookup_table

            enriched = []
            hits = 0
            for item in data:
                key = item.get(key_field)
                new_item = item.copy()
                if key in lookup_dict:
                    lookup_row = lookup_dict[key]
                    for ef in enrich_fields:
                        new_item[ef] = lookup_row.get(ef)
                    hits += 1
                enriched.append(new_item)

            return ActionResult(
                success=True,
                data={"enriched": enriched, "enrich_count": hits, "miss_count": len(data) - hits, "total": len(data)},
                message=f"Lookup enrich: {hits}/{len(data)} records enriched",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich lookup failed: {e}")


class EnrichComputeAction(BaseAction):
    """Compute-based enrichment."""
    action_type = "enrich_compute"
    display_name = "计算丰富"
    description = "基于计算的数据丰富"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            computed_fields = params.get("computed_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            enriched = []
            for item in data:
                new_item = item.copy()
                for cf in computed_fields:
                    field_name = cf.get("name", "computed")
                    source_fields = cf.get("sources", [])
                    operation = cf.get("operation", "add")

                    if operation == "add" and len(source_fields) == 2:
                        new_item[field_name] = item.get(source_fields[0], 0) + item.get(source_fields[1], 0)
                    elif operation == "subtract" and len(source_fields) == 2:
                        new_item[field_name] = item.get(source_fields[0], 0) - item.get(source_fields[1], 0)
                    elif operation == "multiply" and len(source_fields) == 2:
                        new_item[field_name] = item.get(source_fields[0], 0) * item.get(source_fields[1], 0)
                    elif operation == "concat":
                        new_item[field_name] = "".join(str(item.get(sf, "")) for sf in source_fields)
                    elif operation == "upper":
                        new_item[field_name] = str(item.get(source_fields[0], "")).upper()
                    elif operation == "lower":
                        new_item[field_name] = str(item.get(source_fields[0], "")).lower()
                enriched.append(new_item)

            return ActionResult(
                success=True,
                data={"enriched": enriched, "computed_fields": len(computed_fields), "count": len(enriched)},
                message=f"Computed enrich: added {len(computed_fields)} fields to {len(enriched)} records",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich compute failed: {e}")


class EnrichJoinAction(BaseAction):
    """Join-based enrichment."""
    action_type = "enrich_join"
    display_name = "连接丰富"
    description = "基于连接的数据丰富"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            join_data = params.get("join_data", [])
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            prefix = params.get("prefix", "enriched_")

            if not data:
                return ActionResult(success=False, message="data is required")

            right_index = {item.get(right_key): item for item in join_data}
            enriched = []
            for item in data:
                new_item = item.copy()
                key = item.get(left_key)
                if key in right_index:
                    join_row = right_index[key]
                    for k, v in join_row.items():
                        if k != right_key:
                            new_item[f"{prefix}{k}"] = v
                enriched.append(new_item)

            return ActionResult(
                success=True,
                data={"enriched": enriched, "join_count": len(enriched), "total": len(data)},
                message=f"Join enrich: enriched {len(enriched)} records with prefix '{prefix}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich join failed: {e}")


class EnrichDefaultAction(BaseAction):
    """Default value enrichment."""
    action_type = "enrich_default"
    display_name = "默认值丰富"
    description = "填充默认值丰富"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            default_values = params.get("default_values", {})

            if not data:
                return ActionResult(success=False, message="data is required")

            enriched = []
            for item in data:
                new_item = item.copy()
                for field, default in default_values.items():
                    if field not in new_item or new_item[field] is None:
                        new_item[field] = default
                enriched.append(new_item)

            return ActionResult(
                success=True,
                data={"enriched": enriched, "default_fields": len(default_values), "count": len(enriched)},
                message=f"Default enrich: filled {len(default_values)} default fields for {len(enriched)} records",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich default failed: {e}")


class EnrichBatchAction(BaseAction):
    """Batch enrichment."""
    action_type = "enrich_batch"
    display_name = "批量丰富"
    description = "批量数据丰富"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            enrichment_operations = params.get("enrichment_operations", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            enriched = data
            for op in enrichment_operations:
                op_type = op.get("type", "")
                if op_type == "lookup":
                    lookup_result = EnrichLookupAction().execute(context, {"data": enriched, **op})
                    if lookup_result.success:
                        enriched = lookup_result.data.get("enriched", enriched)
                elif op_type == "compute":
                    compute_result = EnrichComputeAction().execute(context, {"data": enriched, **op})
                    if compute_result.success:
                        enriched = compute_result.data.get("enriched", enriched)
                elif op_type == "default":
                    default_result = EnrichDefaultAction().execute(context, {"data": enriched, **op})
                    if default_result.success:
                        enriched = default_result.data.get("enriched", enriched)

            return ActionResult(
                success=True,
                data={"enriched": enriched, "operations_applied": len(enrichment_operations), "count": len(enriched)},
                message=f"Batch enrich: applied {len(enrichment_operations)} operations to {len(enriched)} records",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich batch failed: {e}")
