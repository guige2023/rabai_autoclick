"""Data enrichment action module for RabAI AutoClick.

Provides data enrichment operations:
- EnrichLookupAction: Enrich with lookup table
- EnrichMergeAction: Merge/enrich with external data
- EnrichComputeAction: Compute derived fields
- EnrichNormalizeAction: Normalize data
"""

from typing import Any, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnrichLookupAction(BaseAction):
    """Enrich data with lookup table."""
    action_type = "enrich_lookup"
    display_name = "查找表增强"
    description = "使用查找表增强数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            lookup_table = params.get("lookup_table", {})
            key_field = params.get("key_field", "")
            enrich_fields = params.get("enrich_fields", [])
            default_value = params.get("default_value", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            enriched = []
            for record in data:
                if isinstance(record, dict):
                    new_record = dict(record)
                    key = record.get(key_field)
                    if key is not None:
                        lookup_data = lookup_table.get(str(key), {})
                        for field in enrich_fields:
                            if isinstance(lookup_data, dict):
                                new_record[field] = lookup_data.get(field, default_value)
                            else:
                                new_record[field] = lookup_data if lookup_data is not None else default_value
                    enriched.append(new_record)
                else:
                    enriched.append(record)

            return ActionResult(
                success=True,
                message=f"Enriched {len(enriched)} records",
                data={"enriched": enriched, "count": len(enriched)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Enrich lookup failed: {str(e)}")


class EnrichMergeAction(BaseAction):
    """Merge external data for enrichment."""
    action_type = "enrich_merge"
    display_name = "数据合并增强"
    description = "合并外部数据进行增强"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            external_data = params.get("external_data", [])
            key_field = params.get("key_field", "")
            merge_type = params.get("merge_type", "left")
            suffix = params.get("suffix", "_ext")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]
            if not isinstance(external_data, list):
                external_data = [external_data]

            ext_dict = {}
            for ext in external_data:
                if isinstance(ext, dict):
                    key = ext.get(key_field)
                    if key is not None:
                        ext_dict[key] = ext

            merged = []
            for record in data:
                if isinstance(record, dict):
                    new_record = dict(record)
                    key = record.get(key_field)
                    if key is not None and key in ext_dict:
                        ext_record = ext_dict[key]
                        for k, v in ext_record.items():
                            if k != key_field:
                                if k in new_record:
                                    new_record[f"{k}{suffix}"] = v
                                else:
                                    new_record[k] = v
                    merged.append(new_record)
                else:
                    merged.append(record)

            return ActionResult(
                success=True,
                message=f"Merged {len(merged)} records with external data",
                data={"merged": merged, "count": len(merged), "merge_type": merge_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Enrich merge failed: {str(e)}")


class EnrichComputeAction(BaseAction):
    """Compute derived fields."""
    action_type = "enrich_compute"
    display_name = "计算增强"
    description = "计算派生字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            computations = params.get("computations", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            enriched = []
            for record in data:
                if isinstance(record, dict):
                    new_record = dict(record)
                    for comp in computations:
                        field_name = comp.get("field", "")
                        expression = comp.get("expression", "")
                        func_ref = comp.get("func_ref", None)

                        if func_ref:
                            try:
                                new_record[field_name] = func_ref(record)
                            except Exception:
                                new_record[field_name] = None
                        elif expression:
                            try:
                                new_record[field_name] = eval(expression, {"__builtins__": {}}, {"r": record})
                            except Exception:
                                new_record[field_name] = None
                    enriched.append(new_record)
                else:
                    enriched.append(record)

            return ActionResult(
                success=True,
                message=f"Computed {len(computations)} fields for {len(enriched)} records",
                data={"enriched": enriched, "count": len(enriched), "computed_fields": len(computations)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Enrich compute failed: {str(e)}")


class EnrichNormalizeAction(BaseAction):
    """Normalize data."""
    action_type = "enrich_normalize"
    display_name = "数据标准化"
    description = "标准化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            method = params.get("method", "minmax")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                data = [data]

            if not fields:
                return ActionResult(success=False, message="fields are required for normalization")

            if method == "minmax":
                for field in fields:
                    values = []
                    for record in data:
                        if isinstance(record, dict) and field in record:
                            try:
                                values.append(float(record[field]))
                            except (ValueError, TypeError):
                                pass
                    if values:
                        min_val = min(values)
                        max_val = max(values)
                        range_val = max_val - min_val
                        if range_val > 0:
                            for record in data:
                                if isinstance(record, dict) and field in record:
                                    try:
                                        record[field] = (float(record[field]) - min_val) / range_val
                                    except (ValueError, TypeError):
                                        pass

            elif method == "zscore":
                for field in fields:
                    values = []
                    for record in data:
                        if isinstance(record, dict) and field in record:
                            try:
                                values.append(float(record[field]))
                            except (ValueError, TypeError):
                                pass
                    if values:
                        mean = sum(values) / len(values)
                        variance = sum((x - mean) ** 2 for x in values) / len(values)
                        std = variance ** 0.5
                        if std > 0:
                            for record in data:
                                if isinstance(record, dict) and field in record:
                                    try:
                                        record[field] = (float(record[field]) - mean) / std
                                    except (ValueError, TypeError):
                                        pass

            return ActionResult(
                success=True,
                message=f"Normalized {len(fields)} fields using {method}",
                data={"data": data, "count": len(data), "method": method}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Enrich normalize failed: {str(e)}")
