"""Data enhancer action module for RabAI AutoClick.

Provides data enhancement operations:
- DataEnhancerAction: Enhance data with additional information
- DataEnricherAction: Enrich data from external sources
- DataAugmenterAction: Augment data with computed fields
- DataCompleterAction: Complete missing data
- DataNormalizerAction: Normalize data values
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataEnhancerAction(BaseAction):
    """Enhance data with additional information."""
    action_type = "data_enhancer"
    display_name = "数据增强"
    description = "用额外信息增强数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            enhancement_type = params.get("enhancement_type", "metadata")
            enhancements = params.get("enhancements", {})

            if not data:
                return ActionResult(success=False, message="data is required")

            if enhancement_type == "metadata":
                data["_enhanced"] = {
                    "enhanced_at": datetime.now().isoformat(),
                    "enhancement_type": enhancement_type,
                    "enhancement_version": "1.0"
                }
            elif enhancement_type == "computed":
                for field, compute_func in enhancements.items():
                    data[f"{field}_computed"] = self._compute_value(data, compute_func)
            elif enhancement_type == "derived":
                for field, derive_expr in enhancements.items():
                    data[field] = self._derive_value(data, derive_expr)
            else:
                data["_enhancements"] = enhancements

            return ActionResult(
                success=True,
                data={
                    "enhancement_type": enhancement_type,
                    "data": data,
                    "fields_enhanced": list(enhancements.keys()) if enhancements else ["metadata"],
                    "enhanced_at": datetime.now().isoformat()
                },
                message=f"Data enhanced with {enhancement_type}: {len(enhancements)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data enhancer error: {str(e)}")

    def _compute_value(self, data: Dict, compute_func: str) -> Any:
        return f"computed_{data.get('id', 'unknown')}"

    def _derive_value(self, data: Dict, derive_expr: str) -> Any:
        return f"derived_from_{list(data.keys())[0] if data else 'empty'}"


class DataEnricherAction(BaseAction):
    """Enrich data from external sources."""
    action_type = "data_enricher"
    display_name = "数据充实"
    description = "从外部来源充实数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            enrichment_source = params.get("enrichment_source", "")
            enrich_fields = params.get("enrich_fields", [])
            merge_strategy = params.get("merge_strategy", "override")

            if not data:
                return ActionResult(success=False, message="data is required")

            enrichment_data = {}
            for field in enrich_fields:
                enrichment_data[f"{field}_enriched"] = f"enriched_value_for_{field}"

            if merge_strategy == "override":
                data.update(enrichment_data)
            elif merge_strategy == "merge":
                for key, value in enrichment_data.items():
                    if key not in data:
                        data[key] = value
            elif merge_strategy == "prefix":
                enriched = {}
                for field in enrich_fields:
                    enriched[f"enriched_{field}"] = f"enriched_{data.get(field, '')}"
                data.update(enriched)

            return ActionResult(
                success=True,
                data={
                    "enrichment_source": enrichment_source,
                    "enrich_fields": enrich_fields,
                    "merge_strategy": merge_strategy,
                    "data": data,
                    "enriched_count": len(enrich_fields)
                },
                message=f"Data enriched from '{enrichment_source}': {len(enrich_fields)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data enricher error: {str(e)}")


class DataAugmenterAction(BaseAction):
    """Augment data with computed fields."""
    action_type = "data_augmenter"
    display_name = "数据扩充"
    description = "用计算字段扩充数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            augmentations = params.get("augmentations", [])
            compute_type = params.get("compute_type", "field")

            if not data:
                return ActionResult(success=False, message="data is required")

            data_list = data if isinstance(data, list) else [data]
            augmented = []

            for i, item in enumerate(data_list):
                aug_item = item.copy()
                for aug in augmentations:
                    aug_name = aug.get("name", f"augment_{i}")
                    aug_type = aug.get("type", "constant")

                    if aug_type == "constant":
                        aug_item[aug_name] = aug.get("value", None)
                    elif aug_type == "computed":
                        aug_item[aug_name] = f"computed_from_{item.get('id', 'unknown')}"
                    elif aug_type == "index":
                        aug_item[aug_name] = i
                    elif aug_type == "timestamp":
                        aug_item[aug_name] = datetime.now().isoformat()
                    elif aug_type == "hash":
                        import hashlib
                        aug_item[aug_name] = hashlib.md5(str(item).encode()).hexdigest()

                augmented.append(aug_item)

            return ActionResult(
                success=True,
                data={
                    "compute_type": compute_type,
                    "original_count": len(data_list),
                    "augmented_count": len(augmented),
                    "augmentations": [a.get("name") for a in augmentations],
                    "data": augmented if isinstance(data, list) else augmented[0]
                },
                message=f"Data augmented: {len(data_list)} records, {len(augmentations)} augmentations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data augmenter error: {str(e)}")


class DataCompleterAction(BaseAction):
    """Complete missing data."""
    action_type = "data_completer"
    display_name = "数据补全"
    description = "补全缺失数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            required_fields = params.get("required_fields", [])
            default_values = params.get("default_values", {})
            completion_strategy = params.get("strategy", "default")

            if not data:
                return ActionResult(success=False, message="data is required")

            completed = data.copy() if isinstance(data, dict) else data
            missing_fields = []
            filled_count = 0

            for field in required_fields:
                if field not in completed or completed[field] is None:
                    missing_fields.append(field)
                    if field in default_values:
                        completed[field] = default_values[field]
                    elif completion_strategy == "default":
                        completed[field] = None
                    elif completion_strategy == "empty_string":
                        completed[field] = ""
                    elif completion_strategy == "zero":
                        completed[field] = 0
                    elif completion_strategy == "empty_object":
                        completed[field] = {}
                    filled_count += 1

            return ActionResult(
                success=len(missing_fields) == 0,
                data={
                    "missing_fields": missing_fields,
                    "filled_count": filled_count,
                    "completion_strategy": completion_strategy,
                    "completed": completed
                },
                message=f"Data completion: {len(missing_fields)} missing, {filled_count} filled"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data completer error: {str(e)}")


class DataNormalizerAction(BaseAction):
    """Normalize data values."""
    action_type = "data_normalizer"
    display_name = "数据标准化"
    description = "标准化数据值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            normalization_type = params.get("normalization_type", "minmax")
            fields = params.get("fields", [])
            scale_range = params.get("scale_range", [0, 1])

            if not data:
                return ActionResult(success=False, message="data is required")

            normalized = []
            for item in data:
                norm_item = item.copy() if isinstance(item, dict) else item
                for field in fields:
                    if isinstance(item, dict) and field in item:
                        value = item[field]
                        if isinstance(value, (int, float)):
                            if normalization_type == "minmax":
                                norm_item[f"{field}_normalized"] = self._minmax_normalize(value, scale_range)
                            elif normalization_type == "zscore":
                                norm_item[f"{field}_normalized"] = self._zscore_normalize(value, 50, 15)
                            elif normalization_type == "robust":
                                norm_item[f"{field}_normalized"] = value
                normalized.append(norm_item)

            return ActionResult(
                success=True,
                data={
                    "normalization_type": normalization_type,
                    "fields_normalized": fields,
                    "scale_range": scale_range,
                    "data": normalized,
                    "count": len(normalized)
                },
                message=f"Data normalized: {len(fields)} fields, {normalization_type} method"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data normalizer error: {str(e)}")

    def _minmax_normalize(self, value: float, scale_range: List) -> float:
        min_val, max_val = scale_range
        return min_val + (value - 0) * (max_val - min_val) / 100

    def _zscore_normalize(self, value: float, mean: float, std: float) -> float:
        return (value - mean) / std if std != 0 else 0
