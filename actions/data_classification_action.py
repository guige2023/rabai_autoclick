"""Data classification action module for RabAI AutoClick.

Provides data classification operations:
- RuleBasedClassifierAction: Rule-based data classification
- LabelEncoderAction: Encode categorical labels
- OneHotEncodeAction: One-hot encode categorical features
- DataTypeClassifierAction: Classify data by type
"""

import hashlib
from typing import Any, Dict, List, Optional, Set
from collections import Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RuleBasedClassifierAction(BaseAction):
    """Rule-based data classification."""
    action_type = "rule_based_classifier"
    display_name = "规则分类器"
    description = "基于规则的数据分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", [])
            default_class = params.get("default_class", "unknown")
            match_mode = params.get("match_mode", "first")

            if not isinstance(data, list):
                data = [data]

            if not rules:
                return ActionResult(success=False, message="rules is required")

            classified = []
            class_counts = Counter()

            for item in data:
                if not isinstance(item, dict):
                    item = {"value": item}

                assigned_class = default_class

                for rule in rules:
                    rule_name = rule.get("name", "unnamed")
                    conditions = rule.get("conditions", [])
                    classification = rule.get("class", rule_name)

                    all_match = True
                    for cond in conditions:
                        field = cond.get("field")
                        operator = cond.get("operator", "eq")
                        value = cond.get("value")

                        item_value = item.get(field) if field else item.get("value")

                        matched = False
                        if operator == "eq":
                            matched = item_value == value
                        elif operator == "ne":
                            matched = item_value != value
                        elif operator == "gt":
                            matched = item_value is not None and item_value > value
                        elif operator == "lt":
                            matched = item_value is not None and item_value < value
                        elif operator == "ge":
                            matched = item_value is not None and item_value >= value
                        elif operator == "le":
                            matched = item_value is not None and item_value <= value
                        elif operator == "contains":
                            matched = value in str(item_value) if item_value else False
                        elif operator == "startswith":
                            matched = str(item_value).startswith(str(value)) if item_value else False
                        elif operator == "endswith":
                            matched = str(item_value).endswith(str(value)) if item_value else False
                        elif operator == "in":
                            matched = item_value in value if isinstance(value, (list, tuple, set)) else False
                        elif operator == "regex":
                            import re
                            matched = isinstance(item_value, str) and re.search(str(value), item_value) is not None
                        elif operator == "exists":
                            matched = item_value is not None
                        elif operator == "is_null":
                            matched = item_value is None

                        if not matched:
                            all_match = False
                            break

                    if all_match:
                        assigned_class = classification
                        if match_mode == "first":
                            break

                classified_item = {**item, "class": assigned_class}
                classified.append(classified_item)
                class_counts[assigned_class] += 1

            return ActionResult(
                success=True,
                message=f"Classified {len(data)} items into {len(class_counts)} classes",
                data={
                    "classified": classified,
                    "class_counts": dict(class_counts),
                    "class_count": len(class_counts),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RuleBasedClassifier error: {e}")


class LabelEncoderAction(BaseAction):
    """Encode categorical labels as integers."""
    action_type = "label_encoder"
    display_name = "标签编码"
    description = "将分类标签编码为整数"

    def __init__(self):
        super().__init__()
        self._label_map: Dict[str, int] = {}
        self._reverse_map: Dict[int, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", [])
            field = params.get("field", "label")
            classes = params.get("classes", None)

            if not isinstance(data, list):
                data = [data]

            if action == "fit":
                if classes:
                    self._label_map = {c: i for i, c in enumerate(classes)}
                else:
                    unique_labels = set()
                    for item in data:
                        if isinstance(item, dict):
                            unique_labels.add(str(item.get(field, "")))
                        else:
                            unique_labels.add(str(item))
                    self._label_map = {c: i for i, c in enumerate(sorted(unique_labels))}
                self._reverse_map = {v: k for k, v in self._label_map.items()}

                return ActionResult(
                    success=True,
                    message=f"LabelEncoder fitted with {len(self._label_map)} classes",
                    data={"classes": list(self._label_map.keys()), "mapping": self._label_map},
                )

            elif action == "encode":
                if not self._label_map:
                    return ActionResult(success=False, message="Encoder not fitted. Call 'fit' first.")

                encoded = []
                unknown_count = 0
                for item in data:
                    if isinstance(item, dict):
                        label = str(item.get(field, ""))
                        encoded_value = self._label_map.get(label, -1)
                        if encoded_value == -1:
                            unknown_count += 1
                        encoded.append({**item, f"{field}_encoded": encoded_value})
                    else:
                        label = str(item)
                        encoded_value = self._label_map.get(label, -1)
                        if encoded_value == -1:
                            unknown_count += 1
                        encoded.append({"original": item, "encoded": encoded_value})

                return ActionResult(
                    success=True,
                    message=f"Encoded {len(encoded)} items ({unknown_count} unknown)",
                    data={"encoded": encoded, "unknown_count": unknown_count, "mapping": self._label_map},
                )

            elif action == "decode":
                if not self._reverse_map:
                    return ActionResult(success=False, message="Encoder not fitted.")

                values = params.get("values", [])
                decoded = [self._reverse_map.get(v, None) for v in values]
                return ActionResult(success=True, message=f"Decoded {len(decoded)} values", data={"decoded": decoded})

            elif action == "inverse":
                if not self._reverse_map:
                    return ActionResult(success=False, message="Encoder not fitted.")

                encoded = []
                for item in data:
                    if isinstance(item, dict):
                        encoded_value = item.get(f"{field}_encoded")
                        decoded_label = self._reverse_map.get(encoded_value, None)
                        encoded.append({**item, f"{field}_decoded": decoded_label})
                    else:
                        encoded.append({"original": item, "decoded": self._reverse_map.get(item)})

                return ActionResult(success=True, message=f"Decoded {len(encoded)} items", data={"decoded": encoded})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"LabelEncoder error: {e}")


class OneHotEncodeAction(BaseAction):
    """One-hot encode categorical features."""
    action_type = "onehot_encode"
    display_name = "独热编码"
    description = "对分类特征进行独热编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "category")
            prefix = params.get("prefix", None)
            drop_first = params.get("drop_first", False)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            unique_values = set()
            for item in data:
                if isinstance(item, dict):
                    unique_values.add(str(item.get(field, "")))
                else:
                    unique_values.add(str(item))

            sorted_values = sorted(unique_values)
            if drop_first:
                sorted_values = sorted_values[1:]

            if prefix is None:
                prefix = field

            encoded = []
            for item in data:
                if isinstance(item, dict):
                    value = str(item.get(field, ""))
                    encoded_item = {**item}
                    for val in sorted_values:
                        col_name = f"{prefix}_{val}"
                        encoded_item[col_name] = 1 if value == val else 0
                    encoded.append(encoded_item)
                else:
                    value = str(item)
                    encoded_item = {"original": item}
                    for val in sorted_values:
                        col_name = f"{prefix}_{val}"
                        encoded_item[col_name] = 1 if value == val else 0
                    encoded.append(encoded_item)

            return ActionResult(
                success=True,
                message=f"One-hot encoded {len(data)} items with {len(sorted_values)} categories",
                data={
                    "encoded": encoded,
                    "categories": sorted_values,
                    "num_categories": len(sorted_values),
                    "new_columns": [f"{prefix}_{v}" for v in sorted_values],
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OneHotEncode error: {e}")


class DataTypeClassifierAction(BaseAction):
    """Classify data by type."""
    action_type = "data_type_classifier"
    display_name = "数据类型分类"
    description = "根据数据类型分类数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            output_format = params.get("output_format", "summary")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            type_counts = Counter()
            typed_items = []

            for item in data:
                if isinstance(item, dict):
                    item_types = {}
                    for field, value in item.items():
                        detected_type = self._detect_type(value)
                        item_types[field] = detected_type
                        type_counts[detected_type] += 1
                    typed_items.append({**item, "_detected_types": item_types})
                else:
                    detected_type = self._detect_type(item)
                    type_counts[detected_type] += 1
                    typed_items.append({"value": item, "type": detected_type})

            summary = {
                "total_items": len(data),
                "type_distribution": dict(type_counts),
                "dominant_type": type_counts.most_common(1)[0][0] if type_counts else None,
            }

            if output_format == "summary":
                return ActionResult(success=True, message="Type classification complete", data=summary)
            elif output_format == "detailed":
                return ActionResult(success=True, message="Type classification complete", data={"summary": summary, "items": typed_items})
            elif output_format == "typed":
                return ActionResult(success=True, message="Type classification complete", data={"typed_data": typed_items})

            return ActionResult(success=True, message="Type classification complete", data=summary)
        except Exception as e:
            return ActionResult(success=False, message=f"DataTypeClassifier error: {e}")

    def _detect_type(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            if value.lower() in ("true", "false"):
                return "boolean_string"
            try:
                int(value)
                return "digit_string"
            except ValueError:
                pass
            try:
                float(value)
                return "float_string"
            except ValueError:
                pass
            if len(value) == 10 and value.isdigit():
                return "date_string"
            return "text"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return "unknown"
