"""Feature engineering action module for RabAI AutoClick.

Provides feature engineering operations:
- PolynomialFeaturesAction: Generate polynomial features
- InteractionFeaturesAction: Generate interaction features
- BinningFeaturesAction: Bin continuous features
- TextFeaturesAction: Extract text features
"""

import math
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PolynomialFeaturesAction(BaseAction):
    """Generate polynomial features."""
    action_type = "polynomial_features"
    display_name = "多项式特征"
    description = "生成多项式特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            degree = params.get("degree", 2)
            fields = params.get("fields", [])
            include_bias = params.get("include_bias", False)
            interaction_only = params.get("interaction_only", False)

            if not isinstance(data, list):
                data = [data]

            if not fields:
                if data and isinstance(data[0], dict):
                    numeric_fields = [k for k, v in data[0].items() if isinstance(v, (int, float))]
                    fields = numeric_fields[:5]
                else:
                    fields = ["value"]

            feature_names = []
            for f in fields:
                if include_bias:
                    feature_names.append(f"1")
                for d in range(1, degree + 1):
                    if interaction_only and d > 1:
                        continue
                    feature_names.append(f"{f}^{d}")

            if not interaction_only:
                for i in range(len(fields)):
                    for j in range(i + 1, len(fields)):
                        for d1 in range(1, degree + 1):
                            for d2 in range(1, degree + 1):
                                if d1 + d2 <= degree:
                                    feature_names.append(f"{fields[i]}^{d1}_{fields[j]}^{d2}")

            transformed = []
            for item in data:
                if not isinstance(item, dict):
                    item = {"value": item}
                features = []
                for f in fields:
                    val = item.get(f, 0)
                    if not isinstance(val, (int, float)):
                        val = 0
                    if include_bias:
                        features.append(1)
                    for d in range(1, degree + 1):
                        features.append(val ** d)

                if not interaction_only:
                    for i in range(len(fields)):
                        vi = item.get(fields[i], 0) if isinstance(item.get(fields[i]), (int, float)) else 0
                        for j in range(i + 1, len(fields)):
                            vj = item.get(fields[j], 0) if isinstance(item.get(fields[j]), (int, float)) else 0
                            for d1 in range(1, degree + 1):
                                for d2 in range(1, degree + 1):
                                    if d1 + d2 <= degree:
                                        features.append((vi ** d1) * (vj ** d2))

                transformed.append({**item, "polynomial_features": features, "feature_names": feature_names})

            return ActionResult(
                success=True,
                message=f"Generated {len(feature_names)} polynomial features for {len(data)} items",
                data={
                    "transformed": transformed,
                    "feature_names": feature_names,
                    "num_features": len(feature_names),
                    "degree": degree,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PolynomialFeatures error: {e}")


class InteractionFeaturesAction(BaseAction):
    """Generate interaction features."""
    action_type = "interaction_features"
    display_name = "交互特征"
    description = "生成特征交互"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_pairs = params.get("field_pairs", [])
            operations = params.get("operations", ["multiply"])
            interaction_prefix = params.get("prefix", "interact")

            if not isinstance(data, list):
                data = [data]

            if not field_pairs and data and isinstance(data[0], dict):
                numeric_fields = [k for k, v in data[0].items() if isinstance(v, (int, float))]
                field_pairs = [(numeric_fields[i], numeric_fields[j]) for i in range(len(numeric_fields)) for j in range(i + 1, len(numeric_fields))]

            transformed = []
            for item in data:
                if not isinstance(item, dict):
                    item = {"value": item}
                result = {**item}

                for f1, f2 in field_pairs:
                    v1 = item.get(f1, 0) if isinstance(item.get(f1), (int, float)) else 0
                    v2 = item.get(f2, 0) if isinstance(item.get(f2), (int, float)) else 0

                    for op in operations:
                        col_name = f"{interaction_prefix}_{f1}_{op}_{f2}"
                        if op == "multiply":
                            result[col_name] = v1 * v2
                        elif op == "add":
                            result[col_name] = v1 + v2
                        elif op == "subtract":
                            result[col_name] = v1 - v2
                        elif op == "divide":
                            result[col_name] = v1 / v2 if v2 != 0 else 0
                        elif op == "ratio":
                            result[col_name] = v1 / v2 if v2 != 0 else 0
                        elif op == "diff_ratio":
                            result[col_name] = (v1 - v2) / (v1 + v2) if (v1 + v2) != 0 else 0

                transformed.append(result)

            new_cols = len(field_pairs) * len(operations)

            return ActionResult(
                success=True,
                message=f"Generated {new_cols} interaction features for {len(data)} items",
                data={"transformed": transformed, "new_columns": new_cols, "field_pairs": len(field_pairs)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"InteractionFeatures error: {e}")


class BinningFeaturesAction(BaseAction):
    """Bin continuous features."""
    action_type = "binning_features"
    display_name = "分箱特征"
    description = "对连续特征进行分箱"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            binning_type = params.get("binning_type", "equal_width")
            num_bins = params.get("num_bins", 5)
            custom_boundaries = params.get("boundaries", None)
            labels = params.get("labels", None)

            if not isinstance(data, list):
                data = [data]

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field, 0)
                else:
                    v = item
                if isinstance(v, (int, float)):
                    values.append(v)

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            if custom_boundaries:
                boundaries = sorted(custom_boundaries)
            elif binning_type == "equal_width":
                min_val = min(values)
                max_val = max(values)
                bin_width = (max_val - min_val) / num_bins
                boundaries = [min_val + i * bin_width for i in range(num_bins + 1)]
            elif binning_type == "equal_frequency":
                sorted_values = sorted(values)
                bin_size = len(sorted_values) / num_bins
                boundaries = [sorted_values[int(i * bin_size)] for i in range(num_bins)]
                boundaries.append(sorted_values[-1])
            else:
                boundaries = [min(values) + i * (max(values) - min(values)) / num_bins for i in range(num_bins + 1)]

            if labels is None:
                labels = [f"bin_{i}" for i in range(len(boundaries) - 1)]

            transformed = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field, 0)
                else:
                    v = item

                bin_idx = 0
                for i in range(len(boundaries) - 1):
                    if v >= boundaries[i] and v < boundaries[i + 1]:
                        bin_idx = i
                        break
                if v >= boundaries[-1]:
                    bin_idx = len(boundaries) - 2

                result = {**item}
                result[f"{field}_bin"] = bin_idx
                result[f"{field}_bin_label"] = labels[bin_idx] if bin_idx < len(labels) else f"bin_{bin_idx}"
                result[f"{field}_bin_range"] = (boundaries[bin_idx], boundaries[bin_idx + 1])
                transformed.append(result)

            bin_counts = Counter(item[f"{field}_bin"] for item in transformed)

            return ActionResult(
                success=True,
                message=f"Binned {len(data)} items into {len(boundaries) - 1} bins",
                data={
                    "transformed": transformed,
                    "boundaries": boundaries,
                    "num_bins": len(boundaries) - 1,
                    "bin_counts": dict(bin_counts),
                    "labels": labels,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BinningFeatures error: {e}")


class TextFeaturesAction(BaseAction):
    """Extract text features."""
    action_type = "text_features"
    display_name = "文本特征"
    description = "提取文本特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            text_field = params.get("text_field", "text")
            features = params.get("features", ["length", "word_count"])

            if not isinstance(data, list):
                data = [data]

            transformed = []
            for item in data:
                if isinstance(item, dict):
                    text = str(item.get(text_field, ""))
                else:
                    text = str(item)

                result = {**item}
                text_features = {}

                if "length" in features:
                    text_features["length"] = len(text)

                if "word_count" in features:
                    text_features["word_count"] = len(text.split())

                if "char_count" in features:
                    text_features["char_count"] = len(text.replace(" ", ""))

                if "avg_word_length" in features:
                    words = text.split()
                    avg_len = sum(len(w) for w in words) / len(words) if words else 0
                    text_features["avg_word_length"] = round(avg_len, 2)

                if "sentence_count" in features:
                    text_features["sentence_count"] = len(re.split(r"[.!?]+", text))

                if "uppercase_count" in features:
                    text_features["uppercase_count"] = sum(1 for c in text if c.isupper())

                if "lowercase_count" in features:
                    text_features["lowercase_count"] = sum(1 for c in text if c.islower())

                if "digit_count" in features:
                    text_features["digit_count"] = sum(1 for c in text if c.isdigit())

                if "special_char_count" in features:
                    text_features["special_char_count"] = sum(1 for c in text if not c.isalnum() and not c.isspace())

                if "whitespace_count" in features:
                    text_features["whitespace_count"] = sum(1 for c in text if c.isspace())

                if "has_url" in features:
                    text_features["has_url"] = bool(re.search(r"https?://\S+", text))

                if "has_email" in features:
                    text_features["has_email"] = bool(re.search(r"\S+@\S+\.\S+", text))

                if "unique_word_count" in features:
                    words = text.lower().split()
                    text_features["unique_word_count"] = len(set(words))

                if "lexical_diversity" in features:
                    words = text.lower().split()
                    text_features["lexical_diversity"] = len(set(words)) / len(words) if words else 0

                result["text_features"] = text_features
                transformed.append(result)

            return ActionResult(
                success=True,
                message=f"Extracted text features for {len(data)} items",
                data={"transformed": transformed, "items_processed": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TextFeatures error: {e}")
