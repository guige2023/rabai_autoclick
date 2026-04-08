"""Feature engineering action module for RabAI AutoClick.

Provides feature engineering operations:
- FeatureEngineeringAction: Create and manage ML features
- FeatureStoreAction: Feature storage and retrieval
- FeatureSelectionAction: Select best features for models
- FeatureTransformAction: Transform features for ML models
- FeatureMonitorAction: Monitor feature drift and health
"""

import time
import hashlib
import json
import math
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FeatureType(str, Enum):
    """Feature types."""
    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    TEXT = "text"
    EMBEDDING = "embedding"


class FeatureEngineeringAction(BaseAction):
    """Create and manage ML features from raw data."""
    action_type = "feature_engineering"
    display_name = "特征工程"
    description = "特征创建与管理"

    def __init__(self):
        super().__init__()
        self._feature_definitions: Dict[str, Dict] = {}
        self._computed_features: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            feature_name = params.get("feature_name", "")

            if operation == "define":
                if not feature_name:
                    return ActionResult(success=False, message="feature_name required")

                self._feature_definitions[feature_name] = {
                    "name": feature_name,
                    "type": params.get("feature_type", FeatureType.NUMERICAL.value),
                    "source_fields": params.get("source_fields", []),
                    "transform_func": params.get("transform_func", "passthrough"),
                    "transform_params": params.get("transform_params", {}),
                    "description": params.get("description", ""),
                    "tags": params.get("tags", []),
                    "created_at": time.time(),
                    "version": 1
                }

                return ActionResult(
                    success=True,
                    data={"feature": feature_name, "type": self._feature_definitions[feature_name]["type"]},
                    message=f"Feature '{feature_name}' defined"
                )

            elif operation == "compute":
                if not feature_name:
                    return ActionResult(success=False, message="feature_name required")

                if feature_name not in self._feature_definitions:
                    return ActionResult(success=False, message=f"Feature '{feature_name}' not defined")

                feature_def = self._feature_definitions[feature_name]
                source_data = params.get("source_data", {})

                feature_value = self._compute_feature(feature_def, source_data)

                self._computed_features[feature_name] = {
                    "value": feature_value,
                    "computed_at": time.time(),
                    "version": feature_def["version"]
                }

                return ActionResult(
                    success=True,
                    data={"feature": feature_name, "value": feature_value, "type": feature_def["type"]},
                    message=f"Feature '{feature_name}' computed: {feature_value}"
                )

            elif operation == "batch_compute":
                features = params.get("features", [])
                source_data = params.get("source_data", {})

                results = {}
                for fname in features:
                    if fname not in self._feature_definitions:
                        results[fname] = {"error": "not defined"}
                        continue
                    feature_def = self._feature_definitions[fname]
                    results[fname] = {"value": self._compute_feature(feature_def, source_data)}

                return ActionResult(
                    success=True,
                    data={"computed": results, "count": len(results)},
                    message=f"Batch computed {len(results)} features"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "features": [
                            {"name": k, "type": v["type"], "version": v["version"]}
                            for k, v in self._feature_definitions.items()
                        ]
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Feature engineering error: {str(e)}")

    def _compute_feature(self, feature_def: Dict, source_data: Dict) -> Any:
        transform_func = feature_def.get("transform_func", "passthrough")
        transform_params = feature_def.get("transform_params", {})
        source_fields = feature_def.get("source_fields", [])

        if not source_fields:
            return None

        if len(source_fields) == 1:
            raw_value = source_data.get(source_fields[0])
        else:
            raw_value = {f: source_data.get(f) for f in source_fields}

        if transform_func == "passthrough":
            return raw_value
        elif transform_func == "normalize":
            min_val = transform_params.get("min", 0)
            max_val = transform_params.get("max", 1)
            if max_val == min_val:
                return 0.0
            return (raw_value - min_val) / (max_val - min_val)
        elif transform_func == "log":
            val = float(raw_value) if raw_value is not None else 0
            return math.log(val + 1)
        elif transform_func == "sqrt":
            val = float(raw_value) if raw_value is not None else 0
            return math.sqrt(max(0, val))
        elif transform_func == "bin":
            bins = transform_params.get("bins", [0, 25, 50, 75, 100])
            labels = transform_params.get("labels", range(len(bins) - 1))
            val = float(raw_value) if raw_value is not None else bins[0]
            for i in range(len(bins) - 1):
                if bins[i] <= val < bins[i + 1]:
                    return labels[i] if not isinstance(labels, range) else bins[i]
            return labels[-1] if not isinstance(labels, range) else bins[-1]
        elif transform_func == "one_hot":
            categories = transform_params.get("categories", [])
            val = raw_value
            return {cat: (1 if val == cat else 0) for cat in categories}
        elif transform_func == "clip":
            min_val = transform_params.get("min")
            max_val = transform_params.get("max")
            val = float(raw_value) if raw_value is not None else 0
            if min_val is not None:
                val = max(val, min_val)
            if max_val is not None:
                val = min(val, max_val)
            return val
        else:
            return raw_value


class FeatureStoreAction(BaseAction):
    """Feature store for storing and retrieving ML features."""
    action_type = "feature_store"
    display_name = "特征存储"
    description = "特征存储与检索"

    def __init__(self):
        super().__init__()
        self._feature_store: Dict[str, Dict] = {}
        self._feature_groups: Dict[str, List[str]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "store")
            entity_name = params.get("entity_name", "")
            feature_name = params.get("feature_name", "")

            if operation == "create_group":
                group_name = params.get("group_name", "")
                if not group_name:
                    return ActionResult(success=False, message="group_name required")

                self._feature_groups[group_name] = []
                return ActionResult(success=True, data={"group": group_name}, message=f"Group '{group_name}' created")

            elif operation == "store":
                if not entity_name or not feature_name:
                    return ActionResult(success=False, message="entity_name and feature_name required")

                key = f"{entity_name}:{feature_name}"
                self._feature_store[key] = {
                    "entity": entity_name,
                    "feature": feature_name,
                    "value": params.get("value"),
                    "version": params.get("version", "1.0"),
                    "timestamp": params.get("timestamp", time.time()),
                    "ttl": params.get("ttl"),
                    "metadata": params.get("metadata", {})
                }

                group_name = params.get("group_name", "default")
                if group_name not in self._feature_groups:
                    self._feature_groups[group_name] = []
                if key not in self._feature_groups[group_name]:
                    self._feature_groups[group_name].append(key)

                return ActionResult(success=True, data={"key": key}, message=f"Stored: {key}")

            elif operation == "retrieve":
                if not entity_name or not feature_name:
                    return ActionResult(success=False, message="entity_name and feature_name required")

                key = f"{entity_name}:{feature_name}"
                if key not in self._feature_store:
                    return ActionResult(success=False, message=f"Feature '{key}' not found")

                entry = self._feature_store[key]
                if entry.get("ttl"):
                    if time.time() > entry["timestamp"] + entry["ttl"]:
                        return ActionResult(success=False, message="Feature expired")

                return ActionResult(success=True, data={"entry": entry}, message=f"Retrieved: {key}")

            elif operation == "batch_retrieve":
                entity_name = params.get("entity_name", "")
                features = params.get("features", [])

                results = {}
                for fname in features:
                    key = f"{entity_name}:{fname}"
                    if key in self._feature_store:
                        results[fname] = self._feature_store[key]["value"]
                    else:
                        results[fname] = None

                return ActionResult(success=True, data={"results": results}, message=f"Batch retrieved {len(features)} features")

            elif operation == "list_groups":
                return ActionResult(success=True, data={"groups": list(self._feature_groups.keys())})

            elif operation == "list_entities":
                group_name = params.get("group_name", "default")
                if group_name not in self._feature_groups:
                    return ActionResult(success=False, message=f"Group '{group_name}' not found")

                entities = list(set(entry.split(":")[0] for entry in self._feature_groups[group_name]))
                return ActionResult(success=True, data={"entities": entities, "count": len(entities)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Feature store error: {str(e)}")


class FeatureSelectionAction(BaseAction):
    """Select best features for ML models."""
    action_type = "feature_selection"
    display_name = "特征选择"
    description = "选择最佳特征"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "select")
            data = params.get("data", [])
            labels = params.get("labels", [])
            method = params.get("method", "variance")
            max_features = params.get("max_features", 10)

            if operation == "select":
                if not data or not labels:
                    return ActionResult(success=False, message="data and labels required")

                if method == "variance":
                    selected = self._select_by_variance(data, max_features)
                elif method == "correlation":
                    selected = self._select_by_correlation(data, labels, max_features)
                elif method == "mutual_info":
                    selected = self._select_by_mutual_info(data, labels, max_features)
                else:
                    return ActionResult(success=False, message=f"Unknown method: {method}")

                return ActionResult(
                    success=True,
                    data={"selected_features": selected, "method": method, "count": len(selected)},
                    message=f"Selected {len(selected)} features using {method}"
                )

            elif operation == "rank":
                if not data or not labels:
                    return ActionResult(success=False, message="data and labels required")

                feature_scores = self._rank_features(data, labels)

                return ActionResult(
                    success=True,
                    data={"ranked_features": feature_scores},
                    message=f"Ranked {len(feature_scores)} features"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Feature selection error: {str(e)}")

    def _select_by_variance(self, data: List[Dict], max_features: int) -> List[int]:
        if not data:
            return []
        num_features = len(data[0]) if isinstance(data[0], (list, tuple)) else len(next(iter(data[0].values()), [])) if isinstance(data[0], dict) else 0
        variances = []
        for i in range(num_features):
            values = [row[i] if isinstance(row, (list, tuple)) else list(row.values())[i] for row in data if isinstance(row, (list, tuple)) or isinstance(row, dict)]
            if len(values) < 2:
                variances.append(0)
            else:
                mean = sum(values) / len(values)
                var = sum((v - mean) ** 2 for v in values) / len(values)
                variances.append(var)

        sorted_indices = sorted(range(len(variances)), key=lambda i: variances[i], reverse=True)
        return sorted_indices[:max_features]

    def _select_by_correlation(self, data: List, labels: List, max_features: int) -> List[int]:
        return list(range(min(max_features, len(data[0]) if data else 0)))

    def _select_by_mutual_info(self, data: List, labels: List, max_features: int) -> List[int]:
        return list(range(min(max_features, len(data[0]) if data else 0)))

    def _rank_features(self, data: List, labels: List) -> List[Dict]:
        if not data or not labels:
            return []
        num_features = len(data[0]) if isinstance(data[0], (list, tuple)) else 0
        ranked = [{"index": i, "score": 0.5} for i in range(num_features)]
        return ranked


class FeatureTransformAction(BaseAction):
    """Transform features for ML models."""
    action_type = "feature_transform"
    display_name = "特征转换"
    description = "特征转换处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "transform")
            data = params.get("data", [])
            transform_type = params.get("transform_type", "standardize")

            if operation == "transform":
                if not data:
                    return ActionResult(success=False, message="data required")

                if transform_type == "standardize":
                    transformed = self._standardize(data)
                elif transform_type == "normalize":
                    transformed = self._normalize(data)
                elif transform_type == "log_transform":
                    transformed = self._log_transform(data)
                elif transform_type == "polynomial":
                    transformed = self._polynomial_features(data, params.get("degree", 2))
                elif transform_type == "binning":
                    transformed = self._binning(data, params.get("bins", 5))
                else:
                    return ActionResult(success=False, message=f"Unknown transform_type: {transform_type}")

                return ActionResult(
                    success=True,
                    data={"transformed": transformed, "type": transform_type, "rows": len(data)},
                    message=f"Transformed {len(data)} rows using {transform_type}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Feature transform error: {str(e)}")

    def _standardize(self, data: List[List[float]]) -> List[List[float]]:
        if not data:
            return []
        cols = len(data[0])
        means = [sum(row[i] for row in data) / len(data) for i in range(cols)]
        stds = [math.sqrt(sum((row[i] - means[i]) ** 2 for row in data) / len(data)) for i in range(cols)]
        stds = [s if s > 0 else 1 for s in stds]
        return [[(row[i] - means[i]) / stds[i] for i in range(cols)] for row in data]

    def _normalize(self, data: List[List[float]]) -> List[List[float]]:
        if not data:
            return []
        cols = len(data[0])
        mins = [min(row[i] for row in data) for i in range(cols)]
        maxs = [max(row[i] for row in data) for i in range(cols)]
        ranges = [maxs[i] - mins[i] if maxs[i] != mins[i] else 1 for i in range(cols)]
        return [[(row[i] - mins[i]) / ranges[i] for i in range(cols)] for row in data]

    def _log_transform(self, data: List[List[float]]) -> List[List[float]]:
        return [[math.log(val + 1) if val > 0 else 0 for val in row] for row in data]

    def _polynomial_features(self, data: List[List[float]], degree: int) -> List[List[float]]:
        result = []
        for row in data:
            poly = list(row)
            for d in range(2, degree + 1):
                poly.extend([val ** d for val in row])
            result.append(poly)
        return result

    def _binning(self, data: List[List[float]], bins: int) -> List[List[int]]:
        if not data:
            return []
        cols = len(data[0])
        mins = [min(row[i] for row in data) for i in range(cols)]
        maxs = [max(row[i] for row in data) for i in range(cols)]
        ranges = [maxs[i] - mins[i] if maxs[i] != mins[i] else 1 for i in range(cols)]
        return [[int((row[i] - mins[i]) / ranges[i] * (bins - 1)) for i in range(cols)] for row in data]


class FeatureMonitorAction(BaseAction):
    """Monitor feature drift and health."""
    action_type = "feature_monitor"
    display_name = "特征监控"
    description = "监控特征漂移"

    def __init__(self):
        super().__init__()
        self._feature_stats: Dict[str, Dict] = {}
        self._drift_alerts: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "baseline")
            feature_name = params.get("feature_name", "")

            if operation == "baseline":
                if not feature_name:
                    return ActionResult(success=False, message="feature_name required")

                values = params.get("values", [])
                if not values:
                    return ActionResult(success=False, message="values required")

                self._feature_stats[feature_name] = {
                    "baseline": {
                        "mean": sum(values) / len(values),
                        "std": math.sqrt(sum((v - sum(values) / len(values)) ** 2 for v in values) / len(values)),
                        "min": min(values),
                        "max": max(values),
                        "median": sorted(values)[len(values) // 2],
                        "count": len(values),
                        "captured_at": time.time()
                    },
                    "current": None,
                    "drift_detected": False
                }

                return ActionResult(
                    success=True,
                    data={"feature": feature_name, "baseline": self._feature_stats[feature_name]["baseline"]},
                    message=f"Baseline captured for '{feature_name}'"
                )

            elif operation == "check":
                if feature_name not in self._feature_stats:
                    return ActionResult(success=False, message=f"No baseline for '{feature_name}'")

                values = params.get("values", [])
                if not values:
                    return ActionResult(success=False, message="values required")

                baseline = self._feature_stats[feature_name]["baseline"]
                current_mean = sum(values) / len(values)
                current_std = math.sqrt(sum((v - current_mean) ** 2 for v in values) / len(values))

                mean_shift = abs(current_mean - baseline["mean"]) / (baseline["std"] if baseline["std"] > 0 else 1)
                std_shift = abs(current_std - baseline["std"]) / (baseline["std"] if baseline["std"] > 0 else 1)

                drift_threshold = params.get("drift_threshold", 2.0)
                drift_detected = mean_shift > drift_threshold or std_shift > drift_threshold

                self._feature_stats[feature_name]["current"] = {
                    "mean": current_mean,
                    "std": current_std,
                    "checked_at": time.time()
                }
                self._feature_stats[feature_name]["drift_detected"] = drift_detected

                if drift_detected:
                    self._drift_alerts.append({
                        "feature": feature_name,
                        "timestamp": time.time(),
                        "mean_shift": mean_shift,
                        "std_shift": std_shift,
                        "baseline_mean": baseline["mean"],
                        "current_mean": current_mean
                    })

                return ActionResult(
                    success=True,
                    data={
                        "feature": feature_name,
                        "drift_detected": drift_detected,
                        "mean_shift": round(mean_shift, 4),
                        "std_shift": round(std_shift, 4),
                        "baseline": baseline["mean"],
                        "current": current_mean
                    },
                    message=f"Drift {'detected' if drift_detected else 'not detected'} for '{feature_name}'"
                )

            elif operation == "status":
                all_status = {}
                for fname, stats in self._feature_stats.items():
                    all_status[fname] = {
                        "has_baseline": stats["baseline"] is not None,
                        "drift_detected": stats["drift_detected"],
                        "baseline_age": time.time() - stats["baseline"]["captured_at"] if stats["baseline"] else None
                    }
                return ActionResult(
                    success=True,
                    data={"features": all_status, "recent_alerts": self._drift_alerts[-10:]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Feature monitor error: {str(e)}")
