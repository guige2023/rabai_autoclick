"""Feature engineering action module for RabAI AutoClick.

Provides feature transforms: polynomial features, interaction terms,
binning, target encoding, and automated feature generation.
"""

from __future__ import annotations

import sys
import os
import math
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PolynomialFeaturesAction(BaseAction):
    """Generate polynomial and interaction feature terms.
    
    Creates degree-2 (or higher) polynomial features from numeric
    columns. Includes interaction terms and powers.
    
    Args:
        degree: Maximum polynomial degree (default 2)
        interaction_only: If True, exclude powers of individual features
        include_bias: If True, include constant term column
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        degree: int = 2,
        interaction_only: bool = False,
        include_bias: bool = False
    ) -> ActionResult:
        try:
            if degree < 2:
                return ActionResult(success=False, error="degree must be >= 2")

            # Extract numeric vectors
            vectors: List[List[float]] = []
            for row in data:
                try:
                    vec = [float(row[c]) for c in columns if c in row and row[c] is not None]
                    if len(vec) == len(columns):
                        vectors.append(vec)
                except (ValueError, TypeError):
                    continue

            if not vectors:
                return ActionResult(success=False, error="No valid numeric rows")

            result_rows: List[Dict[str, Any]] = []
            feature_names: List[str] = []

            if include_bias:
                feature_names.append("bias")

            for i, col in enumerate(columns):
                if not interaction_only:
                    for d in range(2, degree + 1):
                        feature_names.append(f"{col}_pow{d}")

            for i in range(len(columns)):
                for j in range(i, len(columns)):
                    if i != j:
                        feature_names.append(f"{columns[i]}_x_{columns[j]}")

            for vec in vectors:
                row_out: Dict[str, Any] = {}
                if include_bias:
                    row_out["bias"] = 1.0

                for d in range(2, degree + 1):
                    for i, col in enumerate(columns):
                        row_out[f"{col}_pow{d}"] = vec[i] ** d

                for i in range(len(columns)):
                    for j in range(i, len(columns)):
                        if i != j:
                            row_out[f"{columns[i]}_x_{columns[j]}"] = vec[i] * vec[j]

                result_rows.append(row_out)

            return ActionResult(success=True, data={
                "feature_names": feature_names,
                "n_features": len(feature_names),
                "n_samples": len(result_rows),
                "sample": result_rows[0] if result_rows else {}
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class InteractionFeaturesAction(BaseAction):
    """Generate pairwise interaction features between columns.
    
    Creates ratio, product, difference, and sum features for
    all numeric column pairs.
    
    Args:
        interaction_types: List of ["product", "ratio", "difference", "sum"]
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        interaction_types: Optional[List[str]] = None,
        min_ratio_denominator: float = 1e-8
    ) -> ActionResult:
        try:
            types = interaction_types or ["product", "ratio", "difference", "sum"]
            valid_types = {"product", "ratio", "difference", "sum"}
            for t in types:
                if t not in valid_types:
                    return ActionResult(success=False, error=f"Unknown type: {t}")

            vectors: List[Tuple[int, List[float]]] = []
            for idx, row in enumerate(data):
                try:
                    vec = [float(row[c]) for c in columns if c in row and row[c] is not None]
                    if len(vec) == len(columns):
                        vectors.append((idx, vec))
                except (ValueError, TypeError):
                    continue

            feature_names: List[str] = []
            for i in range(len(columns)):
                for j in range(i + 1, len(columns)):
                    for t in types:
                        feature_names.append(f"{columns[i]}_{t}_{columns[j]}")

            result_rows: List[Dict[str, Any]] = []
            for idx, vec in vectors:
                row_out: Dict[str, Any] = {"_idx": idx}
                for i in range(len(columns)):
                    for j in range(i + 1, len(columns)):
                        a, b = vec[i], vec[j]
                        if "product" in types:
                            row_out[f"{columns[i]}_product_{columns[j]}"] = a * b
                        if "ratio" in types:
                            denom = b if abs(b) > min_ratio_denominator else min_ratio_denominator * (1 if b >= 0 else -1)
                            row_out[f"{columns[i]}_ratio_{columns[j]}"] = a / denom
                        if "difference" in types:
                            row_out[f"{columns[i]}_difference_{columns[j]}"] = a - b
                        if "sum" in types:
                            row_out[f"{columns[i]}_sum_{columns[j]}"] = a + b
                result_rows.append(row_out)

            return ActionResult(success=True, data={
                "feature_names": feature_names,
                "n_features": len(feature_names),
                "n_samples": len(result_rows),
                "sample": result_rows[0] if result_rows else {}
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class TargetEncodingAction(BaseAction):
    """Target encoding for categorical features.
    
    Replaces categorical values with the mean of the target variable
    for that category. Supports smoothing (regularization).
    
    Args:
        smoothing: Smoothing factor (higher = more regularization toward global mean)
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        categorical_column: str,
        target_column: str,
        smoothing: float = 10.0
    ) -> ActionResult:
        try:
            # Extract valid (cat, target) pairs
            pairs: List[Tuple[str, float]] = []
            for row in data:
                if categorical_column in row and target_column in row:
                    try:
                        cat = str(row[categorical_column])
                        tgt = float(row[target_column])
                        pairs.append((cat, tgt))
                    except (ValueError, TypeError):
                        continue

            if not pairs:
                return ActionResult(success=False, error="No valid cat-target pairs")

            global_mean = sum(t for _, t in pairs) / len(pairs)

            # Group by category
            cat_targets: Dict[str, List[float]] = defaultdict(list)
            for cat, tgt in pairs:
                cat_targets[cat].append(tgt)

            # Compute smoothed means
            encoding_map: Dict[str, float] = {}
            for cat, targets in cat_targets.items():
                n = len(targets)
                cat_mean = sum(targets) / n
                # Smoothed encoding
                encoding_map[cat] = (n * cat_mean + smoothing * global_mean) / (n + smoothing)

            # Apply encoding
            result_rows = []
            for row in data:
                new_row = row.copy()
                cat_val = str(row.get(categorical_column, ""))
                new_row[f"{categorical_column}_encoded"] = encoding_map.get(cat_val, global_mean)
                result_rows.append(new_row)

            return ActionResult(success=True, data={
                "encoding_map": encoding_map,
                "global_mean": round(global_mean, 6),
                "n_categories": len(encoding_map),
                "n_encoded": len(result_rows)
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class QuantileBinningAction(BaseAction):
    """Bin numeric features into quantile-based buckets.
    
    Creates evenly-sized bins based on quantile boundaries.
    Useful for converting continuous features to categorical.
    
    Args:
        n_bins: Number of bins to create
        labels: Optional labels for each bin (e.g., ["Q1","Q2","Q3","Q4"])
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        n_bins: int = 4,
        labels: Optional[List[str]] = None,
        include_boundary: bool = False
    ) -> ActionResult:
        try:
            if n_bins < 2:
                return ActionResult(success=False, error="n_bins must be >= 2")

            values: List[float] = []
            for row in data:
                if column in row and row[column] is not None:
                    try:
                        values.append(float(row[column]))
                    except (ValueError, TypeError):
                        continue

            if len(values) < n_bins:
                return ActionResult(success=False, error="Not enough values for binning")

            sorted_vals = sorted(values)
            n = len(sorted_vals)

            # Compute quantile boundaries
            boundaries: List[float] = []
            for i in range(1, n_bins):
                idx = int(n * i / n_bins)
                boundaries.append(sorted_vals[idx])
            boundaries.sort()

            default_label = f"bin_0"
            bin_labels = labels or [f"bin_{i}" for i in range(n_bins)]

            def get_bin_idx(val: float) -> int:
                for i, boundary in enumerate(boundaries):
                    if val <= boundary:
                        return i
                return n_bins - 1

            result_rows = []
            for row in data:
                new_row = row.copy()
                if column in row and row[column] is not None:
                    try:
                        val = float(row[column])
                        idx = get_bin_idx(val)
                        new_row[f"{column}_bin"] = bin_labels[idx] if idx < len(bin_labels) else default_label
                        new_row[f"{column}_bin_idx"] = idx
                        if include_boundary:
                            new_row[f"{column}_boundary"] = boundaries[idx] if idx < len(boundaries) else sorted_vals[-1]
                    except (ValueError, TypeError):
                        new_row[f"{column}_bin"] = "invalid"
                        new_row[f"{column}_bin_idx"] = -1
                result_rows.append(new_row)

            return ActionResult(success=True, data={
                "n_bins": n_bins,
                "boundaries": [round(b, 4) for b in boundaries],
                "labels": bin_labels,
                "distribution": {
                    bin_labels[i]: sum(1 for row in result_rows if row.get(f"{column}_bin_idx") == i)
                    for i in range(n_bins)
                }
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class RollingFeatureAction(BaseAction):
    """Generate rolling window features on time series data.
    
    Creates lag features, rolling mean/std/min/max, and
    expanding window statistics.
    
    Args:
        window_size: Size of rolling window
        features: List of ["lag1","lag2","rolling_mean","rolling_std","rolling_min","rolling_max","expanding_mean"]
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        sort_key: Optional[str] = None,
        window_size: int = 3,
        features: Optional[List[str]] = None
    ) -> ActionResult:
        try:
            feat_list = features or ["lag1", "rolling_mean"]
            valid_feats = {"lag1", "lag2", "lag3", "rolling_mean", "rolling_std",
                           "rolling_min", "rolling_max", "expanding_mean", "expanding_std"}
            for f in feat_list:
                if f not in valid_feats:
                    return ActionResult(success=False, error=f"Unknown feature: {f}")

            # Sort data if sort_key provided
            work_data = sorted(data, key=lambda d: str(d.get(sort_key, ""))) if sort_key else data[:]

            values: List[Optional[float]] = []
            for row in work_data:
                if column in row and row[column] is not None:
                    try:
                        values.append(float(row[column]))
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    values.append(None)

            result_rows: List[Dict[str, Any]] = []
            for i, row in enumerate(work_data):
                new_row = row.copy()

                for f in feat_list:
                    if f == "lag1":
                        new_row[f"{column}_lag1"] = values[i - 1] if i >= 1 else None
                    elif f == "lag2":
                        new_row[f"{column}_lag2"] = values[i - 2] if i >= 2 else None
                    elif f == "lag3":
                        new_row[f"{column}_lag3"] = values[i - 3] if i >= 3 else None
                    elif f.startswith("rolling"):
                        wins = values[max(0, i - window_size + 1):i + 1]
                        wins = [w for w in wins if w is not None]
                        if wins:
                            if "mean" in f:
                                new_row[f"{column}_rolling_mean"] = sum(wins) / len(wins)
                            elif "std" in f:
                                mean = sum(wins) / len(wins)
                                var = sum((x - mean) ** 2 for x in wins) / len(wins)
                                new_row[f"{column}_rolling_std"] = math.sqrt(var) if var > 0 else 0.0
                            elif "min" in f:
                                new_row[f"{column}_rolling_min"] = min(wins)
                            elif "max" in f:
                                new_row[f"{column}_rolling_max"] = max(wins)
                    elif f.startswith("expanding"):
                        prior = values[:i]
                        prior = [v for v in prior if v is not None]
                        if prior:
                            if "mean" in f:
                                new_row[f"{column}_expanding_mean"] = sum(prior) / len(prior)
                            elif "std" in f:
                                mean = sum(prior) / len(prior)
                                var = sum((x - mean) ** 2 for x in prior) / len(prior)
                                new_row[f"{column}_expanding_std"] = math.sqrt(var) if var > 0 else 0.0

                result_rows.append(new_row)

            return ActionResult(success=True, data={
                "n_samples": len(result_rows),
                "window_size": window_size,
                "features_created": feat_list
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
