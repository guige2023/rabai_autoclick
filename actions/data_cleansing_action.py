"""Data cleansing action module for RabAI AutoClick.

Provides data cleaning operations: missing value imputation,
outlier handling, duplicate detection, and consistency normalization.
"""

from __future__ import annotations

import sys
import os
import re
import math
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MissingValueImputerAction(BaseAction):
    """Impute missing values with various strategies.
    
    Supports mean, median, mode, forward-fill, backward-fill,
    constant, and KNN-like imputation strategies.
    
    Args:
        strategy: Imputation strategy
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        strategy: str = "mean",
        constant_value: Optional[Union[int, float, str]] = None,
        group_by: Optional[str] = None
    ) -> ActionResult:
        try:
            # Extract non-null values
            if group_by:
                groups: Dict[Any, List[float]] = {}
                for row in data:
                    if column in row and row[column] is not None:
                        try:
                            grp = row.get(group_by)
                            if grp not in groups:
                                groups[grp] = []
                            groups[grp].append(float(row[column]))
                        except (ValueError, TypeError):
                            continue

                result = []
                for row in data:
                    new_row = row.copy()
                    if column not in row or row[column] is None:
                        grp = row.get(group_by)
                        if grp in groups and groups[grp]:
                            vals = groups[grp]
                            if strategy == "mean":
                                fill_val = sum(vals) / len(vals)
                            elif strategy == "median":
                                s = sorted(vals)
                                mid = len(s) // 2
                                fill_val = s[mid]
                            elif strategy == "mode":
                                fill_val = Counter(vals).most_common(1)[0][0]
                            else:
                                fill_val = constant_value
                        else:
                            fill_val = constant_value
                        new_row[column] = fill_val
                    result.append(new_row)
            else:
                valid = []
                for row in data:
                    if column in row and row[column] is not None:
                        try:
                            valid.append(float(row[column]))
                        except (ValueError, TypeError):
                            continue

                if not valid:
                    return ActionResult(success=False, error="No valid values to compute imputation")

                if strategy == "mean":
                    fill_val = sum(valid) / len(valid)
                elif strategy == "median":
                    s = sorted(valid)
                    mid = len(s) // 2
                    fill_val = s[mid]
                elif strategy == "mode":
                    fill_val = Counter(valid).most_common(1)[0][0]
                elif strategy == "forward_fill":
                    fill_val = None
                elif strategy == "backward_fill":
                    fill_val = None
                elif strategy == "constant":
                    if constant_value is None:
                        return ActionResult(success=False, error="constant_value required")
                    fill_val = constant_value
                else:
                    return ActionResult(success=False, error=f"Unknown strategy: {strategy}")

                result = []
                prev_val = fill_val
                for row in data:
                    new_row = row.copy()
                    if column not in row or row[column] is None:
                        if strategy == "forward_fill":
                            new_row[column] = prev_val
                        elif strategy == "backward_fill":
                            new_row[column] = prev_val
                        else:
                            new_row[column] = fill_val
                    else:
                        try:
                            prev_val = float(row[column])
                        except (ValueError, TypeError):
                            pass
                    result.append(new_row)

            imputed_count = sum(1 for r in result if column in r and r[column] is None)

            return ActionResult(success=True, data={
                "strategy": strategy,
                "imputed_count": imputed_count,
                "fill_value": fill_val,
                "n_rows": len(result)
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class OutlierHandlerAction(BaseAction):
    """Detect and handle outliers using z-score and IQR methods.
    
    Options: cap (winsorize), remove, flag, or transform.
    
    Args:
        method: zscore or iqr
        threshold: Z-score or IQR multiplier threshold
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        method: str = "zscore",
        threshold: float = 3.0,
        handling: str = "flag",  # flag, remove, cap, transform
        lower_cap: Optional[float] = None,
        upper_cap: Optional[float] = None
    ) -> ActionResult:
        try:
            # Extract values
            indices_values: List[Tuple[int, float]] = []
            for idx, row in enumerate(data):
                if column in row and row[column] is not None:
                    try:
                        indices_values.append((idx, float(row[column])))
                    except (ValueError, TypeError):
                        continue

            if len(indices_values) < 4:
                return ActionResult(success=False, error="Need at least 4 values")

            values = [v for _, v in indices_values]

            if method == "zscore":
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = math.sqrt(variance) if variance > 0 else 0.0

                if std == 0:
                    return ActionResult(success=False, error="Zero standard deviation")

                outliers = []
                for idx, v in indices_values:
                    z = abs((v - mean) / std)
                    if z > threshold:
                        outliers.append({"index": idx, "value": v, "z_score": round(z, 4)})

            elif method == "iqr":
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                q1_idx = n // 4
                q3_idx = 3 * n // 4
                q1 = sorted_vals[q1_idx]
                q3 = sorted_vals[q3_idx]
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr

                outliers = []
                for idx, v in indices_values:
                    if v < lower or v > upper:
                        outliers.append({"index": idx, "value": v,
                                        "lower_bound": round(lower, 4), "upper_bound": round(upper, 4)})
            else:
                return ActionResult(success=False, error=f"Unknown method: {method}")

            outlier_indices = {o["index"] for o in outliers}
            result = []
            for idx, row in enumerate(data):
                new_row = row.copy()
                if idx in outlier_indices and handling == "remove":
                    continue
                if idx in outlier_indices and handling == "flag":
                    new_row[f"{column}_outlier"] = True
                if idx in outlier_indices and handling == "cap":
                    if lower_cap is not None and upper_cap is not None:
                        new_row[column] = max(lower_cap, min(upper_cap, row.get(column)))
                result.append(new_row)

            return ActionResult(success=True, data={
                "method": method,
                "handling": handling,
                "outlier_count": len(outliers),
                "outlier_indices": list(outlier_indices)[:20],
                "outlier_rate": round(len(outliers) / len(indices_values), 4)
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class DataConsistencyAction(BaseAction):
    """Enforce consistency across data: referential integrity,
    type consistency, format normalization.
    
    Args:
        strict_types: Enforce column type consistency
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        expected_type: str,  # int, float, str, bool
        normalize_formats: Optional[Dict[str, str]] = None
    ) -> ActionResult:
        try:
            violations = []
            type_violations = []
            format_violations = []

            expected_types = {
                "int": int, "float": (int, float), "str": str,
                "bool": bool
            }
            expected = expected_types.get(expected_type, str)

            if normalize_formats and column in normalize_formats:
                fmt = normalize_formats[column]
                if fmt == "phone":
                    pattern = re.compile(r'[^\d]')
                elif fmt == "email":
                    pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
                elif fmt == "date":
                    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
                else:
                    pattern = None
            else:
                pattern = None

            for idx, row in enumerate(data):
                if column not in row:
                    violations.append({"index": idx, "issue": "missing"})
                    continue

                val = row[column]
                if val is None:
                    violations.append({"index": idx, "issue": "null"})
                    continue

                # Type check
                if expected_type == "int":
                    try:
                        float(val)
                        if isinstance(val, bool):
                            raise ValueError
                    except (ValueError, TypeError):
                        type_violations.append({"index": idx, "value": val})

                # Format check
                if pattern and isinstance(val, str):
                    if not pattern.match(val):
                        format_violations.append({"index": idx, "value": val})

            return ActionResult(success=True, data={
                "column": column,
                "total_violations": len(violations),
                "type_violations": len(type_violations),
                "format_violations": len(format_violations),
                "type_violation_samples": type_violations[:5],
                "format_violation_samples": format_violations[:5],
                "consistency_score": round(
                    max(0, 1 - (len(violations) + len(type_violations) + len(format_violations)) / max(1, len(data))), 4
                )
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class StringNormalizerAction(BaseAction):
    """Normalize string values: whitespace, case, punctuation, encoding.
    
    Args:
        lowercase: Convert to lowercase
        trim: Remove leading/trailing whitespace
        remove_punctuation: Strip punctuation
        normalize_unicode: NFKC normalization
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        column: str,
        lowercase: bool = True,
        trim: bool = True,
        remove_punctuation: bool = False,
        normalize_unicode: bool = False,
        strip_html: bool = False
    ) -> ActionResult:
        try:
            import unicodedata
            import html

            result = []
            stats = {"total": 0, "changed": 0, "empty_after": 0}

            for row in data:
                new_row = row.copy()
                if column in row and row[column] is not None:
                    stats["total"] += 1
                    val = str(row[column])

                    original = val

                    if strip_html:
                        val = html.unescape(val)
                        val = re.sub(r'<[^>]+>', '', val)

                    if normalize_unicode:
                        val = unicodedata.normalize('NFKC', val)

                    if trim:
                        val = val.strip()

                    if lowercase:
                        val = val.lower()

                    if remove_punctuation:
                        val = re.sub(r'[^\w\s]', '', val)

                    # Collapse whitespace
                    val = re.sub(r'\s+', ' ', val)

                    if val != original:
                        stats["changed"] += 1
                    if not val:
                        stats["empty_after"] += 1

                    new_row[column] = val
                result.append(new_row)

            return ActionResult(success=True, data={
                "column": column,
                "total_strings": stats["total"],
                "changed": stats["changed"],
                "empty_after": stats["empty_after"],
                "change_rate": round(stats["changed"] / max(1, stats["total"]), 4)
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
