"""Data transformation action module for RabAI AutoClick.

Provides data transformation operations:
- NormalizeAction: Normalize numeric data
- EncodeCategoricalAction: Encode categorical variables
- ImputeMissingAction: Impute missing values
- ReshapeAction: Reshape data structures
- GroupAggregateAction: Group and aggregate data
- DeduplicateAction: Remove duplicate records
"""

import statistics
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NormalizeAction(BaseAction):
    """Normalize numeric data."""
    action_type = "normalize"
    display_name = "数据归一化"
    description = "归一化数值数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            values = params.get("values", [])
            method = params.get("method", "minmax")

            if not values:
                return ActionResult(success=False, message="values list is required")

            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (TypeError, ValueError):
                    pass

            if not numeric_values:
                return ActionResult(success=False, message="No numeric values found")

            if method == "minmax":
                min_val = min(numeric_values)
                max_val = max(numeric_values)
                if max_val == min_val:
                    normalized = [0.5] * len(numeric_values)
                else:
                    normalized = [(v - min_val) / (max_val - min_val) for v in numeric_values]

            elif method == "zscore":
                mean = statistics.mean(numeric_values)
                stdev = statistics.stdev(numeric_values) if len(numeric_values) > 1 else 1
                if stdev == 0:
                    normalized = [0.0] * len(numeric_values)
                else:
                    normalized = [(v - mean) / stdev for v in numeric_values]

            elif method == "robust":
                sorted_vals = sorted(numeric_values)
                median = statistics.median(numeric_values)
                q1 = sorted_vals[len(sorted_vals) // 4]
                q3 = sorted_vals[3 * len(sorted_vals) // 4]
                iqr = q3 - q1
                if iqr == 0:
                    normalized = [0.0] * len(numeric_values)
                else:
                    normalized = [(v - median) / iqr for v in numeric_values]

            elif method == "log":
                import math
                normalized = [math.log(v + 1) for v in numeric_values]

            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            return ActionResult(
                success=True,
                message=f"Normalized {len(normalized)} values using {method}",
                data={"normalized": normalized, "method": method, "count": len(normalized)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Normalize error: {str(e)}")


class EncodeCategoricalAction(BaseAction):
    """Encode categorical variables."""
    action_type = "encode_categorical"
    display_name = "类别编码"
    description = "编码分类变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            values = params.get("values", [])
            encoding_type = params.get("encoding_type", "label")
            categories = params.get("categories", None)

            if not values:
                return ActionResult(success=False, message="values list is required")

            if encoding_type == "label":
                if categories is None:
                    unique_values = list(dict.fromkeys(values))
                else:
                    unique_values = categories

                label_map = {v: i for i, v in enumerate(unique_values)}
                encoded = [label_map.get(v, -1) for v in values]

                return ActionResult(
                    success=True,
                    message=f"Label encoded {len(encoded)} values",
                    data={"encoded": encoded, "label_map": label_map}
                )

            elif encoding_type == "onehot":
                if categories is None:
                    unique_values = list(dict.fromkeys(values))
                else:
                    unique_values = categories

                onehot_map = {v: i for i, v in enumerate(unique_values)}
                encoded = []
                for v in values:
                    row = [0] * len(unique_values)
                    idx = onehot_map.get(v, -1)
                    if idx >= 0:
                        row[idx] = 1
                    encoded.append(row)

                return ActionResult(
                    success=True,
                    message=f"One-hot encoded {len(encoded)} values",
                    data={"encoded": encoded, "categories": unique_values}
                )

            elif encoding_type == "ordinal":
                if categories is None:
                    return ActionResult(success=False, message="categories required for ordinal encoding")

                ordinal_map = {v: i for i, v in enumerate(categories)}
                encoded = [ordinal_map.get(v, -1) for v in values]

                return ActionResult(
                    success=True,
                    message=f"Ordinal encoded {len(encoded)} values",
                    data={"encoded": encoded, "ordinal_map": ordinal_map}
                )

            elif encoding_type == "count":
                from collections import Counter
                counts = Counter(values)
                encoded = [counts[v] for v in values]

                return ActionResult(
                    success=True,
                    message=f"Count encoded {len(encoded)} values",
                    data={"encoded": encoded, "count_map": dict(counts)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown encoding type: {encoding_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Encode error: {str(e)}")


class ImputeMissingAction(BaseAction):
    """Impute missing values."""
    action_type = "impute_missing"
    display_name = "缺失值填充"
    description = "填充数据中的缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            column = params.get("column", "")
            method = params.get("method", "mean")
            constant_value = params.get("constant_value", 0)

            if not records:
                return ActionResult(success=False, message="records list is required")

            if not column and isinstance(records[0], dict):
                return ActionResult(success=False, message="column name required for dict records")

            values = []
            indices = []

            for i, record in enumerate(records):
                if isinstance(record, dict):
                    val = record.get(column)
                elif isinstance(record, (list, tuple)):
                    val = record
                else:
                    val = record

                if val is None or val == "" or (isinstance(val, float) and val != val):
                    indices.append(i)
                else:
                    try:
                        values.append(float(val))
                    except (TypeError, ValueError):
                        indices.append(i)

            fill_value = constant_value

            if method == "mean" and values:
                fill_value = sum(values) / len(values)
            elif method == "median" and values:
                fill_value = statistics.median(values)
            elif method == "mode" and values:
                from collections import Counter
                fill_value = Counter(values).most_common(1)[0][0]
            elif method == "forward":
                last_val = None
                for i, record in enumerate(records):
                    if isinstance(record, dict):
                        val = record.get(column)
                    else:
                        val = record
                    if val is not None and val != "":
                        last_val = val
                    elif i in indices and last_val is not None:
                        indices.remove(i)
                        if isinstance(record, dict):
                            record[column] = last_val
                return ActionResult(
                    success=True,
                    message=f"Forward filled {len(indices)} missing values",
                    data={"records": records, "filled_count": len(indices)}
                )
            elif method == "backward":
                next_val = None
                for i in reversed(range(len(records))):
                    record = records[i]
                    if isinstance(record, dict):
                        val = record.get(column)
                    else:
                        val = record
                    if val is not None and val != "":
                        next_val = val
                    elif i in indices and next_val is not None:
                        indices.remove(i)
                        if isinstance(record, dict):
                            record[column] = next_val
                return ActionResult(
                    success=True,
                    message=f"Backward filled {len(indices)} missing values",
                    data={"records": records, "filled_count": len(indices)}
                )

            for i in indices:
                record = records[i]
                if isinstance(record, dict):
                    record[column] = fill_value

            return ActionResult(
                success=True,
                message=f"Imputed {len(indices)} missing values with {method}",
                data={"records": records, "filled_count": len(indices), "fill_value": fill_value}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Impute error: {str(e)}")


class ReshapeAction(BaseAction):
    """Reshape data structures."""
    action_type = "reshape"
    display_name = "数据重塑"
    description = "重塑数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            from_shape = params.get("from_shape", "flat")
            to_shape = params.get("to_shape", "nested")

            if not data:
                return ActionResult(success=False, message="data list is required")

            if from_shape == "flat" and to_shape == "nested":
                group_by = params.get("group_by", "")
                if not group_by:
                    return ActionResult(success=False, message="group_by required")

                grouped = {}
                for record in data:
                    if isinstance(record, dict):
                        key = record.get(group_by, "unknown")
                        if key not in grouped:
                            grouped[key] = []
                        grouped[key].append(record)

                return ActionResult(
                    success=True,
                    message=f"Reshaped to {len(grouped)} groups",
                    data={"reshaped": grouped, "group_count": len(grouped)}
                )

            elif from_shape == "nested" and to_shape == "flat":
                result = []
                for key, values in data.items() if isinstance(data, dict) else enumerate(data):
                    for item in values:
                        if isinstance(item, dict):
                            item_copy = dict(item)
                            item_copy[params.get("key_field", "group")] = key
                            result.append(item_copy)
                        else:
                            result.append({"value": item, "group": key})

                return ActionResult(
                    success=True,
                    message=f"Reshaped to {len(result)} flat records",
                    data={"reshaped": result, "row_count": len(result)}
                )

            elif from_shape == "wide" and to_shape == "long":
                id_cols = params.get("id_cols", [])
                value_cols = params.get("value_cols", [])
                var_name = params.get("var_name", "variable")
                val_name = params.get("val_name", "value")

                if not id_cols or not value_cols:
                    return ActionResult(success=False, message="id_cols and value_cols required")

                result = []
                for record in data:
                    if not isinstance(record, dict):
                        continue
                    base = {col: record.get(col) for col in id_cols}
                    for vcol in value_cols:
                        row = dict(base)
                        row[var_name] = vcol
                        row[val_name] = record.get(vcol)
                        result.append(row)

                return ActionResult(
                    success=True,
                    message=f"Reshaped from wide to long: {len(result)} rows",
                    data={"reshaped": result, "row_count": len(result)}
                )

            else:
                return ActionResult(success=False, message=f"Unsupported reshape: {from_shape} -> {to_shape}")

        except Exception as e:
            return ActionResult(success=False, message=f"Reshape error: {str(e)}")


class GroupAggregateAction(BaseAction):
    """Group and aggregate data."""
    action_type = "group_aggregate"
    display_name = "分组聚合"
    description = "分组并聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", {})

            if not records:
                return ActionResult(success=False, message="records list is required")

            if isinstance(group_by, str):
                group_by = [group_by]

            if not group_by or not aggregations:
                return ActionResult(success=False, message="group_by and aggregations required")

            groups = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = tuple(record.get(col, None) for col in group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)

            results = []
            for key, group_records in groups.items():
                result = dict(zip(group_by, key))
                for field, agg_func in aggregations.items():
                    values = []
                    for record in group_records:
                        val = record.get(field)
                        if val is not None:
                            try:
                                values.append(float(val))
                            except (TypeError, ValueError):
                                pass

                    if agg_func == "sum":
                        result[f"{field}_sum"] = sum(values) if values else 0
                    elif agg_func == "avg" or agg_func == "mean":
                        result[f"{field}_mean"] = sum(values) / len(values) if values else 0
                    elif agg_func == "count":
                        result[f"{field}_count"] = len(values)
                    elif agg_func == "min":
                        result[f"{field}_min"] = min(values) if values else None
                    elif agg_func == "max":
                        result[f"{field}_max"] = max(values) if values else None
                    elif agg_func == "first":
                        result[f"{field}_first"] = values[0] if values else None
                    elif agg_func == "last":
                        result[f"{field}_last"] = values[-1] if values else None

                results.append(result)

            return ActionResult(
                success=True,
                message=f"Aggregated into {len(results)} groups",
                data={"results": results, "group_count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group aggregate error: {str(e)}")


class DeduplicateAction(BaseAction):
    """Remove duplicate records."""
    action_type = "deduplicate"
    display_name = "去重"
    description = "删除重复记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            key_fields = params.get("key_fields", None)
            keep = params.get("keep", "first")

            if not records:
                return ActionResult(success=False, message="records list is required")

            if key_fields:
                if isinstance(key_fields, str):
                    key_fields = [key_fields]

                seen = {}
                result = []
                duplicates_removed = 0

                for record in records:
                    if not isinstance(record, dict):
                        continue
                    key = tuple(record.get(f) for f in key_fields)

                    if key not in seen:
                        seen[key] = len(result)
                        result.append(record)
                    elif keep == "last":
                        result[seen[key]] = record
                    else:
                        duplicates_removed += 1

                return ActionResult(
                    success=True,
                    message=f"Removed {duplicates_removed} duplicates",
                    data={"records": result, "duplicates_removed": duplicates_removed, "original_count": len(records)}
                )

            else:
                seen = set()
                result = []
                duplicates_removed = 0

                for record in records:
                    key = str(record)
                    if key not in seen:
                        seen.add(key)
                        result.append(record)
                    else:
                        duplicates_removed += 1

                return ActionResult(
                    success=True,
                    message=f"Removed {duplicates_removed} duplicates",
                    data={"records": result, "duplicates_removed": duplicates_removed, "original_count": len(records)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Deduplicate error: {str(e)}")
