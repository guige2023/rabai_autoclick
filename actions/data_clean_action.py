"""Data cleaning action module for RabAI AutoClick.

Provides data cleaning operations:
- CleanStringsAction: Clean and normalize strings
- CleanNumbersAction: Clean numeric data
- CleanDatesAction: Parse and normalize dates
- CleanDuplicatesAction: Handle duplicate records
- CleanOutliersAction: Handle outliers
- CleanMissingAction: Handle missing values
"""

import re
import statistics
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CleanStringsAction(BaseAction):
    """Clean and normalize strings."""
    action_type = "clean_strings"
    display_name = "清理字符串"
    description = "清理和规范化字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            operations = params.get("operations", ["trim"])

            if not records:
                return ActionResult(success=False, message="records list is required")

            cleaned = []
            stats = {"total": len(records), "modified": 0}

            for record in records:
                if isinstance(record, dict):
                    if field and field in record:
                        original = str(record[field])
                        cleaned_val = original

                        for op in operations:
                            if op == "trim":
                                cleaned_val = cleaned_val.strip()
                            elif op == "lower":
                                cleaned_val = cleaned_val.lower()
                            elif op == "upper":
                                cleaned_val = cleaned_val.upper()
                            elif op == "title":
                                cleaned_val = cleaned_val.title()
                            elif op == "remove_extra_spaces":
                                cleaned_val = re.sub(r"\s+", " ", cleaned_val)
                            elif op == "remove_punctuation":
                                cleaned_val = re.sub(r"[^\w\s]", "", cleaned_val)
                            elif op == "remove_digits":
                                cleaned_val = re.sub(r"\d", "", cleaned_val)
                            elif op == "remove_special":
                                cleaned_val = re.sub(r"[^a-zA-Z0-9\s]", "", cleaned_val)
                            elif op == "normalize_whitespace":
                                cleaned_val = " ".join(cleaned_val.split())

                        if cleaned_val != original:
                            stats["modified"] += 1
                        record[field] = cleaned_val

                    cleaned.append(record)
                else:
                    cleaned.append(record)

            return ActionResult(
                success=True,
                message=f"Cleaned {stats['modified']}/{stats['total']} strings",
                data={"records": cleaned, "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean strings error: {str(e)}")


class CleanNumbersAction(BaseAction):
    """Clean numeric data."""
    action_type = "clean_numbers"
    display_name = "清理数字"
    description = "清理数值数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            min_val = params.get("min", None)
            max_val = params.get("max", None)
            default = params.get("default", None)
            round_digits = params.get("round_digits", None)

            if not records:
                return ActionResult(success=False, message="records list is required")

            cleaned = []
            stats = {"total": len(records), "converted": 0, "clamped": 0, "defaults": 0}

            for record in records:
                if isinstance(record, dict) and field in record:
                    original = record[field]

                    try:
                        if isinstance(original, str):
                            cleaned_val = float(original.replace(",", "").replace("$", "").replace("%", ""))
                            stats["converted"] += 1
                        else:
                            cleaned_val = float(original)

                        if round_digits is not None:
                            cleaned_val = round(cleaned_val, round_digits)

                        if min_val is not None and cleaned_val < min_val:
                            cleaned_val = min_val
                            stats["clamped"] += 1
                        elif max_val is not None and cleaned_val > max_val:
                            cleaned_val = max_val
                            stats["clamped"] += 1

                        record[field] = cleaned_val

                    except (TypeError, ValueError):
                        if default is not None:
                            record[field] = default
                            stats["defaults"] += 1
                        else:
                            record[field] = original

                    cleaned.append(record)
                else:
                    cleaned.append(record)

            return ActionResult(
                success=True,
                message=f"Cleaned numbers: {stats['converted']} converted, {stats['clamped']} clamped, {stats['defaults']} defaulted",
                data={"records": cleaned, "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean numbers error: {str(e)}")


class CleanDatesAction(BaseAction):
    """Parse and normalize dates."""
    action_type = "clean_dates"
    display_name = "清理日期"
    description = "解析和规范化日期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            input_formats = params.get("input_formats", ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"])
            output_format = params.get("output_format", "%Y-%m-%d")
            default = params.get("default", None)

            if not records:
                return ActionResult(success=False, message="records list is required")

            cleaned = []
            stats = {"total": len(records), "parsed": 0, "failed": 0}

            for record in records:
                if isinstance(record, dict) and field in record:
                    original = record[field]

                    parsed_date = None

                    if isinstance(original, datetime):
                        parsed_date = original
                    elif isinstance(original, str):
                        for fmt in input_formats:
                            try:
                                parsed_date = datetime.strptime(original.strip(), fmt)
                                break
                            except ValueError:
                                continue

                        try:
                            parsed_date = datetime.fromisoformat(original.replace("Z", "+00:00"))
                        except:
                            pass

                    if parsed_date:
                        record[field] = parsed_date.strftime(output_format)
                        stats["parsed"] += 1
                    else:
                        if default:
                            record[field] = default
                        stats["failed"] += 1

                    cleaned.append(record)
                else:
                    cleaned.append(record)

            return ActionResult(
                success=True,
                message=f"Parsed {stats['parsed']}/{stats['total']} dates",
                data={"records": cleaned, "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean dates error: {str(e)}")


class CleanDuplicatesAction(BaseAction):
    """Handle duplicate records."""
    action_type = "clean_duplicates"
    display_name = "清理重复"
    description = "处理重复记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            key_fields = params.get("key_fields", None)
            keep = params.get("keep", "first")
            mark_duplicates = params.get("mark_duplicates", False)

            if not records:
                return ActionResult(success=False, message="records list is required")

            seen = {}
            result = []
            duplicates = []
            stats = {"total": len(records), "duplicates_found": 0}

            for record in records:
                if not isinstance(record, dict):
                    result.append(record)
                    continue

                if key_fields:
                    if isinstance(key_fields, str):
                        key_fields = [key_fields]
                    key = tuple(record.get(f) for f in key_fields)
                else:
                    key = str(record)

                if key not in seen:
                    seen[key] = len(result)
                    result.append(record)
                    if mark_duplicates:
                        record["_is_duplicate"] = False
                else:
                    stats["duplicates_found"] += 1
                    if keep == "last":
                        old_record = result[seen[key]]
                        result[seen[key]] = record
                        if mark_duplicates:
                            old_record["_is_duplicate"] = True
                    if mark_duplicates:
                        record["_is_duplicate"] = True
                    duplicates.append(record)

            return ActionResult(
                success=True,
                message=f"Found {stats['duplicates_found']} duplicates, {len(result)} unique records",
                data={"records": result, "duplicates": duplicates if mark_duplicates else [], "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean duplicates error: {str(e)}")


class CleanOutliersAction(BaseAction):
    """Handle outliers."""
    action_type = "clean_outliers"
    display_name = "清理异常值"
    description = "处理异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)
            action = params.get("action", "mark")
            replacement = params.get("replacement", None)

            if not records:
                return ActionResult(success=False, message="records list is required")

            values = []
            for record in records:
                if isinstance(record, dict) and field in record:
                    try:
                        values.append(float(record[field]))
                    except:
                        pass

            if len(values) < 4:
                return ActionResult(success=True, message="Not enough data for outlier detection", data={"records": records})

            if method == "iqr":
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                q1 = sorted_vals[n // 4]
                q3 = sorted_vals[3 * n // 4]
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                bounds = (lower, upper)

            elif method == "zscore":
                mean = statistics.mean(values)
                stdev = statistics.stdev(values) if len(values) > 1 else 1
                z_threshold = threshold
                bounds = None

            elif method == "percentile":
                sorted_vals = sorted(values)
                lower_p = threshold * 100 / 2
                upper_p = 100 - lower_p
                lower = sorted_vals[int(len(sorted_vals) * lower_p / 100)]
                upper = sorted_vals[int(len(sorted_vals) * upper_p / 100)]
                bounds = (lower, upper)

            else:
                bounds = None

            result = []
            outliers = []
            stats = {"total": len(records), "outliers_found": 0}

            for record in records:
                if isinstance(record, dict) and field in record:
                    try:
                        val = float(record[field])
                    except:
                        result.append(record)
                        continue

                    is_outlier = False

                    if method == "zscore":
                        zscore = abs((val - mean) / stdev) if stdev > 0 else 0
                        is_outlier = zscore > z_threshold
                    else:
                        is_outlier = val < lower or val > upper

                    if is_outlier:
                        stats["outliers_found"] += 1
                        outliers.append(record)

                        if action == "remove":
                            continue
                        elif action == "replace" and replacement is not None:
                            record[field] = replacement
                        elif action == "cap":
                            if val < lower:
                                record[field] = lower
                            else:
                                record[field] = upper
                        elif action == "mark":
                            record["_is_outlier"] = True

                result.append(record)

            return ActionResult(
                success=True,
                message=f"Found {stats['outliers_found']} outliers using {method}",
                data={"records": result, "outliers": outliers if action == "mark" else [], "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean outliers error: {str(e)}")


class CleanMissingAction(BaseAction):
    """Handle missing values."""
    action_type = "clean_missing"
    display_name = "清理缺失值"
    description = "处理缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            strategy = params.get("strategy", "remove")
            column = params.get("column", None)
            constant_value = params.get("constant_value", 0)
            threshold = params.get("threshold", 0.5)

            if not records:
                return ActionResult(success=False, message="records list is required")

            def is_missing(val):
                if val is None:
                    return True
                if isinstance(val, str) and val.strip() == "":
                    return True
                if isinstance(val, float) and math.isnan(val):
                    return True
                return False

            stats = {"total": len(records), "missing_handled": 0}

            if strategy == "remove":
                if column:
                    result = [r for r in records if not is_missing(r.get(column)) if isinstance(r, dict)]
                    stats["missing_handled"] = len(records) - len(result)
                else:
                    threshold_count = int(len(records[0]) * threshold) if records and isinstance(records[0], dict) else 0
                    result = []
                    for record in records:
                        if isinstance(record, dict):
                            missing_count = sum(1 for v in record.values() if is_missing(v))
                            if missing_count <= threshold_count:
                                result.append(record)
                        else:
                            result.append(record)
                    stats["missing_handled"] = len(records) - len(result)

            elif strategy == "fill":
                if column:
                    values = [r[column] for r in records if isinstance(r, dict) and not is_missing(r.get(column))]
                    fill_value = constant_value

                    if values:
                        if isinstance(values[0], (int, float)):
                            fill_choice = params.get("fill_method", "mean")
                            if fill_choice == "mean":
                                fill_value = sum(values) / len(values)
                            elif fill_choice == "median":
                                fill_value = statistics.median(values)
                            elif fill_choice == "mode":
                                from collections import Counter
                                fill_value = Counter(values).most_common(1)[0][0]

                    for record in records:
                        if isinstance(record, dict) and is_missing(record.get(column)):
                            record[column] = fill_value
                            stats["missing_handled"] += 1

                    result = records
                else:
                    result = records

            else:
                result = records

            return ActionResult(
                success=True,
                message=f"Handled {stats['missing_handled']} missing values",
                data={"records": result, "stats": stats}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clean missing error: {str(e)}")
