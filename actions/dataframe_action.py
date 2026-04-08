"""DataFrame action module for RabAI AutoClick.

Provides DataFrame operations using pandas:
- DataFrameLoadAction: Load data into DataFrame
- DataFrameTransformAction: Transform DataFrame
- DataFrameAggregateAction: Aggregate DataFrame data
- DataFrameMergeAction: Merge multiple DataFrames
"""

import json
import io
from typing import Any, Dict, List, Optional, Union


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class DataFrameLoadAction(BaseAction):
    """Load data into a pandas DataFrame."""
    action_type = "dataframe_load"
    display_name = "DataFrame加载"
    description = "将数据加载到pandas DataFrame"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            if not PANDAS_AVAILABLE:
                return ActionResult(success=False, message="pandas is not installed")

            source_type = params.get("source_type", "json")
            data = params.get("data", None)
            filepath = params.get("filepath", None)
            columns = params.get("columns", None)
            index_col = params.get("index_col", None)

            df = None

            if source_type == "json":
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, str):
                    df = pd.read_json(io.StringIO(data))
                elif filepath:
                    df = pd.read_json(filepath)
            elif source_type == "csv":
                if filepath:
                    df = pd.read_csv(filepath, index_col=index_col)
                elif isinstance(data, str):
                    df = pd.read_csv(io.StringIO(data))
            elif source_type == "dict":
                if isinstance(data, dict):
                    df = pd.DataFrame(data)
            elif source_type == "list":
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                    if columns:
                        df.columns = columns

            if df is None:
                return ActionResult(success=False, message="Could not load data")

            return ActionResult(
                success=True,
                message=f"Loaded DataFrame with {len(df)} rows, {len(df.columns)} columns",
                data={
                    "shape": df.shape,
                    "columns": list(df.columns),
                    "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                    "head": df.head(5).to_dict(orient="records")
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"DataFrame load failed: {str(e)}")


class DataFrameTransformAction(BaseAction):
    """Transform a pandas DataFrame."""
    action_type = "dataframe_transform"
    display_name = "DataFrame转换"
    description = "转换pandas DataFrame"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            if not PANDAS_AVAILABLE:
                return ActionResult(success=False, message="pandas is not installed")

            data = params.get("data", [])
            operations = params.get("operations", [])
            columns_rename = params.get("columns_rename", {})
            drop_columns = params.get("drop_columns", [])
            fill_na = params.get("fill_na", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            df = pd.DataFrame(data)

            for op in operations:
                op_type = op.get("type", "")

                if op_type == "filter":
                    column = op.get("column", "")
                    operator = op.get("operator", "==")
                    value = op.get("value", None)
                    if column in df.columns:
                        if operator == "==":
                            df = df[df[column] == value]
                        elif operator == "!=":
                            df = df[df[column] != value]
                        elif operator == ">":
                            df = df[df[column] > value]
                        elif operator == "<":
                            df = df[df[column] < value]
                        elif operator == ">=":
                            df = df[df[column] >= value]
                        elif operator == "<=":
                            df = df[df[column] <= value]
                        elif operator == "contains":
                            df = df[df[column].astype(str).str.contains(str(value), na=False)]

                elif op_type == "sort":
                    by = op.get("by", "")
                    ascending = op.get("ascending", True)
                    if by in df.columns:
                        df = df.sort_values(by=by, ascending=ascending)

                elif op_type == "groupby":
                    by = op.get("by", [])
                    agg_func = op.get("agg_func", "sum")
                    result_col = op.get("result_column", "result")
                    if all(c in df.columns for c in by):
                        df = df.groupby(by)[by[0]].agg(agg_func).reset_index()

            if columns_rename:
                df = df.rename(columns=columns_rename)

            if drop_columns:
                existing_drops = [c for c in drop_columns if c in df.columns]
                if existing_drops:
                    df = df.drop(columns=existing_drops)

            if fill_na is not None:
                df = df.fillna(fill_na)

            return ActionResult(
                success=True,
                message=f"Transformed DataFrame: {len(df)} rows, {len(df.columns)} columns",
                data={
                    "shape": df.shape,
                    "columns": list(df.columns),
                    "transformed_data": df.to_dict(orient="records")
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"DataFrame transform failed: {str(e)}")


class DataFrameAggregateAction(BaseAction):
    """Aggregate DataFrame data."""
    action_type = "dataframe_aggregate"
    display_name = "DataFrame聚合"
    description = "聚合DataFrame数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            if not PANDAS_AVAILABLE:
                return ActionResult(success=False, message="pandas is not installed")

            data = params.get("data", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", [])
            having = params.get("having", None)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_by:
                return ActionResult(success=False, message="group_by is required")
            if not aggregations:
                return ActionResult(success=False, message="aggregations is required")

            df = pd.DataFrame(data)

            if not all(c in df.columns for c in group_by):
                return ActionResult(success=False, message="Some group_by columns not found in data")

            agg_dict = {}
            for agg in aggregations:
                column = agg.get("column", "")
                func = agg.get("func", "sum")
                alias = agg.get("alias", f"{column}_{func}")
                if column in df.columns:
                    agg_dict[column] = func

            if not agg_dict:
                return ActionResult(success=False, message="No valid aggregations found")

            result = df.groupby(group_by).agg(agg_dict).reset_index()
            result.columns = group_by + [agg.get("alias", f"{agg['column']}_{agg['func']}") for agg in aggregations if agg["column"] in df.columns]

            if having:
                op = having.get("operator", " > ")
                value = having.get("value", 0)
                col = having.get("column", result.columns[-1])
                if col in result.columns:
                    if op == ">":
                        result = result[result[col] > value]
                    elif op == ">=":
                        result = result[result[col] >= value]
                    elif op == "<":
                        result = result[result[col] < value]
                    elif op == "<=":
                        result = result[result[col] <= value]

            return ActionResult(
                success=True,
                message=f"Aggregated into {len(result)} groups",
                data={
                    "shape": result.shape,
                    "columns": list(result.columns),
                    "aggregated_data": result.to_dict(orient="records")
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"DataFrame aggregate failed: {str(e)}")


class DataFrameMergeAction(BaseAction):
    """Merge multiple DataFrames."""
    action_type = "dataframe_merge"
    display_name = "DataFrame合并"
    description = "合并多个DataFrame"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            if not PANDAS_AVAILABLE:
                return ActionResult(success=False, message="pandas is not installed")

            left_data = params.get("left_data", [])
            right_data = params.get("right_data", [])
            left_on = params.get("left_on", "")
            right_on = params.get("right_on", "")
            how = params.get("how", "inner")
            suffixes = params.get("suffixes", ["_left", "_right"])

            if not left_data or not right_data:
                return ActionResult(success=False, message="left_data and right_data are required")
            if not left_on or not right_on:
                return ActionResult(success=False, message="left_on and right_on are required")

            left_df = pd.DataFrame(left_data)
            right_df = pd.DataFrame(right_data)

            if left_on not in left_df.columns:
                return ActionResult(success=False, message=f"Column '{left_on}' not in left data")
            if right_on not in right_df.columns:
                return ActionResult(success=False, message=f"Column '{right_on}' not in right data")

            merged = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes)

            return ActionResult(
                success=True,
                message=f"Merged DataFrame: {len(merged)} rows",
                data={
                    "shape": merged.shape,
                    "columns": list(merged.columns),
                    "merged_data": merged.to_dict(orient="records")
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"DataFrame merge failed: {str(e)}")
