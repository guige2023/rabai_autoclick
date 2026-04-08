"""Polars DataFrame action module for RabAI AutoClick.

Provides fast DataFrame operations using the Polars library.
Handles CSV, Parquet, JSON loading and transformations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PolarsLoadAction(BaseAction):
    """Load data into Polars DataFrame from various formats.

    Supports CSV, Parquet, JSON, and Excel files.
    """
    action_type = "polars_load"
    display_name = "Polars加载数据"
    description = "Polars DataFrame数据加载"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Load data into Polars DataFrame.

        Args:
            context: Execution context.
            params: Dict with keys:
                - file_path: Path to data file
                - format: File format (csv, parquet, json, excel)
                - options: Format-specific options (delimiter, infer_schema, etc.)
                - sheet_name: Sheet name for Excel files

        Returns:
            ActionResult with DataFrame info.
        """
        file_path = params.get('file_path', '')
        file_format = params.get('format', '')
        options = params.get('options', {})
        sheet_name = params.get('sheet_name', 0)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"File not found: {file_path}")

        try:
            import polars as pl
        except ImportError:
            return ActionResult(success=False, message="polars not installed. Run: pip install polars")

        start = time.time()
        try:
            if file_format == 'csv' or file_path.endswith('.csv'):
                df = pl.read_csv(file_path, **options)
            elif file_format == 'parquet' or file_path.endswith('.parquet'):
                df = pl.read_parquet(file_path, **options)
            elif file_format == 'json' or file_path.endswith('.json'):
                df = pl.read_json(file_path, **options)
            elif file_format == 'excel' or file_path.endswith(('.xlsx', '.xls')):
                df = pl.read_excel(file_path, sheet_name=sheet_name, **options)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {file_format}")

            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Loaded {len(df)} rows, {len(df.columns)} columns",
                data={
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns,
                    'dtypes': {c: str(df[c].dtype) for c in df.columns},
                    'shape': list(df.shape),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Polars load error: {str(e)}")


class PolarsTransformAction(BaseAction):
    """Apply transformations to Polars DataFrame.

    Supports select, filter, groupby, join, and aggregate operations.
    """
    action_type = "polars_transform"
    display_name = "Polars数据转换"
    description = "Polars DataFrame转换操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform Polars DataFrame.

        Args:
            context: Execution context.
            params: Dict with keys:
                - file_path: Path to input data
                - operations: List of operations to apply
                  Each op: {type: 'select'|'filter'|'groupby'|'join'|'sort', ...}
                - format: Input format
                - output_path: Optional path to save result

        Returns:
            ActionResult with transformation results.
        """
        file_path = params.get('file_path', '')
        operations = params.get('operations', [])
        file_format = params.get('format', '')
        output_path = params.get('output_path', '')

        if not file_path or not os.path.exists(file_path):
            return ActionResult(success=False, message="file_path is required and must exist")

        try:
            import polars as pl
        except ImportError:
            return ActionResult(success=False, message="polars not installed")

        start = time.time()
        try:
            # Load data
            if file_format == 'csv' or file_path.endswith('.csv'):
                df = pl.read_csv(file_path)
            elif file_format == 'parquet' or file_path.endswith('.parquet'):
                df = pl.read_parquet(file_path)
            elif file_format == 'json' or file_path.endswith('.json'):
                df = pl.read_json(file_path)
            else:
                df = pl.read_csv(file_path)

            # Apply operations
            for op in operations:
                op_type = op.get('type', '')
                if op_type == 'select':
                    cols = op.get('columns', [])
                    if cols:
                        df = df.select(cols)
                elif op_type == 'filter':
                    expr = op.get('expr', '')
                    if expr:
                        df = df.filter(pl.col(expr))
                elif op_type == 'groupby':
                    by = op.get('by', [])
                    agg = op.get('agg', [])
                    if by and agg:
                        df = df.group_by(by).agg(agg)
                elif op_type == 'sort':
                    by = op.get('by', [])
                    descending = op.get('descending', False)
                    if by:
                        df = df.sort(by, descending=descending)
                elif op_type == 'limit':
                    n = op.get('n', 100)
                    df = df.head(n)
                elif op_type == 'drop_nulls':
                    subset = op.get('subset', None)
                    df = df.drop_nulls(subset=subset if subset else None)

            # Save if output path provided
            if output_path:
                if output_path.endswith('.csv'):
                    df.write_csv(output_path)
                elif output_path.endswith('.parquet'):
                    df.write_parquet(output_path)
                elif output_path.endswith('.json'):
                    df.write_json(output_path)

            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Transformation completed: {len(df)} rows",
                data={
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns,
                    'output_saved': bool(output_path),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Polars transform error: {str(e)}")


class PolarsAggregateAction(BaseAction):
    """Perform aggregation operations on Polars DataFrame.

    Computes descriptive statistics, grouped aggregates, and pivots.
    """
    action_type = "polars_aggregate"
    display_name = "Polars聚合统计"
    description = "Polars数据聚合分析"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Aggregate Polars DataFrame.

        Args:
            context: Execution context.
            params: Dict with keys:
                - file_path: Path to data file
                - group_by: Column(s) to group by
                - aggregations: Dict of {column: ['mean', 'sum', 'count', ...]}
                - format: Input format

        Returns:
            ActionResult with aggregated data.
        """
        file_path = params.get('file_path', '')
        group_by = params.get('group_by', [])
        aggregations = params.get('aggregations', {})
        file_format = params.get('format', '')

        if not file_path or not os.path.exists(file_path):
            return ActionResult(success=False, message="file_path is required and must exist")
        if not aggregations:
            return ActionResult(success=False, message="aggregations dict is required")

        try:
            import polars as pl
        except ImportError:
            return ActionResult(success=False, message="polars not installed")

        start = time.time()
        try:
            # Load
            if file_format == 'csv' or file_path.endswith('.csv'):
                df = pl.read_csv(file_path)
            elif file_format == 'parquet' or file_path.endswith('.parquet'):
                df = pl.read_parquet(file_path)
            elif file_format == 'json' or file_path.endswith('.json'):
                df = pl.read_json(file_path)
            else:
                df = pl.read_csv(file_path)

            # Build aggregation expressions
            agg_exprs = []
            for col, funcs in aggregations.items():
                if isinstance(funcs, str):
                    funcs = [funcs]
                for func in funcs:
                    if func == 'mean':
                        agg_exprs.append(pl.col(col).mean().alias(f'{col}_mean'))
                    elif func == 'sum':
                        agg_exprs.append(pl.col(col).sum().alias(f'{col}_sum'))
                    elif func == 'count':
                        agg_exprs.append(pl.col(col).count().alias(f'{col}_count'))
                    elif func == 'min':
                        agg_exprs.append(pl.col(col).min().alias(f'{col}_min'))
                    elif func == 'max':
                        agg_exprs.append(pl.col(col).max().alias(f'{col}_max'))
                    elif func == 'std':
                        agg_exprs.append(pl.col(col).std().alias(f'{col}_std'))
                    elif func == 'median':
                        agg_exprs.append(pl.col(col).median().alias(f'{col}_median'))
                    elif func == 'n_unique':
                        agg_exprs.append(pl.col(col).n_unique().alias(f'{col}_n_unique'))

            if group_by:
                result = df.group_by(group_by).agg(agg_exprs)
            else:
                result = df.select(agg_exprs)

            duration = time.time() - start
            return ActionResult(
                success=True, message="Aggregation completed",
                data={
                    'rows': len(result),
                    'columns': result.columns,
                    'shape': list(result.shape),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Polars aggregate error: {str(e)}")
