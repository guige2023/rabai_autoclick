"""Dask distributed computing action module for RabAI AutoClick.

Provides large-scale data processing using Dask for parallel computing.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DaskLoadAction(BaseAction):
    """Load large datasets using Dask for out-of-core processing.

    Supports CSV, Parquet, JSON with Dask's lazy evaluation.
    """
    action_type = "dask_load"
    display_name = "Dask加载数据"
    description = "Dask大规模数据加载"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Load data with Dask.

        Args:
            context: Execution context.
            params: Dict with keys:
                - file_path: Path to data file
                - format: File format (csv, parquet, json)
                - blocksize: Chunk size for CSV loading
                - dtype: Data type hints

        Returns:
            ActionResult with DataFrame metadata.
        """
        file_path = params.get('file_path', '')
        file_format = params.get('format', '')
        blocksize = params.get('blocksize', '100MB')
        dtype = params.get('dtype', None)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"File not found: {file_path}")

        try:
            import dask.dataframe as dd
        except ImportError:
            return ActionResult(success=False, message="dask not installed. Run: pip install dask")

        start = time.time()
        try:
            if file_format == 'csv' or file_path.endswith('.csv'):
                df = dd.read_csv(file_path, blocksize=blocksize, dtype=dtype)
            elif file_format == 'parquet' or file_path.endswith('.parquet'):
                df = dd.read_parquet(file_path)
            elif file_format == 'json' or file_path.endswith('.json'):
                df = dd.read_json(file_path)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {file_format}")

            duration = time.time() - start
            return ActionResult(
                success=True, message="Dask DataFrame loaded",
                data={
                    'npartitions': df.npartitions,
                    'columns': list(df.columns),
                    'dtypes': {c: str(dt) for c, dt in df.dtypes.items()},
                    'nrows_estimate': len(df),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dask load error: {str(e)}")


class DaskComputeAction(BaseAction):
    """Execute Dask computations and trigger lazy evaluation."""
    action_type = "dask_compute"
    display_name = "Dask计算执行"
    description = "Dask延迟计算触发"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute Dask DataFrame.

        Args:
            context: Execution context.
            params: Dict with keys:
                - file_path: Path to Dask-compatible file
                - operations: List of lazy operations to apply before compute
                - format: File format

        Returns:
            ActionResult with computed results.
        """
        file_path = params.get('file_path', '')
        operations = params.get('operations', [])
        file_format = params.get('format', '')

        if not file_path or not os.path.exists(file_path):
            return ActionResult(success=False, message="file_path is required and must exist")

        try:
            import dask.dataframe as dd
        except ImportError:
            return ActionResult(success=False, message="dask not installed")

        start = time.time()
        try:
            if file_format == 'csv' or file_path.endswith('.csv'):
                df = dd.read_csv(file_path)
            elif file_format == 'parquet' or file_path.endswith('.parquet'):
                df = dd.read_parquet(file_path)
            elif file_format == 'json' or file_path.endswith('.json'):
                df = dd.read_json(file_path)
            else:
                df = dd.read_csv(file_path)

            # Apply operations lazily
            for op in operations:
                op_type = op.get('type', '')
                if op_type == 'filter':
                    col = op.get('column', '')
                    val = op.get('value', None)
                    if col and val is not None:
                        df = df[df[col] == val]
                elif op_type == 'select':
                    cols = op.get('columns', [])
                    if cols:
                        df = df[cols]
                elif op_type == 'groupby':
                    by = op.get('by', [])
                    agg = op.get('agg', {})
                    if by and agg:
                        df = df.groupby(by).agg(agg).reset_index()
                elif op_type == 'sort':
                    by = op.get('by', [])
                    df = df.sort_values(by)
                elif op_type == 'head':
                    n = op.get('n', 10)
                    df = df.head(n, npartitions=-1)

            # Trigger computation
            result = df.compute()
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Computed {len(result)} rows",
                data={
                    'rows': len(result),
                    'columns': len(result.columns),
                    'shape': list(result.shape),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dask compute error: {str(e)}")
