"""DataFrame action module for RabAI AutoClick.

Provides generic dataframe operations across pandas/dask/modin.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFrameAction(BaseAction):
    """Generic DataFrame operations.
    
    Supports pandas/dask/modin DataFrame operations including
    column manipulation, type conversion, merging, and statistics.
    """
    action_type = "dataframe"
    display_name = "DataFrame操作"
    description = "通用DataFrame数据操作与转换"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_pd(self):
        """Get pandas (or dask/modin)."""
        try:
            import pandas as pd
            return pd
        except ImportError:
            return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DataFrame operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'create', 'info', 'describe', 'head', 'tail', 'sort', 'select', 'drop', 'rename', 'cast', 'fillna', 'dtypes'
                - data: List of dicts or dict of lists
                - columns: Column names (for select/rename)
                - dtypes: Dict of column -> type mappings (for cast)
                - fill_value: Value for fillna
                - n: Number of rows (for head/tail)
                - sort_by: Column to sort by
        
        Returns:
            ActionResult with operation result.
        """
        pd = self._get_pd()
        if pd is None:
            return ActionResult(success=False, message="Requires pandas. Install: pip install pandas")
        
        command = params.get('command', 'create')
        data = params.get('data', [])
        columns = params.get('columns')
        dtypes = params.get('dtypes', {})
        fill_value = params.get('fill_value')
        n = params.get('n', 10)
        sort_by = params.get('sort_by')
        
        if command == 'create':
            return self._create_df(pd, data)
        
        if not isinstance(data, list):
            df = self._parse_dataframe(pd, data)
            if df is None:
                return ActionResult(success=False, message="data must be a list of dicts or a DataFrame")
        else:
            df = pd.DataFrame(data)
        
        if command == 'info':
            return ActionResult(
                success=True,
                message=f"DataFrame: {df.shape[0]} rows x {df.shape[1]} cols",
                data={'shape': df.shape, 'columns': list(df.columns), 'dtypes': df.dtypes.astype(str).to_dict()}
            )
        
        if command == 'describe':
            desc = df.describe().to_dict()
            return ActionResult(
                success=True,
                message=f"Described {len(df)} rows",
                data={'describe': desc, 'columns': list(df.columns)}
            )
        
        if command == 'head':
            result = df.head(n).to_dict('records')
            return ActionResult(success=True, message=f"First {len(result)} rows", data={'rows': result})
        
        if command == 'tail':
            result = df.tail(n).to_dict('records')
            return ActionResult(success=True, message=f"Last {len(result)} rows", data={'rows': result})
        
        if command == 'sort':
            if not sort_by:
                return ActionResult(success=False, message="sort_by required for sort")
            ascending = params.get('ascending', True)
            sorted_df = df.sort_values(sort_by, ascending=ascending)
            return ActionResult(success=True, message=f"Sorted by {sort_by}", data={'data': sorted_df.to_dict('records')})
        
        if command == 'select':
            if not columns:
                return ActionResult(success=False, message="columns required for select")
            selected = df[columns]
            return ActionResult(success=True, message=f"Selected {len(columns)} columns", data={'data': selected.to_dict('records')})
        
        if command == 'drop':
            if not columns:
                return ActionResult(success=False, message="columns required for drop")
            dropped = df.drop(columns=columns)
            return ActionResult(success=True, message=f"Dropped {len(columns)} columns", data={'data': dropped.to_dict('records'), 'columns': list(dropped.columns)})
        
        if command == 'rename':
            if not columns:
                return ActionResult(success=False, message="columns mapping required for rename")
            renamed = df.rename(columns=columns)
            return ActionResult(success=True, message="Columns renamed", data={'data': renamed.to_dict('records'), 'columns': list(renamed.columns)})
        
        if command == 'cast':
            if not dtypes:
                return ActionResult(success=False, message="dtypes mapping required for cast")
            try:
                df = df.astype(dtypes)
                return ActionResult(success=True, message="Dtypes cast", data={'data': df.to_dict('records'), 'dtypes': df.dtypes.astype(str).to_dict()})
            except Exception as e:
                return ActionResult(success=False, message=f"Cast failed: {e}")
        
        if command == 'fillna':
            filled = df.fillna(fill_value)
            return ActionResult(success=True, message=f"Filled NA with {fill_value}", data={'data': filled.to_dict('records')})
        
        if command == 'dtypes':
            return ActionResult(
                success=True,
                message=f"{len(df.columns)} columns",
                data={'dtypes': df.dtypes.astype(str).to_dict(), 'columns': list(df.columns)}
            )
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _create_df(self, pd: Any, data: Any) -> ActionResult:
        """Create DataFrame."""
        try:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if all(isinstance(v, list) for v in data.values()):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            else:
                return ActionResult(success=False, message="data must be list or dict")
            return ActionResult(
                success=True,
                message=f"Created DataFrame: {df.shape[0]} rows x {df.shape[1]} cols",
                data={'shape': df.shape, 'columns': list(df.columns), 'data': df.to_dict('records')}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create DataFrame: {e}")
    
    def _parse_dataframe(self, pd: Any, data: Any) -> Any:
        """Parse various data formats into DataFrame."""
        if hasattr(data, 'to_pandas'):
            return data.to_pandas()
        if hasattr(data, 'to_dict'):
            return data
        return None
