"""Pandas data processing action module for RabAI AutoClick.

Provides data processing operations:
- PandasReadAction: Read data into DataFrame
- PandasGroupByAction: Group DataFrame
- PandasMergeAction: Merge/join DataFrames
- PandasPivotAction: Pivot table
- PandasAggregateAction: Aggregate data
- PandasFilterAction: Filter rows
- PandasSortAction: Sort DataFrame
- PandasFillNaAction: Fill NA values
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Union

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PandasReadAction(BaseAction):
    """Read data into DataFrame."""
    action_type = "pandas_read"
    display_name = "Pandas读取"
    description = "读取数据为DataFrame"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pandas read."""
        file_path = params.get('file_path', '')
        file_type = params.get('file_type', 'auto')  # auto, csv, excel, json, parquet
        sheet_name = params.get('sheet_name', 0)
        output_var = params.get('output_var', 'dataframe')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import pandas as pd

            resolved_path = context.resolve_value(file_path) if context else file_path

            if file_type == 'auto':
                ext = resolved_path.lower().split('.')[-1]
                if ext in ('csv',):
                    file_type = 'csv'
                elif ext in ('xlsx', 'xls'):
                    file_type = 'excel'
                elif ext in ('json',):
                    file_type = 'json'
                elif ext in ('parquet', 'pq'):
                    file_type = 'parquet'

            if file_type == 'csv':
                df = pd.read_csv(resolved_path)
            elif file_type == 'excel':
                df = pd.read_excel(resolved_path, sheet_name=sheet_name)
            elif file_type == 'json':
                df = pd.read_json(resolved_path)
            elif file_type == 'parquet':
                df = pd.read_parquet(resolved_path)
            else:
                return ActionResult(success=False, message=f"Unsupported file type: {file_type}")

            result = {'shape': df.shape, 'columns': list(df.columns), 'dtypes': {str(c): str(dt) for c, dt in df.dtypes.items()}}
            if context:
                context.set(output_var, df)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Read {df.shape[0]}x{df.shape[1]} DataFrame", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed. Run: pip install pandas")
        except Exception as e:
            return ActionResult(success=False, message=f"Pandas read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'file_type': 'auto', 'sheet_name': 0, 'output_var': 'dataframe'}


class PandasGroupByAction(BaseAction):
    """Group DataFrame by column."""
    action_type = "pandas_groupby"
    display_name = "Pandas分组"
    description = "对DataFrame分组"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute groupby."""
        dataframe_var = params.get('dataframe_var', 'dataframe')
        group_by = params.get('group_by', '')
        agg_func = params.get('agg_func', 'mean')  # mean, sum, count, min, max, std
        agg_columns = params.get('agg_columns', None)
        output_var = params.get('output_var', 'grouped_data')

        if not group_by:
            return ActionResult(success=False, message="group_by is required")

        try:
            import pandas as pd

            resolved_df_var = context.resolve_value(dataframe_var) if context else dataframe_var
            df = context.resolve_value(resolved_df_var) if context else None
            if df is None:
                df = context.resolve_value(dataframe_var) if isinstance(dataframe_var, str) else dataframe_var

            resolved_group = context.resolve_value(group_by) if context else group_by
            resolved_func = context.resolve_value(agg_func) if context else agg_func

            if not isinstance(df, pd.DataFrame):
                return ActionResult(success=False, message=f"Variable {dataframe_var} is not a DataFrame")

            grouped = df.groupby(resolved_group)
            if resolved_func == 'mean':
                result_df = grouped.mean(numeric_only=True)
            elif resolved_func == 'sum':
                result_df = grouped.sum(numeric_only=True)
            elif resolved_func == 'count':
                result_df = grouped.count()
            elif resolved_func == 'min':
                result_df = grouped.min()
            elif resolved_func == 'max':
                result_df = grouped.max()
            elif resolved_func == 'std':
                result_df = grouped.std(numeric_only=True)
            elif resolved_func == 'first':
                result_df = grouped.first()
            else:
                result_df = grouped.agg(resolved_func)

            result = {'shape': result_df.shape, 'columns': list(result_df.columns)}
            if context:
                context.set(output_var, result_df)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Grouped to {result_df.shape[0]} groups", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"GroupBy error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dataframe_var', 'group_by']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'agg_func': 'mean', 'agg_columns': None, 'output_var': 'grouped_data'}


class PandasMergeAction(BaseAction):
    """Merge/join DataFrames."""
    action_type = "pandas_merge"
    display_name = "Pandas合并"
    description = "合并DataFrame"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute merge."""
        left_var = params.get('left_var', '')
        right_var = params.get('right_var', '')
        on = params.get('on', '')
        how = params.get('how', 'inner')  # inner, left, right, outer
        left_on = params.get('left_on', None)
        right_on = params.get('right_on', None)
        output_var = params.get('output_var', 'merged_data')

        if not left_var or not right_var:
            return ActionResult(success=False, message="left_var and right_var are required")

        try:
            import pandas as pd

            left_df = context.resolve_value(left_var) if context else None
            right_df = context.resolve_value(right_var) if context else None

            if not isinstance(left_df, pd.DataFrame):
                return ActionResult(success=False, message=f"{left_var} is not a DataFrame")
            if not isinstance(right_df, pd.DataFrame):
                return ActionResult(success=False, message=f"{right_var} is not a DataFrame")

            resolved_how = context.resolve_value(how) if context else how

            if on:
                resolved_on = context.resolve_value(on) if context else on
                merged = pd.merge(left_df, right_df, on=resolved_on, how=resolved_how)
            elif left_on and right_on:
                resolved_lo = context.resolve_value(left_on) if context else left_on
                resolved_ro = context.resolve_value(right_on) if context else right_on
                merged = pd.merge(left_df, right_df, left_on=resolved_lo, right_on=resolved_ro, how=resolved_how)
            else:
                return ActionResult(success=False, message="Must specify 'on' or both 'left_on' and 'right_on'")

            result = {'shape': merged.shape, 'columns': list(merged.columns)}
            if context:
                context.set(output_var, merged)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Merged {merged.shape[0]}x{merged.shape[1]}", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['left_var', 'right_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'on': '', 'how': 'inner', 'left_on': None, 'right_on': None, 'output_var': 'merged_data'}


class PandasPivotAction(BaseAction):
    """Create pivot table."""
    action_type = "pandas_pivot"
    display_name = "Pandas透视表"
    description = "创建透视表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pivot."""
        dataframe_var = params.get('dataframe_var', 'dataframe')
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', None)
        agg_func = params.get('agg_func', 'mean')
        output_var = params.get('output_var', 'pivot_table')

        if not dataframe_var or not index:
            return ActionResult(success=False, message="dataframe_var and index are required")

        try:
            import pandas as pd

            df = context.resolve_value(dataframe_var) if context else None
            if not isinstance(df, pd.DataFrame):
                return ActionResult(success=False, message=f"{dataframe_var} is not a DataFrame")

            resolved_index = context.resolve_value(index) if context else index
            resolved_columns = context.resolve_value(columns) if context else columns
            resolved_values = context.resolve_value(values) if context else values
            resolved_agg = context.resolve_value(agg_func) if context else agg_func

            kwargs = {'index': resolved_index, 'aggfunc': resolved_agg}
            if resolved_columns:
                kwargs['columns'] = resolved_columns
            if resolved_values:
                kwargs['values'] = resolved_values

            pivot = pd.pivot_table(df, **kwargs)

            result = {'shape': pivot.shape, 'columns': list(pivot.columns)}
            if context:
                context.set(output_var, pivot)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Pivot table: {pivot.shape[0]}x{pivot.shape[1]}", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dataframe_var', 'index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'columns': '', 'values': None, 'agg_func': 'mean', 'output_var': 'pivot_table'}


class PandasFilterAction(BaseAction):
    """Filter DataFrame rows."""
    action_type = "pandas_filter"
    display_name = "Pandas过滤"
    description = "过滤DataFrame行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute filter."""
        dataframe_var = params.get('dataframe_var', 'dataframe')
        column = params.get('column', '')
        operator = params.get('operator', 'eq')
        value = params.get('value', None)
        query = params.get('query', None)  # pandas query string
        output_var = params.get('output_var', 'filtered_df')

        if not dataframe_var:
            return ActionResult(success=False, message="dataframe_var is required")

        try:
            import pandas as pd

            df = context.resolve_value(dataframe_var) if context else None
            if not isinstance(df, pd.DataFrame):
                return ActionResult(success=False, message=f"{dataframe_var} is not a DataFrame")

            if query:
                resolved_query = context.resolve_value(query) if context else query
                filtered = df.query(resolved_query)
            elif column:
                resolved_col = context.resolve_value(column) if context else column
                resolved_val = context.resolve_value(value) if context else value
                resolved_op = context.resolve_value(operator) if context else operator

                col_data = df[resolved_col]
                if resolved_op == 'eq':
                    filtered = df[col_data == resolved_val]
                elif resolved_op == 'ne':
                    filtered = df[col_data != resolved_val]
                elif resolved_op == 'gt':
                    filtered = df[col_data > resolved_val]
                elif resolved_op == 'lt':
                    filtered = df[col_data < resolved_val]
                elif resolved_op == 'ge':
                    filtered = df[col_data >= resolved_val]
                elif resolved_op == 'le':
                    filtered = df[col_data <= resolved_val]
                elif resolved_op == 'contains':
                    filtered = df[col_data.astype(str).str.contains(str(resolved_val))]
                elif resolved_op == 'startswith':
                    filtered = df[col_data.astype(str).str.startswith(str(resolved_val))]
                else:
                    filtered = df
            else:
                filtered = df

            result = {'shape': filtered.shape, 'original_shape': df.shape}
            if context:
                context.set(output_var, filtered)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Filtered {df.shape[0]} -> {filtered.shape[0]} rows", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dataframe_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'column': '', 'operator': 'eq', 'value': None, 'query': None, 'output_var': 'filtered_df'}


class PandasFillNaAction(BaseAction):
    """Fill NA values in DataFrame."""
    action_type = "pandas_fillna"
    display_name = "Pandas填充NA"
    description = "填充DataFrame空值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute fillna."""
        dataframe_var = params.get('dataframe_var', 'dataframe')
        value = params.get('value', 0)
        method = params.get('method', None)  # None, ffill, bfill
        columns = params.get('columns', None)
        output_var = params.get('output_var', 'filled_df')

        if not dataframe_var:
            return ActionResult(success=False, message="dataframe_var is required")

        try:
            import pandas as pd

            df = context.resolve_value(dataframe_var) if context else None
            if not isinstance(df, pd.DataFrame):
                return ActionResult(success=False, message=f"{dataframe_var} is not a DataFrame")

            resolved_value = context.resolve_value(value) if context else value
            resolved_method = context.resolve_value(method) if context else method
            resolved_columns = context.resolve_value(columns) if context else columns

            df_copy = df.copy()
            if resolved_columns:
                for col in resolved_columns:
                    if col in df_copy.columns:
                        if resolved_method:
                            df_copy[col] = df_copy[col].fillna(method=resolved_method)
                        else:
                            df_copy[col] = df_copy[col].fillna(resolved_value)
            else:
                if resolved_method:
                    df_copy = df_copy.fillna(method=resolved_method)
                else:
                    df_copy = df_copy.fillna(resolved_value)

            na_before = df.isna().sum().sum()
            na_after = df_copy.isna().sum().sum()

            result = {'na_before': int(na_before), 'na_after': int(na_after), 'filled': int(na_before - na_after)}
            if context:
                context.set(output_var, df_copy)
            return ActionResult(success=True, message=f"Filled {result['filled']} NA values", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"FillNa error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dataframe_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 0, 'method': None, 'columns': None, 'output_var': 'filled_df'}


class PandasDescribeAction(BaseAction):
    """Describe DataFrame statistics."""
    action_type = "pandas_describe"
    display_name = "Pandas统计"
    description = "DataFrame描述性统计"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute describe."""
        dataframe_var = params.get('dataframe_var', 'dataframe')
        output_var = params.get('output_var', 'df_stats')

        if not dataframe_var:
            return ActionResult(success=False, message="dataframe_var is required")

        try:
            import pandas as pd

            df = context.resolve_value(dataframe_var) if context else None
            if not isinstance(df, pd.DataFrame):
                return ActionResult(success=False, message=f"{dataframe_var} is not a DataFrame")

            stats = df.describe().to_dict()
            result = {
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': {str(c): str(dt) for c, dt in df.dtypes.items()},
                'null_counts': {str(c): int(df[c].isna().sum()) for c in df.columns},
                'describe': stats,
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Stats for {df.shape[0]}x{df.shape[1]} DataFrame", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Describe error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dataframe_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'df_stats'}
