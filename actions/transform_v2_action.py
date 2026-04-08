"""Transform V2 action module for RabAI AutoClick.

Provides advanced data transformation operations including
column mapping, pivoting, unpivoting, and complex transformations.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataTransformer:
    """Advanced data transformation engine."""
    
    @staticmethod
    def rename_keys(data: List[Dict], mapping: Dict[str, str]) -> List[Dict]:
        """Rename keys in a list of dictionaries.
        
        Args:
            data: List of dictionaries.
            mapping: Dict mapping old key to new key.
        
        Returns:
            Transformed list of dictionaries.
        """
        result = []
        for record in data:
            new_record = {}
            for key, value in record.items():
                new_key = mapping.get(key, key)
                new_record[new_key] = value
            result.append(new_record)
        return result
    
    @staticmethod
    def select_keys(data: List[Dict], keys: List[str]) -> List[Dict]:
        """Select specific keys from dictionaries.
        
        Args:
            data: List of dictionaries.
            keys: Keys to keep.
        
        Returns:
            Filtered list of dictionaries.
        """
        return [{k: record.get(k) for k in keys if k in record} for record in data]
    
    @staticmethod
    def drop_keys(data: List[Dict], keys: List[str]) -> List[Dict]:
        """Drop specific keys from dictionaries.
        
        Args:
            data: List of dictionaries.
            keys: Keys to remove.
        
        Returns:
            Transformed list of dictionaries.
        """
        return [{k: v for k, v in record.items() if k not in keys} for record in data]
    
    @staticmethod
    def add_computed(data: List[Dict], computations: Dict[str, str]) -> List[Dict]:
        """Add computed columns using lambda expressions.
        
        Args:
            data: List of dictionaries.
            computations: Dict mapping new column to lambda body.
        
        Returns:
            List with computed columns added.
        """
        result = []
        for record in data:
            new_record = dict(record)
            for col_name, expr in computations.items():
                try:
                    func = eval(f"lambda record: {expr}")
                    new_record[col_name] = func(record)
                except Exception:
                    new_record[col_name] = None
            result.append(new_record)
        return result
    
    @staticmethod
    def unpivot(data: List[Dict], id_cols: List[str], value_col: str, var_col: str = "variable") -> List[Dict]:
        """Unpivot (melt) a list of dictionaries.
        
        Args:
            data: List of dictionaries to unpivot.
            id_cols: Columns to keep as identifiers.
            value_col: Name for the value column.
            var_col: Name for the variable column.
        
        Returns:
            Unpivoted list of dictionaries.
        """
        result = []
        for record in data:
            id_data = {k: record.get(k) for k in id_cols if k in record}
            for key, value in record.items():
                if key not in id_cols:
                    new_record = dict(id_data)
                    new_record[var_col] = key
                    new_record[value_col] = value
                    result.append(new_record)
        return result
    
    @staticmethod
    def pivot(data: List[Dict], index: str, columns: str, values: str, aggfunc: str = "first") -> Dict[str, Any]:
        """Pivot a list of dictionaries.
        
        Args:
            data: List of dictionaries.
            index: Column to use as index.
            columns: Column to use as columns.
            values: Column with values.
            aggfunc: Aggregation function for duplicates.
        
        Returns:
            Pivot table data.
        """
        pivot_dict: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
        
        for record in data:
            idx_val = record.get(index)
            col_val = record.get(columns)
            val = record.get(values)
            
            if idx_val is not None and col_val is not None:
                pivot_dict[idx_val][col_val].append(val)
        
        aggregation_funcs = {
            "first": lambda vals: vals[0] if vals else None,
            "last": lambda vals: vals[-1] if vals else None,
            "sum": lambda vals: sum(v for v in vals if v is not None),
            "count": lambda vals: len(vals),
            "avg": lambda vals: sum(v for v in vals if v is not None) / len([v for v in vals if v is not None]) if any(v is not None for v in vals) else None,
            "min": lambda vals: min((v for v in vals if v is not None), default=None),
            "max": lambda vals: max((v for v in vals if v is not None), default=None),
        }
        
        agg = aggregation_funcs.get(aggfunc, aggregation_funcs["first"])
        
        result_data = {}
        for idx_val, col_data in pivot_dict.items():
            result_data[idx_val] = {col: agg(vals) for col, vals in col_data.items()}
        
        return {
            "data": result_data,
            "index": index,
            "columns": columns,
            "values": values,
            "row_count": len(result_data)
        }
    
    @staticmethod
    def fillna(data: List[Dict], fill_value: Any, columns: List[str] = None) -> List[Dict]:
        """Fill null values.
        
        Args:
            data: List of dictionaries.
            fill_value: Value to use for filling.
            columns: Specific columns to fill (None = all).
        
        Returns:
            List with nulls filled.
        """
        result = []
        for record in data:
            new_record = dict(record)
            cols_to_fill = columns if columns else record.keys()
            for col in cols_to_fill:
                if col in new_record and new_record[col] is None:
                    new_record[col] = fill_value
            result.append(new_record)
        return result
    
    @staticmethod
    def cast_types(data: List[Dict], type_map: Dict[str, str]) -> List[Dict]:
        """Cast column types.
        
        Args:
            data: List of dictionaries.
            type_map: Dict mapping column to type name.
        
        Returns:
            List with cast types.
        """
        type_converters = {
            "int": int,
            "float": float,
            "str": str,
            "bool": lambda x: bool(x) if x is not None else None,
            "json": lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        }
        
        result = []
        for record in data:
            new_record = dict(record)
            for col, type_name in type_map.items():
                if col in new_record and new_record[col] is not None:
                    converter = type_converters.get(type_name)
                    if converter:
                        try:
                            new_record[col] = converter(new_record[col])
                        except (ValueError, TypeError):
                            new_record[col] = None
            result.append(new_record)
        return result


class RenameAction(BaseAction):
    """Rename keys/columns in data."""
    action_type = "rename_keys"
    display_name = "重命名列"
    description = "重命名数据字段"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Rename keys.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, mapping.
        
        Returns:
            ActionResult with transformed data.
        """
        data = params.get('data', [])
        mapping = params.get('mapping', {})
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not mapping:
            return ActionResult(success=False, message="mapping is required")
        
        try:
            result = DataTransformer.rename_keys(data, mapping)
            return ActionResult(success=True, message=f"Renamed {len(mapping)} columns", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Rename failed: {str(e)}")


class SelectColumnsAction(BaseAction):
    """Select specific columns from data."""
    action_type = "select_columns"
    display_name = "选择列"
    description = "选择数据字段"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Select columns.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, columns.
        
        Returns:
            ActionResult with selected columns.
        """
        data = params.get('data', [])
        columns = params.get('columns', [])
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not columns:
            return ActionResult(success=False, message="columns is required")
        
        try:
            result = DataTransformer.select_keys(data, columns)
            return ActionResult(success=True, message=f"Selected {len(columns)} columns", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Select failed: {str(e)}")


class ComputeColumnAction(BaseAction):
    """Add computed columns."""
    action_type = "compute_column"
    display_name = "计算列"
    description = "添加计算字段"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add computed columns.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, computations.
        
        Returns:
            ActionResult with computed data.
        """
        data = params.get('data', [])
        computations = params.get('computations', {})
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not computations:
            return ActionResult(success=False, message="computations is required")
        
        try:
            result = DataTransformer.add_computed(data, computations)
            return ActionResult(success=True, message=f"Added {len(computations)} computed columns", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Compute failed: {str(e)}")


class UnpivotAction(BaseAction):
    """Unpivot (melt) data."""
    action_type = "unpivot"
    display_name = "逆透视"
    description = "数据逆透视操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unpivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, id_cols, value_col, var_col.
        
        Returns:
            ActionResult with unpivoted data.
        """
        data = params.get('data', [])
        id_cols = params.get('id_cols', [])
        value_col = params.get('value_col', 'value')
        var_col = params.get('var_col', 'variable')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not id_cols:
            return ActionResult(success=False, message="id_cols is required")
        
        try:
            result = DataTransformer.unpivot(data, id_cols, value_col, var_col)
            return ActionResult(success=True, message=f"Unpivoted to {len(result)} rows", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Unpivot failed: {str(e)}")


class PivotAction(BaseAction):
    """Pivot data."""
    action_type = "pivot"
    display_name = "透视"
    description = "数据透视操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, index, columns, values, aggfunc.
        
        Returns:
            ActionResult with pivoted data.
        """
        data = params.get('data', [])
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'first')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not all([index, columns, values]):
            return ActionResult(success=False, message="index, columns, and values are required")
        
        try:
            result = DataTransformer.pivot(data, index, columns, values, aggfunc)
            return ActionResult(success=True, message=f"Pivoted to {result['row_count']} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot failed: {str(e)}")


class FillnaAction(BaseAction):
    """Fill null values."""
    action_type = "fillna"
    display_name = "填充空值"
    description = "填充缺失值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fill null values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, fill_value, columns.
        
        Returns:
            ActionResult with filled data.
        """
        data = params.get('data', [])
        fill_value = params.get('fill_value', '')
        columns = params.get('columns', None)
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            result = DataTransformer.fillna(data, fill_value, columns)
            return ActionResult(success=True, message="Filled null values", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Fillna failed: {str(e)}")


class CastTypesAction(BaseAction):
    """Cast column types."""
    action_type = "cast_types"
    display_name = "类型转换"
    description = "转换数据类型"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cast column types.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, type_map.
        
        Returns:
            ActionResult with cast data.
        """
        data = params.get('data', [])
        type_map = params.get('type_map', {})
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not type_map:
            return ActionResult(success=False, message="type_map is required")
        
        try:
            result = DataTransformer.cast_types(data, type_map)
            return ActionResult(success=True, message=f"Cast {len(type_map)} columns", data={"data": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Cast failed: {str(e)}")
