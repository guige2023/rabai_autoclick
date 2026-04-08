"""
Data Transformer Action Module.

Transforms data using column mappings, expressions,
function pipelines, and complex data structure operations.

Author: RabAi Team
"""

from __future__ import annotations

import json
import re
import sys
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TransformType(Enum):
    """Types of data transformations."""
    MAP_COLUMNS = "map_columns"
    RENAME_COLUMNS = "rename_columns"
    DROP_COLUMNS = "drop_columns"
    SELECT_COLUMNS = "select_columns"
    ADD_COLUMN = "add_column"
    EXPRESSION = "expression"
    PIPELINE = "pipeline"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    FLATTEN = "flatten"
    NEST = "nest"


@dataclass
class TransformRule:
    """A single transformation rule."""
    type: TransformType
    config: Dict[str, Any]


class DataTransformerAction(BaseAction):
    """Data transformer action.
    
    Transforms data using column mappings, expressions,
    function pipelines, and complex structure operations.
    """
    action_type = "data_transformer"
    display_name = "数据转换"
    description = "数据转换与映射"
    
    def __init__(self):
        super().__init__()
        self._transform_functions: Dict[str, Callable] = {
            "uppercase": lambda x: str(x).upper() if x else "",
            "lowercase": lambda x: str(x).lower() if x else "",
            "trim": lambda x: str(x).strip() if x else "",
            "abs": lambda x: abs(float(x)) if x is not None else None,
            "round": lambda x, decimals=0: round(float(x), decimals) if x is not None else None,
            "floor": lambda x: math.floor(float(x)) if x is not None else None,
            "ceil": lambda x: math.ceil(float(x)) if x is not None else None,
            "len": lambda x: len(x) if x is not None else 0,
            "str": lambda x: str(x) if x is not None else "",
            "int": lambda x: int(float(x)) if x is not None else None,
            "float": lambda x: float(x) if x is not None else None,
            "bool": lambda x: bool(x) if x is not None else False,
            "json.dumps": lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x,
            "json.loads": lambda x: json.loads(x) if isinstance(x, str) else x,
            "to_null": lambda x: None if x in ("", "null", "None", "NA", "N/A") else x,
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: Data to transform
                - operation: transform/pipeline/apply
                - transforms: List of transform rules
                - columns: Column mappings
                - expression: Expression to evaluate
                - function: Function name to apply
                - new_column: Name for new column
                - overwrite: Whether to overwrite existing column
                
        Returns:
            ActionResult with transformed data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "transform")
        data = params.get("data", [])
        transforms = params.get("transforms", [])
        columns = params.get("columns", {})
        expression = params.get("expression")
        function_name = params.get("function")
        new_column = params.get("new_column")
        overwrite = params.get("overwrite", False)
        
        try:
            if operation == "transform":
                result = self._transform(data, transforms, columns, expression, function_name, new_column, overwrite, start_time)
            elif operation == "pipeline":
                result = self._pipeline(data, transforms, start_time)
            elif operation == "apply":
                result = self._apply_function(data, function_name, new_column, overwrite, params, start_time)
            elif operation == "map_columns":
                result = self._map_columns(data, columns, overwrite, start_time)
            elif operation == "rename_columns":
                result = self._rename_columns(data, columns, start_time)
            elif operation == "drop_columns":
                result = self._drop_columns(data, columns, start_time)
            elif operation == "select_columns":
                result = self._select_columns(data, columns, start_time)
            elif operation == "add_column":
                result = self._add_column(data, new_column, expression, function_name, overwrite, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Transform failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _transform(
        self, data: List, transforms: List[Dict], columns: Dict,
        expression: Optional[str], function_name: Optional[str],
        new_column: Optional[str], overwrite: bool, start_time: float
    ) -> ActionResult:
        """Apply transformations to data."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to transform",
                data={"data": []},
                duration=time.time() - start_time
            )
        
        transformed = list(data)
        
        for transform in transforms:
            transform_type = transform.get("type", "expression")
            config = transform.get("config", {})
            
            try:
                ttype = TransformType(transform_type)
            except ValueError:
                continue
            
            if ttype == TransformType.MAP_COLUMNS:
                transformed = self._map_columns(transformed, config.get("mapping", {}), config.get("overwrite", False), start_time)
                if isinstance(transformed, ActionResult):
                    transformed = transformed.data.get("data", [])
            
            elif ttype == TransformType.RENAME_COLUMNS:
                result = self._rename_columns(transformed, config.get("rename", {}), start_time)
                transformed = result.data.get("data", [])
            
            elif ttype == TransformType.EXPRESSION:
                expr_result = self._add_column(transformed, config.get("new_column"), config.get("expression"), None, config.get("overwrite", False), start_time)
                transformed = expr_result.data.get("data", [])
        
        if columns:
            result = self._map_columns(transformed, columns, overwrite, start_time)
            transformed = result.data.get("data", [])
        
        if expression and new_column:
            result = self._add_column(transformed, new_column, expression, None, overwrite, start_time)
            transformed = result.data.get("data", [])
        
        if function_name and new_column:
            result = self._apply_function(transformed, function_name, new_column, overwrite, {}, start_time)
            transformed = result.data.get("data", [])
        
        return ActionResult(
            success=True,
            message=f"Transformed {len(transformed)} records",
            data={"data": transformed, "transforms_applied": len(transforms)},
            duration=time.time() - start_time
        )
    
    def _pipeline(self, data: List, transforms: List[Dict], start_time: float) -> ActionResult:
        """Execute a pipeline of transforms."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to process",
                data={"data": [], "steps": 0},
                duration=time.time() - start_time
            )
        
        result_data = list(data)
        results = []
        
        for i, step in enumerate(transforms):
            step_type = step.get("type", "expression")
            step_config = step.get("config", {})
            
            step_result = self._execute_transform_step(result_data, step_type, step_config, start_time)
            
            if step_result.success:
                result_data = step_result.data.get("data", result_data)
                results.append({
                    "step": i,
                    "type": step_type,
                    "success": True
                })
            else:
                results.append({
                    "step": i,
                    "type": step_type,
                    "success": False,
                    "error": step_result.message
                })
        
        return ActionResult(
            success=all(r["success"] for r in results),
            message=f"Pipeline complete: {sum(1 for r in results if r['success'])}/{len(results)} steps succeeded",
            data={
                "data": result_data,
                "steps": len(transforms),
                "step_results": results
            },
            duration=time.time() - start_time
        )
    
    def _execute_transform_step(self, data: List, step_type: str, config: Dict, start_time: float) -> ActionResult:
        """Execute a single transform step."""
        try:
            ttype = TransformType(step_type)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown transform type: {step_type}",
                duration=time.time() - start_time
            )
        
        if ttype == TransformType.MAP_COLUMNS:
            return self._map_columns(data, config.get("mapping", {}), config.get("overwrite", False), start_time)
        elif ttype == TransformType.RENAME_COLUMNS:
            return self._rename_columns(data, config.get("rename", {}), start_time)
        elif ttype == TransformType.DROP_COLUMNS:
            return self._drop_columns(data, config.get("columns", []), start_time)
        elif ttype == TransformType.SELECT_COLUMNS:
            return self._select_columns(data, config.get("columns", []), start_time)
        elif ttype == TransformType.ADD_COLUMN:
            return self._add_column(data, config.get("new_column"), config.get("expression"), config.get("function"), config.get("overwrite", False), start_time)
        elif ttype == TransformType.EXPRESSION:
            return self._add_column(data, config.get("new_column"), config.get("expression"), None, config.get("overwrite", False), start_time)
        elif ttype == TransformType.FLATTEN:
            return self._flatten_records(data, config.get("separator", "."), start_time)
        elif ttype == TransformType.NEST:
            return self._nest_records(data, config.get("key_field"), config.get("nested_fields", []), start_time)
        else:
            return ActionResult(success=True, message=f"Step type {step_type} not implemented", data={"data": data}, duration=time.time() - start_time)
    
    def _map_columns(self, data: List[Dict], mapping: Dict[str, str], overwrite: bool, start_time: float) -> ActionResult:
        """Map/transform column values."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        transformed = []
        for record in data:
            new_record = dict(record)
            for source_col, target_col_or_fn in mapping.items():
                if source_col not in record:
                    continue
                
                value = record[source_col]
                
                if callable(target_col_or_fn):
                    value = target_col_or_fn(value)
                    new_record[source_col] = value
                elif isinstance(target_col_or_fn, str):
                    if target_col_or_fn in self._transform_functions:
                        fn = self._transform_functions[target_col_or_fn]
                        value = fn(value)
                        new_record[source_col] = value
                    elif overwrite or target_col_or_fn not in new_record:
                        new_record[target_col_or_fn] = value
                        if target_col_or_fn != source_col:
                            del new_record[source_col]
                    else:
                        new_record[target_col_or_fn] = value
            
            transformed.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Mapped {len(transformed)} records",
            data={"data": transformed, "mapping": list(mapping.keys())},
            duration=time.time() - start_time
        )
    
    def _rename_columns(self, data: List[Dict], rename: Dict[str, str], start_time: float) -> ActionResult:
        """Rename columns."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        transformed = []
        for record in data:
            new_record = {}
            for key, value in record.items():
                new_key = rename.get(key, key)
                new_record[new_key] = value
            transformed.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Renamed columns in {len(transformed)} records",
            data={"data": transformed, "renamed": list(rename.keys())},
            duration=time.time() - start_time
        )
    
    def _drop_columns(self, data: List[Dict], columns: List[str], start_time: float) -> ActionResult:
        """Drop specified columns."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        transformed = []
        for record in data:
            new_record = {k: v for k, v in record.items() if k not in columns}
            transformed.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Dropped columns from {len(transformed)} records",
            data={"data": transformed, "dropped": columns},
            duration=time.time() - start_time
        )
    
    def _select_columns(self, data: List[Dict], columns: List[str], start_time: float) -> ActionResult:
        """Select only specified columns."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        transformed = []
        for record in data:
            new_record = {k: v for k, v in record.items() if k in columns}
            transformed.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Selected {len(columns)} columns from {len(transformed)} records",
            data={"data": transformed, "selected": columns},
            duration=time.time() - start_time
        )
    
    def _add_column(
        self, data: List[Dict], new_column: str, expression: Optional[str],
        function_name: Optional[str], overwrite: bool, start_time: float
    ) -> ActionResult:
        """Add a new computed column."""
        if not data or not new_column:
            return ActionResult(success=False, message="Missing data or column name", duration=time.time() - start_time)
        
        transformed = []
        
        for record in data:
            new_record = dict(record)
            
            if function_name and function_name in self._transform_functions:
                fn = self._transform_functions[function_name]
                
                if expression:
                    try:
                        context = dict(record)
                        value = self._evaluate_expression(expression, context)
                        new_record[new_column] = value
                    except Exception:
                        new_record[new_column] = None
                else:
                    values = [record.get(c) for c in columns if c in record]
                    if len(values) == 1:
                        try:
                            new_record[new_column] = fn(values[0])
                        except Exception:
                            new_record[new_column] = None
                    else:
                        try:
                            new_record[new_column] = fn(values)
                        except Exception:
                            new_record[new_column] = None
            
            elif expression:
                try:
                    context = dict(record)
                    value = self._evaluate_expression(expression, context)
                    new_record[new_column] = value
                except Exception:
                    new_record[new_column] = None
            
            if not overwrite and new_column in new_record and expression is None and function_name is None:
                alt_name = f"{new_column}_computed"
                new_record[alt_name] = new_record[new_column]
                new_column = alt_name
            
            transformed.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Added column '{new_column}' to {len(transformed)} records",
            data={"data": transformed, "new_column": new_column},
            duration=time.time() - start_time
        )
    
    def _apply_function(
        self, data: List, function_name: str, new_column: Optional[str],
        overwrite: bool, params: Dict, start_time: float
    ) -> ActionResult:
        """Apply a function to data."""
        if function_name not in self._transform_functions:
            return ActionResult(
                success=False,
                message=f"Unknown function: {function_name}",
                duration=time.time() - start_time
            )
        
        fn = self._transform_functions[function_name]
        
        if isinstance(data, list) and data and isinstance(data[0], dict):
            result = self._add_column(data, new_column or f"{function_name}_result", None, function_name, overwrite, start_time)
            return result
        
        elif isinstance(data, list):
            results = []
            for item in data:
                try:
                    results.append(fn(item))
                except Exception:
                    results.append(None)
            
            return ActionResult(
                success=True,
                message=f"Applied {function_name} to {len(results)} items",
                data={"data": results, "function": function_name},
                duration=time.time() - start_time
            )
        
        else:
            try:
                result = fn(data)
                return ActionResult(
                    success=True,
                    message=f"Applied {function_name}",
                    data={"data": result, "function": function_name},
                    duration=time.time() - start_time
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Function failed: {str(e)}",
                    duration=time.time() - start_time
                )
    
    def _evaluate_expression(self, expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate a simple expression."""
        import math
        safe_names = {"math": math, "str": str, "int": int, "float": float, "len": len, "abs": abs, "round": round, "min": min, "max": max, "sum": sum}
        safe_dict = {**safe_names, **context}
        
        for key in list(context.keys()):
            safe_dict[key] = context[key]
        
        try:
            result = eval(expression, {"__builtins__": {}}, safe_dict)
            return result
        except Exception:
            for key, value in context.items():
                placeholder = f"{{{key}}}"
                if placeholder in expression:
                    expression = expression.replace(placeholder, repr(value))
            
            try:
                result = eval(expression, {"__builtins__": {}}, safe_dict)
                return result
            except Exception:
                return None
    
    def _flatten_records(self, data: List[Dict], separator: str, start_time: float) -> ActionResult:
        """Flatten nested records."""
        flattened = []
        for record in data:
            flat = self._flatten_dict(record, separator)
            flattened.append(flat)
        
        return ActionResult(
            success=True,
            message=f"Flattened {len(flattened)} records",
            data={"data": flattened},
            duration=time.time() - start_time
        )
    
    def _flatten_dict(self, d: Dict, sep: str, parent_key: str = "") -> Dict:
        """Recursively flatten a dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, sep, new_key).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self._flatten_dict(item, sep, f"{new_key}[{i}]").items())
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _nest_records(self, data: List[Dict], key_field: str, nested_fields: List[str], start_time: float) -> ActionResult:
        """Nest fields into a nested object."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        nested = []
        for record in data:
            new_record = {k: v for k, v in record.items() if k not in nested_fields}
            nested_obj = {k: record.get(k) for k in nested_fields if k in record}
            new_record[key_field] = nested_obj
            nested.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Nested {len(nested)} records",
            data={"data": nested},
            duration=time.time() - start_time
        )
    
    def register_function(self, name: str, fn: Callable) -> None:
        """Register a custom transformation function."""
        self._transform_functions[name] = fn
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate transformer parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
