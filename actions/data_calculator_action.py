"""Data calculator action module for RabAI AutoClick.

Provides data calculation capabilities with arithmetic operations,
statistical functions, and formula evaluation.
"""

import sys
import os
import math
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCalculatorAction(BaseAction):
    """Data calculator action for arithmetic and statistical operations.
    
    Supports arithmetic operations, statistical functions,
    and formula evaluation on data fields.
    """
    action_type = "data_calculator"
    display_name = "数据计算器"
    description = "算术与统计计算"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute calculation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: calc|stats|formula
                data: Input data
                field: Field to calculate on
                fields: Fields for multi-field operations
                operation_type: Type of operation (sum, avg, etc.)
                formula: Formula string for formula operations.
        
        Returns:
            ActionResult with calculated results.
        """
        operation = params.get('operation', 'calc')
        
        if operation == 'calc':
            return self._calculate(params)
        elif operation == 'stats':
            return self._statistics(params)
        elif operation == 'formula':
            return self._formula(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _calculate(self, params: Dict[str, Any]) -> ActionResult:
        """Perform field calculations."""
        data = params.get('data', [])
        field = params.get('field')
        operation_type = params.get('operation_type', 'sum')
        result_field = params.get('result_field', f'{operation_type}_result')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not field:
            return ActionResult(success=False, message="field is required")
        
        values = [item.get(field) for item in data if item.get(field) is not None]
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        
        result = None
        
        if operation_type == 'sum':
            result = sum(numeric_values) if numeric_values else 0
        elif operation_type == 'avg' or operation_type == 'mean':
            result = sum(numeric_values) / len(numeric_values) if numeric_values else 0
        elif operation_type == 'min':
            result = min(numeric_values) if numeric_values else None
        elif operation_type == 'max':
            result = max(numeric_values) if numeric_values else None
        elif operation_type == 'count':
            result = len(values)
        elif operation_type == 'product':
            result = 1
            for v in numeric_values:
                result *= v
        elif operation_type == 'median':
            sorted_vals = sorted(numeric_values)
            n = len(sorted_vals)
            result = sorted_vals[n // 2] if n > 0 else None
        elif operation_type == 'stddev':
            if len(numeric_values) > 1:
                mean = sum(numeric_values) / len(numeric_values)
                variance = sum((x - mean) ** 2 for x in numeric_values) / len(numeric_values)
                result = math.sqrt(variance)
            else:
                result = 0
        elif operation_type == 'variance':
            if len(numeric_values) > 1:
                mean = sum(numeric_values) / len(numeric_values)
                result = sum((x - mean) ** 2 for x in numeric_values) / len(numeric_values)
            else:
                result = 0
        
        output = []
        for item in data:
            new_item = dict(item)
            new_item[result_field] = self._compute_for_item(item.get(field), operation_type)
            output.append(new_item)
        
        return ActionResult(
            success=True,
            message=f"Calculated {operation_type}: {result}",
            data={
                'result': result,
                'items': output,
                'operation': operation_type,
                'field': field,
                'result_field': result_field
            }
        )
    
    def _compute_for_item(self, value: Any, operation_type: str) -> Any:
        """Compute operation for single item value."""
        if value is None:
            return None
        
        numeric_values = [value] if isinstance(value, (int, float)) else []
        
        if not numeric_values:
            return None
        
        if operation_type == 'double':
            return value * 2
        elif operation_type == 'square':
            return value ** 2
        elif operation_type == 'sqrt':
            return math.sqrt(value) if value >= 0 else None
        elif operation_type == 'abs':
            return abs(value)
        elif operation_type == 'log':
            return math.log(value) if value > 0 else None
        elif operation_type == 'round':
            return round(value)
        elif operation_type == 'ceil':
            return math.ceil(value)
        elif operation_type == 'floor':
            return math.floor(value)
        
        return value
    
    def _statistics(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate statistics for a field."""
        data = params.get('data', [])
        field = params.get('field')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not field:
            values = [item for item in data if isinstance(item, (int, float))]
        else:
            values = [item.get(field) for item in data if isinstance(item.get(field), (int, float))]
        
        if not values:
            return ActionResult(success=False, message="No numeric values found")
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        mean = sum(values) / n
        
        variance = sum((x - mean) ** 2 for x in values) / n
        stddev = math.sqrt(variance)
        
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        
        return ActionResult(
            success=True,
            message=f"Statistics for {n} values",
            data={
                'count': n,
                'sum': sum(values),
                'mean': mean,
                'median': sorted_values[n // 2],
                'min': min(values),
                'max': max(values),
                'stddev': stddev,
                'variance': variance,
                'q1': sorted_values[q1_idx],
                'q3': sorted_values[q3_idx],
                'range': max(values) - min(values)
            }
        )
    
    def _formula(self, params: Dict[str, Any]) -> ActionResult:
        """Evaluate formula on data items."""
        data = params.get('data', [])
        formula = params.get('formula', '')
        result_field = params.get('result_field', 'formula_result')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not formula:
            return ActionResult(success=False, message="formula is required")
        
        output = []
        
        for item in data:
            try:
                result = self._eval_formula(formula, item)
                new_item = dict(item)
                new_item[result_field] = result
                output.append(new_item)
            except Exception:
                new_item = dict(item)
                new_item[result_field] = None
                output.append(new_item)
        
        return ActionResult(
            success=True,
            message=f"Evaluated formula on {len(data)} items",
            data={
                'items': output,
                'formula': formula,
                'result_field': result_field
            }
        )
    
    def _eval_formula(self, formula: str, item: Dict) -> Any:
        """Evaluate formula against item."""
        expr = formula
        
        for key, value in item.items():
            placeholder = f'{{{key}}}'
            expr = expr.replace(placeholder, repr(value))
        
        for op in ['+', '-', '*', '/', '**', '%', '(', ')']:
            pass
        
        try:
            result = eval(expr, {"__builtins__": {}, "math": math}, {})
            return result
        except Exception:
            return None
