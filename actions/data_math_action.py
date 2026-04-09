"""Data math operations action module for RabAI AutoClick.

Provides mathematical operations for data:
- BasicMathAction: Basic arithmetic operations
- StatisticalMathAction: Statistical calculations
- TrigonometricMathAction: Trigonometric operations
- LinearAlgebraAction: Linear algebra operations
- MathFunctionsAction: Special math functions
"""

from typing import Any, Dict, List, Optional
import math
from collections import Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BasicMathAction(BaseAction):
    """Basic arithmetic operations."""
    action_type = "basic_math"
    display_name = "基础数学运算"
    description = "执行基础算术运算"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            a = params.get("a", 0)
            b = params.get("b", 0)
            
            if operation == "add":
                result = a + b
            elif operation == "subtract":
                result = a - b
            elif operation == "multiply":
                result = a * b
            elif operation == "divide":
                if b == 0:
                    return ActionResult(success=False, message="Division by zero")
                result = a / b
            elif operation == "modulo":
                result = a % b
            elif operation == "power":
                result = a ** b
            elif operation == "floor_divide":
                result = a // b
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
            return ActionResult(
                success=True,
                message=f"Operation '{operation}' result: {result}",
                data={
                    "operation": operation,
                    "a": a,
                    "b": b,
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class StatisticalMathAction(BaseAction):
    """Statistical calculations."""
    action_type = "statistical_math"
    display_name = "统计运算"
    description = "执行统计计算"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "mean")
            data = params.get("data", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            numeric_data = [x for x in data if isinstance(x, (int, float))]
            
            if not numeric_data:
                return ActionResult(success=False, message="No numeric data found")
            
            if operation == "mean":
                result = sum(numeric_data) / len(numeric_data)
            elif operation == "median":
                sorted_data = sorted(numeric_data)
                n = len(sorted_data)
                if n % 2 == 0:
                    result = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
                else:
                    result = sorted_data[n // 2]
            elif operation == "mode":
                counter = Counter(numeric_data)
                result = counter.most_common(1)[0][0]
            elif operation == "std":
                mean = sum(numeric_data) / len(numeric_data)
                variance = sum((x - mean) ** 2 for x in numeric_data) / len(numeric_data)
                result = math.sqrt(variance)
            elif operation == "variance":
                mean = sum(numeric_data) / len(numeric_data)
                result = sum((x - mean) ** 2 for x in numeric_data) / len(numeric_data)
            elif operation == "sum":
                result = sum(numeric_data)
            elif operation == "product":
                result = 1
                for x in numeric_data:
                    result *= x
            elif operation == "min":
                result = min(numeric_data)
            elif operation == "max":
                result = max(numeric_data)
            elif operation == "range":
                result = max(numeric_data) - min(numeric_data)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
            return ActionResult(
                success=True,
                message=f"Statistical operation '{operation}' result: {result}",
                data={
                    "operation": operation,
                    "result": result,
                    "data_count": len(numeric_data)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class TrigonometricMathAction(BaseAction):
    """Trigonometric operations."""
    action_type = "trigonometric_math"
    display_name = "三角函数运算"
    description = "执行三角函数运算"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sin")
            value = params.get("value", 0)
            degrees = params.get("degrees", False)
            
            if degrees:
                value_rad = math.radians(value)
            else:
                value_rad = value
            
            if operation == "sin":
                result = math.sin(value_rad)
            elif operation == "cos":
                result = math.cos(value_rad)
            elif operation == "tan":
                result = math.tan(value_rad)
            elif operation == "asin":
                result = math.asin(value)
                if degrees:
                    result = math.degrees(result)
            elif operation == "acos":
                result = math.acos(value)
                if degrees:
                    result = math.degrees(result)
            elif operation == "atan":
                result = math.atan(value)
                if degrees:
                    result = math.degrees(result)
            elif operation == "sinh":
                result = math.sinh(value_rad)
            elif operation == "cosh":
                result = math.cosh(value_rad)
            elif operation == "tanh":
                result = math.tanh(value_rad)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
            return ActionResult(
                success=True,
                message=f"Trigonometric operation '{operation}' result: {result}",
                data={
                    "operation": operation,
                    "input_value": value,
                    "degrees": degrees,
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LinearAlgebraAction(BaseAction):
    """Linear algebra operations."""
    action_type = "linear_algebra"
    display_name = "线性代数运算"
    description = "执行线性代数运算"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "dot")
            matrix_a = params.get("matrix_a", [])
            matrix_b = params.get("matrix_b", [])
            
            if operation == "dot":
                if not matrix_a or not matrix_b:
                    return ActionResult(success=False, message="Both matrices required")
                result = self._dot_product(matrix_a, matrix_b)
            elif operation == "transpose":
                if not matrix_a:
                    return ActionResult(success=False, message="matrix_a required")
                result = self._transpose(matrix_a)
            elif operation == "determinant":
                if not matrix_a:
                    return ActionResult(success=False, message="matrix_a required")
                result = self._determinant(matrix_a)
            elif operation == "inverse":
                if not matrix_a:
                    return ActionResult(success=False, message="matrix_a required")
                result = self._inverse(matrix_a)
            elif operation == "add":
                if not matrix_a or not matrix_b:
                    return ActionResult(success=False, message="Both matrices required")
                result = self._add_matrices(matrix_a, matrix_b)
            elif operation == "subtract":
                if not matrix_a or not matrix_b:
                    return ActionResult(success=False, message="Both matrices required")
                result = self._subtract_matrices(matrix_a, matrix_b)
            elif operation == "multiply":
                if not matrix_a or not matrix_b:
                    return ActionResult(success=False, message="Both matrices required")
                result = self._multiply_matrices(matrix_a, matrix_b)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
            return ActionResult(
                success=True,
                message=f"Linear algebra operation '{operation}' complete",
                data={
                    "operation": operation,
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _dot_product(self, a: List, b: List) -> float:
        return sum(x * y for x, y in zip(a, b))
    
    def _transpose(self, matrix: List[List]) -> List[List]:
        if not matrix:
            return []
        return list(zip(*matrix))
    
    def _determinant(self, matrix: List[List]) -> float:
        n = len(matrix)
        if n == 1:
            return matrix[0][0]
        if n == 2:
            return matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]
        
        det = 0
        for j in range(n):
            sub = [row[:j] + row[j+1:] for row in matrix[1:]]
            det += ((-1) ** j) * matrix[0][j] * self._determinant(sub)
        
        return det
    
    def _inverse(self, matrix: List[List]) -> List[List]:
        n = len(matrix)
        det = self._determinant(matrix)
        if abs(det) < 1e-10:
            raise ValueError("Matrix is singular")
        
        if n == 2:
            return [[matrix[1][1] / det, -matrix[0][1] / det],
                    [-matrix[1][0] / det, matrix[0][0] / det]]
        
        raise NotImplementedError("Inverse for larger matrices not implemented")
    
    def _add_matrices(self, a: List[List], b: List[List]) -> List[List]:
        return [[a[i][j] + b[i][j] for j in range(len(a[0]))] for i in range(len(a))]
    
    def _subtract_matrices(self, a: List[List], b: List[List]) -> List[List]:
        return [[a[i][j] - b[i][j] for j in range(len(a[0]))] for i in range(len(a))]
    
    def _multiply_matrices(self, a: List[List], b: List[List]) -> List[List]:
        result = []
        for i in range(len(a)):
            row = []
            for j in range(len(b[0])):
                val = sum(a[i][k] * b[k][j] for k in range(len(b)))
                row.append(val)
            result.append(row)
        return result


class MathFunctionsAction(BaseAction):
    """Special math functions."""
    action_type = "math_functions"
    display_name = "特殊数学函数"
    description = "执行特殊数学函数运算"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "abs")
            value = params.get("value", 0)
            
            if operation == "abs":
                result = abs(value)
            elif operation == "ceil":
                result = math.ceil(value)
            elif operation == "floor":
                result = math.floor(value)
            elif operation == "round":
                result = round(value)
            elif operation == "sqrt":
                if value < 0:
                    return ActionResult(success=False, message="Cannot take sqrt of negative number")
                result = math.sqrt(value)
            elif operation == "log":
                if value <= 0:
                    return ActionResult(success=False, message="Cannot take log of non-positive number")
                result = math.log(value)
            elif operation == "log10":
                if value <= 0:
                    return ActionResult(success=False, message="Cannot take log10 of non-positive number")
                result = math.log10(value)
            elif operation == "exp":
                result = math.exp(value)
            elif operation == "factorial":
                if value < 0:
                    return ActionResult(success=False, message="Cannot take factorial of negative number")
                result = math.factorial(int(value))
            elif operation == "gcd":
                b = params.get("b", 0)
                result = math.gcd(int(value), int(b))
            elif operation == "is_finite":
                result = math.isfinite(value)
            elif operation == "is_inf":
                result = math.isinf(value)
            elif operation == "is_nan":
                result = math.isnan(value)
            elif operation == "degrees":
                result = math.degrees(value)
            elif operation == "radians":
                result = math.radians(value)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
            
            return ActionResult(
                success=True,
                message=f"Math function '{operation}' result: {result}",
                data={
                    "operation": operation,
                    "input": value,
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
