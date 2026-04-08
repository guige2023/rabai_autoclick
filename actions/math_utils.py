"""Math utilities action module for RabAI AutoClick.

Provides mathematical operation actions including arithmetic,
statistics, randomization, and number formatting.
"""

import sys
import os
import math
import random
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MathCalculateAction(BaseAction):
    """Perform arithmetic calculations.
    
    Supports basic operations (+, -, *, /, //, %, **),
    parentheses, and built-in math functions.
    """
    action_type = "math_calculate"
    display_name = "数学计算"
    description = "执行数学运算，支持加减乘除和函数"

    VALID_OPS = ['+', '-', '*', '/', '//', '%', '**']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform calculation.
        
        Args:
            context: Execution context.
            params: Dict with keys: expression, precision,
                   save_to_var.
        
        Returns:
            ActionResult with calculation result.
        """
        expression = params.get('expression', '')
        precision = params.get('precision', None)
        save_to_var = params.get('save_to_var', None)

        if not expression:
            return ActionResult(
                success=False,
                message="Expression cannot be empty"
            )

        # Replace common operators
        expr = expression.replace('^', '**')

        # Replace variables from context
        if hasattr(context, 'variables'):
            for key, value in context.variables.items():
                if isinstance(value, (int, float)):
                    expr = expr.replace(key, str(value))

        # Validate expression (basic security check)
        dangerous = ['import', 'eval', 'exec', 'open', '__']
        for d in dangerous:
            if d in expr:
                return ActionResult(
                    success=False,
                    message=f"Forbidden pattern in expression: {d}"
                )

        try:
            # Use safe evaluation
            allowed_names = {
                'abs': abs, 'round': round, 'min': min, 'max': max,
                'sum': sum, 'pow': pow, 'len': len,
                'pi': math.pi, 'e': math.e,
                'sin': math.sin, 'cos': math.cos, 'tan': math.tan,
                'sqrt': math.sqrt, 'log': math.log, 'log10': math.log10,
                'exp': math.exp, 'floor': math.floor, 'ceil': math.ceil,
            }
            result = eval(expr, {"__builtins__": {}}, allowed_names)

            # Apply precision
            if precision is not None and isinstance(result, float):
                result = round(result, int(precision))

            result_data = {
                'result': result,
                'expression': expression,
                'type': type(result).__name__
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"计算结果: {result}",
                data=result_data
            )
        except ZeroDivisionError:
            return ActionResult(
                success=False,
                message="Division by zero"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算错误: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'precision': None,
            'save_to_var': None
        }


class MathStatisticsAction(BaseAction):
    """Calculate statistics on list of numbers.
    
    Supports sum, mean, median, mode, std, variance,
    min, max, range, and percentile.
    """
    action_type = "math_statistics"
    display_name = "统计计算"
    description = "计算统计数据：均值、中位数、标准差等"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys: values (list), precision,
                   save_to_var.
        
        Returns:
            ActionResult with statistics dictionary.
        """
        values = params.get('values', [])
        precision = params.get('precision', 2)
        save_to_var = params.get('save_to_var', None)

        if not values:
            return ActionResult(
                success=False,
                message="Values list is empty"
            )

        # Convert to floats
        try:
            numbers = [float(v) for v in values]
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"Cannot convert values to numbers: {e}"
            )

        n = len(numbers)
        if n == 0:
            return ActionResult(
                success=False,
                message="Empty values after conversion"
            )

        # Calculate statistics
        total = sum(numbers)
        mean_val = total / n
        sorted_vals = sorted(numbers)
        
        # Median
        if n % 2 == 0:
            median_val = (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
        else:
            median_val = sorted_vals[n//2]

        # Mode (most frequent)
        from collections import Counter
        counter = Counter(numbers)
        mode_val = counter.most_common(1)[0][0] if counter else None

        # Variance and std
        variance_val = sum((x - mean_val) ** 2 for x in numbers) / n
        std_val = math.sqrt(variance_val)

        # Min, max, range
        min_val = min(numbers)
        max_val = max(numbers)
        range_val = max_val - min_val

        stats = {
            'count': n,
            'sum': round(total, precision),
            'mean': round(mean_val, precision),
            'median': round(median_val, precision),
            'mode': round(mode_val, precision) if mode_val is not None else None,
            'std': round(std_val, precision),
            'variance': round(variance_val, precision),
            'min': round(min_val, precision),
            'max': round(max_val, precision),
            'range': round(range_val, precision)
        }

        if save_to_var:
            context.variables[save_to_var] = stats

        return ActionResult(
            success=True,
            message=f"统计: 均值={stats['mean']}, 中位数={stats['median']}, 标准差={stats['std']}",
            data=stats
        )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'precision': 2,
            'save_to_var': None
        }


class MathRandomAction(BaseAction):
    """Generate random numbers.
    
    Supports integer range, float range, choice from list,
    shuffle, and random sampling.
    """
    action_type = "math_random"
    display_name = "随机数生成"
    description = "生成随机数，支持整数、浮点数和列表选择"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate random value.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, min_val, max_val,
                   count, precision, choices, save_to_var.
        
        Returns:
            ActionResult with random value(s).
        """
        mode = params.get('mode', 'integer')
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        count = params.get('count', 1)
        precision = params.get('precision', 2)
        choices = params.get('choices', None)
        save_to_var = params.get('save_to_var', None)

        # Set seed for reproducibility if needed
        seed = params.get('seed', None)
        if seed is not None:
            random.seed(seed)

        results = []

        if mode == 'integer':
            for _ in range(count):
                results.append(random.randint(int(min_val), int(max_val)))
        elif mode == 'float':
            for _ in range(count):
                val = random.uniform(float(min_val), float(max_val))
                results.append(round(val, precision))
        elif mode == 'choice' and choices:
            if isinstance(choices, str):
                choices = choices.split(',')
            for _ in range(min(count, len(choices))):
                results.append(random.choice(choices))
        elif mode == 'sample' and choices:
            if isinstance(choices, str):
                choices = choices.split(',')
            results = random.sample(choices, min(count, len(choices)))
        elif mode == 'uuid':
            import uuid
            for _ in range(count):
                results.append(str(uuid.uuid4()))
        else:
            return ActionResult(
                success=False,
                message=f"Invalid mode or missing choices: {mode}"
            )

        result_data = {
            'results': results,
            'mode': mode,
            'count': len(results)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"生成 {len(results)} 个随机值",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['mode']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min_val': 0,
            'max_val': 100,
            'count': 1,
            'precision': 2,
            'choices': None,
            'seed': None,
            'save_to_var': None
        }


class MathClampAction(BaseAction):
    """Clamp a number within a range.
    
    Constrains a value between min and max boundaries.
    """
    action_type = "math_clamp"
    display_name = "数值限幅"
    description = "将数值限制在指定范围内"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Clamp a value.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, min_val, max_val,
                   save_to_var.
        
        Returns:
            ActionResult with clamped value.
        """
        value = params.get('value', 0)
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        save_to_var = params.get('save_to_var', None)

        try:
            val = float(value)
            min_v = float(min_val)
            max_v = float(max_val)

            clamped = max(min_v, min(val, max_v))

            result_data = {
                'original': val,
                'clamped': clamped,
                'min': min_v,
                'max': max_v,
                'was_clamped': clamped != val
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"限幅: {val} -> {clamped}" + (" (已限幅)" if result_data['was_clamped'] else ""),
                data=result_data
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"Invalid number: {e}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'min_val', 'max_val']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}
