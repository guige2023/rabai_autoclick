"""Math action module for RabAI AutoClick.

Provides mathematical operation actions.
"""

import math
import random
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MathCalculateAction(BaseAction):
    """Perform mathematical calculation.
    
    Evaluates math expressions.
    """
    action_type = "math_calculate"
    display_name = "数学计算"
    description = "执行数学表达式计算"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate expression.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: expression, precision.
        
        Returns:
            ActionResult with calculation result.
        """
        expression = params.get('expression', '')
        precision = params.get('precision', 10)
        
        if not expression:
            return ActionResult(success=False, message="expression required")
        
        try:
            # Safely evaluate math expression
            allowed_names = {
                'abs': abs, 'min': min, 'max': max, 'pow': pow,
                'round': round, 'floor': math.floor, 'ceil': math.ceil,
                'sqrt': math.sqrt, 'sin': math.sin, 'cos': math.cos,
                'tan': math.tan, 'log': math.log, 'log10': math.log10,
                'pi': math.pi, 'e': math.e, 'exp': math.exp
            }
            
            result = eval(expression, {"__builtins__": {}}, allowed_names)
            
            if isinstance(result, float):
                result = round(result, precision)
            
            return ActionResult(
                success=True,
                message=f"Result: {result}",
                data={'expression': expression, 'result': result}
            )
            
        except ZeroDivisionError:
            return ActionResult(success=False, message="Division by zero")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Calculation error: {e}",
                data={'error': str(e)}
            )


class MathRandomAction(BaseAction):
    """Generate random numbers.
    
    Creates random integers or floats.
    """
    action_type = "math_random"
    display_name = "随机数生成"
    description = "生成随机数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random number.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: min_val, max_val, count, as_float.
        
        Returns:
            ActionResult with random number(s).
        """
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        count = params.get('count', 1)
        as_float = params.get('as_float', False)
        
        if count < 1 or count > 10000:
            return ActionResult(success=False, message="count must be 1-10000")
        
        try:
            numbers = []
            for _ in range(count):
                if as_float:
                    numbers.append(random.uniform(min_val, max_val))
                else:
                    numbers.append(random.randint(min_val, max_val))
            
            result = numbers[0] if count == 1 else numbers
            
            return ActionResult(
                success=True,
                message=f"Generated {count} random number(s)",
                data={'numbers': result, 'count': count, 'min': min_val, 'max': max_val}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Random error: {e}",
                data={'error': str(e)}
            )


class MathRoundAction(BaseAction):
    """Round number to precision.
    
    Rounds float to specified decimal places.
    """
    action_type = "math_round"
    display_name = "四舍五入"
    description = "数值四舍五入"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Round number.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: value, decimals.
        
        Returns:
            ActionResult with rounded value.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 0)
        
        try:
            result = round(value, decimals)
            
            return ActionResult(
                success=True,
                message=f"Rounded: {result}",
                data={'original': value, 'rounded': result, 'decimals': decimals}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Round error: {e}",
                data={'error': str(e)}
            )


class MathClampAction(BaseAction):
    """Clamp value between min and max.
    
    Constrains value to a range.
    """
    action_type = "math_clamp"
    display_name = "数值范围限制"
    description = "将数值限制在指定范围内"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clamp value.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: value, min_val, max_val.
        
        Returns:
            ActionResult with clamped value.
        """
        value = params.get('value', 0)
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        
        if min_val > max_val:
            return ActionResult(success=False, message="min_val must be <= max_val")
        
        result = max(min_val, min(max_val, value))
        
        return ActionResult(
            success=True,
            message=f"Clamped: {result}",
            data={'original': value, 'clamped': result, 'min': min_val, 'max': max_val}
        )
