"""Math operations action module for RabAI AutoClick.

Provides math operations:
- MathBasicAction: Basic arithmetic
- MathAdvancedAction: Advanced math functions
- MathStatisticsAction: Statistical functions
- MathTrigonometryAction: Trigonometric functions
- MathRandomAction: Random number generation
"""

import math
import random
import statistics
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathBasicAction(BaseAction):
    """Basic arithmetic."""
    action_type = "math_basic"
    display_name = "基础数学"
    description = "基础数学运算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            a = params.get("a", 0)
            b = params.get("b", 0)
            values = params.get("values", [])

            if values:
                nums = [float(v) for v in values if v is not None]
            else:
                nums = [float(a), float(b)]

            if operation == "add":
                result = sum(nums)
            elif operation == "subtract":
                if len(nums) >= 2:
                    result = nums[0] - sum(nums[1:])
                else:
                    result = nums[0]
            elif operation == "multiply":
                result = 1
                for n in nums:
                    result *= n
            elif operation == "divide":
                if nums[1] == 0:
                    return ActionResult(success=False, message="Division by zero")
                result = nums[0] / nums[1]
            elif operation == "modulo":
                result = nums[0] % nums[1]
            elif operation == "power":
                result = nums[0] ** nums[1]
            elif operation == "sqrt":
                result = math.sqrt(nums[0])
            elif operation == "abs":
                result = abs(nums[0])
            elif operation == "floor":
                result = math.floor(nums[0])
            elif operation == "ceil":
                result = math.ceil(nums[0])
            elif operation == "round":
                decimals = params.get("decimals", 0)
                result = round(nums[0], decimals)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"{operation} = {result}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Math error: {str(e)}")


class MathAdvancedAction(BaseAction):
    """Advanced math functions."""
    action_type = "math_advanced"
    display_name = "高等数学"
    description = "高等数学函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            value = params.get("value", 0)
            values = params.get("values", [])

            if values:
                nums = [float(v) for v in values]
            else:
                nums = [float(value)]

            if operation == "log":
                base = params.get("base", math.e)
                if len(nums) >= 2:
                    result = math.log(nums[1], nums[0])
                else:
                    result = math.log(nums[0], base)
            elif operation == "log10":
                result = math.log10(nums[0])
            elif operation == "log2":
                result = math.log2(nums[0])
            elif operation == "exp":
                result = math.exp(nums[0])
            elif operation == "factorial":
                result = math.factorial(int(nums[0]))
            elif operation == "gcd":
                if len(nums) >= 2:
                    result = math.gcd(int(nums[0]), int(nums[1]))
                else:
                    result = nums[0]
            elif operation == "lcm":
                if len(nums) >= 2:
                    result = abs(nums[0] * nums[1]) // math.gcd(int(nums[0]), int(nums[1]))
                else:
                    result = nums[0]
            elif operation == "degrees":
                result = math.degrees(nums[0])
            elif operation == "radians":
                result = math.radians(nums[0])
            elif operation == "hypot":
                result = math.hypot(*nums)
            elif operation == "isfinite":
                result = math.isfinite(nums[0])
            elif operation == "isinf":
                result = math.isinf(nums[0])
            elif operation == "isnan":
                result = math.isnan(nums[0])
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"{operation} = {result}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Advanced math error: {str(e)}")


class MathStatisticsAction(BaseAction):
    """Statistical functions."""
    action_type = "math_statistics"
    display_name = "统计函数"
    description = "统计函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "mean")
            values = params.get("values", [])

            if not values:
                return ActionResult(success=False, message="No values provided")

            nums = [float(v) for v in values]

            if operation == "mean":
                result = statistics.mean(nums)
            elif operation == "median":
                result = statistics.median(nums)
            elif operation == "mode":
                result = statistics.mode(nums)
            elif operation == "stdev":
                result = statistics.stdev(nums)
            elif operation == "variance":
                result = statistics.variance(nums)
            elif operation == "pvariance":
                result = statistics.pvariance(nums)
            elif operation == "sum":
                result = sum(nums)
            elif operation == "min":
                result = min(nums)
            elif operation == "max":
                result = max(nums)
            elif operation == "count":
                result = len(nums)
            elif operation == "median_low":
                result = statistics.median_low(nums)
            elif operation == "median_high":
                result = statistics.median_high(nums)
            elif operation == "quantiles":
                n = params.get("n", 4)
                result = statistics.quantiles(nums, n=n)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"{operation} = {result}", data={"result": result, "count": len(nums)})

        except Exception as e:
            return ActionResult(success=False, message=f"Statistics error: {str(e)}")


class MathTrigonometryAction(BaseAction):
    """Trigonometric functions."""
    action_type = "math_trig"
    display_name = "三角函数"
    description = "三角函数运算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sin")
            value = params.get("value", 0)
            degrees = params.get("degrees", False)

            angle = float(value)
            if degrees:
                angle = math.radians(angle)

            if operation == "sin":
                result = math.sin(angle)
            elif operation == "cos":
                result = math.cos(angle)
            elif operation == "tan":
                result = math.tan(angle)
            elif operation == "asin":
                result = math.asin(angle)
                if degrees:
                    result = math.degrees(result)
            elif operation == "acos":
                result = math.acos(angle)
                if degrees:
                    result = math.degrees(result)
            elif operation == "atan":
                result = math.atan(angle)
                if degrees:
                    result = math.degrees(result)
            elif operation == "atan2":
                y = params.get("y", 0)
                result = math.atan2(float(y), angle)
                if degrees:
                    result = math.degrees(result)
            elif operation == "sinh":
                result = math.sinh(angle)
            elif operation == "cosh":
                result = math.cosh(angle)
            elif operation == "tanh":
                result = math.tanh(angle)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"{operation}({value}) = {result}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Trig error: {str(e)}")


class MathRandomAction(BaseAction):
    """Random number generation."""
    action_type = "math_random"
    display_name = "随机数"
    description = "随机数生成"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "random")
            min_val = params.get("min", 0)
            max_val = params.get("max", 1)
            count = params.get("count", 1)
            seed = params.get("seed", None)

            if seed is not None:
                random.seed(seed)

            if operation == "random":
                results = [random.random() for _ in range(count)]
            elif operation == "uniform":
                results = [random.uniform(min_val, max_val) for _ in range(count)]
            elif operation == "randint":
                results = [random.randint(int(min_val), int(max_val)) for _ in range(count)]
            elif operation == "randrange":
                step = params.get("step", 1)
                results = [random.randrange(int(min_val), int(max_val), step) for _ in range(count)]
            elif operation == "choice":
                choices = params.get("choices", [min_val, max_val])
                results = [random.choice(choices) for _ in range(count)]
            elif operation == "shuffle":
                items = params.get("items", list(range(int(min_val), int(max_val) + 1)))
                result_list = list(items)
                random.shuffle(result_list)
                results = result_list
            elif operation == "sample":
                population = params.get("population", list(range(int(min_val), int(max_val) + 1)))
                results = random.sample(population, min(count, len(population)))
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(
                success=True,
                message=f"Generated {len(results)} random values",
                data={"results": results, "count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Random error: {str(e)}")
