"""Math3 action module for RabAI AutoClick.

Provides additional math operations:
- MathSumAction: Sum of list
- MathProductAction: Product of list
- MathFactorialAction: Factorial
- MathGcdAction: Greatest common divisor
- MathLcmAction: Least common multiple
- MathIsPrimeAction: Check if prime
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathSumAction(BaseAction):
    """Sum of list."""
    action_type = "math_sum"
    display_name = "求和"
    description = "计算列表元素之和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with sum.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'sum_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = sum(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"求和: {result}",
                data={
                    'count': len(items),
                    'sum': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_result'}


class MathProductAction(BaseAction):
    """Product of list."""
    action_type = "math_product"
    display_name = "求积"
    description = "计算列表元素之积"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute product.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with product.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'product_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = 1
            for item in items:
                result *= item

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"求积: {result}",
                data={
                    'count': len(items),
                    'product': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"求积失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'product_result'}


class MathFactorialAction(BaseAction):
    """Factorial."""
    action_type = "math_factorial"
    display_name = "阶乘"
    description = "计算阶乘"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute factorial.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with factorial.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'factorial_result')

        try:
            resolved_value = int(context.resolve_value(value))

            if resolved_value < 0:
                return ActionResult(
                    success=False,
                    message="阶乘不支持负数"
                )

            result = math.factorial(resolved_value)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"阶乘: {resolved_value}! = {result}",
                data={
                    'value': resolved_value,
                    'factorial': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算阶乘失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'factorial_result'}


class MathGcdAction(BaseAction):
    """Greatest common divisor."""
    action_type = "math_gcd"
    display_name = "最大公约数"
    description = "计算最大公约数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gcd.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with gcd.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'gcd_result')

        try:
            resolved_v1 = int(context.resolve_value(value1))
            resolved_v2 = int(context.resolve_value(value2))

            result = math.gcd(resolved_v1, resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大公约数: gcd({resolved_v1}, {resolved_v2}) = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'gcd': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算最大公约数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'gcd_result'}


class MathLcmAction(BaseAction):
    """Least common multiple."""
    action_type = "math_lcm"
    display_name = "最小公倍数"
    description = "计算最小公倍数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lcm.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with lcm.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'lcm_result')

        try:
            resolved_v1 = int(context.resolve_value(value1))
            resolved_v2 = int(context.resolve_value(value2))

            if resolved_v1 == 0 or resolved_v2 == 0:
                result = 0
            else:
                result = abs(resolved_v1 * resolved_v2) // math.gcd(resolved_v1, resolved_v2)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小公倍数: lcm({resolved_v1}, {resolved_v2}) = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'lcm': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算最小公倍数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lcm_result'}


class MathIsPrimeAction(BaseAction):
    """Check if prime."""
    action_type = "math_is_prime"
    display_name = "检查素数"
    description = "检查是否为素数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is prime.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with prime check result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'is_prime_result')

        try:
            resolved_value = int(context.resolve_value(value))

            if resolved_value < 2:
                result = False
            elif resolved_value == 2:
                result = True
            elif resolved_value % 2 == 0:
                result = False
            else:
                result = all(resolved_value % i != 0 for i in range(3, int(math.sqrt(resolved_value)) + 1, 2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"素数检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_prime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查素数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_prime_result'}
