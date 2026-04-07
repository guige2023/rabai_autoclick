"""Math11 action module for RabAI AutoClick.

Provides additional math operations:
- MathFactorialAction: Calculate factorial
- MathPrimeAction: Check if prime
- MathGCDAction: Calculate GCD
- MathLCMAction: Calculate LCM
- MathPowAction: Calculate power
- MathSqrtAction: Calculate square root
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MathFactorialAction(BaseAction):
    """Calculate factorial."""
    action_type = "math11_factorial"
    display_name = "计算阶乘"
    description = "计算阶乘"
    version = "11.0"

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
            import math

            resolved = int(context.resolve_value(value)) if value else 0

            if resolved < 0:
                return ActionResult(
                    success=False,
                    message=f"阶乘不能为负数: {resolved}"
                )

            result = math.factorial(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"阶乘: {result}",
                data={
                    'value': resolved,
                    'result': result,
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


class MathPrimeAction(BaseAction):
    """Check if prime."""
    action_type = "math11_prime"
    display_name = "判断质数"
    description = "判断是否为质数"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute prime check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with prime check result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'prime_result')

        try:
            import math

            resolved = int(context.resolve_value(value)) if value else 0

            if resolved < 2:
                result = False
            else:
                result = all(resolved % i != 0 for i in range(2, int(math.sqrt(resolved)) + 1))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断质数: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断质数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'prime_result'}


class MathGCDAction(BaseAction):
    """Calculate GCD."""
    action_type = "math11_gcd"
    display_name = "计算最大公约数"
    description = "计算最大公约数"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute GCD.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with GCD.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'gcd_result')

        try:
            import math

            resolved1 = int(context.resolve_value(value1)) if value1 else 0
            resolved2 = int(context.resolve_value(value2)) if value2 else 0

            result = math.gcd(resolved1, resolved2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大公约数: {result}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
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


class MathLCMAction(BaseAction):
    """Calculate LCM."""
    action_type = "math11_lcm"
    display_name = "计算最小公倍数"
    description = "计算最小公倍数"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute LCM.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with LCM.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'lcm_result')

        try:
            import math

            resolved1 = int(context.resolve_value(value1)) if value1 else 0
            resolved2 = int(context.resolve_value(value2)) if value2 else 0

            if resolved1 == 0 or resolved2 == 0:
                result = 0
            else:
                result = abs(resolved1 * resolved2) // math.gcd(resolved1, resolved2)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小公倍数: {result}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
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


class MathPowAction(BaseAction):
    """Calculate power."""
    action_type = "math11_pow"
    display_name = "计算幂"
    description = "计算幂"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute power.

        Args:
            context: Execution context.
            params: Dict with base, exponent, output_var.

        Returns:
            ActionResult with power result.
        """
        base = params.get('base', 0)
        exponent = params.get('exponent', 1)
        output_var = params.get('output_var', 'pow_result')

        try:
            resolved_base = float(context.resolve_value(base)) if base else 0
            resolved_exp = float(context.resolve_value(exponent)) if exponent else 1

            result = resolved_base ** resolved_exp
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"幂: {result}",
                data={
                    'base': resolved_base,
                    'exponent': resolved_exp,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算幂失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['base', 'exponent']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pow_result'}


class MathSqrtAction(BaseAction):
    """Calculate square root."""
    action_type = "math11_sqrt"
    display_name = "计算平方根"
    description = "计算平方根"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute square root.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with square root.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'sqrt_result')

        try:
            import math

            resolved = float(context.resolve_value(value)) if value else 0

            if resolved < 0:
                return ActionResult(
                    success=False,
                    message=f"负数不能计算平方根: {resolved}"
                )

            result = math.sqrt(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平方根: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算平方根失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sqrt_result'}