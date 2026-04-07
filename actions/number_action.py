"""Number action module for RabAI AutoClick.

Provides number operations:
- NumberAbsAction: Absolute value
- NumberSignAction: Sign of number
- NumberEvenAction: Check if even
- NumberOddAction: Check if odd
- NumberPrimeAction: Check if prime
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NumberAbsAction(BaseAction):
    """Absolute value."""
    action_type = "number_abs"
    display_name = "绝对值"
    description = "计算数字的绝对值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute absolute value.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with absolute value.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'abs_result')

        try:
            resolved = float(context.resolve_value(value))
            result = abs(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对值: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"绝对值计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'abs_result'}


class NumberSignAction(BaseAction):
    """Sign of number."""
    action_type = "number_sign"
    display_name = "数字符号"
    description = "获取数字的正负符号"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sign.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with sign.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'sign_result')

        try:
            resolved = float(context.resolve_value(value))

            if resolved > 0:
                result = 1
                sign_str = '正数'
            elif resolved < 0:
                result = -1
                sign_str = '负数'
            else:
                result = 0
                sign_str = '零'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数字符号: {sign_str}",
                data={
                    'value': resolved,
                    'result': result,
                    'sign': sign_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取数字符号失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sign_result'}


class NumberEvenAction(BaseAction):
    """Check if even."""
    action_type = "number_even"
    display_name = "判断偶数"
    description = "判断数字是否为偶数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute even check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with even check result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'even_result')

        try:
            resolved = int(context.resolve_value(value))
            result = resolved % 2 == 0
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断偶数: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断偶数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'even_result'}


class NumberOddAction(BaseAction):
    """Check if odd."""
    action_type = "number_odd"
    display_name = "判断奇数"
    description = "判断数字是否为奇数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute odd check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with odd check result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'odd_result')

        try:
            resolved = int(context.resolve_value(value))
            result = resolved % 2 != 0
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"判断奇数: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断奇数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'odd_result'}


class NumberPrimeAction(BaseAction):
    """Check if prime."""
    action_type = "number_prime"
    display_name = "判断质数"
    description = "判断数字是否为质数"
    version = "1.0"

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
            resolved = int(context.resolve_value(value))

            if resolved < 2:
                result = False
            elif resolved == 2:
                result = True
            elif resolved % 2 == 0:
                result = False
            else:
                result = True
                for i in range(3, int(resolved**0.5) + 1, 2):
                    if resolved % i == 0:
                        result = False
                        break

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