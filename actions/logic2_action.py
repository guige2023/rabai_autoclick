"""Logic2 action module for RabAI AutoClick.

Provides additional logic operations:
- LogicAndAction: Logical AND
- LogicOrAction: Logical OR
- LogicNotAction: Logical NOT
- LogicXorAction: Logical XOR
- LogicNandAction: Logical NAND
- LogicNorAction: Logical NOR
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogicAndAction(BaseAction):
    """Logical AND."""
    action_type = "logic_and"
    display_name = "逻辑与"
    description = "逻辑与运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical and.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with logical AND result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'and_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))

            result = resolved_v1 and resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与: {resolved_v1} AND {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑与失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'and_result'}


class LogicOrAction(BaseAction):
    """Logical OR."""
    action_type = "logic_or"
    display_name = "逻辑或"
    description = "逻辑或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical or.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with logical OR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'or_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))

            result = resolved_v1 or resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或: {resolved_v1} OR {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'or_result'}


class LogicNotAction(BaseAction):
    """Logical NOT."""
    action_type = "logic_not"
    display_name = "逻辑非"
    description = "逻辑非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical not.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with logical NOT result.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'not_result')

        try:
            resolved_value = bool(context.resolve_value(value))

            result = not resolved_value
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑非: NOT {resolved_value} = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'not_result'}


class LogicXorAction(BaseAction):
    """Logical XOR."""
    action_type = "logic_xor"
    display_name = "逻辑异或"
    description = "逻辑异或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical xor.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with logical XOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'xor_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))

            result = resolved_v1 != resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑异或: {resolved_v1} XOR {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑异或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xor_result'}


class LogicNandAction(BaseAction):
    """Logical NAND."""
    action_type = "logic_nand"
    display_name = "逻辑与非"
    description = "逻辑与非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical nand.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with logical NAND result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'nand_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))

            result = not (resolved_v1 and resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与非: {resolved_v1} NAND {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑与非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nand_result'}


class LogicNorAction(BaseAction):
    """Logical NOR."""
    action_type = "logic_nor"
    display_name = "逻辑或非"
    description = "逻辑或非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical nor.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with logical NOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'nor_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))

            result = not (resolved_v1 or resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或非: {resolved_v1} NOR {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑或非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nor_result'}
