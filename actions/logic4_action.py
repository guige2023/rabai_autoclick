"""Logic4 action module for RabAI AutoClick.

Provides additional logic operations:
- LogicNandAction: NAND operation
- LogicNorAction: NOR operation
- LogicXnorAction: XNOR operation
- LogicImpliesAction: Logical implication
- LogicEquivalentAction: Logical equivalence
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogicNandAction(BaseAction):
    """NAND operation."""
    action_type = "logic4_nand"
    display_name = "与非"
    description = "逻辑与非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NAND.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with NAND result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'nand_result')

        try:
            resolved1 = bool(context.resolve_value(value1))
            resolved2 = bool(context.resolve_value(value2))

            result = not (resolved1 and resolved2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"与非: not ({resolved1} and {resolved2}) = {result}",
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
                message=f"与非运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nand_result'}


class LogicNorAction(BaseAction):
    """NOR operation."""
    action_type = "logic4_nor"
    display_name = "或非"
    description = "逻辑或非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NOR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with NOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'nor_result')

        try:
            resolved1 = bool(context.resolve_value(value1))
            resolved2 = bool(context.resolve_value(value2))

            result = not (resolved1 or resolved2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"或非: not ({resolved1} or {resolved2}) = {result}",
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
                message=f"或非运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nor_result'}


class LogicXnorAction(BaseAction):
    """XNOR operation."""
    action_type = "logic4_xnor"
    display_name = "异或非"
    description = "逻辑异或非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XNOR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with XNOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'xnor_result')

        try:
            resolved1 = bool(context.resolve_value(value1))
            resolved2 = bool(context.resolve_value(value2))

            result = resolved1 == resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"异或非: {resolved1} == {resolved2} = {result}",
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
                message=f"异或非运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xnor_result'}


class LogicImpliesAction(BaseAction):
    """Logical implication."""
    action_type = "logic4_implies"
    display_name = "蕴含"
    description = "逻辑蕴含 (p -> q)"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute implication.

        Args:
            context: Execution context.
            params: Dict with p, q, output_var.

        Returns:
            ActionResult with implication result.
        """
        p = params.get('p', False)
        q = params.get('q', False)
        output_var = params.get('output_var', 'implies_result')

        try:
            resolved_p = bool(context.resolve_value(p))
            resolved_q = bool(context.resolve_value(q))

            result = (not resolved_p) or resolved_q
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"蕴含: {resolved_p} -> {resolved_q} = {result}",
                data={
                    'p': resolved_p,
                    'q': resolved_q,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"蕴含运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['p', 'q']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'implies_result'}


class LogicEquivalentAction(BaseAction):
    """Logical equivalence."""
    action_type = "logic4_equivalent"
    display_name = "等价"
    description = "逻辑等价 (p <-> q)"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute equivalence.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with equivalence result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'equivalent_result')

        try:
            resolved1 = bool(context.resolve_value(value1))
            resolved2 = bool(context.resolve_value(value2))

            result = resolved1 == resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"等价: {resolved1} <-> {resolved2} = {result}",
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
                message=f"等价运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'equivalent_result'}
