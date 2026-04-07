"""Condition action module for RabAI AutoClick.

Provides conditional operations:
- ConditionIfAction: If condition
- ConditionSwitchAction: Switch case
- ConditionAndAction: Logical AND
- ConditionOrAction: Logical OR
- ConditionNotAction: Logical NOT
- ConditionCompareAction: Compare values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConditionIfAction(BaseAction):
    """If condition."""
    action_type = "condition_if"
    display_name = "条件判断"
    description = "条件判断"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute if.

        Args:
            context: Execution context.
            params: Dict with value, operator, compare_value, then_value, else_value, output_var.

        Returns:
            ActionResult with selected value.
        """
        value = params.get('value', None)
        operator = params.get('operator', 'equals')
        compare_value = params.get('compare_value', None)
        then_value = params.get('then_value', True)
        else_value = params.get('else_value', False)
        output_var = params.get('output_var', 'condition_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_op = context.resolve_value(operator)
            resolved_compare = context.resolve_value(compare_value) if compare_value is not None else None
            resolved_then = context.resolve_value(then_value)
            resolved_else = context.resolve_value(else_value)

            result = self._evaluate(resolved_value, resolved_op, resolved_compare)

            selected = resolved_then if result else resolved_else
            context.set(output_var, selected)

            return ActionResult(
                success=True,
                message=f"条件{'满足' if result else '不满足'}: {selected}",
                data={'result': selected, 'condition_met': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"条件判断失败: {str(e)}")

    def _evaluate(self, value: Any, op: str, compare: Any) -> bool:
        if op == 'equals':
            return value == compare
        elif op == 'not_equals':
            return value != compare
        elif op == 'greater':
            return float(value) > float(compare) if value and compare else False
        elif op == 'less':
            return float(value) < float(compare) if value and compare else False
        elif op == 'greater_equals':
            return float(value) >= float(compare) if value and compare else False
        elif op == 'less_equals':
            return float(value) <= float(compare) if value and compare else False
        elif op == 'contains':
            return str(compare) in str(value) if value else False
        elif op == 'truthy':
            return bool(value)
        elif op == 'falsy':
            return not value
        return False

    def get_required_params(self) -> List[str]:
        return ['value', 'operator', 'compare_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'then_value': True, 'else_value': False, 'output_var': 'condition_result'}


class ConditionSwitchAction(BaseAction):
    """Switch case."""
    action_type = "condition_switch"
    display_name = "分支选择"
    description = "分支选择"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute switch.

        Args:
            context: Execution context.
            params: Dict with value, cases, default, output_var.

        Returns:
            ActionResult with matched value.
        """
        value = params.get('value', None)
        cases = params.get('cases', {})
        default = params.get('default', None)
        output_var = params.get('output_var', 'switch_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_cases = context.resolve_value(cases) if cases else {}
            resolved_default = context.resolve_value(default) if default is not None else None

            result = resolved_cases.get(resolved_value, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分支匹配: {result}",
                data={'result': result, 'matched': resolved_value, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"分支选择失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'cases']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'switch_result'}


class ConditionAndAction(BaseAction):
    """Logical AND."""
    action_type = "condition_and"
    display_name = "逻辑与"
    description = "逻辑与运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute AND.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with AND result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'and_result')

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = context.resolve_value(values)
            result = all(resolved_values)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与: {result}",
                data={'result': result, 'values': resolved_values, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"逻辑与失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'and_result'}


class ConditionOrAction(BaseAction):
    """Logical OR."""
    action_type = "condition_or"
    display_name = "逻辑或"
    description = "逻辑或运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OR.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with OR result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'or_result')

        valid, msg = self.validate_type(values, list, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = context.resolve_value(values)
            result = any(resolved_values)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或: {result}",
                data={'result': result, 'values': resolved_values, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"逻辑或失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'or_result'}


class ConditionNotAction(BaseAction):
    """Logical NOT."""
    action_type = "condition_not"
    display_name = "逻辑非"
    description = "逻辑非运算"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NOT.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with NOT result.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'not_result')

        try:
            resolved_value = context.resolve_value(value)
            result = not resolved_value

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑非: {result}",
                data={'result': result, 'input': resolved_value, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"逻辑非失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'not_result'}


class ConditionCompareAction(BaseAction):
    """Compare values."""
    action_type = "condition_compare"
    display_name = "值比较"
    description = "比较两个值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compare.

        Args:
            context: Execution context.
            params: Dict with value1, operator, value2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value1 = params.get('value1', None)
        operator = params.get('operator', 'equals')
        value2 = params.get('value2', None)
        output_var = params.get('output_var', 'compare_result')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_op = context.resolve_value(operator)
            resolved_v2 = context.resolve_value(value2)

            result = self._compare(resolved_v1, resolved_op, resolved_v2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {result}",
                data={'result': result, 'value1': resolved_v1, 'operator': resolved_op, 'value2': resolved_v2, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"值比较失败: {str(e)}")

    def _compare(self, v1: Any, op: str, v2: Any) -> bool:
        if op == 'equals' or op == '==':
            return v1 == v2
        elif op == 'not_equals' or op == '!=':
            return v1 != v2
        elif op == 'greater' or op == '>':
            try:
                return float(v1) > float(v2)
            except:
                return str(v1) > str(v2)
        elif op == 'less' or op == '<':
            try:
                return float(v1) < float(v2)
            except:
                return str(v1) < str(v2)
        elif op == 'greater_equals' or op == '>=':
            try:
                return float(v1) >= float(v2)
            except:
                return str(v1) >= str(v2)
        elif op == 'less_equals' or op == '<=':
            try:
                return float(v1) <= float(v2)
            except:
                return str(v1) <= str(v2)
        return False

    def get_required_params(self) -> List[str]:
        return ['value1', 'operator', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}
