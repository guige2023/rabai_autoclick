"""Variable action module for RabAI AutoClick.

Provides actions for manipulating workflow variables:
- SetVariableAction: Set a variable value
- GetVariableAction: Get a variable value
- DeleteVariableAction: Delete a variable
- ClearVariablesAction: Clear all variables
- MathOperationAction: Perform mathematical operations
- StringOperationAction: Perform string operations
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SetVariableAction(BaseAction):
    """Set a variable in the workflow context."""
    action_type = "set_variable"
    display_name = "设置变量"
    description = "设置工作流上下文中的变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute setting a variable.

        Args:
            context: Execution context.
            params: Dict with name, value, expression.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')
        value = params.get('value', None)
        expression = params.get('expression', None)

        # Validate name
        if not name:
            return ActionResult(
                success=False,
                message="未指定变量名"
            )
        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # If expression is provided, evaluate it
            if expression is not None:
                if isinstance(expression, str):
                    resolved = context.resolve_value(expression)
                    if isinstance(resolved, str) and '{{' in expression:
                        # It was a variable reference that was resolved
                        value = resolved
                    else:
                        value = context.safe_exec(expression)
                else:
                    value = expression

            context.set(name, value)

            return ActionResult(
                success=True,
                message=f"变量已设置: {name} = {repr(value)[:50]}",
                data={'name': name, 'value': value}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value': None,
            'expression': None
        }


class GetVariableAction(BaseAction):
    """Get a variable value from the workflow context."""
    action_type = "get_variable"
    display_name = "获取变量"
    description = "获取工作流上下文中的变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting a variable.

        Args:
            context: Execution context.
            params: Dict with name, default, output_var.

        Returns:
            ActionResult with variable value.
        """
        name = params.get('name', '')
        default = params.get('default', None)
        output_var = params.get('output_var', None)

        # Validate name
        if not name:
            return ActionResult(
                success=False,
                message="未指定变量名"
            )
        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            value = context.get(name, default)

            # Store in output variable if specified
            if output_var:
                context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取变量: {name} = {repr(value)[:50]}",
                data={'name': name, 'value': value}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'default': None,
            'output_var': None
        }


class DeleteVariableAction(BaseAction):
    """Delete a variable from the workflow context."""
    action_type = "delete_variable"
    display_name = "删除变量"
    description = "删除工作流上下文中的变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deleting a variable.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')

        # Validate name
        if not name:
            return ActionResult(
                success=False,
                message="未指定变量名"
            )
        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            deleted = context.delete(name)

            if deleted:
                return ActionResult(
                    success=True,
                    message=f"变量已删除: {name}",
                    data={'name': name, 'deleted': True}
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"变量不存在: {name}",
                    data={'name': name, 'deleted': False}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ClearVariablesAction(BaseAction):
    """Clear all variables from the workflow context."""
    action_type = "clear_variables"
    display_name = "清除变量"
    description = "清除工作流上下文中的所有变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clearing all variables.

        Args:
            context: Execution context.
            params: Dict with confirm (bool).

        Returns:
            ActionResult indicating success.
        """
        confirm = params.get('confirm', True)

        valid, msg = self.validate_type(confirm, bool, 'confirm')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            context.clear()

            return ActionResult(
                success=True,
                message="所有变量已清除",
                data={'cleared': True}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清除变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'confirm': True}


class MathOperationAction(BaseAction):
    """Perform mathematical operations on variables."""
    action_type = "math_op"
    display_name = "数学运算"
    description = "对变量执行数学运算"

    VALID_OPERATIONS: List[str] = [
        'add', 'subtract', 'multiply', 'divide',
        'modulo', 'power', 'floor_div',
        'abs', 'round', 'min', 'max'
    ]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mathematical operation.

        Args:
            context: Execution context.
            params: Dict with operation, operand1, operand2, output_var.

        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '')
        operand1 = params.get('operand1', 0)
        operand2 = params.get('operand2', 0)
        output_var = params.get('output_var', '_math_result')

        # Validate operation
        if not operation:
            return ActionResult(
                success=False,
                message="未指定运算类型"
            )
        valid, msg = self.validate_in(
            operation, self.VALID_OPERATIONS, 'operation'
        )
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result: float

            # Resolve operands if they are strings (variable references)
            op1 = operand1
            op2 = operand2

            if isinstance(operand1, str):
                op1 = context.get(operand1, operand1)
            if isinstance(operand2, str):
                op2 = context.get(operand2, operand2)

            # Convert to numbers
            try:
                op1 = float(op1) if op1 is not None else 0
                op2 = float(op2) if op2 is not None else 0
            except (ValueError, TypeError):
                return ActionResult(
                    success=False,
                    message=f"无法将操作数转换为数字: {op1}, {op2}"
                )

            # Perform operation
            if operation == 'add':
                result = op1 + op2
            elif operation == 'subtract':
                result = op1 - op2
            elif operation == 'multiply':
                result = op1 * op2
            elif operation == 'divide':
                if op2 == 0:
                    return ActionResult(
                        success=False,
                        message="除数不能为零"
                    )
                result = op1 / op2
            elif operation == 'modulo':
                if op2 == 0:
                    return ActionResult(
                        success=False,
                        message="除数不能为零"
                    )
                result = op1 % op2
            elif operation == 'power':
                result = op1 ** op2
            elif operation == 'floor_div':
                if op2 == 0:
                    return ActionResult(
                        success=False,
                        message="除数不能为零"
                    )
                result = op1 // op2
            elif operation == 'abs':
                result = abs(op1)
            elif operation == 'round':
                result = round(op1, int(op2) if op2 else 0)
            elif operation == 'min':
                result = min(op1, op2)
            elif operation == 'max':
                result = max(op1, op2)

            # Store result
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"运算完成: {operand1} {operation} {operand2} = {result}",
                data={
                    'operation': operation,
                    'operand1': op1,
                    'operand2': op2,
                    'result': result
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数学运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'operand1': 0,
            'operand2': 0,
            'output_var': '_math_result'
        }


class StringOperationAction(BaseAction):
    """Perform string operations on variables."""
    action_type = "string_op"
    display_name = "字符串操作"
    description = "对字符串执行操作"

    VALID_OPERATIONS: List[str] = [
        'concat', 'substring', 'replace', 'split',
        'upper', 'lower', 'strip', 'length', 'contains',
        'startswith', 'endswith'
    ]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a string operation.

        Args:
            context: Execution context.
            params: Dict with operation, value, param1, param2, output_var.

        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '')
        value = params.get('value', '')
        param1 = params.get('param1', '')
        param2 = params.get('param2', '')
        output_var = params.get('output_var', '_string_result')

        # Validate operation
        if not operation:
            return ActionResult(
                success=False,
                message="未指定字符串操作类型"
            )
        valid, msg = self.validate_in(
            operation, self.VALID_OPERATIONS, 'operation'
        )
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Resolve value if it's a variable reference
            str_value = value
            if isinstance(value, str):
                resolved = context.get(value, value)
                if resolved != value:
                    str_value = resolved

            str_value = str(str_value) if str_value is not None else ''

            result: Any = None

            if operation == 'concat':
                result = str_value + str(param1)
            elif operation == 'substring':
                start = int(param1) if param1 else 0
                end = int(param2) if param2 else None
                result = str_value[start:end]
            elif operation == 'replace':
                result = str_value.replace(str(param1), str(param2))
            elif operation == 'split':
                result = str_value.split(str(param1))
            elif operation == 'upper':
                result = str_value.upper()
            elif operation == 'lower':
                result = str_value.lower()
            elif operation == 'strip':
                result = str_value.strip()
            elif operation == 'length':
                result = len(str_value)
            elif operation == 'contains':
                result = str(param1) in str_value
            elif operation == 'startswith':
                result = str_value.startswith(str(param1))
            elif operation == 'endswith':
                result = str_value.endswith(str(param1))

            # Store result
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串操作完成: {operation}",
                data={
                    'operation': operation,
                    'input': str_value[:50],
                    'result': str(result)[:50] if isinstance(result, str) else result
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串操作失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value': '',
            'param1': '',
            'param2': '',
            'output_var': '_string_result'
        }