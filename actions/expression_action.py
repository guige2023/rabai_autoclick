"""Expression action module for RabAI AutoClick.

Provides expression evaluation operations:
- ExpressionEvaluateAction: Evaluate expression
- ExpressionParseAction: Parse expression
- ExpressionCompileAction: Compile expression
- ExpressionVariablesAction: Get expression variables
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExpressionEvaluateAction(BaseAction):
    """Evaluate expression."""
    action_type = "expression_evaluate"
    display_name = "计算表达式"
    description = "计算表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute evaluate.

        Args:
            context: Execution context.
            params: Dict with expression, output_var.

        Returns:
            ActionResult with evaluation result.
        """
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'expression_result')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(expression)

            result = context.safe_exec(f"return_value = {resolved}")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"表达式计算: {result}",
                data={
                    'expression': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算表达式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expression_result'}


class ExpressionParseAction(BaseAction):
    """Parse expression."""
    action_type = "expression_parse"
    display_name = "解析表达式"
    description = "解析表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with expression.

        Returns:
            ActionResult with parsed result.
        """
        expression = params.get('expression', '')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(expression)

            import ast
            tree = ast.parse(resolved, mode='eval')

            node_types = []
            for node in ast.walk(tree):
                node_types.append(type(node).__name__)

            return ActionResult(
                success=True,
                message=f"表达式解析: {len(node_types)} 节点",
                data={
                    'expression': resolved,
                    'node_types': node_types,
                    'valid': True
                }
            )
        except SyntaxError as e:
            return ActionResult(
                success=True,
                message=f"表达式解析失败: {str(e)}",
                data={
                    'expression': resolved,
                    'valid': False,
                    'error': str(e)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析表达式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ExpressionCompileAction(BaseAction):
    """Compile expression."""
    action_type = "expression_compile"
    display_name = "编译表达式"
    description = "编译表达式为代码对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compile.

        Args:
            context: Execution context.
            params: Dict with expression, output_var.

        Returns:
            ActionResult with compile result.
        """
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'compiled_code')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(expression)

            code = compile(resolved, '<string>', 'eval')

            return ActionResult(
                success=True,
                message="表达式编译成功",
                data={
                    'expression': resolved,
                    'compiled': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"编译表达式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compiled_code'}


class ExpressionVariablesAction(BaseAction):
    """Get expression variables."""
    action_type = "expression_variables"
    display_name = "获取表达式变量"
    description = "获取表达式中的变量名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute variables.

        Args:
            context: Execution context.
            params: Dict with expression, output_var.

        Returns:
            ActionResult with variable names.
        """
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'expression_variables')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(expression)

            import ast
            tree = ast.parse(resolved, mode='eval')

            variables = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    variables.add(node.id)

            result = sorted(list(variables))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"表达式变量: {result}",
                data={
                    'expression': resolved,
                    'variables': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取表达式变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expression_variables'}
