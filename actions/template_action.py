"""Template action module for RabAI AutoClick.

Provides template operations:
- TemplateRenderAction: Render template with variables
- TemplateFormatAction: Format string with values
"""

import string
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TemplateRenderAction(BaseAction):
    """Render template with variables."""
    action_type = "template_render"
    display_name = "渲染模板"
    description = "使用变量渲染模板字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute template rendering.

        Args:
            context: Execution context.
            params: Dict with template, variables, output_var.

        Returns:
            ActionResult with rendered string.
        """
        template = params.get('template', '')
        variables = params.get('variables', {})
        output_var = params.get('output_var', 'template_result')

        valid, msg = self.validate_type(template, str, 'template')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(variables, dict, 'variables')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_template = context.resolve_value(template)
            resolved_vars = context.resolve_value(variables)

            # Create template and render
            tmpl = string.Template(resolved_template)
            result = tmpl.safe_substitute(resolved_vars)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"模板渲染完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"模板渲染失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['template']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'variables': {}, 'output_var': 'template_result'}


class TemplateFormatAction(BaseAction):
    """Format string with values."""
    action_type = "template_format"
    display_name = "格式化字符串"
    description = "使用.format()格式化字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string formatting.

        Args:
            context: Execution context.
            params: Dict with template, values, output_var.

        Returns:
            ActionResult with formatted string.
        """
        template = params.get('template', '')
        values = params.get('values', {})
        output_var = params.get('output_var', 'template_result')

        valid, msg = self.validate_type(template, str, 'template')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(values, dict, 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_template = context.resolve_value(template)
            resolved_values = context.resolve_value(values)

            result = resolved_template.format(**resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except KeyError as e:
            return ActionResult(
                success=False,
                message=f"格式化失败: 缺少键 {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['template', 'values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'template_result'}