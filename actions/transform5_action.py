"""Transform5 action module for RabAI AutoClick.

Provides additional transform operations:
- TransformCamelCaseAction: Convert to camelCase
- TransformSnakeCaseAction: Convert to snake_case
- TransformPascalCaseAction: Convert to PascalCase
- TransformKebabCaseAction: Convert to kebab-case
- TransformTitleCaseAction: Convert to Title Case
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformCamelCaseAction(BaseAction):
    """Convert to camelCase."""
    action_type = "transform5_camel"
    display_name = "驼峰命名"
    description = "转换为驼峰命名"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute camelCase.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with camelCase text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'camel_text')

        try:
            resolved = context.resolve_value(text)
            words = str(resolved).replace('-', ' ').replace('_', ' ').split()
            if words:
                result = words[0].lower() + ''.join(w.capitalize() for w in words[1:])
            else:
                result = ''
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"驼峰命名: {result}",
                data={
                    'original': resolved,
                    'camel': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"驼峰命名转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'camel_text'}


class TransformSnakeCaseAction(BaseAction):
    """Convert to snake_case."""
    action_type = "transform5_snake"
    display_name = "蛇形命名"
    description = "转换为蛇形命名"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute snake_case.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with snake_case text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'snake_text')

        try:
            import re
            resolved = context.resolve_value(text)
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str(resolved))
            result = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).replace('-', '_').replace(' ', '_').lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"蛇形命名: {result}",
                data={
                    'original': resolved,
                    'snake': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"蛇形命名转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'snake_text'}


class TransformPascalCaseAction(BaseAction):
    """Convert to PascalCase."""
    action_type = "transform5_pascal"
    display_name = "帕斯卡命名"
    description = "转换为帕斯卡命名"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute PascalCase.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with PascalCase text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'pascal_text')

        try:
            resolved = context.resolve_value(text)
            words = str(resolved).replace('-', ' ').replace('_', ' ').split()
            result = ''.join(w.capitalize() for w in words)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"帕斯卡命名: {result}",
                data={
                    'original': resolved,
                    'pascal': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"帕斯卡命名转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pascal_text'}


class TransformKebabCaseAction(BaseAction):
    """Convert to kebab-case."""
    action_type = "transform5_kebab"
    display_name = "短横线命名"
    description = "转换为短横线命名"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute kebab-case.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with kebab-case text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'kebab_text')

        try:
            import re
            resolved = context.resolve_value(text)
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', str(resolved))
            result = re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).replace('_', '-').replace(' ', '-').lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"短横线命名: {result}",
                data={
                    'original': resolved,
                    'kebab': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"短横线命名转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'kebab_text'}


class TransformTitleCaseAction(BaseAction):
    """Convert to Title Case."""
    action_type = "transform5_title"
    display_name = "标题命名"
    description = "转换为首字母大写"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Title Case.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with Title Case text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'title_text')

        try:
            resolved = context.resolve_value(text)
            result = str(resolved).title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标题命名: {result}",
                data={
                    'original': resolved,
                    'title': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"标题命名转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'title_text'}