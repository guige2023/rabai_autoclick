"""HTML action module for RabAI AutoClick.

Provides HTML operations:
- HtmlEscapeAction: Escape HTML
- HtmlUnescapeAction: Unescape HTML
- HtmlStripTagsAction: Strip HTML tags
- HtmlContainsAction: Check if contains tag
"""

import re
import html
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HtmlEscapeAction(BaseAction):
    """Escape HTML."""
    action_type = "html_escape"
    display_name = "HTML转义"
    description = "转义HTML特殊字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML escaping.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with escaped string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'html_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = html.escape(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="HTML转义完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML转义失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_result'}


class HtmlUnescapeAction(BaseAction):
    """Unescape HTML."""
    action_type = "html_unescape"
    display_name = "HTML反转义"
    description = "反转义HTML实体"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML unescaping.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with unescaped string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'html_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = html.unescape(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="HTML反转义完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML反转义失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_result'}


class HtmlStripTagsAction(BaseAction):
    """Strip HTML tags."""
    action_type = "html_strip_tags"
    display_name = "去除HTML标签"
    description = "去除HTML标签"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML tag stripping.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with stripped string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'html_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = re.sub(r'<[^>]+>', '', resolved)
            # Also unescape any HTML entities
            result = html.unescape(result)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="去除HTML标签完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除HTML标签失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_result'}


class HtmlContainsAction(BaseAction):
    """Check if HTML contains tag."""
    action_type = "html_contains_tag"
    display_name = "检查HTML标签"
    description = "检查HTML是否包含指定标签"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML tag check.

        Args:
            context: Execution context.
            params: Dict with value, tag, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', '')
        tag = params.get('tag', '')
        output_var = params.get('output_var', 'html_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(tag, str, 'tag')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_tag = context.resolve_value(tag)

            # Simple check for tag presence
            pattern = f'<{resolved_tag}[^>]*>'
            result = bool(re.search(pattern, resolved_value, re.IGNORECASE))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标签检查: {'包含' if result else '不包含'}",
                data={
                    'contains': result,
                    'tag': resolved_tag,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查HTML标签失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'tag']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_result'}