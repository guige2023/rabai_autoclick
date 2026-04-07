"""HTML2 action module for RabAI AutoClick.

Provides additional HTML operations:
- HTMLStripTagsAction: Strip HTML tags
- HTMLEscapeAction: HTML escape
- HTMLUnescapeAction: HTML unescape
- HTMLGetTextAction: Get plain text from HTML
- HTMLLinkifyAction: Convert URLs to links
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HTMLStripTagsAction(BaseAction):
    """Strip HTML tags."""
    action_type = "html2_strip_tags"
    display_name = "移除HTML标签"
    description = "移除HTML标签"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip tags.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with stripped HTML.
        """
        html = params.get('html', '')
        output_var = params.get('output_var', 'stripped_html')

        try:
            import re

            resolved = context.resolve_value(html)
            result = re.sub(r'<[^>]+>', '', str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"移除HTML标签: 完成",
                data={
                    'original': resolved,
                    'stripped': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除HTML标签失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stripped_html'}


class HTMLEscapeAction(BaseAction):
    """HTML escape."""
    action_type = "html2_escape"
    display_name = "HTML转义"
    description = "HTML特殊字符转义"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML escape.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with escaped HTML.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'escaped_html')

        try:
            import html

            resolved = context.resolve_value(text)
            result = html.escape(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML转义: 完成",
                data={
                    'original': resolved,
                    'escaped': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML转义失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'escaped_html'}


class HTMLUnescapeAction(BaseAction):
    """HTML unescape."""
    action_type = "html2_unescape"
    display_name = "HTML反转义"
    description = "HTML反转义"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML unescape.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with unescaped HTML.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'unescaped_html')

        try:
            import html

            resolved = context.resolve_value(text)
            result = html.unescape(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML反转义: 完成",
                data={
                    'original': resolved,
                    'unescaped': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML反转义失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unescaped_html'}


class HTMLGetTextAction(BaseAction):
    """Get plain text from HTML."""
    action_type = "html2_get_text"
    display_name = "获取HTML文本"
    description = "从HTML提取纯文本"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get text.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with plain text.
        """
        html = params.get('html', '')
        output_var = params.get('output_var', 'html_text')

        try:
            import re

            resolved = context.resolve_value(html)
            result = re.sub(r'<[^>]+>', '', str(resolved))
            result = ' '.join(result.split())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取HTML文本: 完成",
                data={
                    'original': resolved,
                    'text': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取HTML文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_text'}


class HTMLLinkifyAction(BaseAction):
    """Convert URLs to links."""
    action_type = "html2_linkify"
    display_name = "URL转链接"
    description = "将URL转换为HTML链接"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute linkify.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with linkified text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'linkified_text')

        try:
            import re

            resolved = context.resolve_value(text)
            result = re.sub(
                r'(http[s]?://\S+)',
                r'<a href="\1">\1</a>',
                str(resolved)
            )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL转链接: 完成",
                data={
                    'original': resolved,
                    'linkified': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL转链接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'linkified_text'}