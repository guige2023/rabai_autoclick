"""HTML action module for RabAI AutoClick.

Provides HTML operations:
- HtmlParseAction: Parse HTML
- HtmlEscapeAction: Escape HTML
- HtmlUnescapeAction: Unescape HTML
- HtmlStripAction: Strip HTML tags
- HtmlExtractTextAction: Extract text from HTML
"""

import html as html_module
import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HtmlParseAction(BaseAction):
    """Parse HTML."""
    action_type = "html_parse"
    display_name = "HTML解析"
    description = "解析HTML"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with parsed data.
        """
        html_content = params.get('html', '')
        output_var = params.get('output_var', 'html_parsed')

        valid, msg = self.validate_type(html_content, str, 'html')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_html = context.resolve_value(html_content)

            tags = re.findall(r'<(\w+)', resolved_html)
            attributes = re.findall(r'(\w+)=["\']([^"\']*)["\']', resolved_html)

            result = {
                'tags': list(set(tags)),
                'attributes': dict(attributes),
                'length': len(resolved_html),
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML解析完成: {len(tags)} 个标签",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HTML解析失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_parsed'}


class HtmlEscapeAction(BaseAction):
    """Escape HTML."""
    action_type = "html_escape"
    display_name = "HTML转义"
    description = "HTML特殊字符转义"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute escape.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with escaped HTML.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'html_escaped')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            escaped = html_module.escape(resolved_text)

            context.set(output_var, escaped)

            return ActionResult(
                success=True,
                message=f"HTML已转义",
                data={'escaped': escaped, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HTML转义失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_escaped'}


class HtmlUnescapeAction(BaseAction):
    """Unescape HTML."""
    action_type = "html_unescape"
    display_name = "HTML反转义"
    description = "HTML反转义"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unescape.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with unescaped HTML.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'html_unescaped')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            unescaped = html_module.unescape(resolved_text)

            context.set(output_var, unescaped)

            return ActionResult(
                success=True,
                message=f"HTML已反转义",
                data={'unescaped': unescaped, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HTML反转义失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_unescaped'}


class HtmlStripAction(BaseAction):
    """Strip HTML tags."""
    action_type = "html_strip"
    display_name = "去除HTML标签"
    description = "去除HTML标签"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with stripped text.
        """
        html_content = params.get('html', '')
        output_var = params.get('output_var', 'html_stripped')

        valid, msg = self.validate_type(html_content, str, 'html')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_html = context.resolve_value(html_content)

            stripped = re.sub(r'<[^>]+>', '', resolved_html)
            stripped = re.sub(r'\s+', ' ', stripped).strip()

            context.set(output_var, stripped)

            return ActionResult(
                success=True,
                message=f"HTML标签已去除",
                data={'stripped': stripped, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"去除HTML标签失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_stripped'}


class HtmlExtractTextAction(BaseAction):
    """Extract text from HTML."""
    action_type = "html_extract_text"
    display_name = "提取HTML文本"
    description = "提取HTML文本"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with extracted text.
        """
        html_content = params.get('html', '')
        output_var = params.get('output_var', 'html_text')

        valid, msg = self.validate_type(html_content, str, 'html')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_html = context.resolve_value(html_content)

            stripped = re.sub(r'<script[^>]*>.*?</script>', '', resolved_html, flags=re.DOTALL | re.IGNORECASE)
            stripped = re.sub(r'<style[^>]*>.*?</style>', '', stripped, flags=re.DOTALL | re.IGNORECASE)
            stripped = re.sub(r'<[^>]+>', '', stripped)
            stripped = re.sub(r'&nbsp;', ' ', stripped)
            stripped = re.sub(r'&[a-z]+;', '', stripped)
            stripped = re.sub(r'\s+', ' ', stripped).strip()

            context.set(output_var, stripped)

            return ActionResult(
                success=True,
                message=f"HTML文本已提取: {len(stripped)} 字符",
                data={'text': stripped, 'length': len(stripped), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"提取HTML文本失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_text'}
