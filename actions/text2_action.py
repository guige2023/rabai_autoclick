"""Text2 action module for RabAI AutoClick.

Provides additional text operations:
- TextWrapAction: Wrap text
- TextIndentAction: Indent text
- TextDedentAction: Remove indentation
- TextTruncateAction: Truncate text
- TextWordCountAction: Count words
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextWrapAction(BaseAction):
    """Wrap text."""
    action_type = "text2_wrap"
    display_name = "文本换行"
    description = "将文本换行到指定宽度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text wrap.

        Args:
            context: Execution context.
            params: Dict with text, width, output_var.

        Returns:
            ActionResult with wrapped text.
        """
        text = params.get('text', '')
        width = params.get('width', 80)
        output_var = params.get('output_var', 'wrapped_text')

        try:
            import textwrap

            resolved_text = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width))

            result = textwrap.fill(resolved_text, width=resolved_width)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本换行完成",
                data={
                    'original': resolved_text,
                    'result': result,
                    'width': resolved_width,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文本换行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'wrapped_text'}


class TextIndentAction(BaseAction):
    """Indent text."""
    action_type = "text2_indent"
    display_name = "文本缩进"
    description = "为文本添加缩进"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text indent.

        Args:
            context: Execution context.
            params: Dict with text, prefix, output_var.

        Returns:
            ActionResult with indented text.
        """
        text = params.get('text', '')
        prefix = params.get('prefix', '    ')
        output_var = params.get('output_var', 'indented_text')

        try:
            import textwrap

            resolved_text = context.resolve_value(text)
            resolved_prefix = context.resolve_value(prefix)

            result = textwrap.indent(resolved_text, resolved_prefix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本缩进完成",
                data={
                    'original': resolved_text,
                    'result': result,
                    'prefix': resolved_prefix,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文本缩进失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'prefix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'indented_text'}


class TextDedentAction(BaseAction):
    """Remove indentation."""
    action_type = "text2_dedent"
    display_name = "文本去缩进"
    description = "移除文本的公共缩进"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text dedent.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with dedented text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'dedented_text')

        try:
            import textwrap

            resolved_text = context.resolve_value(text)

            result = textwrap.dedent(resolved_text)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本去缩进完成",
                data={
                    'original': resolved_text,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文本去缩进失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dedented_text'}


class TextTruncateAction(BaseAction):
    """Truncate text."""
    action_type = "text2_truncate"
    display_name = "截断文本"
    description = "截断文本到指定长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text truncate.

        Args:
            context: Execution context.
            params: Dict with text, length, suffix, output_var.

        Returns:
            ActionResult with truncated text.
        """
        text = params.get('text', '')
        length = params.get('length', 100)
        suffix = params.get('suffix', '...')
        output_var = params.get('output_var', 'truncated_text')

        try:
            resolved_text = context.resolve_value(text)
            resolved_length = int(context.resolve_value(length))
            resolved_suffix = context.resolve_value(suffix) if suffix else '...'

            if len(resolved_text) <= resolved_length:
                result = resolved_text
            else:
                result = resolved_text[:resolved_length - len(resolved_suffix)] + resolved_suffix

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本截断完成",
                data={
                    'original': resolved_text,
                    'result': result,
                    'length': resolved_length,
                    'suffix': resolved_suffix,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截断文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'length']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'suffix': '...', 'output_var': 'truncated_text'}


class TextWordCountAction(BaseAction):
    """Count words."""
    action_type = "text2_word_count"
    display_name = "统计词数"
    description = "统计文本的单词数量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute word count.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with word count.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'word_count')

        try:
            resolved_text = context.resolve_value(text)

            words = resolved_text.split()
            word_count = len(words)
            char_count = len(resolved_text)
            char_count_no_space = len(resolved_text.replace(' ', ''))

            result = {
                'words': word_count,
                'characters': char_count,
                'characters_no_space': char_count_no_space,
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"词数统计: {word_count} 词, {char_count} 字符",
                data={
                    'word_count': word_count,
                    'char_count': char_count,
                    'char_count_no_space': char_count_no_space,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计词数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'word_count'}