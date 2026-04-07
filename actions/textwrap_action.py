"""Textwrap action module for RabAI AutoClick.

Provides text wrapping and indentation operations:
- TextwrapWrapAction: Wrap text to specified width
- TextwrapFillAction: Wrap and fill text
- TextwrapIndentAction: Add prefix to each line
- TextwrapDedentAction: Remove common leading whitespace
- TextwrapShortenAction: Truncate text with ellipsis
- TextwrapWrapManyAction: Wrap multiple strings
"""

from typing import Any, Dict, List, Optional, Union
import textwrap
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextwrapWrapAction(BaseAction):
    """Wrap text to specified width."""
    action_type = "textwrap_wrap"
    display_name = "文本换行"
    description = "将文本按指定宽度换行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap wrap operation.

        Args:
            context: Execution context.
            params: Dict with text, width, break_long_words, break_on_hyphens, drop_whitespace, output_var.

        Returns:
            ActionResult with wrapped text (as list of lines).
        """
        text = params.get('text', '')
        width = params.get('width', 70)
        break_long_words = params.get('break_long_words', True)
        break_on_hyphens = params.get('break_on_hyphens', True)
        drop_whitespace = params.get('drop_whitespace', True)
        output_var = params.get('output_var', 'textwrap_result')

        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            resolved_text = context.resolve_value(text)
            resolved_width = context.resolve_value(width)
            resolved_break_long = context.resolve_value(break_long_words)
            resolved_break_hyphen = context.resolve_value(break_on_hyphens)
            resolved_drop_ws = context.resolve_value(drop_whitespace)

            wrapper = textwrap.TextWrapper(
                width=resolved_width,
                break_long_words=resolved_break_long,
                break_on_hyphens=resolved_break_hyphen,
                drop_whitespace=resolved_drop_ws
            )

            wrapped = wrapper.wrap(resolved_text)

            context.set(output_var, wrapped)
            context.set(f'{output_var}_string', '\n'.join(wrapped))
            return ActionResult(success=True, data=wrapped,
                               message=f"Wrapped text to {len(wrapped)} lines")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap wrap error: {str(e)}")


class TextwrapFillAction(BaseAction):
    """Wrap text and fill (single string output)."""
    action_type = "textwrap_fill"
    display_name = "文本填充"
    description = "将文本换行并合并为单字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap fill operation.

        Args:
            context: Execution context.
            params: Dict with text, width, initial_indent, subsequent_indent, output_var.

        Returns:
            ActionResult with filled text string.
        """
        text = params.get('text', '')
        width = params.get('width', 70)
        initial_indent = params.get('initial_indent', '')
        subsequent_indent = params.get('subsequent_indent', '')
        output_var = params.get('output_var', 'textwrap_filled')

        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            resolved_text = context.resolve_value(text)
            resolved_width = context.resolve_value(width)
            resolved_init = context.resolve_value(initial_indent)
            resolved_sub = context.resolve_value(subsequent_indent)

            wrapper = textwrap.TextWrapper(
                width=resolved_width,
                initial_indent=resolved_init,
                subsequent_indent=resolved_sub
            )

            filled = wrapper.fill(resolved_text)

            context.set(output_var, filled)
            return ActionResult(success=True, data=filled,
                               message=f"Filled text: {len(filled)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap fill error: {str(e)}")


class TextwrapIndentAction(BaseAction):
    """Add prefix to each line."""
    action_type = "textwrap_indent"
    display_name = "文本缩进"
    description = "为每行添加前缀/缩进"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap indent operation.

        Args:
            context: Execution context.
            params: Dict with text, prefix, predicate, output_var.

        Returns:
            ActionResult with indented text.
        """
        text = params.get('text', '')
        prefix = params.get('prefix', '    ')
        predicate = params.get('predicate', None)
        output_var = params.get('output_var', 'textwrap_indented')

        if not text:
            return ActionResult(success=False, message="text is required")
        if not prefix:
            return ActionResult(success=False, message="prefix is required")

        try:
            resolved_text = context.resolve_value(text)
            resolved_prefix = context.resolve_value(prefix)

            if predicate is not None:
                resolved_predicate = context.resolve_value(predicate)
                indented = textwrap.indent(resolved_text, resolved_prefix, predicate=resolved_predicate)
            else:
                indented = textwrap.indent(resolved_text, resolved_prefix)

            context.set(output_var, indented)
            return ActionResult(success=True, data=indented,
                               message=f"Indented text: {len(indented)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap indent error: {str(e)}")


class TextwrapDedentAction(BaseAction):
    """Remove common leading whitespace."""
    action_type = "textwrap_dedent"
    display_name = "文本去缩进"
    description = "移除共同的前导空白"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap dedent operation.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with dedented text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'textwrap_dedented')

        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            resolved_text = context.resolve_value(text)

            dedented = textwrap.dedent(resolved_text)

            context.set(output_var, dedented)
            return ActionResult(success=True, data=dedented,
                               message=f"Dedented text: {len(dedented)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap dedent error: {str(e)}")


class TextwrapShortenAction(BaseAction):
    """Truncate text with ellipsis."""
    action_type = "textwrap_shorten"
    display_name = "文本截断"
    description = "截断文本并添加省略号"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap shorten operation.

        Args:
            context: Execution context.
            params: Dict with text, width, placeholder, output_var.

        Returns:
            ActionResult with shortened text.
        """
        text = params.get('text', '')
        width = params.get('width', 80)
        placeholder = params.get('placeholder', ' [...]')
        output_var = params.get('output_var', 'textwrap_shortened')

        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            resolved_text = context.resolve_value(text)
            resolved_width = context.resolve_value(width)
            resolved_placeholder = context.resolve_value(placeholder)

            shortened = textwrap.shorten(
                resolved_text,
                width=resolved_width,
                placeholder=resolved_placeholder
            )

            context.set(output_var, shortened)
            return ActionResult(success=True, data=shortened,
                               message=f"Shortened text: {len(shortened)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap shorten error: {str(e)}")


class TextwrapWrapManyAction(BaseAction):
    """Wrap multiple strings."""
    action_type = "textwrap_wrap_many"
    display_name = "批量文本换行"
    description = "批量将多个字符串换行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute textwrap wrap many operation.

        Args:
            context: Execution context.
            params: Dict with texts, width, break_long_words, output_var.

        Returns:
            ActionResult with list of wrapped texts.
        """
        texts = params.get('texts', [])
        width = params.get('width', 70)
        break_long_words = params.get('break_long_words', True)
        output_var = params.get('output_var', 'textwrap_many_result')

        if not texts:
            return ActionResult(success=False, message="texts is required")

        try:
            resolved_texts = context.resolve_value(texts)
            resolved_width = context.resolve_value(width)
            resolved_break = context.resolve_value(break_long_words)

            wrapper = textwrap.TextWrapper(
                width=resolved_width,
                break_long_words=resolved_break
            )

            results = []
            for text in resolved_texts:
                if isinstance(text, str):
                    wrapped = wrapper.wrap(text)
                    results.append('\n'.join(wrapped))
                else:
                    results.append(text)

            context.set(output_var, results)
            return ActionResult(success=True, data=results,
                               message=f"Wrapped {len(results)} texts")

        except Exception as e:
            return ActionResult(success=False, message=f"Textwrap wrap many error: {str(e)}")
