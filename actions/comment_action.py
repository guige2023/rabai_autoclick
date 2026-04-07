"""Comment action module for RabAI AutoClick.

Provides comment operations:
- CommentAction: Add comment
- CommentBlockStartAction: Start comment block
- CommentBlockEndAction: End comment block
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CommentAction(BaseAction):
    """Add comment."""
    action_type = "comment"
    display_name = "添加注释"
    description = "添加注释"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute comment.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult indicating comment.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'comment_text')

        try:
            resolved_text = context.resolve_value(text)
            context.set(output_var, resolved_text)

            return ActionResult(
                success=True,
                message=f"注释: {resolved_text}",
                data={
                    'text': resolved_text,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加注释失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'comment_text'}


class CommentBlockStartAction(BaseAction):
    """Start comment block."""
    action_type = "comment_block_start"
    display_name = "开始注释块"
    description = "开始注释块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute block start.

        Args:
            context: Execution context.
            params: Dict with title.

        Returns:
            ActionResult indicating block start.
        """
        title = params.get('title', 'Comment Block')

        try:
            resolved_title = context.resolve_value(title)
            context.set('_comment_block_title', resolved_title)
            context.set('_comment_block_lines', [])

            return ActionResult(
                success=True,
                message=f"注释块开始: {resolved_title}",
                data={'title': resolved_title}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开始注释块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': 'Comment Block'}


class CommentBlockEndAction(BaseAction):
    """End comment block."""
    action_type = "comment_block_end"
    display_name = "结束注释块"
    description = "结束注释块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute block end.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with block content.
        """
        output_var = params.get('output_var', 'comment_block')

        try:
            title = context.get('_comment_block_title', 'Comment Block')
            lines = context.get('_comment_block_lines', [])

            block_content = f"=== {title} ===\n" + "\n".join(lines)
            context.set(output_var, block_content)

            context.delete('_comment_block_title')
            context.delete('_comment_block_lines')

            return ActionResult(
                success=True,
                message=f"注释块结束: {len(lines)} 行",
                data={
                    'title': title,
                    'lines': len(lines),
                    'content': block_content,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"结束注释块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'comment_block'}
