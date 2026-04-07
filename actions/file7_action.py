"""File7 action module for RabAI AutoClick.

Provides additional file operations:
- FileCountLinesAction: Count lines in file
- FileCountWordsAction: Count words in file
- FileCountCharsAction: Count characters in file
- FileGetExtensionAction: Get file extension
- FileRemoveExtensionAction: Remove file extension
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileCountLinesAction(BaseAction):
    """Count lines in file."""
    action_type = "file7_count_lines"
    display_name = "统计文件行数"
    description = "统计文件的行数"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count lines.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with line count.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'line_count')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                lines = len(f.readlines())

            context.set(output_var, lines)

            return ActionResult(
                success=True,
                message=f"行数统计: {lines}",
                data={
                    'file_path': resolved_path,
                    'line_count': lines,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计文件行数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'line_count'}


class FileCountWordsAction(BaseAction):
    """Count words in file."""
    action_type = "file7_count_words"
    display_name = "统计文件单词数"
    description = "统计文件的单词数"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count words.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with word count.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'word_count')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                content = f.read()
                words = len(content.split())

            context.set(output_var, words)

            return ActionResult(
                success=True,
                message=f"单词数统计: {words}",
                data={
                    'file_path': resolved_path,
                    'word_count': words,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计文件单词数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'word_count'}


class FileCountCharsAction(BaseAction):
    """Count characters in file."""
    action_type = "file7_count_chars"
    display_name = "统计文件字符数"
    description = "统计文件的字符数"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count chars.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with char count.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'char_count')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                content = f.read()
                chars = len(content)

            context.set(output_var, chars)

            return ActionResult(
                success=True,
                message=f"字符数统计: {chars}",
                data={
                    'file_path': resolved_path,
                    'char_count': chars,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计文件字符数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'char_count'}


class FileGetExtensionAction(BaseAction):
    """Get file extension."""
    action_type = "file7_get_extension"
    display_name = "获取文件扩展名"
    description = "获取文件的扩展名"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get extension.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with extension.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'extension')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            _, ext = os.path.splitext(resolved_path)
            extension = ext.lstrip('.') if ext else ''

            context.set(output_var, extension)

            return ActionResult(
                success=True,
                message=f"扩展名: {extension}",
                data={
                    'file_path': resolved_path,
                    'extension': extension,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'extension'}


class FileRemoveExtensionAction(BaseAction):
    """Remove file extension."""
    action_type = "file7_remove_extension"
    display_name = "移除文件扩展名"
    description = "移除文件的扩展名"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove extension.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with path without extension.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'path_without_ext')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            base, _ = os.path.splitext(resolved_path)

            context.set(output_var, base)

            return ActionResult(
                success=True,
                message=f"无扩展名路径: {base}",
                data={
                    'original': resolved_path,
                    'path_without_ext': base,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除文件扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_without_ext'}