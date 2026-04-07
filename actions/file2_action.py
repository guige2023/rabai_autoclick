"""File2 action module for RabAI AutoClick.

Provides advanced file operations:
- FileCopyAction: Copy file
- FileMoveAction: Move file
- FileRenameAction: Rename file
- FileTouchAction: Touch file (create/update timestamp)
"""

import shutil
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件到目标位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with source, destination.

        Returns:
            ActionResult indicating success.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源文件不存在: {resolved_source}"
                )

            if os.path.isdir(resolved_source):
                shutil.copytree(resolved_source, resolved_dest)
            else:
                shutil.copy2(resolved_source, resolved_dest)

            return ActionResult(
                success=True,
                message=f"文件已复制: {resolved_source} -> {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileMoveAction(BaseAction):
    """Move file."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动文件到目标位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute move.

        Args:
            context: Execution context.
            params: Dict with source, destination.

        Returns:
            ActionResult indicating success.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源文件不存在: {resolved_source}"
                )

            shutil.move(resolved_source, resolved_dest)

            return ActionResult(
                success=True,
                message=f"文件已移动: {resolved_source} -> {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileRenameAction(BaseAction):
    """Rename file."""
    action_type = "file_rename"
    display_name = "重命名文件"
    description = "重命名文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rename.

        Args:
            context: Execution context.
            params: Dict with path, new_name.

        Returns:
            ActionResult indicating success.
        """
        path = params.get('path', '')
        new_name = params.get('new_name', '')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new_name, str, 'new_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_new_name = context.resolve_value(new_name)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            directory = os.path.dirname(resolved_path)
            new_path = os.path.join(directory, resolved_new_name)

            os.rename(resolved_path, new_path)

            return ActionResult(
                success=True,
                message=f"文件已重命名: {resolved_path} -> {new_path}",
                data={
                    'old_path': resolved_path,
                    'new_path': new_path
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重命名文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'new_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileTouchAction(BaseAction):
    """Touch file (create/update timestamp)."""
    action_type = "file_touch"
    display_name = "更新文件时间戳"
    description = "更新文件时间戳或创建新文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute touch.

        Args:
            context: Execution context.
            params: Dict with path.

        Returns:
            ActionResult indicating success.
        """
        path = params.get('path', '')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            if os.path.exists(resolved_path):
                # Update timestamp
                os.utime(resolved_path, None)
                message = f"时间戳已更新: {resolved_path}"
            else:
                # Create new file
                open(resolved_path, 'a').close()
                message = f"文件已创建: {resolved_path}"

            return ActionResult(
                success=True,
                message=message,
                data={'path': resolved_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"更新文件时间戳失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}