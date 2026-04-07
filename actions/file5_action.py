"""File5 action module for RabAI AutoClick.

Provides additional file operations:
- FileCopyAction: Copy file
- FileMoveAction: Move file
- FileRenameAction: Rename file
- FileExistsAction: Check if file exists
- FileSizeAction: Get file size
"""

import shutil
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file5_copy"
    display_name = "文件复制"
    description = "复制文件到目标位置"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file copy.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with copy status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'copy_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            shutil.copy2(resolved_source, resolved_dest)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件复制完成: {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件复制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copy_status'}


class FileMoveAction(BaseAction):
    """Move file."""
    action_type = "file5_move"
    display_name = "文件移动"
    description = "移动文件到目标位置"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file move.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with move status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'move_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            shutil.move(resolved_source, resolved_dest)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件移动完成: {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件移动失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'move_status'}


class FileRenameAction(BaseAction):
    """Rename file."""
    action_type = "file5_rename"
    display_name = "文件重命名"
    description = "重命名文件"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file rename.

        Args:
            context: Execution context.
            params: Dict with path, new_name, output_var.

        Returns:
            ActionResult with rename status.
        """
        path = params.get('path', '')
        new_name = params.get('new_name', '')
        output_var = params.get('output_var', 'rename_status')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new_name, str, 'new_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_new_name = context.resolve_value(new_name)

            directory = os.path.dirname(resolved_path)
            new_path = os.path.join(directory, resolved_new_name)

            os.rename(resolved_path, new_path)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件重命名完成: {new_path}",
                data={
                    'original': resolved_path,
                    'new_path': new_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件重命名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'new_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rename_status'}


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file5_exists"
    display_name = "文件存在检查"
    description = "检查文件是否存在"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file exists check.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            exists = os.path.exists(resolved_path)
            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"文件存在检查: {'是' if exists else '否'}",
                data={
                    'path': resolved_path,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class FileSizeAction(BaseAction):
    """Get file size."""
    action_type = "file5_size"
    display_name = "文件大小"
    description = "获取文件大小"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file size.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with file size.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'size_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            size = os.path.getsize(resolved_path)
            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"文件大小: {size} 字节",
                data={
                    'path': resolved_path,
                    'size': size,
                    'size_human': self._human_size(size),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件大小失败: {str(e)}"
            )

    def _human_size(self, size):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'size_result'}