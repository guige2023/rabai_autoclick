"""File3 action module for RabAI AutoClick.

Provides additional file operations:
- FileExistsAction: Check if file exists
- FileSizeAction: Get file size
- FileCopyAction: Copy file
- FileMoveAction: Move file
- FileDeleteAction: Delete file
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file3_exists"
    display_name = "检查文件存在"
    description = "检查文件是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file exists.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with check result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            exists = os.path.exists(resolved)
            is_file = os.path.isfile(resolved) if exists else False
            is_dir = os.path.isdir(resolved) if exists else False

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"文件{'存在' if exists else '不存在'}",
                data={
                    'path': resolved,
                    'exists': exists,
                    'is_file': is_file,
                    'is_dir': is_dir,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class FileSizeAction(BaseAction):
    """Get file size."""
    action_type = "file3_size"
    display_name = "获取文件大小"
    description = "获取文件大小(字节)"

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
        output_var = params.get('output_var', 'file_size')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)

            if not os.path.isfile(resolved):
                return ActionResult(
                    success=False,
                    message=f"不是文件: {resolved}"
                )

            size = os.path.getsize(resolved)
            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"文件大小: {size} 字节",
                data={
                    'path': resolved,
                    'size': size,
                    'size_kb': round(size / 1024, 2),
                    'size_mb': round(size / (1024 * 1024), 2),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'file_size'}


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file3_copy"
    display_name = "复制文件"
    description = "复制文件到目标路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file copy.

        Args:
            context: Execution context.
            params: Dict with source, dest, output_var.

        Returns:
            ActionResult with copy result.
        """
        source = params.get('source', '')
        dest = params.get('dest', '')
        output_var = params.get('output_var', 'copy_result')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dest, str, 'dest')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import shutil

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(dest)

            shutil.copy2(resolved_source, resolved_dest)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件已复制: {resolved_source} -> {resolved_dest}",
                data={
                    'source': resolved_source,
                    'dest': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'dest']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copy_result'}


class FileMoveAction(BaseAction):
    """Move file."""
    action_type = "file3_move"
    display_name = "移动文件"
    description = "移动文件到目标路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file move.

        Args:
            context: Execution context.
            params: Dict with source, dest, output_var.

        Returns:
            ActionResult with move result.
        """
        source = params.get('source', '')
        dest = params.get('dest', '')
        output_var = params.get('output_var', 'move_result')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dest, str, 'dest')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import shutil

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(dest)

            shutil.move(resolved_source, resolved_dest)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件已移动: {resolved_source} -> {resolved_dest}",
                data={
                    'source': resolved_source,
                    'dest': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'dest']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'move_result'}


class FileDeleteAction(BaseAction):
    """Delete file."""
    action_type = "file3_delete"
    display_name = "删除文件"
    description = "删除文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file delete.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with delete result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'delete_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import os

            resolved = context.resolve_value(path)

            if not os.path.isfile(resolved):
                return ActionResult(
                    success=False,
                    message=f"不是文件: {resolved}"
                )

            os.remove(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件已删除: {resolved}",
                data={
                    'path': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_result'}