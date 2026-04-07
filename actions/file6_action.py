"""File6 action module for RabAI AutoClick.

Provides additional file operations:
- FileDeleteAction: Delete file
- FileCreateDirAction: Create directory
- FileDeleteDirAction: Delete directory
- FileListDirAction: List directory contents
- FileExtensionAction: Get file extension
"""

import shutil
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileDeleteAction(BaseAction):
    """Delete file."""
    action_type = "file6_delete"
    display_name = "文件删除"
    description = "删除文件"
    version = "6.0"

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
            ActionResult with delete status.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'delete_status')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件删除失败: 文件不存在"
                )

            os.remove(resolved_path)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件删除完成: {resolved_path}",
                data={
                    'path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_status'}


class FileCreateDirAction(BaseAction):
    """Create directory."""
    action_type = "file6_create_dir"
    display_name = "创建目录"
    description = "创建目录"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create directory.

        Args:
            context: Execution context.
            params: Dict with path, parents, output_var.

        Returns:
            ActionResult with create status.
        """
        path = params.get('path', '')
        parents = params.get('parents', True)
        output_var = params.get('output_var', 'create_dir_status')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_parents = bool(context.resolve_value(parents))

            os.makedirs(resolved_path, exist_ok=True)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"创建目录完成: {resolved_path}",
                data={
                    'path': resolved_path,
                    'parents': resolved_parents,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'parents': True, 'output_var': 'create_dir_status'}


class FileDeleteDirAction(BaseAction):
    """Delete directory."""
    action_type = "file6_delete_dir"
    display_name = "删除目录"
    description = "删除目录"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete directory.

        Args:
            context: Execution context.
            params: Dict with path, recursive, output_var.

        Returns:
            ActionResult with delete status.
        """
        path = params.get('path', '')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'delete_dir_status')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_recursive = bool(context.resolve_value(recursive))

            if resolved_recursive:
                shutil.rmtree(resolved_path)
            else:
                os.rmdir(resolved_path)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"删除目录完成: {resolved_path}",
                data={
                    'path': resolved_path,
                    'recursive': resolved_recursive,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'recursive': False, 'output_var': 'delete_dir_status'}


class FileListDirAction(BaseAction):
    """List directory contents."""
    action_type = "file6_list_dir"
    display_name = "列出目录"
    description = "列出目录内容和文件"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list directory.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with directory contents.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'list_dir_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            contents = os.listdir(resolved_path)
            dirs = [c for c in contents if os.path.isdir(os.path.join(resolved_path, c))]
            files = [c for c in contents if os.path.isfile(os.path.join(resolved_path, c))]

            context.set(output_var, contents)

            return ActionResult(
                success=True,
                message=f"列出目录完成: {len(contents)} 项",
                data={
                    'path': resolved_path,
                    'contents': contents,
                    'directories': dirs,
                    'files': files,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_dir_result'}


class FileExtensionAction(BaseAction):
    """Get file extension."""
    action_type = "file6_extension"
    display_name = "文件扩展名"
    description = "获取文件扩展名"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file extension.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with file extension.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'extension_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            _, ext = os.path.splitext(resolved_path)
            result = ext.lstrip('.') if ext else ''

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件扩展名: {result if result else '(无)'}",
                data={
                    'path': resolved_path,
                    'extension': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'extension_result'}