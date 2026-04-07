"""Path action module for RabAI AutoClick.

Provides path manipulation operations:
- PathJoinAction: Join path components
- PathDirnameAction: Get directory name
- PathBasenameAction: Get base name
- PathExistsAction: Check if path exists
- PathIsFileAction: Check if is file
- PathIsDirAction: Check if is directory
- PathGetSizeAction: Get file size
- PathNormalizeAction: Normalize path
- PathExpandUserAction: Expand user path
- PathAbsoluteAction: Get absolute path
"""

import os
import pathlib
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathJoinAction(BaseAction):
    """Join path components."""
    action_type = "path_join"
    display_name = "路径连接"
    description = "连接路径组件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path joining.

        Args:
            context: Execution context.
            params: Dict with parts, output_var.

        Returns:
            ActionResult with joined path.
        """
        parts = params.get('parts', [])
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(parts, (list, tuple), 'parts')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_parts = [context.resolve_value(p) for p in parts]
            result = os.path.join(*resolved_parts)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径已连接: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['parts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathDirnameAction(BaseAction):
    """Get directory name."""
    action_type = "path_dirname"
    display_name = "获取目录名"
    description = "获取路径的目录部分"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting directory name.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with directory name.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.dirname(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"目录名: {result}",
                data={
                    'dirname': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取目录名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathBasenameAction(BaseAction):
    """Get base name."""
    action_type = "path_basename"
    display_name = "获取文件名"
    description = "获取路径的文件名部分"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting base name.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with base name.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.basename(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件名: {result}",
                data={
                    'basename': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "path_exists"
    display_name = "路径存在检查"
    description = "检查路径是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking path exists.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.exists(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径存在: {'是' if result else '否'}",
                data={
                    'exists': result,
                    'path': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathIsFileAction(BaseAction):
    """Check if is file."""
    action_type = "path_is_file"
    display_name = "文件检查"
    description = "检查路径是否为文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking is file.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is file result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.isfile(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"是文件: {'是' if result else '否'}",
                data={
                    'is_file': result,
                    'path': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathIsDirAction(BaseAction):
    """Check if is directory."""
    action_type = "path_is_dir"
    display_name = "目录检查"
    description = "检查路径是否为目录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking is directory.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is directory result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.isdir(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"是目录: {'是' if result else '否'}",
                data={
                    'is_dir': result,
                    'path': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"目录检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathGetSizeAction(BaseAction):
    """Get file size."""
    action_type = "path_get_size"
    display_name = "获取文件大小"
    description = "获取文件大小（字节）"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting file size.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with file size.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved}"
                )

            if not os.path.isfile(resolved):
                return ActionResult(
                    success=False,
                    message=f"路径不是文件: {resolved}"
                )

            result = os.path.getsize(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件大小: {result} 字节",
                data={
                    'size': result,
                    'path': resolved,
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
        return {'output_var': 'path_result'}


class PathNormalizeAction(BaseAction):
    """Normalize path."""
    action_type = "path_normalize"
    display_name = "规范化路径"
    description = "规范化路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute normalizing path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with normalized path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.normpath(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"规范化路径: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"规范化路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathExpandUserAction(BaseAction):
    """Expand user path."""
    action_type = "path_expand_user"
    display_name = "扩展用户路径"
    description = "扩展路径中的~用户目录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute expanding user path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with expanded path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.expanduser(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扩展用户路径: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扩展用户路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathAbsoluteAction(BaseAction):
    """Get absolute path."""
    action_type = "path_absolute"
    display_name = "获取绝对路径"
    description = "获取绝对路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting absolute path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with absolute path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.abspath(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对路径: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取绝对路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}