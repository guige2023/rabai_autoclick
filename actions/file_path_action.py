"""File path action module for RabAI AutoClick.

Provides file path operations:
- PathSplitAction: Split path into directory and file
- PathExtensionAction: Get file extension
- PathWithoutExtensionAction: Get path without extension
- PathChangeExtensionAction: Change file extension
- PathJoinMultipleAction: Join multiple path components
"""

import os
import os.path
from typing import Any, Dict, List, Optional

import sys
import os as os_module
_parent_dir = os_module.path.dirname(os_module.path.dirname(os_module.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathSplitAction(BaseAction):
    """Split path into directory and file."""
    action_type = "path_split"
    display_name = "拆分路径"
    description = "将路径拆分为目录和文件名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path split.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with split path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            dirname, basename = os.path.split(resolved)
            context.set(output_var, {'dirname': dirname, 'basename': basename})

            return ActionResult(
                success=True,
                message=f"路径拆分: {dirname} / {basename}",
                data={
                    'dirname': dirname,
                    'basename': basename,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拆分路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathExtensionAction(BaseAction):
    """Get file extension."""
    action_type = "path_extension"
    display_name = "获取扩展名"
    description = "获取文件扩展名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get extension.

        Args:
            context: Execution context.
            params: Dict with path, include_dot, output_var.

        Returns:
            ActionResult with extension.
        """
        path = params.get('path', '')
        include_dot = params.get('include_dot', False)
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            ext = os.path.splitext(resolved)[1]
            if not include_dot and ext.startswith('.'):
                ext = ext[1:]
            context.set(output_var, ext)

            return ActionResult(
                success=True,
                message=f"扩展名: {ext}",
                data={
                    'extension': ext,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'include_dot': False, 'output_var': 'path_result'}


class PathWithoutExtensionAction(BaseAction):
    """Get path without extension."""
    action_type = "path_without_extension"
    display_name = "去除扩展名"
    description = "获取不带扩展名的路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute without extension.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with path without extension.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.splitext(resolved)[0]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"无扩展名路径: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathChangeExtensionAction(BaseAction):
    """Change file extension."""
    action_type = "path_change_extension"
    display_name = "修改扩展名"
    description = "修改文件扩展名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute change extension.

        Args:
            context: Execution context.
            params: Dict with path, new_extension, output_var.

        Returns:
            ActionResult with new path.
        """
        path = params.get('path', '')
        new_extension = params.get('new_extension', '')
        output_var = params.get('output_var', 'path_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new_extension, str, 'new_extension')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_ext = context.resolve_value(new_extension)

            if resolved_ext and not resolved_ext.startswith('.'):
                resolved_ext = '.' + resolved_ext

            result = os.path.splitext(resolved_path)[0] + resolved_ext
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"新路径: {result}",
                data={
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"修改扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'new_extension']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_result'}


class PathJoinMultipleAction(BaseAction):
    """Join multiple path components."""
    action_type = "path_join_multiple"
    display_name = "多段路径连接"
    description = "连接多个路径段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join multiple.

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

        if len(parts) < 2:
            return ActionResult(
                success=False,
                message="至少需要2个路径段"
            )

        try:
            resolved_parts = [context.resolve_value(p) for p in parts]
            result = os.path.join(*resolved_parts)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径连接: {result}",
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