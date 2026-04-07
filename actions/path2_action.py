"""Path2 action module for RabAI AutoClick.

Provides additional path operations:
- PathJoinAction: Join path components
- PathDirnameAction: Get directory name
- PathBasenameAction: Get base name
- PathExtAction: Get file extension
- PathExistsAction: Check if path exists
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathJoinAction(BaseAction):
    """Join path components."""
    action_type = "path_join"
    display_name = "连接路径"
    description = "连接多个路径组件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path join.

        Args:
            context: Execution context.
            params: Dict with parts, output_var.

        Returns:
            ActionResult with joined path.
        """
        parts = params.get('parts', [])
        output_var = params.get('output_var', 'path_result')

        try:
            resolved_parts = context.resolve_value(parts)

            if isinstance(resolved_parts, list):
                result = os.path.join(*[str(p) for p in resolved_parts])
            else:
                result = os.path.join(str(resolved_parts))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径连接: {result}",
                data={
                    'result': result,
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
        """Execute get dirname.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with directory name.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'dirname_result')

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
                    'original': resolved,
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
        return {'output_var': 'dirname_result'}


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
        """Execute get basename.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with base name.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'basename_result')

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
                    'original': resolved,
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
        return {'output_var': 'basename_result'}


class PathExtAction(BaseAction):
    """Get file extension."""
    action_type = "path_ext"
    display_name = "获取扩展名"
    description = "获取文件的扩展名"

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
        output_var = params.get('output_var', 'ext_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_include_dot = context.resolve_value(include_dot) if include_dot else False

            result = os.path.splitext(resolved)[1]
            if not resolved_include_dot and result:
                result = result[1:]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扩展名: {result}",
                data={
                    'original': resolved,
                    'extension': result,
                    'include_dot': resolved_include_dot,
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
        return {'include_dot': False, 'output_var': 'ext_result'}


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "path_exists"
    display_name = "检查路径"
    description = "检查路径是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check path exists.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with check result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_exists')

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
                message=f"路径{'存在' if exists else '不存在'}",
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
                message=f"检查路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_exists'}