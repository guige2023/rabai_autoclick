"""Path5 action module for RabAI AutoClick.

Provides additional path operations:
- PathJoinAction: Join path components
- PathDirnameAction: Get directory name
- PathBasenameAction: Get base name
- PathExistsAction: Check if path exists
- PathExpandUserAction: Expand user path
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
    action_type = "path5_join"
    display_name = "路径拼接"
    description = "拼接路径组件"
    version = "5.0"

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
        output_var = params.get('output_var', 'joined_path')

        try:
            resolved = context.resolve_value(parts)

            if isinstance(resolved, str):
                resolved = resolved.split('/')

            result = os.path.join(*resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径拼接完成: {result}",
                data={
                    'parts': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径拼接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['parts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'joined_path'}


class PathDirnameAction(BaseAction):
    """Get directory name."""
    action_type = "path5_dirname"
    display_name = "获取目录名"
    description = "获取路径的目录部分"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dirname.

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
                message=f"获取目录名: {result}",
                data={
                    'path': resolved,
                    'result': result,
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
    action_type = "path5_basename"
    display_name = "获取基名"
    description = "获取路径的文件名部分"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute basename.

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
                message=f"获取基名: {result}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取基名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'basename_result'}


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "path5_exists"
    display_name = "路径存在检查"
    description = "检查路径是否存在"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path exists check.

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
            resolved = context.resolve_value(path)
            result = os.path.exists(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径存在检查: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class PathExpandUserAction(BaseAction):
    """Expand user path."""
    action_type = "path5_expanduser"
    display_name = "展开用户路径"
    description = "展开路径中的~用户目录"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute expand user.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with expanded path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'expanded_path')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.expanduser(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"展开用户路径: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"展开用户路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expanded_path'}