"""Path6 action module for RabAI AutoClick.

Provides additional path operations:
- PathJoinAction: Join path components
- PathNormalizeAction: Normalize path
- PathIsAbsoluteAction: Check if path is absolute
- PathDirnameAction: Get directory name
- PathBasenameAction: Get base name
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathJoinAction(BaseAction):
    """Join path components."""
    action_type = "path6_join"
    display_name = "连接路径"
    description = "连接路径组件"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join path.

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

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = os.path.join(*resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径连接: {result}",
                data={
                    'parts': resolved,
                    'joined': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['parts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'joined_path'}


class PathNormalizeAction(BaseAction):
    """Normalize path."""
    action_type = "path6_normalize"
    display_name = "规范化路径"
    description = "规范化路径"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute normalize path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with normalized path.
        """
        input_path = params.get('path', '')
        output_var = params.get('output_var', 'normalized_path')

        try:
            resolved = context.resolve_value(input_path)

            result = os.path.normpath(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径规范化: {result}",
                data={
                    'original': resolved,
                    'normalized': result,
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
        return {'output_var': 'normalized_path'}


class PathIsAbsoluteAction(BaseAction):
    """Check if path is absolute."""
    action_type = "path6_is_absolute"
    display_name = "判断绝对路径"
    description = "判断路径是否为绝对路径"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is absolute.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is absolute result.
        """
        input_path = params.get('path', '')
        output_var = params.get('output_var', 'is_absolute')

        try:
            resolved = context.resolve_value(input_path)

            result = os.path.isabs(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对路径判断: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'is_absolute': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断绝对路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_absolute'}


class PathDirnameAction(BaseAction):
    """Get directory name."""
    action_type = "path6_dirname"
    display_name = "获取目录名"
    description = "获取路径的目录名"
    version = "6.0"

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
        input_path = params.get('path', '')
        output_var = params.get('output_var', 'dirname')

        try:
            resolved = context.resolve_value(input_path)

            result = os.path.dirname(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"目录名: {result}",
                data={
                    'path': resolved,
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
        return {'output_var': 'dirname'}


class PathBasenameAction(BaseAction):
    """Get base name."""
    action_type = "path6_basename"
    display_name = "获取文件名"
    description = "获取路径的文件名"
    version = "6.0"

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
        input_path = params.get('path', '')
        output_var = params.get('output_var', 'basename')

        try:
            resolved = context.resolve_value(input_path)

            result = os.path.basename(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件名: {result}",
                data={
                    'path': resolved,
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
        return {'output_var': 'basename'}