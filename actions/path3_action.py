"""Path3 action module for RabAI AutoClick.

Provides additional path operations:
- PathIsAbsAction: Check if absolute path
- PathIsRelAction: Check if relative path
- PathNormpathAction: Normalize path
- PathAbspathAction: Get absolute path
- PathRelpathAction: Get relative path
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathIsAbsAction(BaseAction):
    """Check if absolute path."""
    action_type = "path3_is_abs"
    display_name = "判断绝对路径"
    description = "检查路径是否为绝对路径"

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
            ActionResult with check result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_abs_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.isabs(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"绝对路径: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'result': result,
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
        return {'output_var': 'is_abs_result'}


class PathIsRelAction(BaseAction):
    """Check if relative path."""
    action_type = "path3_is_rel"
    display_name = "判断相对路径"
    description = "检查路径是否为相对路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is relative.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with check result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_rel_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = not os.path.isabs(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"相对路径: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断相对路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_rel_result'}


class PathNormpathAction(BaseAction):
    """Normalize path."""
    action_type = "path3_normpath"
    display_name = "规范化路径"
    description = "规范化路径格式"

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
        path = params.get('path', '')
        output_var = params.get('output_var', 'normalized_path')

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
                    'original': resolved,
                    'result': result,
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


class PathAbspathAction(BaseAction):
    """Get absolute path."""
    action_type = "path3_abspath"
    display_name = "获取绝对路径"
    description = "获取路径的绝对路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute absolute path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with absolute path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'absolute_path')

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
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'absolute_path'}


class PathRelpathAction(BaseAction):
    """Get relative path."""
    action_type = "path3_relpath"
    display_name = "获取相对路径"
    description = "获取路径相对于另一路径的相对路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute relative path.

        Args:
            context: Execution context.
            params: Dict with path, start, output_var.

        Returns:
            ActionResult with relative path.
        """
        path = params.get('path', '')
        start = params.get('start', None)
        output_var = params.get('output_var', 'relative_path')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_start = context.resolve_value(start) if start else os.getcwd()

            result = os.path.relpath(resolved, resolved_start)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"相对路径: {result}",
                data={
                    'path': resolved,
                    'start': resolved_start,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取相对路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': None, 'output_var': 'relative_path'}
