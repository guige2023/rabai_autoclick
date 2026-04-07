"""Path11 action module for RabAI AutoClick.

Provides additional path operations:
- PathExistsAction: Check if path exists
- PathIsFileAction: Check if file
- PathIsDirAction: Check if directory
- PathGetSizeAction: Get file size
- PathGetModifiedAction: Get modified time
- PathNormalizeAction: Normalize path
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "path11_exists"
    display_name = "路径存在"
    description = "检查路径是否存在"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path exists.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        try:
            resolved = context.resolve_value(path)

            result = os.path.exists(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径存在: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查路径存在失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class PathIsFileAction(BaseAction):
    """Check if file."""
    action_type = "path11_isfile"
    display_name = "是文件"
    description = "检查是否为文件"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is file.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is file result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'isfile_result')

        try:
            resolved = context.resolve_value(path)

            result = os.path.isfile(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"是文件: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'is_file': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查是文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isfile_result'}


class PathIsDirAction(BaseAction):
    """Check if directory."""
    action_type = "path11_isdir"
    display_name: "是目录"
    description = "检查是否为目录"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is directory.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is directory result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'isdir_result')

        try:
            resolved = context.resolve_value(path)

            result = os.path.isdir(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"是目录: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'is_dir': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查是目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isdir_result'}


class PathGetSizeAction(BaseAction):
    """Get file size."""
    action_type = "path11_getsize"
    display_name = "获取文件大小"
    description = "获取文件大小"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get size.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with file size.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'size_result')

        try:
            resolved = context.resolve_value(path)

            if not os.path.isfile(resolved):
                return ActionResult(
                    success=False,
                    message=f"不是文件: {resolved}"
                )

            result = os.path.getsize(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件大小: {result}字节",
                data={
                    'path': resolved,
                    'size': result,
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
        return {'output_var': 'size_result'}


class PathGetModifiedAction(BaseAction):
    """Get modified time."""
    action_type = "path11_getmtime"
    display_name = "获取修改时间"
    description = "获取文件修改时间"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get modified time.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with modified time.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'mtime_result')

        try:
            from datetime import datetime

            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(
                    success=False,
                    message=f"路径不存在: {resolved}"
                )

            timestamp = os.path.getmtime(resolved)
            dt = datetime.fromtimestamp(timestamp)

            result = {
                'timestamp': timestamp,
                'datetime': dt.isoformat(),
                'date': dt.strftime('%Y-%m-%d'),
                'time': dt.strftime('%H:%M:%S')
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"修改时间: {dt.isoformat()}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取修改时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mtime_result'}


class PathNormalizeAction(BaseAction):
    """Normalize path."""
    action_type = "path11_normalize"
    display_name = "规范化路径"
    description = "规范化路径"
    version = "11.0"

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