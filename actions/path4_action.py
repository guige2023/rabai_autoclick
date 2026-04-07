"""Path4 action module for RabAI AutoClick.

Provides additional path operations:
- PathSplitAction: Split path into directory and file
- PathSplitextAction: Split path into name and extension
- PathCommonpathAction: Get common path
- PathisdirAction: Check if directory
- PathIsfileAction: Check if file
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathSplitAction(BaseAction):
    """Split path into directory and file."""
    action_type = "path4_split"
    display_name = "分割路径"
    description = "将路径分割为目录和文件名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split path.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with split path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'split_path')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            dirpath, filename = os.path.split(resolved)
            result = {'directory': dirpath, 'filename': filename}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"路径分割: {dirpath} + {filename}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'split_path'}


class PathSplitextAction(BaseAction):
    """Split path into name and extension."""
    action_type = "path4_splitext"
    display_name = "分割扩展名"
    description = "将路径分割为文件名和扩展名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split extension.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with split extension.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'splitext_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            name, ext = os.path.splitext(resolved)
            result = {'name': name, 'extension': ext}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扩展名分割: {name} + {ext}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割扩展名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'splitext_result'}


class PathCommonpathAction(BaseAction):
    """Get common path."""
    action_type = "path4_commonpath"
    display_name = "公共路径"
    description = "获取多个路径的公共路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute common path.

        Args:
            context: Execution context.
            params: Dict with paths, output_var.

        Returns:
            ActionResult with common path.
        """
        paths = params.get('paths', [])
        output_var = params.get('output_var', 'common_path')

        try:
            resolved = context.resolve_value(paths)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="paths 必须是列表"
                )

            if len(resolved) < 2:
                return ActionResult(
                    success=False,
                    message="paths 至少需要2个路径"
                )

            result = os.path.commonpath(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"公共路径: {result}",
                data={
                    'paths': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取公共路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['paths']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'common_path'}


class PathisdirAction(BaseAction):
    """Check if directory."""
    action_type = "path4_isdir"
    display_name = "判断目录"
    description = "检查路径是否为目录"

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
            ActionResult with directory check.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_dir_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.isdir(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"目录判断: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_dir_result'}


class PathIsfileAction(BaseAction):
    """Check if file."""
    action_type = "path4_isfile"
    display_name = "判断文件"
    description = "检查路径是否为文件"

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
            ActionResult with file check.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_file_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            result = os.path.isfile(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件判断: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_file_result'}
