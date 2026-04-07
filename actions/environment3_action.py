"""Environment3 action module for RabAI AutoClick.

Provides additional environment operations:
- EnvironmentExpandAction: Expand environment variables
- EnvironmentGetAllAction: Get all environment variables
- EnvironmentSetAction: Set environment variable
- EnvironmentUnsetAction: Unset environment variable
- EnvironmentIsFileAction: Check if path is a file
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvironmentExpandAction(BaseAction):
    """Expand environment variables."""
    action_type = "environment3_expand"
    display_name = "展开环境变量"
    description = "展开路径中的环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute expand.

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
            result = os.path.expanduser(os.path.expandvars(resolved))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"展开环境变量: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"展开环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expanded_path'}


class EnvironmentGetAllAction(BaseAction):
    """Get all environment variables."""
    action_type = "environment3_get_all"
    display_name = "获取所有环境变量"
    description = "获取所有环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get all.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with all env vars.
        """
        output_var = params.get('output_var', 'env_vars')

        try:
            result = dict(os.environ)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"环境变量: {len(result)} 个",
                data={
                    'count': len(result),
                    'env_vars': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'env_vars'}


class EnvironmentSetAction(BaseAction):
    """Set environment variable."""
    action_type = "environment3_set"
    display_name = "设置环境变量"
    description = "设置环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with key, value, output_var.

        Returns:
            ActionResult with set result.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'set_result')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            os.environ[resolved_key] = str(resolved_value)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"环境变量已设置: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class EnvironmentUnsetAction(BaseAction):
    """Unset environment variable."""
    action_type = "environment3_unset"
    display_name = "删除环境变量"
    description = "删除环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unset.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with unset result.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'unset_result')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            if resolved_key in os.environ:
                del os.environ[resolved_key]
                result = True
            else:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"环境变量已删除: {resolved_key}",
                data={
                    'key': resolved_key,
                    'existed': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unset_result'}


class EnvironmentIsFileAction(BaseAction):
    """Check if path is a file."""
    action_type = "environment3_is_file"
    display_name = "判断是否为文件"
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
            expanded = os.path.expanduser(os.path.expandvars(resolved))
            result = os.path.isfile(expanded)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件判断: {'是' if result else '否'}",
                data={
                    'path': resolved,
                    'expanded': expanded,
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
