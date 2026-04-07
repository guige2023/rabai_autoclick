"""Env11 action module for RabAI AutoClick.

Provides additional environment operations:
- EnvGetAction: Get environment variable
- EnvSetAction: Set environment variable
- EnvListAction: List all environment variables
- EnvHomeAction: Get home directory
- EnvCwdAction: Get current working directory
- EnvTempDirAction: Get temp directory
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvGetAction(BaseAction):
    """Get environment variable."""
    action_type = "env11_get"
    display_name = "获取环境变量"
    description = "获取环境变量"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute env get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var.

        Returns:
            ActionResult with env value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'env_value')

        try:
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None

            result = os.environ.get(resolved_key, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取环境变量: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'env_value'}


class EnvSetAction(BaseAction):
    """Set environment variable."""
    action_type = "env11_set"
    display_name = "设置环境变量"
    description = "设置环境变量"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute env set.

        Args:
            context: Execution context.
            params: Dict with key, value, output_var.

        Returns:
            ActionResult with set status.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'set_status')

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            os.environ[resolved_key] = resolved_value
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"设置环境变量: {resolved_key}",
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
        return {'output_var': 'set_status'}


class EnvListAction(BaseAction):
    """List all environment variables."""
    action_type = "env11_list"
    display_name = "列出环境变量"
    description = "列出所有环境变量"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute env list.

        Args:
            context: Execution context.
            params: Dict with prefix, output_var.

        Returns:
            ActionResult with env list.
        """
        prefix = params.get('prefix', None)
        output_var = params.get('output_var', 'env_list')

        try:
            resolved_prefix = context.resolve_value(prefix) if prefix else None

            if resolved_prefix:
                result = {k: v for k, v in os.environ.items() if k.startswith(resolved_prefix)}
            else:
                result = dict(os.environ)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列出环境变量: {len(result)}个",
                data={
                    'count': len(result),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'prefix': None, 'output_var': 'env_list'}


class EnvHomeAction(BaseAction):
    """Get home directory."""
    action_type = "env11_home"
    display_name = "获取主目录"
    description = "获取用户主目录"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute home directory.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with home directory.
        """
        output_var = params.get('output_var', 'home_dir')

        try:
            result = os.path.expanduser('~')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"主目录: {result}",
                data={
                    'home': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取主目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'home_dir'}


class EnvCwdAction(BaseAction):
    """Get current working directory."""
    action_type = "env11_cwd"
    display_name = "获取当前目录"
    description = "获取当前工作目录"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cwd.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with cwd.
        """
        output_var = params.get('output_var', 'cwd')

        try:
            result = os.getcwd()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前目录: {result}",
                data={
                    'cwd': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cwd'}


class EnvTempDirAction(BaseAction):
    """Get temp directory."""
    action_type = "env11_tempdir"
    display_name = "获取临时目录"
    description = "获取临时目录"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute temp directory.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with temp directory.
        """
        output_var = params.get('output_var', 'temp_dir')

        try:
            result = tempfile.gettempdir()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"临时目录: {result}",
                data={
                    'temp_dir': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取临时目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'temp_dir'}