"""Environment action module for RabAI AutoClick.

Provides environment operations:
- EnvironmentGetAction: Get environment variable
- EnvironmentSetAction: Set environment variable
- EnvironmentListAction: List all environment variables
- EnvironmentUnsetAction: Unset environment variable
"""

import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvironmentGetAction(BaseAction):
    """Get environment variable."""
    action_type = "environment_get"
    display_name = "获取环境变量"
    description = "获取环境变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get environment variable.

        Args:
            context: Execution context.
            params: Dict with name, default, output_var.

        Returns:
            ActionResult with environment variable value.
        """
        name = params.get('name', '')
        default = params.get('default', '')
        output_var = params.get('output_var', 'env_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            result = os.environ.get(resolved_name, default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取环境变量: {resolved_name}",
                data={
                    'name': resolved_name,
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
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': '', 'output_var': 'env_value'}


class EnvironmentSetAction(BaseAction):
    """Set environment variable."""
    action_type = "environment_set"
    display_name = "设置环境变量"
    description = "设置环境变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set environment variable.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')
        value = params.get('value', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = context.resolve_value(value)
            os.environ[resolved_name] = resolved_value

            return ActionResult(
                success=True,
                message=f"已设置环境变量: {resolved_name}",
                data={
                    'name': resolved_name,
                    'value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class EnvironmentListAction(BaseAction):
    """List all environment variables."""
    action_type = "environment_list"
    display_name = "列出环境变量"
    description = "列出所有环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list environment variables.

        Args:
            context: Execution context.
            params: Dict with filter, output_var.

        Returns:
            ActionResult with environment variables.
        """
        filter_str = params.get('filter', '')
        output_var = params.get('output_var', 'env_variables')

        try:
            resolved_filter = context.resolve_value(filter_str) if filter_str else ''

            env_vars = dict(os.environ)

            if resolved_filter:
                env_vars = {k: v for k, v in env_vars.items() if resolved_filter.lower() in k.lower()}

            context.set(output_var, env_vars)

            return ActionResult(
                success=True,
                message=f"环境变量列表: {len(env_vars)} 个",
                data={
                    'count': len(env_vars),
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
        return {'filter': '', 'output_var': 'env_variables'}


class EnvironmentUnsetAction(BaseAction):
    """Unset environment variable."""
    action_type = "environment_unset"
    display_name = "删除环境变量"
    description = "删除环境变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unset environment variable.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            if resolved_name in os.environ:
                del os.environ[resolved_name]

            return ActionResult(
                success=True,
                message=f"已删除环境变量: {resolved_name}",
                data={'name': resolved_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}