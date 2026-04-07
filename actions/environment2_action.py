"""Environment2 action module for RabAI AutoClick.

Provides additional environment operations:
- EnvGetAction: Get environment variable
- EnvSetAction: Set environment variable
- EnvListAction: List all environment variables
- EnvHasAction: Check if environment variable exists
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvGetAction(BaseAction):
    """Get environment variable."""
    action_type = "env_get"
    display_name = "获取环境变量"
    description = "获取指定环境变量的值"

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
            ActionResult with variable value.
        """
        name = params.get('name', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'env_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_default = context.resolve_value(default) if default is not None else None

            result = os.environ.get(resolved_name, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取环境变量: {resolved_name}",
                data={
                    'name': resolved_name,
                    'value': result,
                    'exists': result is not None,
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
        return {'default': None, 'output_var': 'env_value'}


class EnvSetAction(BaseAction):
    """Set environment variable."""
    action_type = "env_set"
    display_name = "设置环境变量"
    description = "设置环境变量的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set environment variable.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with set result.
        """
        name = params.get('name', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'env_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = context.resolve_value(value)

            os.environ[resolved_name] = str(resolved_value)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"设置环境变量: {resolved_name} = {resolved_value}",
                data={
                    'name': resolved_name,
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
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'env_result'}


class EnvListAction(BaseAction):
    """List all environment variables."""
    action_type = "env_list"
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
        filter_str = params.get('filter', None)
        output_var = params.get('output_var', 'env_list')

        try:
            resolved_filter = context.resolve_value(filter_str) if filter_str else None

            env_dict = dict(os.environ)

            if resolved_filter:
                resolved_filter = resolved_filter.lower()
                env_dict = {
                    k: v for k, v in env_dict.items()
                    if resolved_filter in k.lower()
                }

            context.set(output_var, env_dict)

            return ActionResult(
                success=True,
                message=f"环境变量列表: {len(env_dict)} 个",
                data={
                    'count': len(env_dict),
                    'variables': env_dict,
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
        return {'filter': None, 'output_var': 'env_list'}


class EnvHasAction(BaseAction):
    """Check if environment variable exists."""
    action_type = "env_has"
    display_name = "检查环境变量"
    description = "检查环境变量是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check environment variable.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with check result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'env_exists')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            result = resolved_name in os.environ

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"环境变量存在: {'是' if result else '否'}",
                data={
                    'name': resolved_name,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'env_exists'}