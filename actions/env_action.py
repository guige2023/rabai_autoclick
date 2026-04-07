"""Environment action module for RabAI AutoClick.

Provides environment variable operations:
- EnvGetAction: Get environment variable
- EnvSetAction: Set environment variable
- EnvListAction: List all environment variables
- EnvDeleteAction: Delete environment variable
- EnvExpandAction: Expand environment variables in string
- EnvExistsAction: Check if environment variable exists
"""

import os
import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvGetAction(BaseAction):
    """Get environment variable."""
    action_type = "env_get"
    display_name = "获取环境变量"
    description = "获取环境变量值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with name, default, output_var.

        Returns:
            ActionResult with value.
        """
        name = params.get('name', '')
        default = params.get('default', '')
        output_var = params.get('output_var', 'env_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_default = context.resolve_value(default) if default else None

            value = os.environ.get(resolved_name, resolved_default)
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"{resolved_name} = {value}",
                data={'name': resolved_name, 'value': value, 'output_var': output_var}
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


class EnvSetAction(BaseAction):
    """Set environment variable."""
    action_type = "env_set"
    display_name = "设置环境变量"
    description = "设置环境变量值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with name, value, persist.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')
        value = params.get('value', '')
        persist = params.get('persist', False)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = context.resolve_value(value)
            resolved_persist = context.resolve_value(persist)

            os.environ[resolved_name] = resolved_value

            if resolved_persist:
                # Try to persist to shell profile (macOS/Linux)
                profile_files = ['~/.bashrc', '~/.zshrc', '~/.profile']
                for pf in profile_files:
                    expanded = os.path.expanduser(pf)
                    if os.path.exists(expanded):
                        with open(expanded, 'a') as f:
                            f.write(f'\nexport {resolved_name}="{resolved_value}"\n')
                        break

            return ActionResult(
                success=True,
                message=f"已设置: {resolved_name} = {resolved_value}",
                data={'name': resolved_name, 'value': resolved_value}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'persist': False}


class EnvListAction(BaseAction):
    """List all environment variables."""
    action_type = "env_list"
    display_name = "列出环境变量"
    description = "列出所有环境变量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with filter, output_var.

        Returns:
            ActionResult with environment variables.
        """
        filter_str = params.get('filter', '')
        output_var = params.get('output_var', 'env_vars')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_filter = context.resolve_value(filter_str) if filter_str else ''

            all_vars = dict(os.environ)

            if resolved_filter:
                all_vars = {k: v for k, v in all_vars.items() if resolved_filter.lower() in k.lower()}

            context.set(output_var, all_vars)

            return ActionResult(
                success=True,
                message=f"环境变量: {len(all_vars)} 个",
                data={'count': len(all_vars), 'vars': all_vars, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filter': '', 'output_var': 'env_vars'}


class EnvDeleteAction(BaseAction):
    """Delete environment variable."""
    action_type = "env_delete"
    display_name = "删除环境变量"
    description = "删除环境变量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

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
                message=f"已删除: {resolved_name}",
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


class EnvExpandAction(BaseAction):
    """Expand environment variables in string."""
    action_type = "env_expand"
    display_name = "展开环境变量"
    description = "展开字符串中的环境变量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute expand.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with expanded string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'env_expanded')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)

            expanded = os.path.expandvars(resolved_text)
            context.set(output_var, expanded)

            return ActionResult(
                success=True,
                message=f"已展开: {expanded[:50]}...",
                data={'original': resolved_text, 'expanded': expanded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"展开环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'env_expanded'}


class EnvExistsAction(BaseAction):
    """Check if environment variable exists."""
    action_type = "env_exists"
    display_name = "检查环境变量"
    description = "检查环境变量是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exists.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with exists flag.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'env_exists')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            exists = resolved_name in os.environ
            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"{resolved_name} {'存在' if exists else '不存在'}",
                data={'exists': exists, 'name': resolved_name, 'output_var': output_var}
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
