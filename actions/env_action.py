"""Environment variable action module for RabAI AutoClick.

Provides environment operations:
- EnvGetAction: Get environment variable
- EnvSetAction: Set environment variable
- EnvListAction: List environment variables
- EnvExpandAction: Expand environment variables in string
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvGetAction(BaseAction):
    """Get environment variable."""
    action_type = "env_get"
    display_name = "环境变量获取"
    description = "获取环境变量"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute env get."""
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'env_value')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            value = os.environ.get(resolved_key, default)

            if context:
                context.set(output_var, value)
            return ActionResult(success=True, message=f"{resolved_key} = {value}", data={'key': resolved_key, 'value': value})
        except Exception as e:
            return ActionResult(success=False, message=f"Env get error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'env_value'}


class EnvSetAction(BaseAction):
    """Set environment variable."""
    action_type = "env_set"
    display_name = "环境变量设置"
    description = "设置环境变量"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute env set."""
        key = params.get('key', '')
        value = params.get('value', '')
        export = params.get('export', True)
        output_var = params.get('output_var', 'env_set_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value

            if export:
                os.environ[resolved_key] = str(resolved_value)
            else:
                os.environ[resolved_key] = str(resolved_value)

            result = {'key': resolved_key, 'value': str(resolved_value), 'exported': export}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Set {resolved_key} = {resolved_value}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Env set error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'export': True, 'output_var': 'env_set_result'}


class EnvListAction(BaseAction):
    """List environment variables."""
    action_type = "env_list"
    display_name = "环境变量列表"
    description = "列出环境变量"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute env list."""
        filter_key = params.get('filter', '')
        limit = params.get('limit', 100)
        output_var = params.get('output_var', 'env_list')

        try:
            resolved_filter = context.resolve_value(filter_key) if context else filter_key
            resolved_limit = context.resolve_value(limit) if context else limit

            env_vars = os.environ
            if resolved_filter:
                env_vars = {k: v for k, v in env_vars.items() if resolved_filter.lower() in k.lower()}

            items = sorted(env_vars.items())[:resolved_limit]
            result = {'variables': [{'key': k, 'value': v} for k, v in items], 'count': len(items)}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Listed {len(items)} env vars", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Env list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filter': '', 'limit': 100, 'output_var': 'env_list'}


class EnvExpandAction(BaseAction):
    """Expand environment variables in string."""
    action_type = "env_expand"
    display_name = "环境变量展开"
    description = "展开字符串中的环境变量"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute env expand."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'expanded_value')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            expanded = os.path.expanduser(os.path.expandvars(resolved_value))

            if context:
                context.set(output_var, expanded)
            return ActionResult(success=True, message=expanded, data={'original': resolved_value, 'expanded': expanded})
        except Exception as e:
            return ActionResult(success=False, message=f"Env expand error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expanded_value'}
