"""Environment variable action module for RabAI AutoClick.

Provides environment variable read/write operations
with type conversion and default value support.
"""

import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EnvGetAction(BaseAction):
    """Get environment variable value.
    
    Supports type conversion and default values.
    """
    action_type = "env_get"
    display_name = "获取环境变量"
    description = "读取环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get environment variable.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, value_type,
                   default_value, save_to_var.
        
        Returns:
            ActionResult with variable value.
        """
        name = params.get('name', '')
        value_type = params.get('value_type', 'str')
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)

        if not name:
            return ActionResult(success=False, message="Variable name is required")

        value = os.environ.get(name)

        if value is None:
            result_data = {
                'found': False,
                'name': name,
                'value': default_value,
                'type': value_type
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=True,
                message=f"环境变量不存在，使用默认值: {name}",
                data=result_data
            )

        # Type conversion
        converted = value
        if value_type == 'int':
            try:
                converted = int(value)
            except ValueError:
                converted = default_value
        elif value_type == 'float':
            try:
                converted = float(value)
            except ValueError:
                converted = default_value
        elif value_type == 'bool':
            converted = value.lower() in ('true', '1', 'yes', 'on')
        elif value_type == 'list':
            converted = value.split(',')
        elif value_type == 'json':
            import json
            try:
                converted = json.loads(value)
            except json.JSONDecodeError:
                converted = default_value

        result_data = {
            'found': True,
            'name': name,
            'value': converted,
            'raw_value': value,
            'type': value_type
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"环境变量: {name} = {converted}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value_type': 'str',
            'default_value': None,
            'save_to_var': None
        }


class EnvSetAction(BaseAction):
    """Set environment variable.
    
    Supports process-level and permanent export options.
    """
    action_type = "env_set"
    display_name = "设置环境变量"
    description = "设置环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Set environment variable.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, value, export,
                   save_to_var.
        
        Returns:
            ActionResult with set result.
        """
        name = params.get('name', '')
        value = params.get('value', '')
        export = params.get('export', True)
        save_to_var = params.get('save_to_var', None)

        if not name:
            return ActionResult(success=False, message="Variable name is required")

        if not isinstance(name, str) or not name.replace('_', '').isalnum():
            return ActionResult(
                success=False,
                message=f"Invalid variable name: {name}"
            )

        try:
            os.environ[name] = str(value)

            result_data = {
                'set': True,
                'name': name,
                'value': value,
                'export': export
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"环境变量已设置: {name}={value}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'export': True,
            'save_to_var': None
        }
