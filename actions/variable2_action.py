"""Variable2 action module for RabAI AutoClick.

Provides advanced variable operations:
- VariableCopyAction: Copy variable value
- VariableDeleteAction: Delete variable
- VariableExistsAction: Check if variable exists
- VariableTypeAction: Get variable type
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class VariableCopyAction(BaseAction):
    """Copy variable value."""
    action_type = "variable_copy"
    display_name = "复制变量"
    description = "复制变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy variable.

        Args:
            context: Execution context.
            params: Dict with source_var, dest_var.

        Returns:
            ActionResult indicating success.
        """
        source_var = params.get('source_var', '')
        dest_var = params.get('dest_var', '')

        valid, msg = self.validate_type(source_var, str, 'source_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dest_var, str, 'dest_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source_var)
            context.set(dest_var, resolved_source)

            return ActionResult(
                success=True,
                message=f"变量已复制: {source_var} -> {dest_var}",
                data={
                    'source': source_var,
                    'destination': dest_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source_var', 'dest_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class VariableDeleteAction(BaseAction):
    """Delete variable."""
    action_type = "variable_delete"
    display_name = "删除变量"
    description = "删除变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete variable.

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
            context.delete(resolved_name)

            return ActionResult(
                success=True,
                message=f"变量已删除: {resolved_name}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class VariableExistsAction(BaseAction):
    """Check if variable exists."""
    action_type = "variable_exists"
    display_name = "检查变量存在"
    description = "检查变量是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check exists.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with exists result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'variable_exists')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            result = context.exists(resolved_name)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"变量存在: {'是' if result else '否'}",
                data={
                    'exists': result,
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'variable_exists'}


class VariableTypeAction(BaseAction):
    """Get variable type."""
    action_type = "variable_type"
    display_name = "获取变量类型"
    description = "获取变量类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get type.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with type.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'variable_type')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            value = context.get(resolved_name)
            type_name = type(value).__name__
            context.set(output_var, type_name)

            return ActionResult(
                success=True,
                message=f"变量类型: {type_name}",
                data={
                    'type': type_name,
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取变量类型失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'variable_type'}