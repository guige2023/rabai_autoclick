"""Variable3 action module for RabAI AutoClick.

Provides additional variable operations:
- VariableDeleteAction: Delete variable
- VariableExistsAction: Check if variable exists
- VariableCopyAction: Copy variable
- VariableClearAllAction: Clear all variables
- VariableListAction: List all variables
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class VariableDeleteAction(BaseAction):
    """Delete variable."""
    action_type = "variable3_delete"
    display_name = "删除变量"
    description = "删除指定变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete variable.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with delete result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'delete_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)

            existed = context.delete(resolved_name)
            context.set(output_var, existed)

            return ActionResult(
                success=True,
                message=f"删除变量: {'成功' if existed else '变量不存在'}",
                data={
                    'name': resolved_name,
                    'existed': existed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_result'}


class VariableExistsAction(BaseAction):
    """Check if variable exists."""
    action_type = "variable3_exists"
    display_name = "检查变量存在"
    description = "检查变量是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check variable exists.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with check result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'exists_result')

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
                    'name': resolved_name,
                    'exists': result,
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
        return {'output_var': 'exists_result'}


class VariableCopyAction(BaseAction):
    """Copy variable."""
    action_type = "variable3_copy"
    display_name = "复制变量"
    description = "复制变量到新名称"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy variable.

        Args:
            context: Execution context.
            params: Dict with source, dest, output_var.

        Returns:
            ActionResult with copy result.
        """
        source = params.get('source', '')
        dest = params.get('dest', '')
        output_var = params.get('output_var', 'copy_result')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dest, str, 'dest')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(dest)

            value = context.get(resolved_source)
            context.set(resolved_dest, value)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"复制变量: {resolved_source} -> {resolved_dest}",
                data={
                    'source': resolved_source,
                    'dest': resolved_dest,
                    'value': value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'dest']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copy_result'}


class VariableClearAllAction(BaseAction):
    """Clear all variables."""
    action_type = "variable3_clear_all"
    display_name = "清空所有变量"
    description = "清空所有变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear all variables.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear result.
        """
        output_var = params.get('output_var', 'clear_result')

        try:
            if hasattr(context, 'clear_all'):
                context.clear_all()
                context.set(output_var, True)
            else:
                return ActionResult(
                    success=False,
                    message="上下文不支持清空所有变量"
                )

            return ActionResult(
                success=True,
                message="所有变量已清空",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}


class VariableListAction(BaseAction):
    """List all variables."""
    action_type = "variable3_list"
    display_name = "列出变量"
    description = "列出所有变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list variables.

        Args:
            context: Execution context.
            params: Dict with filter, output_var.

        Returns:
            ActionResult with variable list.
        """
        filter_str = params.get('filter', None)
        output_var = params.get('output_var', 'variable_list')

        try:
            resolved_filter = context.resolve_value(filter_str) if filter_str else None

            if hasattr(context, 'get_all'):
                all_vars = context.get_all()
            elif hasattr(context, 'variables'):
                all_vars = context.variables
            else:
                return ActionResult(
                    success=False,
                    message="上下文不支持列出变量"
                )

            if resolved_filter:
                filtered = {
                    k: v for k, v in all_vars.items()
                    if resolved_filter.lower() in k.lower()
                }
            else:
                filtered = all_vars

            context.set(output_var, filtered)

            return ActionResult(
                success=True,
                message=f"变量列表: {len(filtered)} 个",
                data={
                    'variables': filtered,
                    'count': len(filtered),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filter': None, 'output_var': 'variable_list'}