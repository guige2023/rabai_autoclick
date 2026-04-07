"""String9 action module for RabAI AutoClick.

Provides additional string operations:
- StringReplaceAction: Replace substring once
- StringReplaceAllAction: Replace all occurrences
- StringSplitAction: Split string
- StringRsplitAction: Split string from right
- StringPartitionAction: Partition string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringReplaceAction(BaseAction):
    """Replace substring once."""
    action_type = "string9_replace"
    display_name = "替换子串"
    description = "替换子串首次出现"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace.

        Args:
            context: Execution context.
            params: Dict with value, old, new, output_var.

        Returns:
            ActionResult with replaced string.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        output_var = params.get('output_var', 'replace_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(old, str, 'old')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new, str, 'new')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)

            result = resolved.replace(resolved_old, resolved_new, 1)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换子串完成",
                data={
                    'original': resolved,
                    'old': resolved_old,
                    'new': resolved_new,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"替换子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'replace_result'}


class StringReplaceAllAction(BaseAction):
    """Replace all occurrences."""
    action_type = "string9_replace_all"
    display_name = "替换所有"
    description = "替换所有出现的子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace all.

        Args:
            context: Execution context.
            params: Dict with value, old, new, output_var.

        Returns:
            ActionResult with replaced string.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        output_var = params.get('output_var', 'replace_all_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(old, str, 'old')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new, str, 'new')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)

            result = resolved.replace(resolved_old, resolved_new)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换所有完成",
                data={
                    'original': resolved,
                    'old': resolved_old,
                    'new': resolved_new,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"替换所有失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'replace_all_result'}


class StringSplitAction(BaseAction):
    """Split string."""
    action_type = "string9_split"
    display_name = "分割字符串"
    description = "按分隔符分割字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split.

        Args:
            context: Execution context.
            params: Dict with value, separator, maxsplit, output_var.

        Returns:
            ActionResult with split list.
        """
        value = params.get('value', '')
        separator = params.get('separator', None)
        maxsplit = params.get('maxsplit', -1)
        output_var = params.get('output_var', 'split_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sep = context.resolve_value(separator) if separator is not None else None
            resolved_maxsplit = int(context.resolve_value(maxsplit)) if maxsplit != -1 else -1

            if resolved_sep is None:
                parts = resolved.split(maxsplit=resolved_maxsplit)
            else:
                parts = resolved.split(resolved_sep, maxsplit=resolved_maxsplit)

            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"分割字符串: {len(parts)} 部分",
                data={
                    'original': resolved,
                    'separator': resolved_sep,
                    'result': parts,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': None, 'maxsplit': -1, 'output_var': 'split_result'}


class StringRsplitAction(BaseAction):
    """Split string from right."""
    action_type = "string9_rsplit"
    display_name = "从右分割"
    description = "从右侧按分隔符分割字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rsplit.

        Args:
            context: Execution context.
            params: Dict with value, separator, maxsplit, output_var.

        Returns:
            ActionResult with split list.
        """
        value = params.get('value', '')
        separator = params.get('separator', None)
        maxsplit = params.get('maxsplit', -1)
        output_var = params.get('output_var', 'rsplit_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sep = context.resolve_value(separator) if separator is not None else None
            resolved_maxsplit = int(context.resolve_value(maxsplit)) if maxsplit != -1 else -1

            if resolved_sep is None:
                parts = resolved.rsplit(maxsplit=resolved_maxsplit)
            else:
                parts = resolved.rsplit(resolved_sep, maxsplit=resolved_maxsplit)

            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"从右分割: {len(parts)} 部分",
                data={
                    'original': resolved,
                    'separator': resolved_sep,
                    'result': parts,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从右分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': None, 'maxsplit': -1, 'output_var': 'rsplit_result'}


class StringPartitionAction(BaseAction):
    """Partition string."""
    action_type = "string9_partition"
    display_name = "分区字符串"
    description = "按分隔符将字符串分为三部分"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute partition.

        Args:
            context: Execution context.
            params: Dict with value, separator, output_var.

        Returns:
            ActionResult with partition tuple.
        """
        value = params.get('value', '')
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'partition_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(separator, str, 'separator')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sep = context.resolve_value(separator)

            result = resolved.partition(resolved_sep)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分区字符串: {result}",
                data={
                    'original': resolved,
                    'separator': resolved_sep,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分区字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'separator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'partition_result'}
