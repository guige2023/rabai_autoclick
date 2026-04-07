"""String8 action module for RabAI AutoClick.

Provides additional string operations:
- StringFindAction: Find substring index
- StringRfindAction: Find substring from right
- StringIndexAction: Find substring with error
- StringRindexAction: Find substring from right with error
- StringCountSubstringAction: Count substring occurrences
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringFindAction(BaseAction):
    """Find substring index."""
    action_type = "string8_find"
    display_name = "查找子串"
    description = "查找子串首次出现的位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with index or -1.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'find_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)

            result = resolved.find(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查找子串: 索引 {result}",
                data={
                    'value': resolved,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'find_result'}


class StringRfindAction(BaseAction):
    """Find substring from right."""
    action_type = "string8_rfind"
    display_name = "从右查找子串"
    description = "从右侧查找子串首次出现的位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rfind.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with index or -1.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'rfind_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)

            result = resolved.rfind(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"从右查找子串: 索引 {result}",
                data={
                    'value': resolved,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从右查找子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rfind_result'}


class StringIndexAction(BaseAction):
    """Find substring with error."""
    action_type = "string8_index"
    display_name = "查找子串索引"
    description = "查找子串首次出现的位置，不存在则报错"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with index.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'index_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)

            result = resolved.index(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查找子串索引: {result}",
                data={
                    'value': resolved,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"子串未找到: {resolved_sub}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找子串索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class StringRindexAction(BaseAction):
    """Find substring from right with error."""
    action_type = "string8_rindex"
    display_name = "从右查找子串索引"
    description = "从右侧查找子串首次出现的位置，不存在则报错"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rindex.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with index.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'rindex_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)

            result = resolved.rindex(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"从右查找子串索引: {result}",
                data={
                    'value': resolved,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"子串未找到: {resolved_sub}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从右查找子串索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rindex_result'}


class StringCountSubstringAction(BaseAction):
    """Count substring occurrences."""
    action_type = "string8_count"
    display_name = "计数子串"
    description = "统计子串出现的次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with count.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'count_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)

            result = resolved.count(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"计数子串: {result} 次",
                data={
                    'value': resolved,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}
