"""Debug action module for RabAI AutoClick.

Provides debugging and troubleshooting actions:
- DebugPrintAction: Print debug information
- DumpContextAction: Dump all context variables
- AssertEqualAction: Assert two values are equal
- AssertNotEqualAction: Assert two values are not equal
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DebugPrintAction(BaseAction):
    """Print debug information."""
    action_type = "debug_print"
    display_name = "调试打印"
    description = "打印调试信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute printing debug info.

        Args:
            context: Execution context.
            params: Dict with message, level.

        Returns:
            ActionResult indicating success.
        """
        message = params.get('message', '')
        level = params.get('level', 'info')

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid_levels = ['debug', 'info', 'warning', 'error']
        valid, msg = self.validate_in(level, valid_levels, 'level')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_message = context.resolve_value(message)

            # Get app logger and log at appropriate level
            from utils.app_logger import app_logger

            if level == 'debug':
                app_logger.debug(f"[DEBUG] {resolved_message}")
            elif level == 'info':
                app_logger.info(f"[INFO] {resolved_message}")
            elif level == 'warning':
                app_logger.warning(f"[WARNING] {resolved_message}")
            elif level == 'error':
                app_logger.error(f"[ERROR] {resolved_message}")

            return ActionResult(
                success=True,
                message=f"调试打印 [{level}]: {resolved_message}",
                data={'message': resolved_message, 'level': level}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调试打印失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 'info'}


class DumpContextAction(BaseAction):
    """Dump all context variables."""
    action_type = "dump_context"
    display_name = "导出上下文"
    description = "导出所有上下文变量用于调试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dumping context.

        Args:
            context: Execution context.
            params: Dict with output_var, include_history.

        Returns:
            ActionResult with context data.
        """
        output_var = params.get('output_var', 'context_dump')
        include_history = params.get('include_history', False)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(include_history, bool, 'include_history')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result_data = {
                'variables': context.get_all(),
                'variable_count': len(context.get_all())
            }

            if include_history:
                result_data['history'] = context.get_history()
                result_data['history_count'] = len(context.get_history())

            context.set(output_var, result_data)

            return ActionResult(
                success=True,
                message=f"上下文已导出: {result_data['variable_count']} 个变量",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"导出上下文失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_var': 'context_dump',
            'include_history': False
        }


class AssertEqualAction(BaseAction):
    """Assert two values are equal."""
    action_type = "assert_equal"
    display_name = "断言相等"
    description = "断言两个值相等，否则失败"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute asserting equality.

        Args:
            context: Execution context.
            params: Dict with value1, value2, message.

        Returns:
            ActionResult based on assertion result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        error_message = params.get('error_message', 'Assertion failed')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)

            if resolved_v1 == resolved_v2:
                return ActionResult(
                    success=True,
                    message=f"断言通过: {resolved_v1} == {resolved_v2}",
                    data={
                        'value1': resolved_v1,
                        'value2': resolved_v2,
                        'equal': True
                    }
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"断言失败: {resolved_v1} != {resolved_v2}: {error_message}",
                    data={
                        'value1': resolved_v1,
                        'value2': resolved_v2,
                        'equal': False
                    }
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断言评估失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_message': 'Values are not equal'}


class AssertNotEqualAction(BaseAction):
    """Assert two values are not equal."""
    action_type = "assert_not_equal"
    display_name = "断言不等"
    description = "断言两个值不相等，否则失败"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute asserting inequality.

        Args:
            context: Execution context.
            params: Dict with value1, value2, message.

        Returns:
            ActionResult based on assertion result.
        """
        value1 = params.get('value1', None)
        value2 = params.get('value2', None)
        error_message = params.get('error_message', 'Assertion failed')

        try:
            resolved_v1 = context.resolve_value(value1)
            resolved_v2 = context.resolve_value(value2)

            if resolved_v1 != resolved_v2:
                return ActionResult(
                    success=True,
                    message=f"断言通过: {resolved_v1} != {resolved_v2}",
                    data={
                        'value1': resolved_v1,
                        'value2': resolved_v2,
                        'not_equal': True
                    }
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"断言失败: {resolved_v1} == {resolved_v2}: {error_message}",
                    data={
                        'value1': resolved_v1,
                        'value2': resolved_v2,
                        'not_equal': False
                    }
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断言评估失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_message': 'Values are equal'}


class AssertContainsAction(BaseAction):
    """Assert a value contains a substring."""
    action_type = "assert_contains"
    display_name = "断言包含"
    description = "断言值包含子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute asserting contains.

        Args:
            context: Execution context.
            params: Dict with value, substring, message.

        Returns:
            ActionResult based on assertion result.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        error_message = params.get('error_message', 'Assertion failed')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_substring = context.resolve_value(substring)

            if resolved_substring in resolved_value:
                return ActionResult(
                    success=True,
                    message=f"断言通过: '{resolved_substring}' in '{resolved_value[:50]}...'",
                    data={
                        'value': resolved_value,
                        'substring': resolved_substring,
                        'contains': True
                    }
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"断言失败: '{resolved_substring}' not in '{resolved_value[:50]}...': {error_message}",
                    data={
                        'value': resolved_value,
                        'substring': resolved_substring,
                        'contains': False
                    }
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断言评估失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_message': 'Substring not found'}