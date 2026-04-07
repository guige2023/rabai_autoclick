"""Error action module for RabAI AutoClick.

Provides error handling operations:
- ErrorRaiseAction: Raise error
- ErrorCatchAction: Catch error
- ErrorFinallyAction: Finally block
- ErrorEndCatchAction: End catch block
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ErrorRaiseAction(BaseAction):
    """Raise error."""
    action_type = "error_raise"
    display_name = "抛出错误"
    description = "抛出错误"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute raise.

        Args:
            context: Execution context.
            params: Dict with message, error_type.

        Returns:
            ActionResult indicating raise.
        """
        message = params.get('message', 'Unknown error')
        error_type = params.get('error_type', 'RuntimeError')

        try:
            resolved_message = context.resolve_value(message)
            resolved_type = context.resolve_value(error_type)

            error_msg = f"{resolved_type}: {resolved_message}"
            context.set('_error_raised', error_msg)
            context.set('_error_handled', False)

            return ActionResult(
                success=False,
                message=error_msg,
                data={
                    'error_type': resolved_type,
                    'message': resolved_message
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"抛出错误失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_type': 'RuntimeError'}


class ErrorCatchAction(BaseAction):
    """Catch error."""
    action_type = "error_catch"
    display_name = "捕获错误"
    description = "捕获错误"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute catch.

        Args:
            context: Execution context.
            params: Dict with error_types, output_var.

        Returns:
            ActionResult indicating catch setup.
        """
        error_types = params.get('error_types', 'Exception')
        output_var = params.get('output_var', 'caught_error')

        try:
            resolved_types = context.resolve_value(error_types)
            context.set('_catch_error_types', resolved_types)
            context.set('_catch_output_var', output_var)
            context.set('_error_caught', False)

            return ActionResult(
                success=True,
                message=f"错误捕获设置: {resolved_types}",
                data={
                    'error_types': resolved_types,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置错误捕获失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_types': 'Exception', 'output_var': 'caught_error'}


class ErrorFinallyAction(BaseAction):
    """Finally block."""
    action_type = "error_finally"
    display_name = "最终块"
    description = "最终执行块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute finally.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating finally.
        """
        return ActionResult(
            success=True,
            message="执行最终块",
            data={'executed': True}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ErrorEndCatchAction(BaseAction):
    """End catch block."""
    action_type = "error_end_catch"
    display_name = "结束捕获"
    description = "结束错误捕获块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end catch.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating end.
        """
        context.delete('_catch_error_types')
        context.delete('_catch_output_var')
        context.delete('_error_caught')
        context.delete('_error_raised')
        context.delete('_error_handled')

        return ActionResult(
            success=True,
            message="错误捕获块结束"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
