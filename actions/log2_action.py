"""Log2 action module for RabAI AutoClick.

Provides additional logging operations:
- LogDebugAction: Log debug message
- LogInfoAction: Log info message
- LogWarningAction: Log warning message
- LogErrorAction: Log error message
- LogCriticalAction: Log critical message
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogDebugAction(BaseAction):
    """Log debug message."""
    action_type = "log2_debug"
    display_name = "日志调试"
    description = "记录调试级别日志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute debug.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'log_status')

        try:
            import logging

            resolved = context.resolve_value(message) if message else ''

            logging.debug(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"[DEBUG] {resolved}",
                data={
                    'level': 'DEBUG',
                    'message': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日志调试失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_status'}


class LogInfoAction(BaseAction):
    """Log info message."""
    action_type = "log2_info"
    display_name = "日志信息"
    description = "记录信息级别日志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'log_status')

        try:
            import logging

            resolved = context.resolve_value(message) if message else ''

            logging.info(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"[INFO] {resolved}",
                data={
                    'level': 'INFO',
                    'message': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日志信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_status'}


class LogWarningAction(BaseAction):
    """Log warning message."""
    action_type = "log2_warning"
    display_name = "日志警告"
    description = "记录警告级别日志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute warning.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'log_status')

        try:
            import logging

            resolved = context.resolve_value(message) if message else ''

            logging.warning(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"[WARNING] {resolved}",
                data={
                    'level': 'WARNING',
                    'message': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日志警告失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_status'}


class LogErrorAction(BaseAction):
    """Log error message."""
    action_type = "log2_error"
    display_name = "日志错误"
    description = "记录错误级别日志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute error.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'log_status')

        try:
            import logging

            resolved = context.resolve_value(message) if message else ''

            logging.error(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"[ERROR] {resolved}",
                data={
                    'level': 'ERROR',
                    'message': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日志错误失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_status'}


class LogCriticalAction(BaseAction):
    """Log critical message."""
    action_type = "log2_critical"
    display_name = "日志严重错误"
    description = "记录严重错误级别日志"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute critical.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'log_status')

        try:
            import logging

            resolved = context.resolve_value(message) if message else ''

            logging.critical(resolved)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"[CRITICAL] {resolved}",
                data={
                    'level': 'CRITICAL',
                    'message': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日志严重错误失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'log_status'}