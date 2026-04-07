"""Log14 action module for RabAI AutoClick.

Provides additional logging operations:
- LogDebugAction: Log debug message
- LogInfoAction: Log info message
- LogWarningAction: Log warning message
- LogErrorAction: Log error message
- LogCriticalAction: Log critical message
- LogFormatAction: Format log message
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogDebugAction(BaseAction):
    """Log debug message."""
    action_type = "log14_debug"
    display_name = "调试日志"
    description = "记录调试级别日志"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute debug log.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with logged message.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'debug_log')

        try:
            import logging

            resolved_message = context.resolve_value(message) if message else ''

            logger = logging.getLogger('rabai')
            logger.debug(resolved_message)

            result = {
                'level': 'DEBUG',
                'message': resolved_message,
                'timestamp': self._get_timestamp()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"调试日志: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调试日志失败: {str(e)}"
            )

    def _get_timestamp(self) -> str:
        import datetime
        return datetime.datetime.now().isoformat()

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'debug_log'}


class LogInfoAction(BaseAction):
    """Log info message."""
    action_type = "log14_info"
    display_name = "信息日志"
    description = "记录信息级别日志"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info log.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with logged message.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'info_log')

        try:
            import logging

            resolved_message = context.resolve_value(message) if message else ''

            logger = logging.getLogger('rabai')
            logger.info(resolved_message)

            result = {
                'level': 'INFO',
                'message': resolved_message,
                'timestamp': self._get_timestamp()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"信息日志: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"信息日志失败: {str(e)}"
            )

    def _get_timestamp(self) -> str:
        import datetime
        return datetime.datetime.now().isoformat()

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'info_log'}


class LogWarningAction(BaseAction):
    """Log warning message."""
    action_type = "log14_warning"
    display_name = "警告日志"
    description = "记录警告级别日志"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute warning log.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with logged message.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'warning_log')

        try:
            import logging

            resolved_message = context.resolve_value(message) if message else ''

            logger = logging.getLogger('rabai')
            logger.warning(resolved_message)

            result = {
                'level': 'WARNING',
                'message': resolved_message,
                'timestamp': self._get_timestamp()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"警告日志: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"警告日志失败: {str(e)}"
            )

    def _get_timestamp(self) -> str:
        import datetime
        return datetime.datetime.now().isoformat()

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'warning_log'}


class LogErrorAction(BaseAction):
    """Log error message."""
    action_type = "log14_error"
    display_name = "错误日志"
    description = "记录错误级别日志"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute error log.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with logged message.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'error_log')

        try:
            import logging

            resolved_message = context.resolve_value(message) if message else ''

            logger = logging.getLogger('rabai')
            logger.error(resolved_message)

            result = {
                'level': 'ERROR',
                'message': resolved_message,
                'timestamp': self._get_timestamp()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"错误日志: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"错误日志失败: {str(e)}"
            )

    def _get_timestamp(self) -> str:
        import datetime
        return datetime.datetime.now().isoformat()

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'error_log'}


class LogCriticalAction(BaseAction):
    """Log critical message."""
    action_type = "log14_critical"
    display_name = "严重日志"
    description = "记录严重级别日志"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute critical log.

        Args:
            context: Execution context.
            params: Dict with message, output_var.

        Returns:
            ActionResult with logged message.
        """
        message = params.get('message', '')
        output_var = params.get('output_var', 'critical_log')

        try:
            import logging

            resolved_message = context.resolve_value(message) if message else ''

            logger = logging.getLogger('rabai')
            logger.critical(resolved_message)

            result = {
                'level': 'CRITICAL',
                'message': resolved_message,
                'timestamp': self._get_timestamp()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"严重日志: {resolved_message}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"严重日志失败: {str(e)}"
            )

    def _get_timestamp(self) -> str:
        import datetime
        return datetime.datetime.now().isoformat()

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'critical_log'}


class LogFormatAction(BaseAction):
    """Format log message."""
    action_type = "log14_format"
    display_name = "格式化日志"
    description = "格式化日志消息"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute log format.

        Args:
            context: Execution context.
            params: Dict with template, level, output_var.

        Returns:
            ActionResult with formatted log.
        """
        template = params.get('template', '{timestamp} [{level}] {message}')
        level = params.get('level', 'INFO')
        message = params.get('message', '')
        output_var = params.get('output_var', 'formatted_log')

        try:
            import datetime

            resolved_template = context.resolve_value(template) if template else '{timestamp} [{level}] {message}'
            resolved_level = context.resolve_value(level) if level else 'INFO'
            resolved_message = context.resolve_value(message) if message else ''

            timestamp = datetime.datetime.now().isoformat()

            result = resolved_template.format(
                timestamp=timestamp,
                level=resolved_level,
                message=resolved_message
            )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化日志: {result}",
                data={
                    'formatted': result,
                    'timestamp': timestamp,
                    'level': resolved_level,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['template', 'level', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_log'}