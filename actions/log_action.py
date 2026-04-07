"""Log action module for RabAI AutoClick.

Provides logging operations:
- LogInfoAction: Log info
- LogWarningAction: Log warning
- LogErrorAction: Log error
- LogDebugAction: Log debug
- LogSuccessAction: Log success
"""

import logging
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogInfoAction(BaseAction):
    """Log info."""
    action_type = "log_info"
    display_name = "记录信息"
    description = "记录信息日志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info log.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating logged.
        """
        message = params.get('message', '')

        try:
            resolved_message = context.resolve_value(message)

            logging.info(resolved_message)

            return ActionResult(
                success=True,
                message=f"[INFO] {resolved_message}",
                data={'level': 'INFO', 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LogWarningAction(BaseAction):
    """Log warning."""
    action_type = "log_warning"
    display_name = "记录警告"
    description = "记录警告日志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute warning log.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating logged.
        """
        message = params.get('message', '')

        try:
            resolved_message = context.resolve_value(message)

            logging.warning(resolved_message)

            return ActionResult(
                success=True,
                message=f"[WARNING] {resolved_message}",
                data={'level': 'WARNING', 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LogErrorAction(BaseAction):
    """Log error."""
    action_type = "log_error"
    display_name = "记录错误"
    description = "记录错误日志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute error log.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating logged.
        """
        message = params.get('message', '')

        try:
            resolved_message = context.resolve_value(message)

            logging.error(resolved_message)

            return ActionResult(
                success=True,
                message=f"[ERROR] {resolved_message}",
                data={'level': 'ERROR', 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LogDebugAction(BaseAction):
    """Log debug."""
    action_type = "log_debug"
    display_name = "记录调试"
    description = "记录调试日志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute debug log.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating logged.
        """
        message = params.get('message', '')

        try:
            resolved_message = context.resolve_value(message)

            logging.debug(resolved_message)

            return ActionResult(
                success=True,
                message=f"[DEBUG] {resolved_message}",
                data={'level': 'DEBUG', 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LogSuccessAction(BaseAction):
    """Log success."""
    action_type = "log_success"
    display_name = "记录成功"
    description = "记录成功日志"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute success log.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating logged.
        """
        message = params.get('message', '')

        try:
            resolved_message = context.resolve_value(message)

            logging.info(f"[SUCCESS] {resolved_message}")

            return ActionResult(
                success=True,
                message=f"[SUCCESS] {resolved_message}",
                data={'level': 'SUCCESS', 'message': resolved_message}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"记录日志失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
