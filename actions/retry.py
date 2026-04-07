"""Retry action module for RabAI AutoClick.

Provides retry and error handling actions:
- RetryAction: Retry a step multiple times on failure
- IgnoreErrorAction: Continue workflow even if step fails
- FailAction: Always fail a step (for testing)
- ExitWorkflowAction: Exit the workflow with status
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RetryAction(BaseAction):
    """Retry a step multiple times on failure."""
    action_type = "retry"
    display_name = "重试"
    description = "失败时重试指定次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a retry action.

        Args:
            context: Execution context.
            params: Dict with max_retries, retry_delay, step_id.

        Returns:
            ActionResult indicating success.
        """
        max_retries = params.get('max_retries', 3)
        retry_delay = params.get('retry_delay', 1.0)
        step_id = params.get('step_id', None)

        # Validate max_retries
        valid, msg = self.validate_type(max_retries, int, 'max_retries')
        if not valid:
            return ActionResult(success=False, message=msg)
        if max_retries < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'max_retries' must be >= 0, got {max_retries}"
            )

        # Validate retry_delay
        valid, msg = self.validate_type(retry_delay, (int, float), 'retry_delay')
        if not valid:
            return ActionResult(success=False, message=msg)
        if retry_delay < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'retry_delay' must be >= 0, got {retry_delay}"
            )

        # Get current retry count from context
        retry_var = params.get('retry_var', '_retry_count')
        current_retry = context.get(retry_var, 0)

        # Increment retry count
        context.set(retry_var, current_retry + 1)

        return ActionResult(
            success=True,
            message=f"重试 {current_retry + 1}/{max_retries}",
            data={
                'current_retry': current_retry + 1,
                'max_retries': max_retries,
                'retry_delay': retry_delay
            },
            next_step_id=step_id
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_retries': 3,
            'retry_delay': 1.0,
            'step_id': None,
            'retry_var': '_retry_count'
        }


class IgnoreErrorAction(BaseAction):
    """Ignore errors and continue workflow execution."""
    action_type = "ignore_error"
    display_name = "忽略错误"
    description = "忽略步骤错误并继续执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an ignore error action.

        Args:
            context: Execution context.
            params: Dict with error_message.

        Returns:
            ActionResult indicating the error was ignored.
        """
        error_message = params.get('error_message', '')

        # Mark that an error was ignored
        context.set('_last_error_ignored', True)
        context.set('_last_error_message', error_message)

        return ActionResult(
            success=True,
            message=f"错误已忽略: {error_message}",
            data={'error_ignored': True, 'error_message': error_message}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_message': ''}


class FailAction(BaseAction):
    """Always fail (for testing error handling)."""
    action_type = "fail"
    display_name = "故意失败"
    description = "故意失败（用于测试错误处理）"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a fail action.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult that always fails.
        """
        message = params.get('message', '故意失败')

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        return ActionResult(
            success=False,
            message=f"故意失败: {message}"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message': '故意失败'}


class ExitWorkflowAction(BaseAction):
    """Exit the workflow with a status."""
    action_type = "exit_workflow"
    display_name = "退出工作流"
    description = "退出当前工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an exit workflow action.

        Args:
            context: Execution context.
            params: Dict with status, message.

        Returns:
            ActionResult indicating exit.
        """
        status = params.get('status', True)
        message = params.get('message', '退出工作流')

        # Validate status
        valid, msg = self.validate_type(status, bool, 'status')
        if not valid:
            return ActionResult(success=False, message=msg)

        return ActionResult(
            success=status,
            message=message,
            data={'exit_status': status}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'status': True,
            'message': '退出工作流'
        }


class LogWarningAction(BaseAction):
    """Log a warning message without failing."""
    action_type = "log_warning"
    display_name = "记录警告"
    description = "记录警告日志并继续执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a log warning action.

        Args:
            context: Execution context.
            params: Dict with message.

        Returns:
            ActionResult indicating success.
        """
        message = params.get('message', '')

        # Resolve variables
        resolved_message = context.resolve_value(message)

        # Log to app logger
        from utils.app_logger import app_logger
        app_logger.warning(f"[Workflow] {resolved_message}")

        return ActionResult(
            success=True,
            message=f"警告已记录: {resolved_message}",
            data={'message': resolved_message}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message': ''}


class AssertAction(BaseAction):
    """Assert a condition and fail if false."""
    action_type = "assert"
    display_name = "断言"
    description = "断言条件为真，否则失败"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an assert action.

        Args:
            context: Execution context.
            params: Dict with condition, error_message.

        Returns:
            ActionResult based on assertion result.
        """
        condition = params.get('condition', '')
        error_message = params.get('error_message', '断言失败')

        if not condition:
            return ActionResult(
                success=False,
                message="未指定断言条件"
            )

        try:
            # Evaluate the condition
            result = context.safe_exec(condition)
            condition_met = bool(result)

            if condition_met:
                return ActionResult(
                    success=True,
                    message=f"断言通过: {condition}",
                    data={'condition': condition, 'result': result}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"断言失败: {error_message}",
                    data={'condition': condition, 'result': result}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断言评估失败: {str(e)}",
                data={'condition': condition, 'error': str(e)}
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'error_message': '断言失败'}