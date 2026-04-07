"""Retry action module for RabAI AutoClick.

Provides retry operations:
- RetryAction: Retry action
- RetryUntilSuccessAction: Retry until success
- RetryMaxAttemptsAction: Max retry attempts
- RetryEndAction: End retry block
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryAction(BaseAction):
    """Retry action."""
    action_type = "retry"
    display_name = "重试"
    description = "重试操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute retry.

        Args:
            context: Execution context.
            params: Dict with max_attempts, delay, backoff.

        Returns:
            ActionResult with retry status.
        """
        max_attempts = params.get('max_attempts', 3)
        delay = params.get('delay', 1)
        backoff = params.get('backoff', 1)

        try:
            resolved_max = int(context.resolve_value(max_attempts))
            resolved_delay = float(context.resolve_value(delay))
            resolved_backoff = float(context.resolve_value(backoff))

            context.set('_retry_max_attempts', resolved_max)
            context.set('_retry_delay', resolved_delay)
            context.set('_retry_backoff', resolved_backoff)
            context.set('_retry_attempt', 0)
            context.set('_retry_exhausted', False)

            return ActionResult(
                success=True,
                message=f"重试设置: 最多{resolved_max}次",
                data={
                    'max_attempts': resolved_max,
                    'delay': resolved_delay,
                    'backoff': resolved_backoff
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置重试失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_attempts': 3, 'delay': 1, 'backoff': 1}


class RetryUntilSuccessAction(BaseAction):
    """Retry until success."""
    action_type = "retry_until_success"
    display_name = "重试直到成功"
    description = "重试直到成功"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute retry until success.

        Args:
            context: Execution context.
            params: Dict with condition.

        Returns:
            ActionResult with retry status.
        """
        condition = params.get('condition', 'False')

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_condition = context.resolve_value(condition)
            result = context.safe_exec(f"return_value = {resolved_condition}")

            current_attempt = context.get('_retry_attempt', 0) + 1
            context.set('_retry_attempt', current_attempt)

            if result:
                context.set('_retry_exhausted', True)

            return ActionResult(
                success=True,
                message=f"重试条件: {'满足' if result else '不满足'}",
                data={
                    'condition': resolved_condition,
                    'result': result,
                    'attempt': current_attempt
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重试条件判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class RetryMaxAttemptsAction(BaseAction):
    """Max retry attempts."""
    action_type = "retry_max_attempts"
    display_name = "最大重试次数"
    description = "检查最大重试次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max attempts check.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult with attempts status.
        """
        current_attempt = context.get('_retry_attempt', 0)
        max_attempts = context.get('_retry_max_attempts', 3)
        exhausted = context.get('_retry_exhausted', False)

        if exhausted or current_attempt >= max_attempts:
            context.set('_retry_exhausted', True)
            return ActionResult(
                success=True,
                message=f"重试次数已用尽: {current_attempt}/{max_attempts}",
                data={
                    'exhausted': True,
                    'attempt': current_attempt,
                    'max_attempts': max_attempts
                }
            )

        return ActionResult(
            success=True,
            message=f"继续重试: {current_attempt}/{max_attempts}",
            data={
                'exhausted': False,
                'attempt': current_attempt,
                'max_attempts': max_attempts
            }
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class RetryEndAction(BaseAction):
    """End retry block."""
    action_type = "retry_end"
    display_name = "结束重试"
    description = "结束重试块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end retry.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating end.
        """
        final_attempt = context.get('_retry_attempt', 0)
        exhausted = context.get('_retry_exhausted', False)

        context.delete('_retry_max_attempts')
        context.delete('_retry_delay')
        context.delete('_retry_backoff')
        context.delete('_retry_attempt')
        context.delete('_retry_exhausted')

        return ActionResult(
            success=True,
            message=f"重试块结束: {final_attempt}次尝试",
            data={
                'total_attempts': final_attempt,
                'exhausted': exhausted
            }
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
