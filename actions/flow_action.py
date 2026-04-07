"""Flow action module for RabAI AutoClick.

Provides flow control operations:
- FlowStartAction: Start workflow
- FlowEndAction: End workflow
- FlowPauseAction: Pause workflow
- FlowResumeAction: Resume workflow
- FlowStopAction: Stop workflow
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FlowStartAction(BaseAction):
    """Start workflow."""
    action_type = "flow_start"
    display_name = "开始工作流"
    description = "开始工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating start.
        """
        name = params.get('name', 'workflow')

        try:
            resolved_name = context.resolve_value(name)
            context.set('_flow_name', resolved_name)
            context.set('_flow_started', True)
            context.set('_flow_paused', False)
            context.set('_flow_stopped', False)

            return ActionResult(
                success=True,
                message=f"工作流开始: {resolved_name}",
                data={
                    'name': resolved_name,
                    'started': True
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开始工作流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'workflow'}


class FlowEndAction(BaseAction):
    """End workflow."""
    action_type = "flow_end"
    display_name = "结束工作流"
    description = "结束工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating end.
        """
        flow_name = context.get('_flow_name', 'workflow')
        context.set('_flow_stopped', True)

        return ActionResult(
            success=True,
            message=f"工作流结束: {flow_name}",
            data={
                'name': flow_name,
                'stopped': True
            }
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FlowPauseAction(BaseAction):
    """Pause workflow."""
    action_type = "flow_pause"
    display_name = "暂停工作流"
    description = "暂停工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pause.

        Args:
            context: Execution context.
            params: Dict with reason.

        Returns:
            ActionResult indicating pause.
        """
        reason = params.get('reason', '')

        try:
            resolved_reason = context.resolve_value(reason) if reason else 'User requested'
            context.set('_flow_paused', True)
            context.set('_flow_pause_reason', resolved_reason)

            return ActionResult(
                success=True,
                message=f"工作流已暂停: {resolved_reason}",
                data={
                    'paused': True,
                    'reason': resolved_reason
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"暂停工作流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reason': ''}


class FlowResumeAction(BaseAction):
    """Resume workflow."""
    action_type = "flow_resume"
    display_name = "恢复工作流"
    description = "恢复工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resume.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating resume.
        """
        if not context.get('_flow_paused', False):
            return ActionResult(
                success=False,
                message="工作流未暂停"
            )

        pause_reason = context.get('_flow_pause_reason', '')
        context.set('_flow_paused', False)
        context.delete('_flow_pause_reason')

        return ActionResult(
            success=True,
            message=f"工作流已恢复: {pause_reason}",
            data={
                'resumed': True,
                'previous_reason': pause_reason
            }
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FlowStopAction(BaseAction):
    """Stop workflow."""
    action_type = "flow_stop"
    display_name = "停止工作流"
    description = "停止工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop.

        Args:
            context: Execution context.
            params: Dict with reason.

        Returns:
            ActionResult indicating stop.
        """
        reason = params.get('reason', '')

        try:
            resolved_reason = context.resolve_value(reason) if reason else 'User requested'
            context.set('_flow_stopped', True)
            context.set('_flow_stop_reason', resolved_reason)

            return ActionResult(
                success=True,
                message=f"工作流已停止: {resolved_reason}",
                data={
                    'stopped': True,
                    'reason': resolved_reason
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"停止工作流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reason': ''}
