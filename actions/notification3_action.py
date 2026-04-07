"""Notification3 action module for RabAI AutoClick.

Provides additional notification operations:
- NotificationBannerAction: Show banner notification
- NotificationAlertAction: Show alert dialog
- NotificationProgressAction: Show progress indicator
- NotificationActionSheetAction: Show action sheet
- NotificationQuickAction: Show quick action notification
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotificationBannerAction(BaseAction):
    """Show banner notification."""
    action_type = "notification3_banner"
    display_name = "显示横幅通知"
    description = "显示横幅通知"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute banner notification.

        Args:
            context: Execution context.
            params: Dict with title, message, duration, output_var.

        Returns:
            ActionResult with banner status.
        """
        title = params.get('title', '')
        message = params.get('message', '')
        duration = params.get('duration', 5)
        output_var = params.get('output_var', 'banner_status')

        try:
            resolved_title = context.resolve_value(title)
            resolved_message = context.resolve_value(message)
            resolved_duration = int(context.resolve_value(duration)) if duration else 5

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"横幅通知: {resolved_title}",
                data={
                    'title': resolved_title,
                    'message': resolved_message,
                    'duration': resolved_duration,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示横幅通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'duration': 5, 'output_var': 'banner_status'}


class NotificationAlertAction(BaseAction):
    """Show alert dialog."""
    action_type = "notification3_alert"
    display_name = "显示警告对话框"
    description = "显示警告对话框"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute alert dialog.

        Args:
            context: Execution context.
            params: Dict with title, message, button_labels, output_var.

        Returns:
            ActionResult with alert result.
        """
        title = params.get('title', '')
        message = params.get('message', '')
        button_labels = params.get('button_labels', ['OK', 'Cancel'])
        output_var = params.get('output_var', 'alert_result')

        try:
            resolved_title = context.resolve_value(title)
            resolved_message = context.resolve_value(message)
            resolved_buttons = context.resolve_value(button_labels) if button_labels else ['OK', 'Cancel']

            context.set(output_var, 'dismissed')

            return ActionResult(
                success=True,
                message=f"警告对话框: {resolved_title}",
                data={
                    'title': resolved_title,
                    'message': resolved_message,
                    'button_labels': resolved_buttons,
                    'result': 'dismissed',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示警告对话框失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'button_labels': ['OK', 'Cancel'], 'output_var': 'alert_result'}


class NotificationProgressAction(BaseAction):
    """Show progress indicator."""
    action_type = "notification3_progress"
    display_name = "显示进度指示器"
    description = "显示进度指示器"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute progress indicator.

        Args:
            context: Execution context.
            params: Dict with title, progress, output_var.

        Returns:
            ActionResult with progress status.
        """
        title = params.get('title', 'Progress')
        progress = params.get('progress', 0)
        output_var = params.get('output_var', 'progress_status')

        try:
            resolved_title = context.resolve_value(title)
            resolved_progress = float(context.resolve_value(progress)) if progress else 0

            resolved_progress = max(0, min(100, resolved_progress))

            context.set(output_var, resolved_progress)

            return ActionResult(
                success=True,
                message=f"进度: {resolved_progress}%",
                data={
                    'title': resolved_title,
                    'progress': resolved_progress,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示进度指示器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['progress']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': 'Progress', 'output_var': 'progress_status'}


class NotificationActionSheetAction(BaseAction):
    """Show action sheet."""
    action_type = "notification3_actionsheet"
    display_name = "显示操作表"
    description = "显示操作表"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute action sheet.

        Args:
            context: Execution context.
            params: Dict with title, actions, output_var.

        Returns:
            ActionResult with action sheet result.
        """
        title = params.get('title', '')
        actions = params.get('actions', [])
        output_var = params.get('output_var', 'actionsheet_result')

        try:
            resolved_title = context.resolve_value(title) if title else ''
            resolved_actions = context.resolve_value(actions) if actions else []

            context.set(output_var, 'cancelled')

            return ActionResult(
                success=True,
                message=f"操作表: {resolved_title or '选择操作'}",
                data={
                    'title': resolved_title,
                    'actions': resolved_actions,
                    'result': 'cancelled',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示操作表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['actions']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': '', 'output_var': 'actionsheet_result'}


class NotificationQuickAction(BaseAction):
    """Show quick action notification."""
    action_type = "notification3_quick"
    display_name = "显示快捷操作"
    description = "显示快捷操作通知"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quick action.

        Args:
            context: Execution context.
            params: Dict with message, actions, output_var.

        Returns:
            ActionResult with quick action status.
        """
        message = params.get('message', '')
        actions = params.get('actions', [])
        output_var = params.get('output_var', 'quick_action_result')

        try:
            resolved_message = context.resolve_value(message) if message else ''
            resolved_actions = context.resolve_value(actions) if actions else []

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"快捷操作: {resolved_message}",
                data={
                    'message': resolved_message,
                    'actions': resolved_actions,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示快捷操作失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'actions': [], 'output_var': 'quick_action_result'}