"""App2 action module for RabAI AutoClick.

Provides advanced application operations:
- AppActivateAction: Activate application
- AppHideAction: Hide application
- AppUnhideAction: Unhide application
- AppQuitAction: Quit application
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AppActivateAction(BaseAction):
    """Activate application."""
    action_type = "app_activate"
    display_name = "激活应用"
    description = "激活指定应用窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute activate.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_app = context.resolve_value(app_name)

            script = f'''osascript -e 'tell application "{resolved_app}" to activate' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"应用已激活: {resolved_app}",
                data={'app_name': resolved_app}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"激活应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class AppHideAction(BaseAction):
    """Hide application."""
    action_type = "app_hide"
    display_name = "隐藏应用"
    description = "隐藏指定应用"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hide.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_app = context.resolve_value(app_name)

            script = f'''osascript -e 'tell application "{resolved_app}" to hide' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"应用已隐藏: {resolved_app}",
                data={'app_name': resolved_app}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"隐藏应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class AppUnhideAction(BaseAction):
    """Unhide application."""
    action_type = "app_unhide"
    display_name = "显示应用"
    description = "显示指定应用"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unhide.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_app = context.resolve_value(app_name)

            script = f'''osascript -e 'tell application "{resolved_app}" to unhide' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"应用已显示: {resolved_app}",
                data={'app_name': resolved_app}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class AppQuitAction(BaseAction):
    """Quit application."""
    action_type = "app_quit"
    display_name = "退出应用"
    description = "退出指定应用"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quit.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_app = context.resolve_value(app_name)

            script = f'''osascript -e 'tell application "{resolved_app}" to quit' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"应用已退出: {resolved_app}",
                data={'app_name': resolved_app}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"退出应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}