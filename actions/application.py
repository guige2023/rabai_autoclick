"""Application control action module for RabAI AutoClick.

Provides application control actions:
- LaunchAppAction: Launch an application
- QuitAppAction: Quit an application
- GetRunningAppsAction: Get list of running applications
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LaunchAppAction(BaseAction):
    """Launch an application by name or path."""
    action_type = "launch_app"
    display_name = "启动应用"
    description = "通过名称或路径启动应用程序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute launching an application.

        Args:
            context: Execution context.
            params: Dict with app_name or app_path.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')
        app_path = params.get('app_path', '')

        # Validate inputs
        if not app_name and not app_path:
            return ActionResult(
                success=False,
                message="未指定应用名称或路径"
            )

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(app_path, str, 'app_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if app_path:
                # Launch by path
                if not os.path.exists(app_path):
                    return ActionResult(
                        success=False,
                        message=f"应用路径不存在: {app_path}"
                    )
                subprocess.Popen(['open', app_path])
                return ActionResult(
                    success=True,
                    message=f"已启动应用: {app_path}",
                    data={'app_path': app_path}
                )
            else:
                # Launch by name using open
                subprocess.Popen(['open', '-a', app_name])
                return ActionResult(
                    success=True,
                    message=f"已启动应用: {app_name}",
                    data={'app_name': app_name}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"启动应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'app_name': '',
            'app_path': ''
        }


class QuitAppAction(BaseAction):
    """Quit an application by name."""
    action_type = "quit_app"
    display_name = "退出应用"
    description = "退出指定的应用程序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quitting an application.

        Args:
            context: Execution context.
            params: Dict with app_name, force.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')
        force = params.get('force', False)

        # Validate app_name
        if not app_name:
            return ActionResult(
                success=False,
                message="未指定应用名称"
            )
        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate force
        valid, msg = self.validate_type(force, bool, 'force')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if force:
                # Force quit using osascript
                script = f'''
                tell application "{app_name}" to quit
                '''
            else:
                script = f'''
                tell application "{app_name}"
                    if it is running then
                        quit
                    end if
                end tell
                '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=10)

            return ActionResult(
                success=True,
                message=f"已退出应用: {app_name}",
                data={'app_name': app_name, 'force': force}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"退出应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'force': False}


class GetRunningAppsAction(BaseAction):
    """Get list of running applications."""
    action_type = "get_running_apps"
    display_name = "获取运行中的应用"
    description = "获取当前运行中的应用程序列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting running applications.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with list of running apps.
        """
        output_var = params.get('output_var', 'running_apps')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = '''
            tell application "System Events"
                get name of every process whose background only is false
            end tell
            '''

            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=10
            )

            # Parse the list from osascript output
            apps = []
            if result.stdout.strip():
                # Split by comma and clean up
                apps = [
                    app.strip().strip('"')
                    for app in result.stdout.strip().split(', ')
                ]

            # Store in context
            context.set(output_var, apps)

            return ActionResult(
                success=True,
                message=f"找到 {len(apps)} 个运行中的应用",
                data={'apps': apps, 'count': len(apps), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取运行中的应用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'running_apps'}


class IsAppRunningAction(BaseAction):
    """Check if an application is currently running."""
    action_type = "is_app_running"
    display_name = "检查应用运行状态"
    description = "检查指定应用是否正在运行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking if an app is running.

        Args:
            context: Execution context.
            params: Dict with app_name, output_var.

        Returns:
            ActionResult with running status.
        """
        app_name = params.get('app_name', '')
        output_var = params.get('output_var', 'app_running')

        # Validate app_name
        if not app_name:
            return ActionResult(
                success=False,
                message="未指定应用名称"
            )
        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = f'''
            tell application "System Events"
                set appRunning to (name of processes) contains "{app_name}"
                return appRunning
            end tell
            '''

            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=10
            )

            is_running = 'true' in result.stdout.lower()

            # Store in context
            context.set(output_var, is_running)

            status = "运行中" if is_running else "未运行"
            return ActionResult(
                success=True,
                message=f"{app_name}: {status}",
                data={
                    'app_name': app_name,
                    'is_running': is_running,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查应用状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['app_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'app_running'}