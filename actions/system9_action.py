"""System9 action module for RabAI AutoClick.

Provides additional system operations:
- SystemInfoAction: Get system info
- SystemEnvAction: Get environment variable
- SystemSetEnvAction: Set environment variable
- SystemExitAction: Exit with code
- SystemSleepAction: Sleep for seconds
- SystemCommandAction: Execute system command
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SystemInfoAction(BaseAction):
    """Get system info."""
    action_type = "system9_info"
    display_name = "系统信息"
    description = "获取系统信息"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute system info.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with system info.
        """
        output_var = params.get('output_var', 'system_info')

        try:
            import platform

            result = {
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor(),
                'python_version': platform.python_version(),
                'hostname': platform.node()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统信息: {result['system']}",
                data={
                    'info': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'system_info'}


class SystemEnvAction(BaseAction):
    """Get environment variable."""
    action_type = "system9_env"
    display_name = "获取环境变量"
    description = "获取环境变量"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute env get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var.

        Returns:
            ActionResult with env value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'env_value')

        try:
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None

            result = os.environ.get(resolved_key, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"环境变量: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'env_value'}


class SystemSetEnvAction(BaseAction):
    """Set environment variable."""
    action_type = "system9_setenv"
    display_name = "设置环境变量"
    description = "设置环境变量"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute env set.

        Args:
            context: Execution context.
            params: Dict with key, value, output_var.

        Returns:
            ActionResult with set status.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'setenv_status')

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            os.environ[resolved_key] = resolved_value
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"设置环境变量: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'setenv_status'}


class SystemExitAction(BaseAction):
    """Exit with code."""
    action_type = "system9_exit"
    display_name = "退出系统"
    description = "退出程序"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exit.

        Args:
            context: Execution context.
            params: Dict with code, output_var.

        Returns:
            ActionResult with exit status.
        """
        code = params.get('code', 0)
        output_var = params.get('output_var', 'exit_status')

        try:
            resolved_code = int(context.resolve_value(code)) if code else 0

            context.set(output_var, resolved_code)

            return ActionResult(
                success=True,
                message=f"退出程序: {resolved_code}",
                data={
                    'code': resolved_code,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"退出程序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'code': 0, 'output_var': 'exit_status'}


class SystemSleepAction(BaseAction):
    """Sleep for seconds."""
    action_type = "system9_sleep"
    display_name = "系统休眠"
    description = "程序休眠"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sleep.

        Args:
            context: Execution context.
            params: Dict with seconds, output_var.

        Returns:
            ActionResult with sleep status.
        """
        seconds = params.get('seconds', 1)
        output_var = params.get('output_var', 'sleep_status')

        try:
            import time

            resolved_seconds = float(context.resolve_value(seconds)) if seconds else 1

            time.sleep(resolved_seconds)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"休眠完成: {resolved_seconds}秒",
                data={
                    'seconds': resolved_seconds,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"休眠失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sleep_status'}


class SystemCommandAction(BaseAction):
    """Execute system command."""
    action_type = "system9_command"
    display_name = "执行命令"
    description = "执行系统命令"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute command.

        Args:
            context: Execution context.
            params: Dict with command, shell, output_var.

        Returns:
            ActionResult with command output.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        output_var = params.get('output_var', 'command_result')

        try:
            import subprocess

            resolved_command = context.resolve_value(command)
            resolved_shell = context.resolve_value(shell) if shell else True

            if resolved_shell:
                result = subprocess.run(
                    resolved_command,
                    shell=True,
                    capture_output=True,
                    text=True
                )
            else:
                result = subprocess.run(
                    resolved_command.split(),
                    capture_output=True,
                    text=True
                )

            output = result.stdout if result.returncode == 0 else result.stderr

            context.set(output_var, {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode
            })

            return ActionResult(
                success=result.returncode == 0,
                message=f"命令执行: 返回{result.returncode}",
                data={
                    'command': resolved_command,
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"执行命令失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'output_var': 'command_result'}