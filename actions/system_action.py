"""System action module for RabAI AutoClick.

Provides system operations:
- GetEnvAction: Get environment variable
- SetEnvAction: Set environment variable
- RunCommandAction: Run shell command
- GetHostnameAction: Get hostname
- GetUsernameAction: Get username
- GetPlatformAction: Get platform info
"""

import os
import socket
import getpass
import subprocess
import platform
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GetEnvAction(BaseAction):
    """Get environment variable."""
    action_type = "get_env"
    display_name = "获取环境变量"
    description = "获取环境变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting environment variable.

        Args:
            context: Execution context.
            params: Dict with name, default, output_var.

        Returns:
            ActionResult with environment variable value.
        """
        name = params.get('name', '')
        default = params.get('default', '')
        output_var = params.get('output_var', 'env_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            result = os.environ.get(resolved_name, default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取环境变量: {resolved_name}",
                data={
                    'name': resolved_name,
                    'value': result,
                    'found': result != default,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': '', 'output_var': 'env_value'}


class SetEnvAction(BaseAction):
    """Set environment variable."""
    action_type = "set_env"
    display_name = "设置环境变量"
    description = "设置环境变量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute setting environment variable.

        Args:
            context: Execution context.
            params: Dict with name, value.

        Returns:
            ActionResult indicating success.
        """
        name = params.get('name', '')
        value = params.get('value', '')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = context.resolve_value(value)
            os.environ[resolved_name] = resolved_value

            return ActionResult(
                success=True,
                message=f"已设置环境变量: {resolved_name}",
                data={
                    'name': resolved_name,
                    'value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class RunCommandAction(BaseAction):
    """Run shell command."""
    action_type = "run_command"
    display_name = "运行命令"
    description = "运行shell命令"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute running shell command.

        Args:
            context: Execution context.
            params: Dict with command, timeout, shell, output_var.

        Returns:
            ActionResult with command output.
        """
        command = params.get('command', '')
        timeout = params.get('timeout', 30)
        shell = params.get('shell', True)
        output_var = params.get('output_var', 'command_output')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(timeout, (int, float), 'timeout')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)

            result = subprocess.run(
                resolved_command,
                shell=shell,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            output = {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode,
                'success': result.returncode == 0
            }

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"命令执行完成: 返回码 {result.returncode}",
                data=output
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"命令执行超时: {timeout}秒"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"运行命令失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 30, 'shell': True, 'output_var': 'command_output'}


class GetHostnameAction(BaseAction):
    """Get hostname."""
    action_type = "get_hostname"
    display_name = "获取主机名"
    description = "获取计算机主机名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting hostname.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with hostname.
        """
        output_var = params.get('output_var', 'hostname')

        try:
            result = socket.gethostname()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"主机名: {result}",
                data={
                    'hostname': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取主机名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hostname'}


class GetUsernameAction(BaseAction):
    """Get username."""
    action_type = "get_username"
    display_name = "获取用户名"
    description = "获取当前用户名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting username.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with username.
        """
        output_var = params.get('output_var', 'username')

        try:
            result = getpass.getuser()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"用户名: {result}",
                data={
                    'username': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取用户名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'username'}


class GetPlatformAction(BaseAction):
    """Get platform info."""
    action_type = "get_platform"
    display_name = "获取平台信息"
    description = "获取平台信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting platform info.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with platform info.
        """
        output_var = params.get('output_var', 'platform_info')

        try:
            result = {
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor(),
                'python_version': platform.python_version(),
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平台: {result['system']} {result['release']}",
                data={
                    'platform_info': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取平台信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'platform_info'}