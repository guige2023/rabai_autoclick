"""Terminal3 action module for RabAI AutoClick.

Provides additional terminal/shell operations:
- TerminalRunCommandAction: Run shell command
- TerminalBackgroundAction: Run command in background
- TerminalKillProcessAction: Kill process
- TerminalEnvAction: Get environment variables
- TerminalSetEnvAction: Set environment variable
"""

import subprocess
import os
import signal
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TerminalRunCommandAction(BaseAction):
    """Run shell command."""
    action_type = "terminal3_run"
    display_name = "运行命令"
    description = "运行Shell命令"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute run command.

        Args:
            context: Execution context.
            params: Dict with command, shell, timeout, output_var.

        Returns:
            ActionResult with command output.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'command_result')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)
            resolved_shell = bool(context.resolve_value(shell)) if shell else True
            resolved_timeout = int(context.resolve_value(timeout)) if timeout else 30

            result = subprocess.run(
                resolved_command if resolved_shell else resolved_command.split(),
                shell=resolved_shell,
                capture_output=True,
                text=True,
                timeout=resolved_timeout
            )

            output = {
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'success': result.returncode == 0
            }

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"命令执行完成: 返回码 {result.returncode}",
                data={
                    'returncode': result.returncode,
                    'output_var': output_var
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"命令执行超时: {resolved_timeout}秒"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"命令执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'timeout': 30, 'output_var': 'command_result'}


class TerminalBackgroundAction(BaseAction):
    """Run command in background."""
    action_type = "terminal3_background"
    display_name = "后台运行命令"
    description = "在后台运行Shell命令"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute background command.

        Args:
            context: Execution context.
            params: Dict with command, shell, output_var.

        Returns:
            ActionResult with process info.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        output_var = params.get('output_var', 'process_info')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)
            resolved_shell = bool(context.resolve_value(shell)) if shell else True

            if resolved_shell:
                process = subprocess.Popen(
                    resolved_command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            else:
                process = subprocess.Popen(
                    resolved_command.split(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            result = {
                'pid': process.pid,
                'running': True
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"后台命令启动: PID {process.pid}",
                data={
                    'pid': process.pid,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"后台命令启动失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'output_var': 'process_info'}


class TerminalKillProcessAction(BaseAction):
    """Kill process."""
    action_type = "terminal3_kill"
    display_name = "终止进程"
    description = "终止指定进程"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute kill process.

        Args:
            context: Execution context.
            params: Dict with pid, signal, output_var.

        Returns:
            ActionResult with kill status.
        """
        pid = params.get('pid', 0)
        signal_num = params.get('signal', 15)
        output_var = params.get('output_var', 'kill_status')

        try:
            resolved_pid = int(context.resolve_value(pid))
            resolved_signal = int(context.resolve_value(signal_num)) if signal_num else 15

            os.kill(resolved_pid, resolved_signal)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"进程终止成功: PID {resolved_pid}",
                data={
                    'pid': resolved_pid,
                    'signal': resolved_signal,
                    'output_var': output_var
                }
            )
        except ProcessLookupError:
            return ActionResult(
                success=False,
                message=f"进程终止失败: 进程不存在"
            )
        except PermissionError:
            return ActionResult(
                success=False,
                message=f"进程终止失败: 权限不足"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"进程终止失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'signal': 15, 'output_var': 'kill_status'}


class TerminalEnvAction(BaseAction):
    """Get environment variables."""
    action_type = "terminal3_env"
    display_name = "获取环境变量"
    description = "获取环境变量"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get env.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with environment variable.
        """
        name = params.get('name', None)
        output_var = params.get('output_var', 'env_result')

        try:
            if name:
                resolved_name = context.resolve_value(name)
                result = os.environ.get(resolved_name)
            else:
                result = dict(os.environ)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取环境变量{'成功' if name else '列表'}",
                data={
                    'environment': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': None, 'output_var': 'env_result'}


class TerminalSetEnvAction(BaseAction):
    """Set environment variable."""
    action_type = "terminal3_setenv"
    display_name = "设置环境变量"
    description = "设置环境变量"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set env.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with set status.
        """
        name = params.get('name', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'setenv_status')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_value = context.resolve_value(value) if value else ''

            os.environ[resolved_name] = resolved_value
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"环境变量设置成功: {resolved_name}={resolved_value}",
                data={
                    'name': resolved_name,
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
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'setenv_status'}