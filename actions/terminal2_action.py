"""Terminal2 action module for RabAI AutoClick.

Provides additional terminal operations:
- TerminalRunCommandAction: Run shell command
- TerminalGetOutputAction: Get command output
- TerminalExitCodeAction: Get exit code
"""

import subprocess
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TerminalRunCommandAction(BaseAction):
    """Run shell command."""
    action_type = "terminal2_run"
    display_name = "运行命令"
    description = "运行Shell命令"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute run command.

        Args:
            context: Execution context.
            params: Dict with command, cwd, timeout, output_var.

        Returns:
            ActionResult with command result.
        """
        command = params.get('command', '')
        cwd = params.get('cwd', None)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'command_result')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)
            resolved_cwd = context.resolve_value(cwd) if cwd else None
            resolved_timeout = int(context.resolve_value(timeout)) if timeout else 30

            if resolved_cwd and not os.path.isdir(resolved_cwd):
                return ActionResult(
                    success=False,
                    message=f"目录不存在: {resolved_cwd}"
                )

            result = subprocess.run(
                resolved_command,
                shell=True,
                cwd=resolved_cwd,
                capture_output=True,
                text=True,
                timeout=resolved_timeout
            )

            output = {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode,
                'success': result.returncode == 0,
            }

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"命令执行: 返回码 {result.returncode}",
                data={
                    'command': resolved_command,
                    'output': output,
                    'output_var': output_var
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"命令超时: {resolved_timeout}秒"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"运行命令失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'cwd': None, 'timeout': 30, 'output_var': 'command_result'}


class TerminalGetOutputAction(BaseAction):
    """Get command output."""
    action_type = "terminal2_get_output"
    display_name = "获取命令输出"
    description = "获取命令的标准输出"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get output.

        Args:
            context: Execution context.
            params: Dict with command_result, output_type, output_var.

        Returns:
            ActionResult with output.
        """
        command_result_var = params.get('command_result_var', 'command_result')
        output_type = params.get('output_type', 'stdout')
        output_var = params.get('output_var', 'command_output')

        valid, msg = self.validate_type(command_result_var, str, 'command_result_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(command_result_var)
            resolved_type = context.resolve_value(output_type)

            result = context.get(resolved_var) if isinstance(resolved_var, str) else resolved_var

            if not isinstance(result, dict):
                return ActionResult(
                    success=False,
                    message="command_result 必须是字典类型"
                )

            if resolved_type == 'stdout':
                output = result.get('stdout', '')
            elif resolved_type == 'stderr':
                output = result.get('stderr', '')
            elif resolved_type == 'all':
                output = result.get('stdout', '') + result.get('stderr', '')
            else:
                output = ''

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"获取命令输出: {len(output)} 字符",
                data={
                    'output': output,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取命令输出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command_result_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_type': 'stdout', 'output_var': 'command_output'}


class TerminalExitCodeAction(BaseAction):
    """Get exit code."""
    action_type = "terminal2_exit_code"
    display_name = "获取退出码"
    description = "获取命令的退出码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exit code.

        Args:
            context: Execution context.
            params: Dict with command_result_var, output_var.

        Returns:
            ActionResult with exit code.
        """
        command_result_var = params.get('command_result_var', 'command_result')
        output_var = params.get('output_var', 'exit_code')

        valid, msg = self.validate_type(command_result_var, str, 'command_result_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(command_result_var)

            result = context.get(resolved_var) if isinstance(resolved_var, str) else resolved_var

            if not isinstance(result, dict):
                return ActionResult(
                    success=False,
                    message="command_result 必须是字典类型"
                )

            exit_code = result.get('returncode', -1)
            context.set(output_var, exit_code)

            return ActionResult(
                success=True,
                message=f"退出码: {exit_code}",
                data={
                    'exit_code': exit_code,
                    'success': exit_code == 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取退出码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command_result_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exit_code'}