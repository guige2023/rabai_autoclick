"""Process action module for RabAI AutoClick.

Provides process operations:
- ProcessListAction: List running processes
- ProcessKillAction: Kill a process
- ProcessExistsAction: Check if process exists
- ProcessGetPidAction: Get process PID
- ProcessRunAction: Run a process
"""

import subprocess
import psutil
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "process_list"
    display_name = "列出进程"
    description = "列出所有运行中的进程"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list processes.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with process list.
        """
        output_var = params.get('output_var', 'process_list')

        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'username']):
                try:
                    processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'username': proc.info['username']
                    })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            context.set(output_var, processes)

            return ActionResult(
                success=True,
                message=f"进程列表: {len(processes)} 个进程",
                data={
                    'count': len(processes),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_list'}


class ProcessKillAction(BaseAction):
    """Kill a process."""
    action_type = "process_kill"
    display_name = "终止进程"
    description = "终止指定进程"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute kill process.

        Args:
            context: Execution context.
            params: Dict with pid or name.

        Returns:
            ActionResult indicating success.
        """
        pid = params.get('pid', None)
        name = params.get('name', None)

        if pid is None and name is None:
            return ActionResult(
                success=False,
                message="必须指定 pid 或 name"
            )

        try:
            if pid is not None:
                resolved_pid = context.resolve_value(pid)
                proc = psutil.Process(int(resolved_pid))
                proc.kill()
                return ActionResult(
                    success=True,
                    message=f"进程 {resolved_pid} 已终止"
                )
            else:
                resolved_name = context.resolve_value(name)
                killed = []
                for proc in psutil.process_iter(['pid', 'name']):
                    try:
                        if proc.info['name'].lower() == resolved_name.lower():
                            psutil.Process(proc.info['pid']).kill()
                            killed.append(proc.info['pid'])
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                if killed:
                    return ActionResult(
                        success=True,
                        message=f"已终止 {len(killed)} 个进程: {killed}"
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"未找到进程: {resolved_name}"
                    )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"进程不存在"
            )
        except psutil.AccessDenied:
            return ActionResult(
                success=False,
                message=f"权限不足"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"终止进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'name': None}


class ProcessExistsAction(BaseAction):
    """Check if process exists."""
    action_type = "process_exists"
    display_name = "检查进程"
    description = "检查进程是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check process.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with exists result.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'process_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            exists = False
            for proc in psutil.process_iter(['name']):
                try:
                    if proc.info['name'].lower() == resolved_name.lower():
                        exists = True
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"进程检查: {'存在' if exists else '不存在'}",
                data={
                    'exists': exists,
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_result'}


class ProcessGetPidAction(BaseAction):
    """Get process PID."""
    action_type = "process_get_pid"
    display_name = "获取进程PID"
    description = "获取进程的PID"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get PID.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with PID.
        """
        name = params.get('name', '')
        output_var = params.get('output_var', 'process_result')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            pids = []
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if proc.info['name'].lower() == resolved_name.lower():
                        pids.append(proc.info['pid'])
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            context.set(output_var, pids)

            return ActionResult(
                success=True,
                message=f"找到 {len(pids)} 个进程: {pids}",
                data={
                    'pids': pids,
                    'count': len(pids),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取PID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_result'}


class ProcessRunAction(BaseAction):
    """Run a process."""
    action_type = "process_run"
    display_name = "运行进程"
    description = "运行新进程"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute run process.

        Args:
            context: Execution context.
            params: Dict with command, shell, wait, output_var.

        Returns:
            ActionResult with process output.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        wait = params.get('wait', True)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'process_result')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)

            if wait:
                result = subprocess.run(
                    resolved_command,
                    shell=shell,
                    capture_output=True,
                    text=True,
                    timeout=int(timeout)
                )
                output = {
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode,
                    'success': result.returncode == 0
                }
            else:
                proc = subprocess.Popen(
                    resolved_command,
                    shell=shell,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                output = {
                    'pid': proc.pid,
                    'success': True
                }

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"进程执行完成" if wait else f"进程已启动: PID {proc.pid}",
                data=output
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"进程执行超时: {timeout}秒"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"运行进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'wait': True, 'timeout': 30, 'output_var': 'process_result'}