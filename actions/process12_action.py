"""Process12 action module for RabAI AutoClick.

Provides additional process operations:
- ProcessIDAction: Get current process ID
- ProcessListAction: List running processes
- ProcessKillAction: Kill process
- ProcessMemoryAction: Get process memory usage
- ProcessCPUAction: Get process CPU usage
- ProcessStartAction: Start new process
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessIDAction(BaseAction):
    """Get current process ID."""
    action_type = "process12_getpid"
    display_name = "获取进程ID"
    description = "获取当前进程ID"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get PID.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with PID.
        """
        output_var = params.get('output_var', 'process_id')

        try:
            import os

            result = os.getpid()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"进程ID: {result}",
                data={
                    'pid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取进程ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_id'}


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "process12_list"
    display_name = "列出进程"
    description = "列出运行中的进程"
    version = "12.0"

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
            import os
            import psutil

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
                message=f"列出进程: {len(processes)}个",
                data={
                    'count': len(processes),
                    'processes': processes[:50],  # Limit to first 50
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message=f"psutil模块未安装"
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
    """Kill process."""
    action_type = "process12_kill"
    display_name = "终止进程"
    description = "终止进程"
    version = "12.0"

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
        signal = params.get('signal', 15)
        output_var = params.get('output_var', 'kill_status')

        try:
            import os
            import signal as sig

            resolved_pid = int(context.resolve_value(pid)) if pid else 0
            resolved_signal = int(context.resolve_value(signal)) if signal else 15

            os.kill(resolved_pid, resolved_signal)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"终止进程: {resolved_pid}",
                data={
                    'pid': resolved_pid,
                    'signal': resolved_signal,
                    'output_var': output_var
                }
            )
        except ProcessLookupError:
            return ActionResult(
                success=False,
                message=f"进程不存在: {pid}"
            )
        except PermissionError:
            return ActionResult(
                success=False,
                message=f"权限不足: {pid}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"终止进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'signal': 15, 'output_var': 'kill_status'}


class ProcessMemoryAction(BaseAction):
    """Get process memory usage."""
    action_type = "process12_memory"
    display_name = "获取进程内存"
    description = "获取进程内存使用"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute process memory.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with memory usage.
        """
        pid = params.get('pid', None)
        output_var = params.get('output_var', 'memory_usage')

        try:
            import psutil

            resolved_pid = int(context.resolve_value(pid)) if pid else None

            if resolved_pid:
                proc = psutil.Process(resolved_pid)
            else:
                proc = psutil.Process()

            mem_info = proc.memory_info()
            result = {
                'rss': mem_info.rss,  # Resident Set Size
                'vms': mem_info.vms,  # Virtual Memory Size
                'percent': proc.memory_percent()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"进程内存: {result['rss'] / 1024 / 1024:.2f}MB",
                data={
                    'pid': proc.pid,
                    'memory': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message=f"psutil模块未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取进程内存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'output_var': 'memory_usage'}


class ProcessCPUAction(BaseAction):
    """Get process CPU usage."""
    action_type = "process12_cpu"
    display_name = "获取进程CPU"
    description = "获取进程CPU使用"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute process CPU.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with CPU usage.
        """
        pid = params.get('pid', None)
        output_var = params.get('output_var', 'cpu_usage')

        try:
            import psutil

            resolved_pid = int(context.resolve_value(pid)) if pid else None

            if resolved_pid:
                proc = psutil.Process(resolved_pid)
            else:
                proc = psutil.Process()

            cpu_percent = proc.cpu_percent(interval=0.1)
            result = {
                'percent': cpu_percent,
                'num_threads': proc.num_threads()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"进程CPU: {cpu_percent}%",
                data={
                    'pid': proc.pid,
                    'cpu': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message=f"psutil模块未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取进程CPU失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'output_var': 'cpu_usage'}


class ProcessStartAction(BaseAction):
    """Start new process."""
    action_type = "process12_start"
    display_name = "启动进程"
    description = "启动新进程"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start process.

        Args:
            context: Execution context.
            params: Dict with command, shell, output_var.

        Returns:
            ActionResult with process info.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        output_var = params.get('output_var', 'process_info')

        try:
            import subprocess

            resolved_command = context.resolve_value(command)
            resolved_shell = context.resolve_value(shell) if shell else True

            if resolved_shell:
                proc = subprocess.Popen(
                    resolved_command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            else:
                proc = subprocess.Popen(
                    resolved_command.split(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            result = {
                'pid': proc.pid,
                'returncode': None
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"启动进程: PID {proc.pid}",
                data={
                    'command': resolved_command,
                    'process': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"启动进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'output_var': 'process_info'}