"""Process3 action module for RabAI AutoClick.

Provides additional process operations:
- ProcessListAction: List running processes
- ProcessKillAction: Kill process by PID
- ProcessCpuAction: Get process CPU usage
- ProcessMemoryAction: Get process memory usage
- ProcessExistsAction: Check if process exists
"""

import os
import signal
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "process3_list"
    display_name = "进程列表"
    description = "列出所有运行中的进程"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute process list.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with process list.
        """
        output_var = params.get('output_var', 'process_list')

        try:
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
                message=f"进程列表: {len(processes)} 个进程",
                data={
                    'count': len(processes),
                    'processes': processes[:100],
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="进程列表失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"进程列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_list'}


class ProcessKillAction(BaseAction):
    """Kill process by PID."""
    action_type = "process3_kill"
    display_name = "终止进程"
    description = "根据PID终止进程"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute process kill.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with kill status.
        """
        pid = params.get('pid', 0)
        output_var = params.get('output_var', 'kill_status')

        try:
            resolved_pid = int(context.resolve_value(pid))

            os.kill(resolved_pid, signal.SIGTERM)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"终止进程: PID {resolved_pid}",
                data={
                    'pid': resolved_pid,
                    'output_var': output_var
                }
            )
        except ProcessLookupError:
            return ActionResult(
                success=False,
                message=f"终止进程失败: 进程不存在"
            )
        except PermissionError:
            return ActionResult(
                success=False,
                message=f"终止进程失败: 权限不足"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"终止进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'kill_status'}


class ProcessCpuAction(BaseAction):
    """Get process CPU usage."""
    action_type = "process3_cpu"
    display_name = "进程CPU使用率"
    description = "获取进程的CPU使用率"
    version = "3.0"

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
        output_var = params.get('output_var', 'cpu_result')

        try:
            import psutil

            if pid is None:
                proc = psutil.Process()
                resolved_pid = proc.pid
            else:
                resolved_pid = int(context.resolve_value(pid))
                proc = psutil.Process(resolved_pid)

            cpu_percent = proc.cpu_percent(interval=1.0)
            context.set(output_var, cpu_percent)

            return ActionResult(
                success=True,
                message=f"进程CPU使用率: {cpu_percent}%",
                data={
                    'pid': resolved_pid,
                    'cpu_percent': cpu_percent,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取进程CPU失败: 未安装psutil库"
            )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"获取进程CPU失败: 进程不存在"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取进程CPU失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'output_var': 'cpu_result'}


class ProcessMemoryAction(BaseAction):
    """Get process memory usage."""
    action_type = "process3_memory"
    display_name = "进程内存使用"
    description = "获取进程的内存使用情况"
    version = "3.0"

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
        output_var = params.get('output_var', 'memory_result')

        try:
            import psutil

            if pid is None:
                proc = psutil.Process()
                resolved_pid = proc.pid
            else:
                resolved_pid = int(context.resolve_value(pid))
                proc = psutil.Process(resolved_pid)

            mem_info = proc.memory_info()
            result = {
                'rss': mem_info.rss,
                'vms': mem_info.vms,
                'rss_mb': mem_info.rss / (1024 * 1024),
                'vms_mb': mem_info.vms / (1024 * 1024)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"进程内存: {result['rss_mb']:.2f} MB",
                data={
                    'pid': resolved_pid,
                    'memory': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取进程内存失败: 未安装psutil库"
            )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"获取进程内存失败: 进程不存在"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取进程内存失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'output_var': 'memory_result'}


class ProcessExistsAction(BaseAction):
    """Check if process exists."""
    action_type = "process3_exists"
    display_name = "进程存在检查"
    description = "检查进程是否存在"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute process exists check.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with exists result.
        """
        pid = params.get('pid', 0)
        output_var = params.get('output_var', 'exists_result')

        try:
            import psutil

            resolved_pid = int(context.resolve_value(pid))
            result = psutil.pid_exists(resolved_pid)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"进程存在检查: {'存在' if result else '不存在'}",
                data={
                    'pid': resolved_pid,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="进程存在检查失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"进程存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}